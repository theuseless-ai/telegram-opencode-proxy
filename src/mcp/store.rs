//! Concurrency-safe, single-use file store backing the MCP file-transfer tools
//! (`send_file_to_user` / `fetch_user_file`). See `docs/design/architecture.md`
//! §11. Issue #65.
//!
//! A [`FileStore`] mediates every file that crosses the proxy on behalf of the
//! opencode agent. It is deliberately **thin on memory and fat on disk**: the
//! bytes of each file live in a per-file [`tempfile::NamedTempFile`] and only a
//! small [`FileMeta`] record — slot, filename, mime, size, TTL deadline, and the
//! temp-file handle itself — sits in the in-memory `Mutex<HashMap<Uuid,
//! FileMeta>>`. Keys are random v4 UUIDs, so an id is unguessable and carries no
//! ambient authority; the owning slot is recorded alongside and re-checked on
//! every read.
//!
//! # Lock discipline (mirrors `state.rs`)
//!
//! The expensive work — streaming bytes to disk in [`FileStore::put`], reading
//! them back in [`FileStore::take`] — happens **outside** the lock. The mutex is
//! only ever taken for the O(1) map mutation and released (guard dropped) before
//! the next `.await`. No guard is ever held across a suspension point. Because
//! `std::sync::Mutex` can be poisoned by a panic while held, every acquisition
//! recovers with `unwrap_or_else(PoisonError::into_inner)` — the map is plain
//! data, so a poisoned lock is still safe to use.
//!
//! # Single-use + delete-vs-read safety
//!
//! [`FileStore::take`] is the only reader, and it **removes** the entry from the
//! map under the lock before it reads the disk. That single move is the whole
//! safety story: it transfers ownership of the `NamedTempFile` to the caller, so
//! (a) a file is delivered **at most once** — a concurrent or later second
//! `take` finds the id gone and returns a clean [`TakeError::NotFound`]; and (b)
//! there is no delete-vs-read window, because the sweeper and any rival `take`
//! can only observe an id that is still in the map, and removal is atomic under
//! the lock. When the returned `NamedTempFile` (held inside [`FileMeta`], moved
//! out on take, or dropped by the sweeper) is dropped, the on-disk file is
//! unlinked automatically.
//!
//! # Cross-slot isolation
//!
//! Every `take` verifies the caller's slot equals the slot that produced the id.
//! A mismatch, a missing id, and an already-consumed id all collapse to the
//! **same** opaque [`TakeError::NotFound`] — a caller can never distinguish "no
//! such id" from "that id belongs to another slot", so the store is not an oracle
//! for the existence of another slot's files. A cross-slot probe also never
//! consumes the real owner's file: the entry is only removed on a slot match.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Chunk size for streaming a `put` source to disk. Large enough to keep syscall
/// overhead low, small enough that the mid-write size guard aborts an oversized
/// upload after buffering at most this many extra bytes.
const COPY_CHUNK: usize = 64 * 1024;

/// A stored file's metadata plus its on-disk backing handle. The `NamedTempFile`
/// owns the temp path and unlinks it on drop, so removing a `FileMeta` from the
/// map (via `take` or the TTL sweep) is all it takes to reclaim the file.
struct FileMeta {
    /// The slot that produced this file; a `take` must present the same slot.
    slot: String,
    /// Display name as the user/agent knows it (used when re-sending / labelling).
    filename: String,
    /// The file's MIME type, decided upstream (inbound download or tool arg).
    mime: String,
    /// Byte length written to disk — a capacity hint for the read-back buffer.
    size: u64,
    /// When this entry expires; the TTL sweep drops entries past their deadline.
    deadline: Instant,
    /// The temp file holding the bytes. Auto-unlinks when this `FileMeta` drops.
    file: NamedTempFile,
}

/// The successful result of [`FileStore::take`]: the file's identity plus its
/// full bytes, read back off disk. The backing temp file has already been
/// unlinked by the time this is returned — the caller now owns the only copy.
#[derive(Debug, Clone)]
pub struct Taken {
    /// The file's display name.
    pub filename: String,
    /// The file's MIME type.
    pub mime: String,
    /// The file's full contents.
    pub bytes: Vec<u8>,
}

/// Why a [`FileStore::put`] failed.
#[derive(Debug, Error)]
pub enum PutError {
    /// The source exceeded `max_file_bytes` mid-stream; the partial temp file was
    /// aborted and unlinked (no OOM, no orphaned bytes).
    #[error("file exceeds the {limit}-byte limit")]
    TooLarge {
        /// The configured `max_file_bytes` ceiling that was breached.
        limit: u64,
    },
    /// An I/O error while creating or writing the temp file.
    #[error("writing file to store: {0}")]
    Io(#[source] std::io::Error),
}

/// Why a [`FileStore::take`] failed.
#[derive(Debug, Error)]
pub enum TakeError {
    /// The id is unknown, was already consumed, has expired, or belongs to a
    /// different slot. **Deliberately opaque** — all four collapse here so the
    /// store cannot be probed for another slot's files.
    #[error("no such file, or it was already fetched or expired")]
    NotFound,
    /// An I/O error while reading the temp file back off disk.
    #[error("reading file from store: {0}")]
    Io(#[source] std::io::Error),
}

/// A UUID-keyed, disk-backed, single-use file store shared across all slots.
///
/// Construct one with [`FileStore::new`], hand out `Arc<FileStore>` clones to the
/// MCP tools and the inbound-media path, and start [`FileStore::spawn_ttl_sweep`]
/// once for the process lifetime.
pub struct FileStore {
    /// Lightweight metadata keyed by the file's public UUID. The bytes live on
    /// disk (`FileMeta::file`); only this small record is under the lock.
    inner: Mutex<HashMap<Uuid, FileMeta>>,
    /// Hard ceiling on a single file's size, enforced mid-write by `put`.
    max_file_bytes: u64,
    /// How long a stored file lives before the TTL sweep may reclaim it.
    ttl: Duration,
}

impl FileStore {
    /// Create an empty store. `max_file_bytes` caps each `put` (enforced
    /// mid-stream); `ttl` is how long an un-fetched file survives before the
    /// sweep unlinks it.
    pub fn new(max_file_bytes: u64, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_file_bytes,
            ttl,
        }
    }

    /// Stream `src` to a fresh temp file, register it under a new UUID, and return
    /// that id.
    ///
    /// The bytes are copied to disk in bounded chunks and the running total is
    /// checked **before** each chunk is written, so a source larger than
    /// `max_file_bytes` is aborted with [`PutError::TooLarge`] having buffered at
    /// most one extra chunk — the partial temp file is dropped (unlinked) on the
    /// error path. Only after the whole file is on disk is the lock taken to
    /// insert the metadata; the guard is dropped before returning. The disk write
    /// (the sole `.await`) therefore happens entirely *before* the lock, matching
    /// the `state.rs` discipline.
    pub async fn put<R>(
        &self,
        slot: &str,
        filename: &str,
        mime: &str,
        src: R,
    ) -> Result<Uuid, PutError>
    where
        R: AsyncRead + Unpin,
    {
        let mut src = src;

        // Create the temp file and a second async handle to write through. The
        // `NamedTempFile` keeps ownership of the path (and unlinks it on drop, so
        // any early return below cleans up); the reopened handle is what we stream
        // into.
        let temp = NamedTempFile::new().map_err(PutError::Io)?;
        let mut writer = tokio::fs::File::from_std(temp.reopen().map_err(PutError::Io)?);

        let mut buf = vec![0u8; COPY_CHUNK];
        let mut written: u64 = 0;
        loop {
            let n = src.read(&mut buf).await.map_err(PutError::Io)?;
            if n == 0 {
                break;
            }
            written += n as u64;
            if written > self.max_file_bytes {
                // Abort before writing the overflowing chunk. `temp` (and its
                // reopened `writer`) drop here → the partial file is unlinked.
                return Err(PutError::TooLarge {
                    limit: self.max_file_bytes,
                });
            }
            writer.write_all(&buf[..n]).await.map_err(PutError::Io)?;
        }
        writer.flush().await.map_err(PutError::Io)?;
        drop(writer); // close the write handle; `temp` still owns the file/path.

        let id = Uuid::new_v4();
        let meta = FileMeta {
            slot: slot.to_string(),
            filename: filename.to_string(),
            mime: mime.to_string(),
            size: written,
            deadline: Instant::now() + self.ttl,
            file: temp,
        };
        {
            let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            guard.insert(id, meta);
        } // drop guard before returning — no await under the lock.
        Ok(id)
    }

    /// Consume the file `id` on behalf of `slot`, returning its bytes.
    ///
    /// Under the lock: the id is looked up, its slot is checked against `slot`,
    /// and on a match the entry is **removed** (transferring ownership of the temp
    /// file out of the map) — this single move gives both single-use delivery and
    /// delete-vs-read safety. The guard is then dropped and the bytes are read off
    /// disk asynchronously. Missing, already-consumed, expired, and wrong-slot ids
    /// all return the same opaque [`TakeError::NotFound`]; a wrong-slot probe
    /// leaves the real owner's entry untouched.
    pub async fn take(&self, slot: &str, id: Uuid) -> Result<Taken, TakeError> {
        let meta = {
            let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            // Only remove on a slot match, so a cross-slot probe cannot consume
            // (or even confirm the existence of) another slot's file.
            let owned = matches!(guard.get(&id), Some(m) if m.slot == slot);
            if !owned {
                return Err(TakeError::NotFound);
            }
            guard.remove(&id).expect("entry present under the lock")
        }; // drop guard before the disk read.

        let mut reader = tokio::fs::File::from_std(meta.file.reopen().map_err(TakeError::Io)?);
        let mut bytes = Vec::with_capacity(meta.size as usize);
        reader
            .read_to_end(&mut bytes)
            .await
            .map_err(TakeError::Io)?;
        // `meta.file` (the NamedTempFile) drops at the end of this scope → the
        // temp file is unlinked now that the caller owns the bytes.
        Ok(Taken {
            filename: meta.filename,
            mime: meta.mime,
            bytes,
        })
    }

    /// Drop every entry whose deadline has passed, unlinking its temp file. Takes
    /// the lock briefly and holds no guard across an await (there is none).
    fn sweep_expired(&self) {
        let now = Instant::now();
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        guard.retain(|_id, meta| meta.deadline > now);
    }

    /// Spawn the background TTL sweep: every `interval`, briefly lock the map and
    /// drop expired entries (each dropped [`FileMeta`] unlinks its temp file). The
    /// sweep never touches a `take`-n entry — it has already been removed from the
    /// map. Hold the returned [`JoinHandle`] for the process lifetime (like the
    /// outbox watchers); dropping it does not stop the task, but aborting it does.
    pub fn spawn_ttl_sweep(store: Arc<FileStore>, interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                store.sweep_expired();
            }
        })
    }

    /// Number of files currently held. Test-only.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// The on-disk path backing `id`, if still present. Test-only — lets a test
    /// assert the temp file exists before expiry and is unlinked after.
    #[cfg(test)]
    fn temp_path(&self, id: Uuid) -> Option<std::path::PathBuf> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .map(|m| m.file.path().to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store with a large cap and a long TTL — the common case for tests that
    /// are not exercising the cap or expiry.
    fn roomy() -> FileStore {
        FileStore::new(1 << 20, Duration::from_secs(60))
    }

    #[tokio::test]
    async fn put_take_round_trip() {
        let store = roomy();
        let id = store
            .put("frank", "note.txt", "text/plain", &b"hello world"[..])
            .await
            .expect("put succeeds");

        let taken = store.take("frank", id).await.expect("take succeeds");
        assert_eq!(taken.filename, "note.txt");
        assert_eq!(taken.mime, "text/plain");
        assert_eq!(taken.bytes, b"hello world");
        assert_eq!(store.len(), 0, "take must remove the entry");
    }

    #[tokio::test]
    async fn second_take_is_not_found() {
        let store = roomy();
        let id = store
            .put("frank", "a.bin", "application/octet-stream", &b"data"[..])
            .await
            .unwrap();

        store.take("frank", id).await.expect("first take");
        let err = store.take("frank", id).await.expect_err("second take");
        assert!(matches!(err, TakeError::NotFound), "single-use");
    }

    #[tokio::test]
    async fn wrong_slot_is_not_found_and_does_not_consume() {
        let store = roomy();
        let id = store
            .put("frank", "secret.txt", "text/plain", &b"top secret"[..])
            .await
            .unwrap();

        // A holly request must see a clean NotFound and NOT consume frank's file.
        let err = store.take("holly", id).await.expect_err("cross-slot take");
        assert!(matches!(err, TakeError::NotFound), "cross-slot isolation");
        assert_eq!(
            store.len(),
            1,
            "probe must not consume the real owner's file"
        );

        // The rightful owner can still read it.
        let taken = store.take("frank", id).await.expect("owner take");
        assert_eq!(taken.bytes, b"top secret");
    }

    #[tokio::test]
    async fn over_cap_put_is_rejected() {
        let store = FileStore::new(4, Duration::from_secs(60));
        let err = store
            .put(
                "frank",
                "big.bin",
                "application/octet-stream",
                &b"way too big"[..],
            )
            .await
            .expect_err("over-cap put");
        assert!(matches!(err, PutError::TooLarge { limit: 4 }));
        assert_eq!(store.len(), 0, "rejected put must leave nothing behind");
    }

    #[tokio::test]
    async fn ttl_expiry_unlinks_the_temp_file() {
        let store = FileStore::new(1 << 20, Duration::from_millis(10));
        let id = store
            .put("frank", "ephemeral.txt", "text/plain", &b"soon gone"[..])
            .await
            .unwrap();

        let path = store.temp_path(id).expect("path while present");
        assert!(path.exists(), "temp file exists before expiry");

        tokio::time::sleep(Duration::from_millis(30)).await;
        store.sweep_expired();

        assert_eq!(store.len(), 0, "expired entry swept from the map");
        assert!(!path.exists(), "sweep unlinks the temp file");
        let err = store.take("frank", id).await.expect_err("take after sweep");
        assert!(matches!(err, TakeError::NotFound));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_and_takes_do_not_contend() {
        const N: u32 = 64;
        let store = Arc::new(roomy());

        // Concurrent puts of distinct files → distinct ids.
        let mut put_handles = Vec::with_capacity(N as usize);
        for i in 0..N {
            let store = Arc::clone(&store);
            put_handles.push(tokio::spawn(async move {
                let payload = format!("payload-{i}").into_bytes();
                let id = store
                    .put(
                        "frank",
                        &format!("f{i}.bin"),
                        "application/octet-stream",
                        &payload[..],
                    )
                    .await
                    .expect("concurrent put");
                (id, i)
            }));
        }

        let mut ids = Vec::with_capacity(N as usize);
        for h in put_handles {
            ids.push(h.await.expect("put task"));
        }
        assert_eq!(store.len(), N as usize, "every put landed a distinct id");

        // Concurrent takes of those distinct ids → each gets its own bytes back.
        let mut take_handles = Vec::with_capacity(N as usize);
        for (id, i) in ids {
            let store = Arc::clone(&store);
            take_handles.push(tokio::spawn(async move {
                let taken = store.take("frank", id).await.expect("concurrent take");
                assert_eq!(taken.bytes, format!("payload-{i}").into_bytes());
            }));
        }
        for h in take_handles {
            h.await.expect("take task");
        }
        assert_eq!(store.len(), 0, "all files consumed exactly once");
    }
}
