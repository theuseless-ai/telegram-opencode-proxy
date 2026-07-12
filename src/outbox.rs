//! `notify` watcher on each slot's `./outbox` → send new files to the owning
//! user (`upload_document`/`upload_photo` first). See `docs/design/architecture.md`
//! §2.5. Issue #12.
//!
//! The agent's side of the outbox convention is "write a deliverable to
//! `./outbox/`"; this module is the proxy's side: one filesystem watcher per slot
//! (that has a Telegram owner) fires whenever a file lands there and forwards it
//! via [`files::send_outbound_file`].
//!
//! # Why a debounce
//!
//! A single file write surfaces as a *burst* of `notify` events — a `Create`
//! then one or more `Modify`s (and, on Linux, a `Close(Write)`) — and the early
//! ones can arrive before the writer has finished. We coalesce per path over a
//! short quiet window ([`DEBOUNCE`]) so a file is sent **once**, and **whole**.
//!
//! The sync `notify` callback can't `.await`, so it bridges paths to an async
//! relay task over an unbounded channel. The [`notify::RecommendedWatcher`]
//! handles must be kept alive by the caller (dropping one stops the watch), so
//! [`spawn_watchers`] returns them for `serve` to hold for the process lifetime.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::event::{AccessKind, AccessMode, ModifyKind};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use teloxide::Bot;
use teloxide::types::ChatId;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::telegram::bot::AppState;
use crate::telegram::files;

/// Quiet period after the last filesystem event for a path before it's delivered.
/// Long enough to collapse a create/modify burst and let a writer finish, short
/// enough to feel immediate.
const DEBOUNCE: Duration = Duration::from_millis(400);

/// Start one outbox watcher per configured slot that has a Telegram owner,
/// returning the live watcher handles (which the caller must keep alive — a
/// dropped watcher stops watching).
///
/// A slot with no `telegram_id` has nobody to deliver to and is skipped. The
/// `<workdir>/outbox` directory is created if missing so the watch always has a
/// target, even before the agent writes its first file. A per-slot failure
/// (unwatchable dir, watcher build error) is logged and skipped — it never sinks
/// the other slots or the daemon.
pub fn spawn_watchers(state: &Arc<AppState>) -> Vec<RecommendedWatcher> {
    let mut watchers = Vec::new();
    for slot in &state.cfg.slots {
        let Some(chat_id) = slot.telegram_id else {
            tracing::debug!(slot = %slot.name, "no Telegram owner — outbox watcher skipped");
            continue;
        };
        let outbox = slot.workdir.join("outbox");
        if let Err(err) = std::fs::create_dir_all(&outbox) {
            tracing::warn!(
                slot = %slot.name,
                dir = %outbox.display(),
                error = %err,
                "could not create outbox dir — watcher skipped"
            );
            continue;
        }
        match watch_outbox(
            &outbox,
            state.bot.clone(),
            ChatId(chat_id),
            slot.name.clone(),
        ) {
            Ok(watcher) => {
                tracing::info!(
                    slot = %slot.name,
                    dir = %outbox.display(),
                    chat_id,
                    "watching outbox"
                );
                watchers.push(watcher);
            }
            Err(err) => tracing::warn!(
                slot = %slot.name,
                dir = %outbox.display(),
                error = format!("{err:#}"),
                "could not watch outbox — watcher skipped"
            ),
        }
    }
    watchers
}

/// Build a non-recursive watcher on `dir` and spawn the async relay that delivers
/// each settled file to `chat_id`.
fn watch_outbox(
    dir: &Path,
    bot: Bot,
    chat_id: ChatId,
    slot: String,
) -> notify::Result<RecommendedWatcher> {
    // The callback runs on notify's own thread and can't await, so it just hands
    // relevant paths to the async relay over an unbounded channel.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let mut watcher =
        notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
            Ok(event) if is_write(&event.kind) => {
                for path in event.paths {
                    let _ = tx.send(path);
                }
            }
            Ok(_) => {}
            Err(err) => tracing::warn!(error = %err, "outbox watch error"),
        })?;
    watcher.watch(dir, RecursiveMode::NonRecursive)?;
    tokio::spawn(relay(bot, chat_id, slot, rx, DEBOUNCE));
    Ok(watcher)
}

/// Whether an event kind denotes a file being written (as opposed to a rename,
/// remove, or metadata-only change). Covers all backends: inotify's
/// create/modify/close-write, and the coarser create/modify FSEvents reports.
fn is_write(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(_)
            | EventKind::Modify(ModifyKind::Data(_))
            | EventKind::Modify(ModifyKind::Any)
            | EventKind::Access(AccessKind::Close(AccessMode::Write))
    )
}

/// Debounce paths off `rx` and deliver each once it settles. Every event resets
/// the per-path quiet timer; when `debounce` elapses with no new event, the
/// pending paths are flushed. A closed channel (watcher dropped) flushes and
/// exits.
async fn relay(
    bot: Bot,
    chat_id: ChatId,
    slot: String,
    mut rx: UnboundedReceiver<PathBuf>,
    debounce: Duration,
) {
    use std::collections::HashSet;
    let mut pending: HashSet<PathBuf> = HashSet::new();
    loop {
        let next = if pending.is_empty() {
            rx.recv().await
        } else {
            // Wait for the next event, but no longer than the quiet window; a
            // timeout means the burst settled → flush.
            match tokio::time::timeout(debounce, rx.recv()).await {
                Ok(next) => next,
                Err(_) => {
                    for path in pending.drain() {
                        deliver(&bot, chat_id, &slot, &path).await;
                    }
                    continue;
                }
            }
        };
        match next {
            Some(path) => {
                pending.insert(path);
            }
            None => {
                for path in pending.drain() {
                    deliver(&bot, chat_id, &slot, &path).await;
                }
                break;
            }
        }
    }
}

/// Deliver one settled outbox path to its owner. Skips anything that isn't a
/// regular file (a directory event, or a file already gone) and dotfiles (editor
/// temp files, `.DS_Store`). Send failures are logged, never propagated.
async fn deliver(bot: &Bot, chat_id: ChatId, slot: &str, path: &Path) {
    if is_hidden(path) {
        return;
    }
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => {}
        _ => return, // a directory, or the file vanished — nothing to send.
    }
    match files::send_outbound_file(bot, chat_id, path).await {
        Ok(()) => tracing::info!(slot, path = %path.display(), "sent outbox file"),
        Err(err) => tracing::warn!(
            slot,
            path = %path.display(),
            error = format!("{err:#}"),
            "failed to send outbox file"
        ),
    }
}

/// Whether `path`'s file name starts with a dot — an editor swap/temp file or a
/// hidden metadata file we should not forward.
fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_write_matches_create_modify_and_close_write() {
        use notify::event::{CreateKind, DataChange};
        assert!(is_write(&EventKind::Create(CreateKind::File)));
        assert!(is_write(&EventKind::Modify(ModifyKind::Data(
            DataChange::Any
        ))));
        assert!(is_write(&EventKind::Modify(ModifyKind::Any)));
        assert!(is_write(&EventKind::Access(AccessKind::Close(
            AccessMode::Write
        ))));
        // A read/open access or a remove is not a write we relay.
        assert!(!is_write(&EventKind::Access(AccessKind::Open(
            AccessMode::Read
        ))));
        assert!(!is_write(&EventKind::Remove(
            notify::event::RemoveKind::File
        )));
    }

    #[test]
    fn hidden_files_are_skipped() {
        assert!(is_hidden(Path::new("/wd/outbox/.DS_Store")));
        assert!(is_hidden(Path::new("/wd/outbox/.swp")));
        assert!(!is_hidden(Path::new("/wd/outbox/report.txt")));
    }
}
