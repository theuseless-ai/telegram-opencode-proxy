# Plan: telegram-opencode-proxy — Dependency-Ordered Execution Plan

## Context

Build a single Rust process bridging a Telegram bot (long-poll) and two static `opencode serve`
instances, delivering the "minutes → approve → commit" scenario for two known users on one M3 Ultra.
Design is locked (`docs/design/architecture.md`); this plan sequences the build **wire-first,
pairing-last**, gated behind an opencode API validation spike, and integrates the Oracle review
(9 new issues + resequence of v0.0.1 + split of #4).

**Sources of truth:** `docs/design/architecture.md` (§ references throughout),
`docs/meeting_minutes/2026-07-11-architecture-brainstorm.md`.
**Repo:** `theuseless-ai/telegram-opencode-proxy` (19 issues, 4 milestones).

---

## Key strategic decisions baked into this plan (from Oracle review)

1. **Wave 0 = A0 spike.** No opencode-touching code is written until the live API is validated and
   fixtures are checked in. This de-risks the single largest unknown (opencode V1/V2 wire, event
   taxonomy, permission shape) before it is encoded into `client.rs`/`events.rs`/`types.rs`.
2. **v0.0.1 resequenced wire-first, pairing-last:**
   `#1 scaffold → A0 spike → #2 config(trimmed) → #5 client+supervisor → #6 bot loop (STUB auth)
   → #3 persistence → #4 FULL pairing`.
3. **#4 is split:** (a) minimal env/hardcoded `allowed_chat_id` gate ships with #6 to get the wire
   green; (b) the full pairing subsystem (codes, TTL, rate-limit, admin socket, CLI) lands after.
4. **Deliberate `deny` posture** on `git commit*`/`push*` (and shell) at session-create, held in #5
   until #13 provides a responder — you cannot enable `ask` before a reply path exists.
5. **Dangling-session resilience:** get-or-create must treat an opencode 404 on a stored `session_id`
   as "recreate", so a wiped opencode DB does not brick a user.
6. **Durability contract:** Telegram offset advances **only after enqueue**; in-flight turns are lost
   on crash (acceptable for 2 users). Bounded-channel-full policy is **reject-with-message**.
7. **App-level content-sign-off mechanism** (minutes §2.6 second table) is an **explicit deferral** —
   native permission relay covers the MVP.

---

## Task Dependency Graph (DAG)

Notation: `A → B` means B depends on A. `∥` marks tasks with no ordering between them.

```
                          ┌─────────────────────────── v0.0.1 (walking skeleton) ───────────────────────────┐

T1(#1 scaffold) ─┬─► T-CI(NEW CI) 
                 │
                 └─► T2(#2 config,trimmed) ─┬─► T-SEC(NEW secrets)
                                            │
A0(NEW spike) ──────────────────────────────┴─► T5(#5 client+supervisor) ─► T-HEALTH(NEW readiness/backoff)
                                            │                                     │
                                            └─► T4a(#4a minimal gate) ────────────┤
                                                                                  ▼
                                                                    T6(#6 bot loop, STUB auth)  ◄── WIRE GREEN
                                                                                  │
                                            T3(#3 persistence) ◄──────────────────┤
                                                 │        │                        │
                                                 │        └─► T-SHUT(NEW shutdown/reaping) ◄── needs T5,T3,T6
                                                 ▼
                                            T4b(#4b full pairing)
        T-TEST(NEW test harness) ── spans; seeded by A0 fixtures, grows every wave ──►

                          └───────────────────────────────────────────────────────────────────────────────┘

                          ┌─────────────────────────── v0.0.2 (daily driver) ─────────────────────────────┐
T6 ─► T7(#7 SSE+reconnect) ─► T8(#8 streaming+liveness+tool status) ─► T-TGERR(NEW tg rate-limit/backoff)
T6 ─► T9(#9 mpsc + /stop)          T7 ─► T10(#10 /new /whoami + verbosity plumbing)
(logging) T-LOG(NEW) depends on T6, feeds all
                          └───────────────────────────────────────────────────────────────────────────────┘

                          ┌─────────────────────────── v0.0.3 (full scenario) ───────────────────────────┐
T7,T3,T5 ─► T13(#13 permission relay + git-ask FLIP)   ◄── headline deliverable
T8       ─► T11(#11 inbound files)  ∥  T8 ─► T12(#12 outbox) ─► T-AGENTS(NEW AGENTS.md)
T8,T10   ─► T14(#14 verbosity + sub-agent tags + footer)
T9,T3,T13 ─► /stop-rejects-pending + re-surface-pending-gates (acceptance in T13/T9)
                          └───────────────────────────────────────────────────────────────────────────────┘
```

### Critical path (longest chain to the "minutes → approve → commit" MVP)

```
A0 spike → #1 scaffold → #2 config → #5 client+supervisor → #6 bot loop (wire green)
        → #7 SSE → #13 permission relay (+ #3 persistence feeding pending_approvals)
```

`#3 persistence` is an off-path feeder that must land before `#13` (pending_approvals) and `#4b`
(allowed_users). `#8 streaming` is on the UX path but not on the strict correctness path to the
commit gate. **A0 is a hard gate for the entire critical path** — nothing downstream can be encoded
correctly until it produces fixtures.

### What can be parallelized (given 1–2 people)

- **Wave 0:** `#1 scaffold` runs fully parallel to `A0 spike` (scaffold touches no opencode surface).
- **Within v0.0.1 after wire-green:** `#3 persistence`, `T-SHUT shutdown`, and `T-CI` are independent
  of each other.
- **v0.0.2:** `#9 mpsc/#stop` is independent of `#7/#8` (concurrency plumbing vs. render path);
  `T-LOG` and `T-TGERR` are cross-cutting and can be slotted whenever their dependency is met.
- **v0.0.3:** `#11 inbound files` and `#12 outbox/#get` are independent of `#13`; all three depend
  only on `#8`.

---

## Execution Waves

Milestone/priority per task. Agent column: `sisyphus-junior` (scoped single-module),
`hephaestus` (multi-file/deep), `dev+librarian` (live-system spike — needs a running opencode, not
autonomous codegen). "Modules" reference `architecture.md §4`.

---

### WAVE 0 — De-risk & foundation (BLOCKING GATE)

#### A0 — opencode API validation spike  ·  NEW  ·  v0.0.1  ·  **P0 (FIRST)**
- **Goal:** Validate the live opencode wire and check in fixtures before any client code exists.
- **Depends on:** nothing (external prereq: a running `opencode serve` with the §12 provider config
  resolved on the M3 Ultra).
- **Acceptance criteria:**
  - `GET /doc` (OpenAPI 3.1) captured and committed under `fixtures/opencode/doc.json`; opencode
    version pinned and recorded (§10).
  - Live `/event` SSE traces captured for **(a)** a plain text turn and **(b)** a git-gated turn,
    committed under `fixtures/opencode/events/`. Event-type strings verified against the live stream
    (`session.next.text.delta`, `step.ended`, `session.status`, `permission.asked` — §13/§2.6).
  - Confirmed: `POST /session/:id/message` blocking-vs-async semantics (does it return on turn
    completion or immediately?) — documented, drives #6 vs #7 design.
  - Confirmed: `model` is the **split object** `{providerID, modelID}`, not `"provider/model"` (§12).
  - Confirmed: `PATCH /session/:id` permission ruleset shape (§2.6 gotcha) and
    `POST /permission/:id/reply` V1 body (`reply: once|always|reject`, optional `message`).
  - `permission.asked` payload fields captured verbatim (id, sessionID, permission, patterns,
    metadata, always, tool) — committed as a fixture for #13.
  - Output is a short `fixtures/opencode/README.md` mapping each fixture to the code/tests it feeds.
- **Modules touched:** none (produces `fixtures/`); informs `opencode/types.rs`, `opencode/events.rs`,
  `opencode/client.rs`, `permission.rs`.
- **Agent:** dev+librarian (human-driven capture against live server; librarian for opencode docs).
- **Why P0:** every opencode-touching task encodes assumptions this task verifies. Getting it wrong
  = rework in `types.rs`/`events.rs`/`client.rs`/`permission.rs`.

#### T1 — Project scaffold + deps  ·  #1  ·  v0.0.1  ·  P1
- **Goal:** Cargo project + full module skeleton compiles empty.
- **Depends on:** nothing (parallel with A0).
- **Acceptance criteria:** `cargo init`; deps per §9; module tree per §4 (`config, state,
  persistence, auth, pairing, session, opencode/{client,events,supervisor,types}, telegram/{bot,
  render,files}, permission, outbox`); `tracing` init stub; `cargo build` + `cargo clippy` clean.
- **Modules touched:** all (empty stubs), `Cargo.toml`, `main.rs`.
- **Agent:** sisyphus-junior.

---

### WAVE 1 — Config, secrets, CI (depends on Wave 0)

#### T2 — Config + CLI (trimmed to what the wire needs)  ·  #2  ·  v0.0.1  ·  P1
- **Goal:** Config structs + `clap` CLI, trimmed to only what #5/#6 consume.
- **Depends on:** T1. (Informed by A0 for the `[model]` split.)
- **Acceptance criteria:**
  - TOML: `bot_token` (env `TELOXIDE_TOKEN` wins over file), `admin_socket` (path only; wired in #4b),
    `[[slots]]` (name/opencode_url/workdir), `[model]` selector `{provider_id, model_id}`,
    `[permissions].ask` list (§11).
  - `clap`: `serve` subcommand functional now; `pair` subcommand stubbed (implemented in #4b).
  - **Trim note:** full `[pairing]` config block (`code_ttl_secs`, rate-limit) is added in #4b, not
    here — do not gold-plate the config before the wire is green.
  - Validation: slot ports distinct, workdirs exist, model selector non-empty; clear error messages.
- **Modules touched:** `config.rs`, `main.rs`.
- **Agent:** sisyphus-junior.

#### T-SEC — Secrets handling  ·  NEW  ·  v0.0.1  ·  P1
- **Goal:** Keep the bot token and prompts out of logs and out of committed config.
- **Depends on:** T2.
- **Acceptance criteria:**
  - `bot_token` sourced from env `TELOXIDE_TOKEN`; never required in committed config; example config
    ships a placeholder.
  - Config-file permission check on load (warn/refuse if world-readable).
  - No code path logs the token, prompt bodies, or file contents (verified by a redaction unit test).
  - Admin socket created `0600` and the mode is **verified after bind** (asserted, not assumed) —
    coordinated with #4b.
- **Modules touched:** `config.rs`, `pairing.rs` (socket perms), logging init in `main.rs`.
- **Agent:** sisyphus-junior.

#### T-CI — CI pipeline  ·  NEW  ·  v0.0.1  ·  P2
- **Goal:** GitHub Actions gate on every push/PR.
- **Depends on:** T1.
- **Acceptance criteria:** workflow runs `cargo fmt --check`, `cargo clippy -D warnings`,
  `cargo test`, `cargo build`; green on the scaffold; caches the cargo registry.
- **Modules touched:** `.github/workflows/ci.yml`.
- **Agent:** sisyphus-junior.

---

### WAVE 2 — opencode client + supervisor + readiness (depends on A0 + T2)

#### T5 — opencode client + supervisor  ·  #5  ·  v0.0.1  ·  P1
- **Goal:** reqwest client (blocking prompt path) + supervisor spawning the two `opencode serve`
  procs; codegen/types from A0's `/doc`.
- **Depends on:** A0 (fixtures + `/doc` for `types.rs`), T2 (slots/model config).
- **Acceptance criteria:**
  - `client.rs`: `create_session`, `prompt` (blocking `POST /session/:id/message` per A0 finding),
    `get_messages`, plus stubs for `patch_permission`/`reply_permission`/`read_file` (bodies land in
    #13/#12). `model` serialized as split object `{providerID, modelID}` (§12, verified by A0).
  - `types.rs`: generated/derived from A0 `/doc`, behind a V1/V2 adapter seam (§10).
  - `supervisor.rs`: spawn + keep-alive/restart the two procs per-slot (workdir/port); must **not**
    mark a slot live until its provider config resolves (readiness handled in T-HEALTH; supervisor
    exposes the hook).
  - **Deny posture (A/B decision):** on session-create, PATCH a deliberate `deny` on
    `git commit*`/`git push*` (or shell) — **not** `ask` — because no responder exists until #13.
    This is an explicit acceptance line (§2.6). #13 flips `deny → ask`.
  - **Dangling-session resilience:** get-or-create treats an opencode `404` on a stored `session_id`
    as "recreate a fresh session and update routing" — a wiped opencode DB must not brick a user.
  - Integration-tested against wiremock driven by A0 fixtures (via T-TEST).
- **Modules touched:** `opencode/client.rs`, `opencode/types.rs`, `opencode/supervisor.rs`,
  `session.rs` (get-or-create + PATCH-on-create).
- **Agent:** hephaestus (multi-file, deep, version-adapter seam).

#### T-HEALTH — opencode health / readiness / backoff  ·  NEW  ·  v0.0.1  ·  P1
- **Goal:** Only route to a slot once opencode is actually ready; survive crashes and stale state.
- **Depends on:** T5 (supervisor + client).
- **Acceptance criteria:**
  - Readiness probe: poll `/config` or `/app` until up before marking a slot live (§12 requires the
    provider config resolved — a bare port-open is insufficient).
  - Crash-loop backoff (exponential, capped) with an "instance down" user-facing message path.
  - Stale-socket / stale-port handling: unlink a stale admin socket on start; detect a port already
    held (orphaned prior child) and recover or fail loudly.
- **Modules touched:** `opencode/supervisor.rs`, `state.rs` (instance registry), `main.rs`.
- **Agent:** hephaestus.

---

### WAVE 3 — Wire green: bot loop + minimal gate (depends on T5, T-HEALTH)

#### T4a — Minimal auth gate (split of #4)  ·  #4 (part a)  ·  v0.0.1  ·  P1
- **Goal:** Ship a hardcoded/env `allowed_chat_id` gate so #6 can reach end-to-end without the full
  pairing subsystem.
- **Depends on:** T2.
- **Acceptance criteria:** unknown sender rejected; allowed `chat_id` (from env/config, single value
  or short list) passes; clearly marked TEMPORARY, superseded by #4b. `/whoami`-equivalent debug line
  logs the numeric id to aid bootstrap.
- **Modules touched:** `auth.rs` (minimal path), `telegram/bot.rs`.
- **Agent:** sisyphus-junior.
- **Ships with #6.**

#### T6 — Bot loop + blocking end-to-end reply  ·  #6  ·  v0.0.1  ·  **P1 — WIRE GREEN milestone**
- **Goal:** teloxide long-poll dispatcher: text → session → blocking prompt → chunked 4096 reply.
- **Depends on:** T5, T-HEALTH, T4a.
- **Acceptance criteria:**
  - teloxide `getUpdates` dispatcher + T4a gate; text message → `session.rs` get-or-create → blocking
    `client.prompt` → reply.
  - 4096-char chunker (unit-tested independently — see T-TEST).
  - **Startup provider validation:** validate configured `{provider_id, model_id}` against
    `GET /config/providers` (or A0-confirmed equivalent) at boot; fail fast with a clear message.
  - **Durability contract (partial):** Telegram offset advances **only after** the message is handed
    off/enqueued (§6) — establish this discipline now even though the bounded channel lands in #9.
  - Manual e2e: real Telegram message → real opencode turn → reply, both slots, on the M3 Ultra.
- **Modules touched:** `telegram/bot.rs`, `telegram/render.rs` (chunker), `session.rs`, `main.rs`.
- **Agent:** hephaestus.

---

### WAVE 4 — Persistence + graceful shutdown (depends on wire green)

#### T3 — Persistence (SQLite schema)  ·  #3  ·  v0.0.1  ·  P1
- **Goal:** Durable proxy state: routing, allowed users, pending pairings, pending approvals.
- **Depends on:** T2. (Sequenced after #6 per the resequence, but has no code dependency on #6 —
  can be built in parallel with Wave 3 if a second person is free.)
- **Acceptance criteria:** tables `allowed_users (chat_id→slot)`, `routing (chat_id→session_id)`,
  `pending_pairings (code, chat_id, username, expires_at)`, `pending_approvals` (survive restart);
  rusqlite migrations + typed accessors; WAL mode; opened once, shared via `state.rs`.
- **Modules touched:** `persistence.rs`, `state.rs`.
- **Agent:** sisyphus-junior.

#### T-SHUT — Graceful shutdown + child reaping  ·  NEW  ·  v0.0.1  ·  P1
- **Goal:** Clean teardown — Rust does not reap children on exit, so orphaned opencode procs would
  hold `:4096`/`:4097`.
- **Depends on:** T5 (children), T6 (turn abort path), T3 (SQLite flush).
- **Acceptance criteria:** `SIGTERM`/`SIGINT` handler → abort in-flight turns
  (`POST /session/:id/abort`) → kill both opencode children → unlink admin socket → flush/close
  SQLite. Verified: after shutdown, no orphaned opencode process and both ports free.
- **Modules touched:** `main.rs`, `opencode/supervisor.rs`, `pairing.rs` (socket unlink),
  `persistence.rs`.
- **Agent:** hephaestus.

---

### WAVE 5 — Full pairing subsystem (depends on T3)

#### T4b — Full pairing handshake + admin socket (split of #4)  ·  #4 (part b)  ·  v0.0.1  ·  P1
- **Goal:** Replace T4a with the real confirmation-nonce enrollment (§5).
- **Depends on:** T3 (pending_pairings/allowed_users), T2 (adds full `[pairing]` config now),
  T-SEC (0600 socket verification).
- **Acceptance criteria:**
  - Unknown sender → single-use 6-digit code, TTL (`code_ttl_secs`, default 600), **rate-limited per
    chat_id**; regeneration replaces prior code (§5).
  - Admin Unix socket, **0600 verified** (coordinated with T-SEC); `proxy pair list|approve <code>
    --slot <name>|deny` (§5 CLI table).
  - `approve` verifies code+TTL → binds `chat_id → slot` → writes `allowed_users` → deletes pending →
    bot notifies user "Approved". No config-seeded IDs (bootstrap = shell access).
  - `auth.rs` now enforces against persisted `allowed_users`; T4a temporary path removed.
- **Modules touched:** `pairing.rs`, `auth.rs`, `config.rs` (pairing block), `telegram/bot.rs`
  (notify), `persistence.rs`.
- **Agent:** hephaestus.

#### T-TEST — Test harness + strategy  ·  NEW  ·  v0.0.1  ·  P1  (spans; seeded in Wave 2)
- **Goal:** Establish unit + integration test scaffolding, driven by A0 fixtures.
- **Depends on:** A0 (fixtures). Grows every wave; the *harness* lands early (Wave 2 alongside T5).
- **Acceptance criteria:**
  - **Unit:** pairing TTL + rate-limit, path-traversal guard (for #12), 4096 chunker, model split-
    object serialization, secret redaction (T-SEC).
  - **Integration:** mock opencode HTTP + SSE via `wiremock` driven by A0 fixtures (plain turn +
    git-gated turn); reused by #5, #7, #13.
  - Wired into T-CI so the suite runs on every push.
- **Modules touched:** `tests/`, dev-deps in `Cargo.toml`.
- **Agent:** hephaestus (harness design), then sisyphus-junior for per-wave test additions.

**► v0.0.1 exit criteria:** two enrolled users can each send text and get a blocking, chunked reply
against their own opencode instance; clean shutdown leaves no orphans; CI green; secrets not logged.

---

### WAVE 6 — Streaming path (v0.0.2, depends on T6)

#### T7 — SSE events + reconnect  ·  #7  ·  v0.0.2  ·  P1
- **Goal:** Subscribe `/event`, parse `session.next.*`, reconnect robustly.
- **Depends on:** T6, A0 (event taxonomy fixtures).
- **Acceptance criteria:**
  - `GET /event` subscription; parse `session.next.*` (text/reasoning/tool/step/status) +
    `permission.asked` (§7/§13), verified against A0 fixtures.
  - Reconnect on drop; **explicit dedup-by-part-id reconciliation** on reconnect (do not double-emit
    parts already rendered) — this is a hard acceptance line, plus fallback to
    `GET /session/:id/message` for missed deltas.
- **Modules touched:** `opencode/events.rs`, `state.rs`.
- **Agent:** hephaestus.

#### T8 — Streaming render + typing liveness + tool status  ·  #8  ·  v0.0.2  ·  P1
- **Goal:** Live-edited streaming output with liveness and a flat tool-status line.
- **Depends on:** T7.
- **Acceptance criteria:** `text.delta` → live edit throttled ≤1/sec (§13 coalesce, ignore
  `tool.input.delta`); `typing` chat action re-sent ~4s while busy; flat tool-status line
  (`⚙️ tool: arg` → `✓`/`✗`), failures always shown; 4096 chunk boundaries respected mid-stream.
- **Modules touched:** `telegram/render.rs`, `telegram/bot.rs`.
- **Agent:** hephaestus.

#### T9 — Per-user mpsc serialization + /stop  ·  #9  ·  v0.0.2  ·  P1
- **Goal:** One turn at a time per user; explicit interrupt.
- **Depends on:** T6 (independent of T7/T8 — can run in parallel).
- **Acceptance criteria:**
  - `tokio::sync::mpsc` per user + worker; serialize turns (§6).
  - **Bounded-channel-full policy = reject-with-message** (tell the user "busy, try again", do not
    block the dispatcher) — explicit decision.
  - **Durability:** offset advances only after successful enqueue (§6); in-flight turn loss on crash
    documented as acceptable for 2 users.
  - `/stop` → `POST /session/:id/abort`; **/stop must also reject any pending permission** for that
    user (coordinated with #13 — the reject path is stubbed here, wired in #13).
- **Modules touched:** `state.rs` (queues + workers), `telegram/bot.rs` (`/stop`).
- **Agent:** hephaestus.

#### T10 — Commands (/new /whoami) + verbosity plumbing  ·  #10  ·  v0.0.2  ·  P2
- **Goal:** Session reset, id echo, per-user verbosity state.
- **Depends on:** T7 (verbosity gates the stream from #7/#8).
- **Acceptance criteria:** `/new` resets session (new opencode session, update routing); `/whoami`
  returns numeric id; per-user verbosity state (`quiet/normal/verbose`) stored in `state.rs`
  (behavior wired fully in #14).
- **Modules touched:** `telegram/bot.rs`, `session.rs`, `state.rs`.
- **Agent:** sisyphus-junior.

#### T-TGERR — Telegram error / rate-limit / backoff  ·  NEW  ·  v0.0.2  ·  P1
- **Goal:** Survive Telegram flood control and API errors.
- **Depends on:** T8 (edit budget is the pressure point).
- **Acceptance criteria:** honor `429 retry_after`; guard "message is not modified" (skip no-op
  edits); teloxide throttle adapter enabled; reconcile the ~1 edit/sec stream budget with flood
  control so bursts don't get the bot limited.
- **Modules touched:** `telegram/bot.rs`, `telegram/render.rs`, `main.rs` (throttle adapter).
- **Agent:** sisyphus-junior.

#### T-LOG — Structured logging  ·  NEW  ·  v0.0.2  ·  P2
- **Goal:** Per-turn observability without leaking secrets.
- **Depends on:** T6 (turn lifecycle exists), T-SEC (redaction rules).
- **Acceptance criteria:** per-turn `tracing` span carrying `chat_id` + `session_id`; env-controlled
  level (`RUST_LOG`); secret redaction reused from T-SEC; spans cover enqueue → prompt → render →
  done.
- **Modules touched:** cross-cutting (`main.rs` subscriber, spans in `bot.rs`/`session.rs`/`state.rs`).
- **Agent:** sisyphus-junior.

**► v0.0.2 exit criteria:** streaming live edits with typing liveness and tool status; two-user
routing; `/new` `/whoami` `/stop`; robust under Telegram flood control; structured logs.

---

### WAVE 7 — Full scenario (v0.0.3, depends on T8)

#### T11 — Inbound files (base64 FilePart)  ·  #11  ·  v0.0.3  ·  P1
- **Goal:** Telegram photo/doc → inline `FilePart`.
- **Depends on:** T8.
- **Acceptance criteria:** download from Bot API → base64 → `FilePart {mime, filename, url:
  data:...}` inlined in the prompt (§2.4); validate mime against configured model modalities, reject
  with a clear message when unsupported.
- **Modules touched:** `telegram/files.rs`, `telegram/bot.rs`, `opencode/client.rs`.
- **Agent:** sisyphus-junior.

#### T12 — Outbox watcher  ·  #12  ·  v0.0.3  ·  P1
- **Goal:** Send deliverables back to the owning user.
- **Depends on:** T8 (independent of T11/T13 — parallelizable).
- **Acceptance criteria:** `notify` watcher on each slot's `./outbox` → send new files
  (`send_document`/`send_photo` by mime, `upload_document`/`upload_photo` chat action first), with a
  **canonicalize-within-workdir** path-traversal guard (unit-tested in T-TEST); 50 MB /
  1024-char caption limits respected (§2.5).
- **Modules touched:** `outbox.rs`, `telegram/files.rs`, `telegram/bot.rs`.
- **Agent:** hephaestus (watcher lifecycle + per-slot wiring).

#### T13 — Permission relay + git-ask on session create  ·  #13  ·  v0.0.3  ·  **P1 — headline deliverable**
- **Goal:** The native approval gate: `permission.asked` → Telegram buttons → reply.
- **Depends on:** T7 (SSE carries `permission.asked`), T3 (pending_approvals persistence),
  T5 (session-create permission posture to flip).
- **Acceptance criteria:**
  - **Flip #5's `deny` → `ask`:** PATCH session permission `git commit*`/`push*` = `ask` on create
    now that a responder exists (§2.6 gotcha).
  - `permission.asked` → inline keyboard `[✅ Approve][✏️ Revise][❌ Deny]`; token stashed in
    `state` + `persistence.pending_approvals`; `callback_query` → `POST /permission/:id/reply`
    (`once`/`reject{message}`) behind the V1/V2 adapter (§10). Reject-with-message = revise loop.
  - **`/stop` rejects any pending permission** for the user (wires the stub from #9).
  - **On restart, re-surface pending gates** from `pending_approvals` so an approval isn't lost
    across a proxy restart.
  - End-to-end "minutes → approve → commit" demonstrated on the M3 Ultra (compose with #12 outbox).
- **Modules touched:** `permission.rs`, `opencode/events.rs`, `opencode/client.rs`, `session.rs`,
  `telegram/bot.rs`, `persistence.rs`.
- **Agent:** hephaestus.

#### T14 — Verbosity + sub-agent tags + summary footer  ·  #14  ·  v0.0.3  ·  P2
- **Goal:** Honor the §13 per-stream verbosity table; add sub-agent tag + completion footer.
- **Depends on:** T8, T10.
- **Acceptance criteria:** `/quiet` `/verbose` honor the per-stream table (§13); sub-agent name tag
  on the status line via child-`sessionID`→name lookup (drop tag gracefully if unavailable — no
  tree); completion summary footer (`✓ N tools · M subagent · edited K files`).
- **Modules touched:** `telegram/render.rs`, `state.rs`, `opencode/events.rs`.
- **Agent:** sisyphus-junior.

#### T-AGENTS — AGENTS.md provisioning  ·  NEW  ·  v0.0.3  ·  P2
- **Goal:** The outbox convention only works if the agent knows about it.
- **Depends on:** T12 (defines the outbox contract).
- **Acceptance criteria:** a per-workdir `AGENTS.md` template instructing the agent to write
  deliverables to `./outbox/`; documented owner/process for who writes and maintains it per slot;
  supervisor optionally seeds it into a new workdir on first start.
- **Modules touched:** `docs/` template + `opencode/supervisor.rs` (optional seeding), `session.rs`.
- **Agent:** sisyphus-junior.

**► v0.0.3 exit criteria:** files both ways; native permission relay delivering minutes → approve →
commit; verbosity toggles + sub-agent tags + footer.

---

### FINAL VERIFICATION WAVE (per milestone)

- Run full `cargo test` (unit + wiremock integration) — green.
- `cargo fmt --check` + `cargo clippy -D warnings` — clean.
- Manual e2e on the M3 Ultra for the milestone's headline scenario (wire / streaming / commit-gate).
- Shutdown check: `SIGTERM` leaves no orphaned opencode procs; ports `:4096`/`:4097` free.
- Secret audit: grep logs for token/prompt leakage — none.
- Restart check (v0.0.3): pending approval survives a proxy restart and re-surfaces.

---

## Task → GitHub Issue Mapping

| Plan task | Issue | Title | Milestone | Priority | Status |
|---|---|---|---|---|---|
| A0 | **NEW** | A0: opencode API validation spike | v0.0.1 | P0 | **CREATE** |
| T1 | #1 | A1: Project scaffold + deps | v0.0.1 | P1 | exists |
| T2 | #2 | A2: Config + CLI (serve/pair) + slots — *trim to wire needs* | v0.0.1 | P1 | exists (edit scope note) |
| T-SEC | **NEW** | Secrets handling (env token, config perms, no-log, 0600) | v0.0.1 | P1 | **CREATE** |
| T-CI | **NEW** | CI: GitHub Actions (fmt, clippy -D, test, build) | v0.0.1 | P2 | **CREATE** |
| T5 | #5 | A5: opencode client + supervisor — *+deny posture, +404-recreate* | v0.0.1 | P1 | exists (edit AC) |
| T-HEALTH | **NEW** | opencode health/readiness/backoff | v0.0.1 | P1 | **CREATE** |
| T4a | #4 (part a) | A4a: Minimal auth gate (env allowed_chat_id) | v0.0.1 | P1 | **SPLIT from #4** |
| T6 | #6 | A6: Bot loop + blocking e2e — *STUB auth* | v0.0.1 | P1 | exists (edit AC) |
| T3 | #3 | A3: Persistence (SQLite schema) | v0.0.1 | P1 | exists (re-order) |
| T-SHUT | **NEW** | Graceful shutdown + child reaping | v0.0.1 | P1 | **CREATE** |
| T4b | #4 (part b) | A4b: Full pairing handshake + admin socket | v0.0.1 | P1 | **SPLIT from #4** |
| T-TEST | **NEW** | Test harness + strategy (unit + wiremock integration) | v0.0.1 | P1 | **CREATE** |
| T7 | #7 | B1: SSE events + reconnect — *+dedup-by-part-id* | v0.0.2 | P1 | exists (edit AC) |
| T8 | #8 | B2: Streaming render + typing liveness + tool status | v0.0.2 | P1 | exists |
| T9 | #9 | B3: Per-user mpsc + /stop — *+reject-full, +offset contract, +reject-pending* | v0.0.2 | P1 | exists (edit AC) |
| T10 | #10 | B4: Commands (/new /whoami) + verbosity plumbing | v0.0.2 | P2 | exists |
| T-TGERR | **NEW** | Telegram error/rate-limit/backoff | v0.0.2 | P1 | **CREATE** |
| T-LOG | **NEW** | Structured logging (per-turn spans, redaction) | v0.0.2 | P2 | **CREATE** |
| T11 | #11 | C1: Inbound files (base64 FilePart) | v0.0.3 | P1 | exists |
| T12 | #12 | C2: Outbox watcher | v0.0.3 | P1 | exists |
| T13 | #13 | C3: Permission relay + git-ask — *+flip deny→ask, +reject-pending, +restart re-surface* | v0.0.3 | P1 | exists (edit AC) |
| T14 | #14 | C4: Verbosity + sub-agent tags + summary footer | v0.0.3 | P2 | exists |
| T-AGENTS | **NEW** | AGENTS.md provisioning | v0.0.3 | P2 | **CREATE** |
| — | #15 | Backlog: ractor migration | v0.1.0+ | — | unchanged |
| — | #16 | Backlog: dynamic multi-tenancy | v0.1.0+ | — | unchanged |
| — | #17 | Backlog: webhook ingress | v0.1.0+ | — | unchanged |
| — | #18 | Backlog: per-slot model | v0.1.0+ | — | unchanged |
| — | #19 | Backlog: providers CLI | v0.1.0+ | — | unchanged |
| — | **NEW backlog** | App-level content-sign-off gate (minutes §2.6 table 2) — deferred | v0.1.0+ | P3 | **CREATE (optional)** |

---

## Changes to the Issue Board (apply after plan approval)

### 1. Create 9 new issues (Oracle) + 1 optional deferral

| # | Proposed title | Milestone | Priority label |
|---|---|---|---|
| N1 | `A0: opencode API validation spike (capture /doc + /event fixtures)` | v0.0.1 | P0 |
| N2 | `Graceful shutdown + child reaping (SIGTERM → abort turns, kill opencode, unlink socket, flush SQLite)` | v0.0.1 | P1 |
| N3 | `opencode health/readiness/backoff (readiness probe, crash-loop backoff, stale socket/port)` | v0.0.1 | P1 |
| N4 | `Secrets handling (TELOXIDE_TOKEN env, config perms, no-log, admin socket 0600 verified)` | v0.0.1 | P1 |
| N5 | `Test harness + strategy (unit + wiremock integration from A0 fixtures)` | v0.0.1 | P1 |
| N6 | `Telegram error/rate-limit/backoff (429 retry_after, not-modified guard, throttle adapter)` | v0.0.2 | P1 |
| N7 | `Structured logging (per-turn span chat_id/session_id, env level, redaction)` | v0.0.2 | P2 |
| N8 | `CI: GitHub Actions (fmt, clippy -D warnings, test, build)` | v0.0.1 | P2 |
| N9 | `AGENTS.md provisioning (per-workdir outbox instructions)` | v0.0.3 | P2 |
| N10 (optional) | `Backlog: app-level content-sign-off gate (deferred; native relay covers MVP)` | v0.1.0+ | P3 |

### 2. Split issue #4 into two

- Re-title #4 → **`A4a: Minimal auth gate (env/hardcoded allowed_chat_id)`**, keep on v0.0.1, mark
  it as shipping with #6, scope = temporary gate only.
- Create **`A4b: Full pairing handshake + admin socket`** (codes, TTL, rate-limit, `pair
  list|approve|deny`, 0600 socket), v0.0.1, P1, depends on #3. (Or keep #4 as A4b-full and create a
  new small A4a — either mapping is fine; the split is the point.)

### 3. Edit acceptance criteria on existing issues (no milestone change)

- **#2** — add "trim to what the wire needs; full `[pairing]` config block deferred to A4b".
- **#5** — add: (a) deliberate `deny` on `git commit*/push*` at session-create until #13;
  (b) get-or-create handles dangling `session_id` (opencode 404 → auto-recreate);
  (c) `model` as split `{providerID, modelID}` confirmed by A0.
- **#7** — add: explicit **dedup-by-part-id** reconciliation on reconnect (acceptance criterion).
- **#9** — add: bounded-channel-full = **reject-with-message**; offset advances only after enqueue;
  `/stop` also rejects pending permission (stub, wired in #13).
- **#13** — add: flip #5's `deny → ask`; `/stop` rejects pending permission; on restart re-surface
  pending gates from `pending_approvals`.

### 4. Re-order within v0.0.1 (labels/board order, not milestones)

Apply the wire-first order: **A0 (N1) → #1 → #2 → #5 → #6 → #3 → #4b**, with #4a merged into #6's
wave. N2/N3/N4/N5/N8 slot in as noted (N3 after #5; N2 after #6; N4/N8 early; N5 spans). This
requires **no milestone moves** — all Oracle additions land in the milestone Oracle assigned.

### 5. Suggested `priority:*` labels

If the repo lacks priority labels, create `priority:P0`/`P1`/`P2`/`P3` and apply per the mapping
table so the wire-first critical path is filterable.

---

## Risk Flags

- **A0 is a single point of leverage.** If opencode's live wire differs from the design's assumptions
  (blocking vs async `POST /message`, event-type strings, permission shape, V1 vs V2 in the pinned
  binary — minutes §3), several downstream tasks change. Mitigation: A0 first, fixtures checked in,
  V1/V2 adapter seam in `types.rs`.
- **Local model is the throughput floor** (§6): both opencode instances serialize on one model
  server. Turn latency may be high; the ≤1/sec edit budget and `typing` liveness must mask it.
- **Child reaping** (N2): forgetting this leaves orphaned opencode procs holding ports across
  restarts — a crash-loop trap. It is P1 in v0.0.1 for this reason.
- **Permission posture window:** between #5 (deny) and #13 (ask), git commit/push are *denied*, not
  *asked* — the agent cannot commit at all in v0.0.1/v0.0.2. This is intentional (no responder yet)
  but must be communicated to users, and #13 must flip it.
- **Telegram flood control** (N6): the streaming edit path can trip rate limits under a chatty turn;
  must land in v0.0.2 alongside #8, not after.
- **Version drift** (§10): opencode is mid V1→V2; pin the version and never hard-code event strings
  without A0 verification.
- **Execution is 1–2 people:** "waves" are dependency gates, not staffing. The DAG is what matters;
  don't start an opencode-touching task before A0.

---

## QA Scenarios

- [ ] **A0:** fixtures for a plain turn and a git-gated turn exist and are referenced by tests.
- [ ] **v0.0.1 wire:** enrolled user sends text → receives a chunked reply from their own opencode
      instance; second user routes to the other instance.
- [ ] **Pairing:** unknown sender gets a 6-digit code; `proxy pair approve <code> --slot wife` binds
      and notifies; expired/replayed code is rejected; generation is rate-limited.
- [ ] **Shutdown:** `SIGTERM` aborts the in-flight turn, kills both opencode children, unlinks the
      socket; no orphaned procs; `:4096`/`:4097` free.
- [ ] **Dangling session:** delete opencode's DB; next message auto-recreates a session (no brick).
- [ ] **Secrets:** grep logs — no token, no prompt bodies; admin socket is `0600`.
- [ ] **v0.0.2 streaming:** live-edited answer at ≤1/sec; `typing` persists through a long turn;
      tool failures always shown; reconnect mid-turn does not duplicate parts.
- [ ] **Backpressure:** a second message during a turn is queued; channel-full yields a
      reject-with-message, not a hang.
- [ ] **/stop:** aborts the turn and rejects any pending permission.
- [ ] **v0.0.3 files:** inbound photo reaches the model; a file written to `./outbox` is sent.
- [ ] **Headline:** "summarise as minutes, send me to approve, then commit" → minutes file delivered
      → commit gate buttons → Approve → commit proceeds; Revise feeds corrigible feedback; Deny
      abandons.
- [ ] **Restart mid-gate:** a pending approval survives a proxy restart and re-surfaces to the user.
```
