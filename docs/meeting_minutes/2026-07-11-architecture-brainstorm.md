# Meeting Minutes — Architecture Brainstorm

**Project:** telegram-opencode-proxy (renamed from `telegram-proxy-mcp`)
**Date:** 2026-07-11
**Topic:** Design of a Rust proxy bridging a Telegram bot and the `opencode serve` HTTP API
**Status:** Design agreed; ready to sketch module skeleton before writing Rust.

---

## 1. Goal

Build a **Rust** proxy that sits between a Telegram bot and one or more
`opencode serve` instances (opencode = the AI coding agent CLI, repo now
`anomalyco/opencode`, formerly `sst/opencode`). Users chat with the Telegram
bot; the proxy routes their messages to opencode and relays responses back.

The `-mcp` in the original folder name was **vestigial** — this is a plain
HTTP/bot proxy, not an MCP server. Folder renamed accordingly.

---

## 2. Decisions

### 2.1 Multi-tenancy

The original a/b framing conflated two independent axes; they were separated:

- **Axis 1 — Telegram connection ownership.** A single bot token can only have
  **one** consumer of its update stream (`getUpdates` conflicts across
  processes; a webhook points at one URL). Therefore **one proxy process** owns
  the bot and maintains a routing table. "One proxy per user" is not viable on a
  shared bot token.
- **Axis 2 — opencode topology.** Because opencode **executes code and edits
  files**, a shared workspace is unacceptable (users would clobber each other
  and read each other's files). **Decision: one `opencode serve` instance per
  user**, each bound to an isolated working directory, spawned lazily and reaped
  on idle.

**Chosen architecture:**

```
Telegram ──updates──► [ single Rust proxy ]
                        │  auth gate: telegram_user_id whitelist  ← ONLY isolation point
                        │  state: chat_id → { port, session_id, workdir }
                        │  instance manager: spawn/reap opencode procs per user
                        ├─► opencode serve (user A, workdir ~/work/A)
                        ├─► opencode serve (user B, workdir ~/work/B)
                        └─► ...
```

Per-request directory routing (`?directory=` / `x-opencode-directory`) on a
single shared server exists but is **not hardened** for isolated concurrent
writes (open upstream issues #9366, #6697, #12271). Prefer one process per
workdir for isolation.

### 2.2 Whitelist / auth

- opencode has **no per-user auth** — only server-wide HTTP Basic Auth
  (`OPENCODE_SERVER_PASSWORD`). **The proxy is the sole enforcement point.**
- Whitelist supported via **both** a config file **and** a `--allowed-users`
  CLI flag (flag merges over config).
- **Whitelist by numeric Telegram user ID, not `@username`** — usernames are
  mutable/reassignable and unsafe as an identity anchor for something that
  executes code. Resolve any usernames to numeric IDs at first contact and
  enforce on the number.
- Provide a `/whoami` command that replies with the caller's numeric ID for
  easy onboarding.

### 2.3 Message caching (Q3) — proxy stays stateless

opencode **persists sessions, messages, and parts server-side** in SQLite
(WAL mode, `~/.local/share/opencode/opencode.db`, override via `OPENCODE_DB`).

The proxy therefore **does not cache conversation content**. It only persists a
small mapping:

```
telegram_user_id → { opencode_instance(port), session_id, workdir }
```

Flow: `POST /session` once per user → `POST /session/:id/message` per turn →
`GET /session/:id/message` to replay history if needed.

### 2.4 Files inbound (Q4) — inline base64, no upload endpoint

There is **no multipart upload**. Files/images are attached **inline** in the
prompt as a `FilePart`:

```json
{ "type": "file", "mime": "image/jpeg", "filename": "photo.jpg",
  "url": "data:image/jpeg;base64,<...>" }
```

For a Telegram photo/document: download from the Bot API → base64-encode → send
as `FilePart.url`. Acceptance depends on the configured model/provider's
modalities (some providers reject PDFs / certain image types) — validate per
model.

### 2.5 Files outbound — sending files back to the user

- **Getting bytes is easy** because the proxy is co-located with opencode and
  owns the workdir: read files straight off disk
  (`InputFile::file(path)` / `InputFile::memory(bytes)` in teloxide). Dispatch
  by mime: `image/*` → `send_photo`, else → `send_document` (send as document to
  avoid Telegram re-compressing). Limits: **50 MB** send, **1024-char** caption.
- **Deciding *what* to send:** do **not** auto-dump every file the agent
  touches (it edits source constantly → noise). Use an **explicit "outbox"
  convention**: a reserved dir (`./outbox/`) the proxy watches (`notify` crate);
  the agent writes deliverables there and the proxy sends them. Instruct the
  agent via `AGENTS.md`.
- Plus an explicit **`/get <path>`** pull command (with a path-traversal guard:
  canonicalize and assert the path stays within `workdir`).

### 2.6 Approval workflows (the "minutes → approve → commit" scenario)

Scenario: mid-conversation, ask the agent to produce meeting minutes, send them
to the user to approve, then commit to GitHub only after approval.

This needs a **human-in-the-loop gate** (suspend → approve → resume). Key
finding: **opencode's native permission system handles this**, and it is a
better fit than an app-level "stop and wait" convention for **tool actions**.

- When a gated tool runs, `opencode serve` emits over the SSE stream:
  ```json
  { "type": "permission.asked", "properties": {
      "id": "per_...", "sessionID": "ses_...", "permission": "bash",
      "patterns": ["git push*"], "metadata": {"command": "git push origin main"},
      "always": ["git push*"], "tool": {"messageID":"...","callID":"..."} } }
  ```
  **and the agent's turn genuinely blocks** until a reply — so suspend/resume is
  free; the proxy needs no pending-action resume machinery for tool gates.
- Reply endpoint (V1):
  ```
  POST /permission/:requestID/reply   { "reply": "once" | "always" | "reject", "message"? }
  ```
- `reply:"reject"` with a `message` is fed back to the model as **corrigible
  feedback** → this gives a **revise loop for free** (e.g. reject with
  "add an attendees section" → agent rewrites → re-asks).

**Combined flow:**

```
You: "summarise as minutes, send me to approve, then commit"
  → Agent writes ./outbox/minutes.md      → outbox watcher sends the DOC to Telegram
  → Agent runs `git commit`               → opencode emits permission.asked, TURN BLOCKS
  → Proxy relays gate → Telegram buttons:  [✅ Approve] [✏️ Revise] [❌ Deny]
       ✅ → POST /permission/:id/reply {reply:"once"} → commit proceeds (push = same dance)
       ✏️ → reply {reply:"reject", message:"<edits>"} → agent revises → re-delivers → re-asks
       ❌ → reply {reply:"reject"} → agent abandons
```

Content delivery = outbox; action gate = native permission. They compose.

**⚠️ Critical gotcha:** opencode's default permission is `"*": "allow"`, so
`git commit`/`git push` run with **no prompt** out of the box. You **must** opt
in per session:

```
PATCH /session/:id  { "permission": [
  { "permission": "bash", "pattern": "git commit*", "action": "ask" },
  { "permission": "bash", "pattern": "git push*",   "action": "ask" } ] }
```

Session-level rules persist on the session record and win over agent defaults.
Miss this and the agent commits silently.

**Decision rule for gates:**

| The approval gates…                         | Mechanism |
|---------------------------------------------|-----------|
| A **tool action** (commit, push, edit, shell) | Native permission relay (`permission.asked` ↔ `/permission/:id/reply`). Preferred. |
| **Pure content sign-off** (no agent action after) | App-level inline buttons + follow-up prompt + a persisted pending-actions map. |

### 2.7 Streaming

Assistant output streams via `GET /event` (SSE). Filter
`session.next.text.delta` for the target session; treat
`session.next.step.ended` / `session.status: idle` as "done". Map to Telegram by
editing a placeholder message as chunks arrive (rate-limit to ~1 edit/sec; mind
flood limits and the 4096-char message cap → chunk long output).

---

## 3. Version sensitivity (must handle)

- opencode is mid **v1 → v2** migration; `sst/opencode` → `anomalyco/opencode`.
- `opencode serve` today uses the **V1** permission wire shape above
  (`permission.asked`, `POST /permission/:id/reply`). A **V2**
  (`permission.v2.asked`, `/api/session/:id/permission/...`, with *persisted*
  "always" approvals) exists in-repo but currently ships in a **different
  binary (`lildax`)**, not `opencode serve`.
- The `session.next.*` event taxonomy comes from `dev`-branch source, not stable
  prose docs.

**Action:** pin an opencode version; fetch its live `GET /doc` (OpenAPI 3.1) at
build time and codegen the Rust client rather than hand-writing structs; put the
permission relay behind a thin adapter so the V2 swap is not a rewrite. Verify
event-type strings against a live `/event` connection before hard-coding them.
Note: V1 `reply:"always"` is in-memory only (lost on restart) — for durable
"always allow git this session" use the `PATCH` ruleset instead.

---

## 4. Likely Rust stack

- **Telegram:** `teloxide` (batteries-included) or `frankenstein` (thinner).
- **HTTP to opencode:** `reqwest` + SSE client (`reqwest-eventsource`).
- **Runtime:** `tokio`. **State:** `DashMap` / actor-per-user with channels.
- **Filesystem watch (outbox):** `notify`.
- **Persisted proxy state** (routing table, pending approvals): SQLite.

---

## 5. Open items / next step

- Confirm long-poll vs webhook for the bot (long-poll simpler for self-hosted).
- Instance lifecycle details: spawn latency, idle timeout, max concurrent
  instances, port allocation, crash recovery.
- **Next step:** sketch the proxy's module layout — bot loop / session-instance
  manager / SSE→telegram adapter / permission relay / outbox watcher — before
  writing Rust.

---

## 6. References

- opencode docs: https://opencode.ai/docs (server, sdk, permissions)
- opencode repo: https://github.com/anomalyco/opencode (formerly sst/opencode)
- Live API spec: `GET /doc` on a running `opencode serve` (OpenAPI 3.1)
