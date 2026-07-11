# Architecture & MVP Scope

**Project:** telegram-opencode-proxy
**Date:** 2026-07-11
**Status:** Design locked; ready to scaffold Milestone A.
**Companion:** see `docs/meeting_minutes/2026-07-11-architecture-brainstorm.md` for the
full rationale behind each decision.

---

## 1. What this is

A single Rust process that bridges a Telegram bot and `opencode serve`, so the
user (and their wife) can drive opencode — running against a **local model on an
M3 Ultra** — from Telegram. Self-hosted, one machine, two known users.

---

## 2. MVP scope (LOCKED)

**Target: "Full Scenario" (Milestone C)** — built incrementally A → B → C.

In scope:

- **Long-poll** bot (teloxide `getUpdates`; no webhook, no public URL).
- **Two known users**, enrolled via a **pairing handshake** (no manual user-ID
  lookup): unknown sender → bot issues a single-use 6-digit code → admin approves
  it via a local CLI, binding the account to a slot. Whitelist persists in SQLite.
  See §5.
- **Separate workdirs:** two static `opencode serve` processes
  (`:4096` → user A, `:4097` → user B), proxy routes by `chat_id`.
- **Stateless proxy:** opencode owns message/session persistence (SQLite);
  proxy stores only routing (`chat_id → session_id`) + pending approvals.
- **Streaming** assistant output (SSE → live message edits).
- **Files both ways:** inbound (Telegram photo/doc → base64 `FilePart`);
  outbound (outbox watcher + `/get`).
- **Approval gate:** native opencode permission relay
  (`permission.asked` → Telegram buttons → `/permission/:id/reply`), delivering
  the **minutes → approve → commit** flow.
- **Concurrency:** in-process **`tokio::sync::mpsc`** per user (serialize turns);
  `/stop` → `POST /session/:id/abort`.

## 3. Explicitly OUT of scope (deferred to v2+)

Documented as conscious deferrals, not oversights:

- **Dynamic instance manager / arbitrary-N multi-tenancy** — two users = static
  2-process config. No spawn/reap-per-arbitrary-user.
- **Actor framework (`ractor`/`kameo`)** — the raw `mpsc` + worker is enough at
  this scale. `ractor` (mailboxes + supervision) is the documented upgrade path
  if we ever want supervision as a first-class concern.
- **External message-queue broker** (Redis/RabbitMQ/NATS/Kafka) — not warranted
  single-process. Telegram offset + opencode SQLite already provide durability.
- **Durable proxy-side queue** (`yaque`, SQLite-as-queue) — redundant for 2 users.
- **Webhook ingress** — long-poll only for a home box.
- **Invite-code (bearer-token) enrollment** — rejected in favor of the
  confirmation-nonce pairing in §5: a leaked invite code would grant access,
  whereas a pairing code cannot (approval requires shell/CLI access).
- **Per-request directory routing on one shared opencode** — upstream rough
  edges; we use one process per workdir instead.
- **opencode server Basic Auth** — localhost only; proxy whitelist is the gate.

---

## 4. Internal module architecture

```
┌──────────────┐                      ┌──────────────┐
│  User A (you)│                      │ User B (wife)│      Telegram clients
└──────┬───────┘                      └──────┬───────┘
       └───────────────┬──────────────────────┘
                       │  long-poll getUpdates  (offset = durable inbound buffer, ~24h)
              ┌────────▼─────────┐
              │ Telegram Bot API │  ◄── sendMessage / editMessageText / sendDocument
              └────────┬─────────┘
═══════════════════════│═══════════════════════════════════════ PROXY PROCESS (single, tokio)
                       ▼
   ┌───────────────────────────────────────────────────────────────────────┐
   │  telegram/bot.rs   (teloxide dispatcher)                               │
   │  • text msgs   • /new /whoami /get /stop   • callback_query (buttons)  │
   │  • inbound file download                                              │
   └───────┬──────────────────────────────────────────┬────────────────────┘
           │                                           │
     ┌─────▼─────┐                            ┌────────▼────────┐
     │  auth.rs  │  pairing whitelist         │ telegram/       │  render.rs → chunk 4096,
     │ pairing.rs│  unknown → issue code      │  render+files   │  throttle stream-edit ~1/s
     └─────┬─────┘                            │  base64 FilePart│  files.rs → send_document
           │ chat_id                          └────────▲────────┘
           ▼                                           │ outbound text/files
   ┌───────────────────────────────────────────────────┼───────────────────┐
   │  state.rs  (shared AppState)                       │                   │
   │  ┌─────────────────────────────┐   per-user in-process queue           │
   │  │ routing: chat_id → {port,   │   ┌──────────────┐  ┌──────────────┐  │
   │  │   session_id, workdir}      │   │ mpsc queue A │  │ mpsc queue B │  │
   │  │ pending approvals (token→…) │   └──────┬───────┘  └──────┬───────┘  │
   │  └─────────────────────────────┘          ▼                 ▼          │
   │                                    ┌────────────┐    ┌────────────┐    │
   │                                    │ worker A   │    │ worker B   │  serialize:
   │                                    │ (actor)    │    │ (actor)    │  1 turn at a time
   │                                    └─────┬──────┘    └─────┬──────┘    │
   └──────────────────────────────────────────┼────────────────┼───────────┘
           │                                   │                │
   ┌───────▼────────┐   ┌──────────────┐  ┌────▼─────┐  ┌───────▼──────┐  ┌─────────────┐
   │ persistence.rs │   │  session.rs  │  │opencode/ │  │ permission.rs│  │  outbox.rs  │
   │ SQLite:        │   │ get-or-create│  │ client.rs│  │ asked→buttons│  │ notify      │
   │ routing +      │◄─►│ session;/new │─►│ reqwest  │  │ callback→    │  │ watcher per │
   │ pending appr.  │   │ PATCH git=ask│  │ prompt,  │  │ reply(V1/V2) │  │ ./outbox    │
   │ (survive crash)│   │ on create    │  │ msgs,file│  └──────▲───────┘  └──────┬──────┘
   └────────────────┘   └──────────────┘  └────┬─────┘         │                 │
                        ┌──────────────┐        │      ┌────────┴────────┐        │
                        │ opencode/    │        │      │ opencode/       │        │
                        │ supervisor.rs│        │      │ events.rs (SSE) │        │
                        │ spawn/keep-  │        │      │ /event stream:  │        │
                        │ alive 2 procs│        │      │ text.delta,     │        │
                        └──────┬───────┘        │      │ step.ended,     │        │
                               │                │      │ permission.asked│        │
                               │         POST   │      └────────▲────────┘        │
═══════════════════════════════│════════════════│═══════════════│═════════════════│═══════
                               ▼                ▼               │ SSE             │ reads
              ┌────────────────────────┐  ┌────────────────────┴────────┐        │ files
              │ opencode serve :4096   │  │ opencode serve :4097         │◄───────┘
              │ workdir ~/work/you     │  │ workdir ~/work/wife          │
              │ SQLite session store   │  │ SQLite session store         │
              └───────────┬────────────┘  └──────────────┬───────────────┘
                          └───────────────┬───────────────┘
                                          ▼   OpenAI-compatible API
                          ┌───────────────────────────────┐
                          │  Local model backend (M3 Ultra)│  ← one server; the two
                          │  LM Studio / Ollama / MLX      │    instances SERIALIZE here
                          └───────────────────────────────┘
```

### Module responsibilities

| Module | Responsibility |
|---|---|
| `main.rs` | Load config → spawn opencode procs → start bot + SSE listeners + outbox watchers + admin socket |
| `config.rs` | Config structs, `clap` CLI (`serve` + `pair` subcommands), slot definitions, validation |
| `state.rs` | Shared `AppState`: routing table, pending-approvals, pending-pairings, instance registry |
| `persistence.rs` | SQLite: `allowed_users`, `chat_id → session_id`, `pending_pairings`, pending approvals (survive restart) |
| `auth.rs` | Whitelist check against persisted `allowed_users`; unknown sender → hand off to `pairing.rs` |
| `pairing.rs` | Issue single-use 6-digit codes; admin CLI approve/deny/list over local socket; bind account → slot |
| `session.rs` | Get-or-create session; `/new` reset; PATCH `git commit*`/`push*` = `ask` on create |
| `opencode/client.rs` | reqwest: create_session, prompt_async, get_messages, patch/reply permission, read_file |
| `opencode/events.rs` | SSE `/event` subscribe + parse `session.next.*` + `permission.asked` |
| `opencode/supervisor.rs` | Spawn / keep-alive / restart the two `opencode serve` procs |
| `opencode/types.rs` | API structs (codegen from `/doc`, behind a V1/V2 version adapter) |
| `telegram/bot.rs` | teloxide dispatcher: messages, `/new` `/whoami` `/get` `/stop`, `callback_query`, file download |
| `telegram/render.rs` | opencode output → TG msg; 4096 chunking; stream-edit throttle ~1/s |
| `telegram/files.rs` | Inbound: TG file → base64 `FilePart` · Outbound: send_document/photo by mime |
| `permission.rs` | `permission.asked` → inline keyboard → callback → reply (V1/V2 adapter) |
| `outbox.rs` | `notify` watcher on each `./outbox` → send new files to owning user |

---

## 5. Enrollment / pairing (auth)

Goal: authorize the two users **without ever looking up a numeric Telegram ID**,
while keeping enrollment gated to admin (shell) access — mandatory, since
opencode executes code.

**Design: confirmation-nonce handshake** (chosen over an invite-code/bearer
scheme — see §3). The code flows *user → admin*, so it is **not** an access
credential: a leaked code is useless without the admin's CLI approval.

```
new user ──msg──► bot ──(not in allowed_users)──► create pending
                       │   { code: rand 6-digit, chat_id, username, expires_at }
                       └── reply: "Not authorized. Code 123456 (10 min).
                                   Send it to the admin."
       │
admin ◄── user reads code out-of-band (in person / SMS) ──┘
  │
  └─ CLI:  proxy pair approve 123456 --slot wife
                 │  (over local Unix socket, perms 0600 — admin channel)
                 ▼
             daemon: verify code + TTL → bind chat_id → slot(wife)
                     → write allowed_users → delete pending
                     → bot notifies user "✅ Approved."
```

Rules:

- Codes are **single-use** with a short **TTL** (~10 min); regenerating replaces
  the prior code. Generation is **rate-limited per `chat_id`**.
- The code is a **confirmation nonce**, not a bearer token: it binds the specific
  `chat_id` to the human who reads it back, defeating name-spoofing.
- Approval **binds an account to a slot** (workdir / opencode instance), filling
  in the slot's `telegram_id`.

Admin CLI (same binary, two modes — `serve` vs client subcommands):

| Command | Effect |
|---|---|
| `proxy pair list` | Show pending requests: code, @username, first name, age |
| `proxy pair approve <code> --slot <name>` | Bind chat_id → slot, add to `allowed_users`, notify user |
| `proxy pair deny <code>` | Drop a pending request |

**Bootstrap:** whoever has shell access approves themselves and their wife the
same way — **no config-seeded IDs at all**. "Can approve" == "has shell on the
box" == admin.

**Admin channel security (hold this line):** the admin socket is **local-only**
(Unix domain socket, perms `0600`, or `127.0.0.1`); **never** exposed on the
network. Enrollment must stay gated to shell access.

---

## 6. Queue layers (three) + concurrency policy

1. **Telegram offset** — durable inbound buffer (retained ~24h). Advance offset
   **only after** the message is durably handed off → at-least-once delivery.
2. **`mpsc queue` → worker** — the only queue we build. In-process, per user,
   serializes turns (one at a time). Backpressure via a bounded channel.
3. **opencode session + local model** — per-session serialization + SQLite
   persistence (free). The single local model server is the throughput floor.

**Busy-message policy (chosen): serialize.** A second message while a turn is in
flight is queued. `/stop` maps to `POST /session/:id/abort` for explicit interrupt.

---

## 7. Return-path flows

- **Streaming:** `events.rs` (SSE) `text.delta` → `render.rs` (throttled edits) →
  `bot.rs` → user.
- **Approval:** `events.rs` `permission.asked` → `permission.rs` posts
  `[✅][✏️][❌]`, stashes token in `state`/`persistence` → user taps →
  `callback_query` in `bot.rs` → `permission.rs` → `client.reply_permission`.
  opencode holds the agent turn blocked throughout — no resume machinery needed.
- **Files:** inbound photo/doc → `files.rs` download + base64 → `FilePart` in the
  prompt. Outbound → `outbox.rs` fires → `files.rs` sends. `/get <path>` guarded
  by canonicalize-within-workdir.

---

## 8. Build order (each shippable)

| Milestone | Adds | Proves |
|---|---|---|
| **A** ~1d | config, auth + pairing handshake (CLI approve), `supervisor` (2 procs), blocking `POST /message`, chunked reply | the wire + enrollment work end-to-end |
| **B** ~few days | SSE streaming + live edit, 2-user routing, `/new` `/whoami`, reconnect | daily-usable |
| **C** ~1–2wk | inbound files, outbox + `/get`, permission relay + buttons, git-ask on session create | minutes → approve → commit |

---

## 9. Dependencies (planned)

`tokio`, `teloxide`, `reqwest` + `reqwest-eventsource`, `serde`/`serde_json`,
`clap`, `rusqlite`, `notify`, `tracing`/`tracing-subscriber`, `anyhow`/`thiserror`,
`base64`.

## 10. Version sensitivity (opencode)

Pin an opencode version. Fetch its live `GET /doc` (OpenAPI 3.1) at build time and
codegen the client. Keep the permission relay behind a thin V1/V2 adapter
(`opencode serve` ships **V1** today: `permission.asked`, `POST /permission/:id/reply`).
Verify event-type strings against a live `/event` connection before hard-coding.

---

## 11. Config sketch (`config.toml`)

```toml
bot_token = "…"                 # or env TELOXIDE_TOKEN
admin_socket = "/run/topx/admin.sock"   # local-only CLI ↔ daemon channel (perms 0600)

[pairing]
code_ttl_secs = 600             # single-use 6-digit code lifetime
# Telegram IDs are NOT listed here — bound at pairing time via `proxy pair approve`.

[[slots]]                       # a user "seat"; telegram_id filled in by pairing
name = "you"
opencode_url = "http://127.0.0.1:4096"
workdir = "/Users/you/work/you"

[[slots]]
name = "wife"
opencode_url = "http://127.0.0.1:4097"
workdir = "/Users/you/work/wife"

[model]
# SELECTOR ONLY — the endpoint URL + wire spec live in opencode.json (see §12).
provider_id = "lmstudio"              # must match a provider key in opencode.json
model_id    = "google/gemma-3n-e4b"  # must match a models key under that provider

[permissions]
ask = ["git commit*", "git push*"]   # PATCHed onto each session at creation
```

---

## 12. opencode model provider (external prerequisite)

The model endpoint URL and wire spec are **opencode's** config, **not** the
proxy's. The proxy only selects `{providerID, modelID}` (§11); opencode holds the
URL, the spec, and the key. You set this up once, outside the proxy.

Your local model (LM Studio / Ollama / MLX on the M3 Ultra) is registered as a
**custom OpenAI-compatible provider** in `opencode.json`, via the Vercel AI SDK
`@ai-sdk/openai-compatible` adapter (the `/v1/chat/completions` spec):

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "lmstudio": {                          // ← this key IS the providerID
      "npm": "@ai-sdk/openai-compatible",
      "name": "LM Studio (local)",
      "options": { "baseURL": "http://127.0.0.1:1234/v1" },
      "models": {
        "google/gemma-3n-e4b": {}          // ← this key IS the modelID
      }
    }
  }
}
```

- **`npm`** — `@ai-sdk/openai-compatible` for `/v1/chat/completions` endpoints
  (LM Studio, Ollama at `http://localhost:11434/v1`, vLLM, llama.cpp, and any MLX
  server exposing an OpenAI-compatible API). Use `@ai-sdk/openai` only for
  `/v1/responses`-style endpoints. **No MLX-native adapter exists** — treat MLX as
  "just another OpenAI-compatible endpoint."
- **`options.apiKey`** — optional for local servers; supports `{env:VAR}`.
- **Models must be hand-listed** — there is no `/v1/models` auto-discovery yet
  (upstream issue #6231). Each `modelID` you want must appear under `models`.
- **`providerID` = the `provider` object key; `modelID` = the `models` object key.**
  Both are free-form strings you choose. The proxy's `[model]` (§11) must match
  them exactly.

**Where to put it (matters for the two-instance setup):** opencode merges config,
walking up from each server's working directory and layering it on the global
`~/.config/opencode/opencode.json`. Because the proxy launches the two
`opencode serve` instances in *different* workdirs, put the provider block in the
**global** `~/.config/opencode/opencode.json` so both instances resolve the same
provider regardless of workdir. A per-project `opencode.json` would apply to only
that one workdir. → This is a **launch prerequisite**: `opencode/supervisor.rs`
must not start an instance until its provider config resolves, or the proxy's
`{providerID, modelID}` won't bind.

> **API-shape note (don't get this wrong in `client.rs`):** the HTTP session API
> takes `model` as a **split object** `{ "providerID": "…", "modelID": "…" }` — not
> a combined `"provider/model"` string. (The combined-string form is used only by
> the `opencode.json` top-level `model` key and the `-m` CLI flag, not the API.)
