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
- **Shared opencode with per-request `?directory=` routing** — per-directory
  config (MCP, model) *does* resolve correctly on a shared server (verified
  1.17.18), so this isn't a config-resolution problem. We still run one process
  per workdir to sidestep its **lifecycle** liabilities: no stale-instance
  eviction (#33720), MCP-subprocess cleanup bugs on dispose (#21557, #30123),
  and per-directory model-catalog staleness (#36284). See §12.
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

- **Streaming:** `events.rs` (SSE, **`/global/event`**) `message.part.delta` →
  `render.rs` (throttled edits) → `bot.rs` → user. **Verbosity & liveness policy: §13.**
- **Approval:** `events.rs` `permission.asked` (on **`/global/event`**) → `permission.rs` posts
  `[✅][✏️][❌]`, stashes token in `state`/`persistence` → user taps →
  `callback_query` in `bot.rs` → `permission.rs` → `client.reply_permission`.
  opencode holds the agent turn blocked throughout — no resume machinery needed.
- **Files:** inbound photo/doc → `files.rs` download + base64 → `FilePart` in the
  prompt. Outbound → `outbox.rs` fires → `files.rs` sends. `/get <path>` guarded
  by canonicalize-within-workdir.

---

## 8. Build order (each shippable)

GitHub milestones use these version codes; **A/B/C** stay as shorthand (issue prefixes).

| Milestone | Adds | Proves |
|---|---|---|
| **v0.0.1** · A ~1d | config, auth + pairing handshake (CLI approve), `supervisor` (2 procs), blocking `POST /message`, chunked reply | the wire + enrollment work end-to-end |
| **v0.0.2** · B ~few days | SSE streaming + live edit, `typing` liveness, flat tool-status line, 2-user routing, `/new` `/whoami`, reconnect | daily-usable |
| **v0.0.3** · C ~1–2wk | inbound files, outbox + `/get`, permission relay + buttons, git-ask on session create, `/quiet` `/verbose` + sub-agent tags | minutes → approve → commit |

Deferred items (§3) live under the **v0.1.0+ — Backlog** milestone.

---

## 9. Dependencies (planned)

`tokio`, `teloxide`, `reqwest` + `reqwest-eventsource`, `serde`/`serde_json`,
`clap`, `rusqlite`, `notify`, `tracing`/`tracing-subscriber`, `anyhow`/`thiserror`,
`base64`.

## 10. Version sensitivity + A0-validated wire (opencode 1.17.18)

Pin an opencode version; fetch its live `GET /doc` (OpenAPI 3.1) and codegen the
client. **A0 (issue #20) validated the wire against a live `opencode serve`
1.17.18** — fixtures under `fixtures/opencode/`. Ground truth below **supersedes**
the dev-branch docs the earlier sections were first drafted against:

- **`POST /session/:id/message` is blocking** — returns the completed assistant
  message. The v0.0.1 blocking path is valid.
- **Both API surfaces are live**: V1 (`/session`, `/permission/:id/reply`,
  `/event`, `/global/event`) **and** V2 (`/api/*`). V2 is *not* lildax-only. The
  proxy targets **V1** (validated); keep a thin V1/V2 adapter seam anyway.
- **Subscribe to `/global/event`** (per instance) for the full event set. Real
  event names are **`message.part.delta`** (streaming text), `message.part.updated`,
  `message.updated`, `session.updated`, `session.status`, `session.diff`,
  `step-start`, `reasoning`, `tool`, `busy`, **`permission.asked`** — **NOT** the
  `session.next.*` names. Each frame is wrapped
  `{directory, project, payload:{id, type, properties}}`. The directory-scoped
  `/event` omits `permission.asked`.
- **Reply to a gate** via `POST /permission/:id/reply {reply: once|always|reject, message?}`.
  Request `properties`: `{id, sessionID, permission, patterns, metadata:{command},
  always, tool:{messageID, callID}}`.
- **`model` object differs by endpoint**: `{id, providerID}` on `POST /session`
  vs `{providerID, modelID}` on `POST /session/:id/message`.

Re-run A0 and re-diff `/doc` if you bump the pinned version.

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
provider_id = "llm-lan"                # must match a provider key in opencode.json
model_id    = "Qwen3.6-35B-A3B-bf16"   # must match a models key under that provider

[permissions]
ask = ["git commit*", "git push*"]   # PATCHed onto each session at creation
```

---

## 12. opencode model provider (external prerequisite)

The model endpoint URL and wire spec are **opencode's** config, **not** the
proxy's. The proxy only selects `{providerID, modelID}` (§11); opencode holds the
URL, the spec, and the key. You set this up once, outside the proxy.

Your local model (served OpenAI-compatible on your LAN — here `llm.lan:8080`,
Qwen models) is registered as a **custom OpenAI-compatible provider** in
`opencode.json`, via the Vercel AI SDK `@ai-sdk/openai-compatible` adapter (the
`/v1/chat/completions` spec). This is the actual setup A0 ran against:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "llm-lan": {                              // ← this key IS the providerID
      "npm": "@ai-sdk/openai-compatible",
      "name": "Local LLM (llm.lan)",
      "options": {
        "baseURL": "http://llm.lan:8080/v1",
        "apiKey": "{env:LLM_LAN_KEY}"         // use an env var — never hard-code the key
      },
      "models": {
        "Qwen3.6-35B-A3B-bf16": {}            // ← this key IS the modelID
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

### Per-directory MCP & model config

Everything in `opencode.json` — the `mcp` key **and** `provider`/`model` overrides
— is resolved **per directory**: opencode walks up from the working directory for
a project `opencode.json` / `.opencode`, merged over global
`~/.config/opencode/opencode.json`. This holds in server mode (verified at
opencode 1.17.18): each directory gets its own independently-resolved, cached
config.

Because we run **one `opencode serve` per workdir**, each user's instance picks up
its own workdir's config automatically:

- **Shared defaults** (providers, common MCP servers) → global
  `~/.config/opencode/opencode.json`.
- **Per-user overrides** (different MCP toolset, different model) → a project
  `opencode.json` in that user's workdir (`~/work/you` vs `~/work/wife`).

So per-user MCP + model differences need **zero proxy involvement** — same
boundary as above: the proxy selects `{providerID, modelID}`; opencode's
per-workdir config defines providers *and* MCP servers.

MCP config shape (`opencode.json`):

```json
{ "mcp": {
    "my-local":  { "type": "local",  "command": ["npx", "-y", "…"],
                   "environment": { "TOKEN": "{env:TOKEN}" }, "enabled": true },
    "my-remote": { "type": "remote", "url": "https://…",
                   "headers": { "Authorization": "Bearer …" }, "enabled": true } } }
```

**Capacity note:** on first use of a directory, opencode connects **all** MCP
servers configured for it (not on-demand per tool). Budget roughly
(workdirs × servers-per-workdir) concurrent MCP subprocesses — negligible for two
users, but a real ceiling at scale.

---

## 13. Event relay, verbosity & liveness

opencode's **`/global/event`** SSE stream carries reasoning, tool calls, sub-agent
runs, and step boundaries. Telegram is **linear and rate-limited** (~1 edit/sec),
so we surface a **flat status** — never a tree — plus native chat actions for
liveness. *(Authoritative A0-validated event names are in §10; names below are
conceptual categories.)*

### Liveness via chat actions (all verbosity levels)

- **`typing`** — the ambient "bot is working" signal. Re-send every ~4s while
  `session.status: busy` (it auto-expires ~5s). This is **off** the message-edit
  budget, so "thinking" costs no edits and is not a message.
- **`upload_document` / `upload_photo`** — fired right before an outbox / `/get`
  file send ("sending a file…").

### One live status line per turn (not a log, not a tree)

A single live-edited line shows the **current top-level activity**, replaced each
step; on completion it collapses to a one-line **summary footer** above the answer:

```
🧵 explore ▸ 🔍 grep "load_config"          ← during the turn (live-edited)
────────────────────────────────
✓ 6 tools · 1 subagent · edited 2 files      ← on completion
<answer>
```

**Sub-agents are a *tag*, not a tree.** A `task`-tool child session prefixes the
status line with its name (`🧵 explore ▸ …`); that label is the only nesting cue —
no indentation, no live tree (unreadable and reflows badly in a chat column).
Soft dependency: the tag needs a child-`sessionID` → name lookup; if unavailable,
drop the tag and show the bare activity.

**Tool lines** are lifecycle-driven — `⚙️ <tool>: <key arg>` → `✓` / `✗ <error>`,
arg truncated (`bash: git status`, `📖 read main.rs`, `✏️ edit config.rs`).
Failures are shown at **every** verbosity.

### Coalesce, don't mirror

`render.rs` holds `{ current-activity, answer-buffer }` and flushes to Telegram
**≤ 1/sec**. Stream from `message.part.delta`; take tool state from the `tool`
event (§10) rather than every intermediate delta; never edit per-delta — flood limits.

### Verbosity (per-user toggle: `/quiet` · `/verbose`, stored in `state`)

| Stream | Quiet | Normal (default) | Verbose |
|---|---|---|---|
| `text.*` (answer) | answer only | stream | stream |
| `reasoning.*` | — | `typing` only | `typing` (+ note in status) |
| tool calls | failures only | flat status line | + full args |
| sub-agent (`task` child) | — | `🧵 name ▸` tag | tag on status |
| `step.ended` | — | summary footer | + cost / files touched |
| `permission.asked` | buttons | buttons | buttons |

Liveness (`typing`, upload actions) applies at **all** levels.

### Deliberately excluded

No live tree; no per-step message spam (one edited message, not many); no file
transcript — chat stays lightweight and linear.

### Scope

- **Milestone B:** `typing` liveness + answer streaming + flat tool-status line +
  always-on failures.
- **Milestone C / fast-follow:** `/quiet` `/verbose` toggle, sub-agent tags, the
  summary footer.
