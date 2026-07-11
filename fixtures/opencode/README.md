# opencode A0 validation fixtures

Captured from a **live `opencode serve` v1.17.18** on 2026-07-11 to validate the
wire *before* writing client code (plan Wave 0 / issue #20). Test turns ran
against `deepseek/deepseek-v4-flash` (cloud) purely because it was first in
`/config/providers`; the wire is provider-independent. The proxy's real target is
the local **`llm-lan`** provider (`http://llm.lan:8080/v1`, Qwen models).

## Files

| File | Capture |
|---|---|
| `doc.json` | `GET /doc` — full OpenAPI 3.1 surface |
| `session-create.json` | `POST /session` response |
| `message-response.json` | `POST /session/:id/message` — a **plain** turn (blocking) |
| `events/plain.sse` | `GET /event` stream during the plain turn |
| `session-gated.json` | `POST /session` for the gated turn |
| `patch-permission.json` | `PATCH /session/:id` setting `bash = ask` |
| `pending-permission.json` | `GET /permission` — the pending gate (V1 shape) |
| `permission-reply.json` | `POST /permission/:id/reply {reply:reject}` response |
| `events/gated.sse` | `GET /event` stream during the gated turn |

## Key findings (feed #7, #13, §10/§12/§13)

1. **`POST /session/:id/message` is BLOCKING** — returns the completed assistant
   message (HTTP 200, ~1.9s). v0.0.1's blocking reply path is valid.
2. **Subscribe to `/global/event`** (per opencode instance) — it carries the full
   event set: `message.part.delta` (streaming text), `message.part.updated`,
   `message.updated`, `session.updated`, `session.status`, `session.diff`,
   `step-start`, `text`, `reasoning`, `tool`, `busy`, **`permission.asked`**,
   `server.connected/heartbeat`. The directory-scoped `/event` carries the message
   events but **omits `permission.asked`**; `/api/event` and `/session/:id/event`
   yielded nothing here. Names are **NOT** the dev-branch `session.next.*`. Each
   `/global/event` frame is wrapped: `{directory, project, payload:{id, type, properties}}`.
3. **Both API surfaces are exposed** by `opencode serve`: V1 (`/session`,
   `/permission/:id/reply`, `/event`) **and** V2 (`/api/*`). V2 is *not*
   lildax-only as earlier research suggested. V1 confirmed working here.
4. **Permission gate** — V1 request shape confirmed via `GET /permission` *and* the
   `permission.asked` event on `/global/event` (see `events/gated-global.sse`):
   `{id, sessionID, permission, patterns, metadata:{command}, always, tool:{messageID,callID}}`.
   `POST /permission/:id/reply {reply:"reject"}` → 200. **RESOLVED for #13:** relay
   subscribes `/global/event`, filters `permission.asked`, replies via
   `POST /permission/:id/reply`.
5. **`model` object differs by endpoint**: `POST /session` uses `{id, providerID}`;
   `POST /session/:id/message` uses `{providerID, modelID}`. `client.rs` must handle both.
6. **Local provider**: `llm-lan` → `http://llm.lan:8080/v1`, models
   `Qwen3.6-35B-A3B-bf16` / `Qwen-AgentWorld-35B-A3B-bf16` / `Qwen3.6-35B-A3B-DFlash`.

## Version

opencode **1.17.18**. Re-capture if the pinned version changes.
