# telegram-opencode-proxy

A small **Rust** proxy that lets you drive [opencode](https://opencode.ai)
(`opencode serve`) from a **Telegram bot** — chat with your coding agent, running
against a local model, straight from Telegram.

> **Status:** design phase. This repo currently holds the architecture and design
> docs; implementation (Milestone A) is next.

## What it does

- Bridges the Telegram Bot API (long-poll) and the `opencode serve` HTTP API.
- Multi-user: routes each Telegram user to their own isolated `opencode serve`
  instance / working directory.
- Streams responses back with live status, relays approval prompts (e.g.
  *review → approve → commit*) as inline buttons, and passes files both ways.
- Enrollment via a pairing handshake — no manual user-ID lookup.

## File transfer

Files move over HTTP, not a shared filesystem:

- **Outbound** — the agent calls the MCP tool `send_file_to_user(filename,
  content, caption?)` (base64 bytes) to send a file to its user. The recipient
  is fixed by the workspace's `X-Slot` header, not a tool argument.
- **Inbound** — a Telegram photo/document is announced to the model as a
  one-shot download URL (`GET /files/{id}`); the agent `curl`s it into a
  `downloads/` folder and reads it with its own tools.

Register the tool once per workspace in that opencode instance's
`opencode.json`:

```jsonc
{
  "mcp": {
    "telegram-files": {
      "type": "remote",
      "url": "http://127.0.0.1:4100/mcp",
      "enabled": true,
      "headers": { "X-Slot": "frank" }
    }
  }
}
```

`X-Slot` must match the slot's `name` in `config.toml` exactly (case-sensitive).
See [architecture.md §14](docs/design/architecture.md#14-mcp-file-transfer-server-65)
for the full design.

## Docs

- [Architecture & MVP scope](docs/design/architecture.md) — the source of truth.
- [Design brainstorm minutes](docs/meeting_minutes/2026-07-11-architecture-brainstorm.md)
  — rationale behind each decision.

## Planned stack

`tokio` · `teloxide` · `reqwest` + `reqwest-eventsource` · `rusqlite` · `notify`
