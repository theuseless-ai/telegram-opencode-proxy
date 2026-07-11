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

## Docs

- [Architecture & MVP scope](docs/design/architecture.md) — the source of truth.
- [Design brainstorm minutes](docs/meeting_minutes/2026-07-11-architecture-brainstorm.md)
  — rationale behind each decision.

## Planned stack

`tokio` · `teloxide` · `reqwest` + `reqwest-eventsource` · `rusqlite` · `notify`
