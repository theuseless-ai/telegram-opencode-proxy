# Test harness (issue #24)

Two layers. **Layer 1 is hermetic and runs in `cargo test`** with no network, no
`opencode`, and no model. Layer 2 is local-only.

## Layer 1 — hermetic CI harness

| File | Role |
|---|---|
| `support/mock_opencode.rs` | in-process axum mock of the opencode V1 endpoints the proxy calls (`/config`, `/config/providers`, `/session`, `/session/:id`, `PATCH`, `/session/:id/message`, `/global/event` SSE stub). Two provider-catalogue variants (`start` / `start_without_model`). |
| `support/mock_telegram.rs` | in-process axum mock of the Telegram Bot API, wide enough for teloxide 0.17. Records `sendMessage` calls; can inject `getUpdates`. |
| `support/mod.rs` | pulls both mocks into the test crate. |
| `harness.rs` | integration tests wiring both mocks to the real proxy path. |

### How teloxide is pointed at the mock

teloxide-core 0.13 builds every request URL as `{base}/bot{token}/{Method}`
(`teloxide_core::net::method_url`), where `{Method}` is the payload's
`Payload::NAME` — **PascalCase** (`GetMe`, `SendMessage`, `GetUpdates`, …), set
by the `impl_payload!` macro via `stringify!($Method)`. Every request is a
`POST` with a JSON body; every response must be `{"ok": true, "result": <R>}`
(the untagged `TelegramResponse<R>`). A test swaps the base with:

```rust
Bot::new(token).set_api_url(mock.url.parse()?)   // Bot::set_api_url(reqwest::Url)
```

so the whole `{base}` (including `/bot{token}`) is served by the mock. The
catch-all route `/bot{token}/{method}` accepts any token.

### What the integration tests assert

1. `authorized_text_relays_model_reply` — authorized text → mock records one
   `sendMessage` carrying the model's reply (`echo: <prompt>`).
2. `unauthorized_sender_is_rejected` — unknown sender → a single
   "Not authorized…" reply, no opencode turn.
3. `long_reply_is_split_into_chunks` — a 9000-char reply → multiple
   `sendMessage` chunks, each ≤ 4096 chars, reconstructing the whole.
4. `provider_validation_failure_is_reported_clearly` — the `serve` bring-up
   (`connect_slots`: readiness → catalogue → `validate_model`) errors with a
   clear message naming the missing model and the failing slot.

Run: `cargo test`.

## Layer 2 — full-stack local harness (NOT in CI)

| File | Role |
|---|---|
| `../examples/mock_model.rs` | tiny OpenAI-compatible server (`/v1/models`, `/v1/chat/completions`) returning a canned completion. |
| `../test-fullstack.sh` | starts `mock_model`, writes a temp `opencode.json` pointing at it, starts a **real** `opencode serve`, and asserts a deterministic reply over the exact V1 wire the proxy uses. |

Requires the `opencode` binary; the script `SKIP`s cleanly if it is absent.

**Status:** partial by design (see the escape hatch in the issue). The script
proves `mock_model ↔ real opencode ↔ V1 wire`. Driving the proxy *binary* here
too needs a runnable `mock_telegram` with a control API; that remains a
documented TODO at the foot of `test-fullstack.sh`. The Telegram path is already
proven hermetically by Layer 1.
