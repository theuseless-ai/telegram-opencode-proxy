//! Telegram bot: long-poll dispatcher, render/streaming, file transfer.
//! See `docs/design/architecture.md` §4/§13.

pub mod bot;
pub mod files;
pub mod render;
pub mod retry;
pub mod stream;
