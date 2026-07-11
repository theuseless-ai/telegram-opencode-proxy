//! Inbound: Telegram photo/doc → base64 `FilePart` (data URI). Outbound:
//! `send_document`/`send_photo` by mime; `/get <path>` with within-workdir guard.
//! See `docs/design/architecture.md` §2.4/§2.5. Issues #11/#12.
