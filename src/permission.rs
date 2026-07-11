//! Permission relay: `permission.asked` (on `/global/event`) → Telegram inline
//! buttons → `POST /permission/:id/reply` (V1/V2 adapter); reject-with-message =
//! revise loop; re-surface pending gates on restart.
//! See `docs/design/architecture.md` §2.6/§10. Issue #13.
