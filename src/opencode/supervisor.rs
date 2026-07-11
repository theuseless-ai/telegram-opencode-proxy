//! Spawn / keep-alive / restart the two `opencode serve` procs (per-slot
//! workdir/port); readiness probe (poll `/config` until provider resolves);
//! crash-loop backoff; reap children on shutdown.
//! See `docs/design/architecture.md` §4. Issues #5 / N2 / N3.
