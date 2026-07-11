#!/usr/bin/env bash
# dev.sh — dev convenience for the connect-only proxy.
#
# The proxy does NOT spawn opencode. This script starts one `opencode serve` per
# [[slots]] entry in the config (port parsed from opencode_url, run in that
# slot's workdir), waits for them, then runs the proxy — and reaps the opencode
# instances on exit. In production, run the opencode instances under systemd /
# compose instead and just run `cargo run -- serve`.
#
# Usage: ./dev.sh [config.toml]
set -euo pipefail

CONFIG="${1:-config.toml}"
[ -f "$CONFIG" ] || { echo "config not found: $CONFIG" >&2; exit 1; }

pids=()
cleanup() {
  echo
  echo "stopping opencode instances…"
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

# Emit "url<TAB>workdir" for each slot (assumes opencode_url precedes workdir).
slots=$(awk '
  /^\[\[slots\]\]/            { url=""; wd="" }
  /opencode_url[[:space:]]*=/ { split($0, a, "\""); url=a[2] }
  /workdir[[:space:]]*=/      { split($0, a, "\""); wd=a[2];
                                if (url != "" && wd != "") { print url "\t" wd; url=""; wd="" } }
' "$CONFIG")

[ -n "$slots" ] || { echo "no [[slots]] found in $CONFIG" >&2; exit 1; }

while IFS=$'\t' read -r url workdir; do
  port="${url##*:}"; port="${port%%/*}"
  echo "starting opencode on :$port  (workdir: $workdir)"
  ( cd "$workdir" && exec opencode serve --port "$port" --hostname 127.0.0.1 ) &
  pids+=("$!")
done <<< "$slots"

echo "waiting for opencode to warm up…"
sleep 2

echo "starting proxy…"
exec cargo run -- serve --config "$CONFIG"
