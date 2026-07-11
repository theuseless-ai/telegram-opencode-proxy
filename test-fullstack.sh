#!/usr/bin/env bash
# test-fullstack.sh — LOCAL-ONLY full-stack harness (issue #24, Layer 2).
#
# NOT run by CI. Requires the real `opencode` binary on PATH. It stands up the
# full model stack and asserts a deterministic reply flows through a REAL
# `opencode serve`:
#
#   mock_model (OpenAI-compatible)  ◄──baseURL──  opencode serve  ◄──V1 wire──  this script
#
# Steps:
#   1. start `mock_model` (examples/mock_model.rs) on a free port;
#   2. write a temp `opencode.json` whose provider `baseURL` points at it;
#   3. start a REAL `opencode serve` in a temp workdir against that config;
#   4. wait for readiness (`GET /config`);
#   5. assert `/config/providers` advertises the mock model;
#   6. create a session and POST a blocking message — exactly the endpoints the
#      proxy's `OpencodeClient` calls — and assert the canned reply comes back.
#
# The Telegram half is already covered hermetically by `cargo test` (Layer 1,
# tests/harness.rs). Driving the actual proxy binary here as well would need a
# *runnable* mock_telegram with a control API (inject-update / read-sent); that
# is left as the TODO skeleton at the bottom of this file.
#
# Usage: ./test-fullstack.sh
set -euo pipefail

# ---------------------------------------------------------------------------
# Preconditions (local-only).
# ---------------------------------------------------------------------------
command -v opencode >/dev/null 2>&1 || {
  echo "SKIP: 'opencode' binary not found on PATH — this is a LOCAL-ONLY harness." >&2
  echo "      Install opencode to run the full-stack test; CI relies on 'cargo test'." >&2
  exit 0
}
command -v curl >/dev/null 2>&1 || { echo "need curl" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "need python3 (for JSON parsing)" >&2; exit 1; }

MODEL_PORT=8088
OC_PORT=4099
MODEL_URL="http://127.0.0.1:${MODEL_PORT}"
OC_URL="http://127.0.0.1:${OC_PORT}"
PROVIDER_ID="mock-lan"
MODEL_ID="mock-model"

WORKDIR="$(mktemp -d)"
DATADIR="$(mktemp -d)"
pids=()

cleanup() {
  echo
  echo "cleaning up…"
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  rm -rf "$WORKDIR" "$DATADIR"
}
trap cleanup EXIT INT TERM

fail() { echo "FAIL: $*" >&2; exit 1; }

wait_for() { # url attempts
  local url="$1" attempts="${2:-60}"
  for _ in $(seq 1 "$attempts"); do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  return 1
}

# ---------------------------------------------------------------------------
# 1. mock_model
# ---------------------------------------------------------------------------
echo "==> building + starting mock_model on :${MODEL_PORT}"
cargo build --example mock_model
cargo run --quiet --example mock_model -- "127.0.0.1:${MODEL_PORT}" &
pids+=("$!")
wait_for "${MODEL_URL}/v1/models" 40 || fail "mock_model never became ready"
echo "    mock_model ready"

# ---------------------------------------------------------------------------
# 2. opencode.json pointing at the mock model
# ---------------------------------------------------------------------------
echo "==> writing opencode.json (provider '${PROVIDER_ID}' -> ${MODEL_URL})"
cat >"${WORKDIR}/opencode.json" <<JSON
{
  "\$schema": "https://opencode.ai/config.json",
  "provider": {
    "${PROVIDER_ID}": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Mock LAN (local test)",
      "options": { "baseURL": "${MODEL_URL}/v1" },
      "models": { "${MODEL_ID}": { "name": "Mock Model" } }
    }
  }
}
JSON

# ---------------------------------------------------------------------------
# 3. real opencode serve
# ---------------------------------------------------------------------------
echo "==> starting real 'opencode serve' on :${OC_PORT} (workdir ${WORKDIR})"
( cd "${WORKDIR}" && XDG_DATA_HOME="${DATADIR}" exec opencode serve \
    --port "${OC_PORT}" --hostname 127.0.0.1 ) &
pids+=("$!")

# ---------------------------------------------------------------------------
# 4. readiness
# ---------------------------------------------------------------------------
echo "==> waiting for opencode readiness (${OC_URL}/config)"
wait_for "${OC_URL}/config" 120 || fail "opencode never became ready"
echo "    opencode ready"

# ---------------------------------------------------------------------------
# 5. provider catalogue advertises the mock model
# ---------------------------------------------------------------------------
echo "==> asserting /config/providers advertises ${PROVIDER_ID}/${MODEL_ID}"
PROVIDERS="$(curl -fsS "${OC_URL}/config/providers")"
echo "${PROVIDERS}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
provs = { p['id']: p for p in d.get('providers', []) }
p = provs.get('${PROVIDER_ID}') or sys.exit('provider ${PROVIDER_ID} missing; got ' + ','.join(provs))
sys.exit(0 if '${MODEL_ID}' in p.get('models', {}) else 'model ${MODEL_ID} missing under provider')
" || fail "provider/model not advertised by opencode"
echo "    catalogue OK"

# ---------------------------------------------------------------------------
# 6. session + blocking prompt → canned reply (the proxy's exact V1 wire)
# ---------------------------------------------------------------------------
echo "==> POST /session"
SESSION="$(curl -fsS -X POST "${OC_URL}/session" \
  -H 'content-type: application/json' \
  -d "{\"model\":{\"id\":\"${MODEL_ID}\",\"providerID\":\"${PROVIDER_ID}\"}}")"
SID="$(echo "${SESSION}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
[ -n "${SID}" ] || fail "no session id returned"
echo "    session ${SID}"

echo "==> POST /session/${SID}/message (blocking)"
REPLY="$(curl -fsS -X POST "${OC_URL}/session/${SID}/message" \
  -H 'content-type: application/json' \
  -d "{\"model\":{\"providerID\":\"${PROVIDER_ID}\",\"modelID\":\"${MODEL_ID}\"},\"parts\":[{\"type\":\"text\",\"text\":\"ping\"}]}")"
TEXT="$(echo "${REPLY}" | python3 -c '
import json, sys
m = json.load(sys.stdin)
print("".join(p.get("text","") for p in m.get("parts",[]) if p.get("type")=="text"))
')"
echo "    assistant text: ${TEXT}"
case "${TEXT}" in
  *"PONG from mock_model"*) echo "    reply OK" ;;
  *) fail "unexpected assistant reply: '${TEXT}'" ;;
esac

echo
echo "PASS: mock_model <-> real opencode <-> V1 wire verified."
echo
echo "TODO(Layer 2, #24): also drive the proxy binary end-to-end here. That"
echo "needs a *runnable* mock_telegram (an example binary exposing a control API:"
echo "  POST /control/inject-update   and   GET /control/sent"
echo "wrapping tests/support/mock_telegram.rs), then:"
echo "  TELOXIDE_TOKEN=test cargo run -- serve --config <tmp with opencode_url=${OC_URL}>"
echo "with the bot's api_url set to the mock, inject a text update, and assert the"
echo "recorded sendMessage carries '${TEXT}'. Skeleton only for now — the Telegram"
echo "path is already proven hermetically by 'cargo test' (tests/harness.rs)."
