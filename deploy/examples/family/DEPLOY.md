# Family deployment (Mac mini + Colima + Docker + Dockge)

A worked example of running `telegram-opencode-proxy` and one `opencode` server
per family member, as a single Docker Compose stack, on a headless always-on Mac
mini (`agent.lan`). Colima provides the (headless) container engine; Dockge is a
browser UI for managing it.

> This is an **example to adapt**, not a turnkey product. Names (`alice`, `bob`),
> the LAN model URL (`llm.lan:8080`), IPs, and resource caps are placeholders.

## Why this shape

- **opencode runs in containers**, each with its own bind-mounted workspace and a
  **fixed port** — the proxy never has to guess a port, and a restarted opencode
  comes back on the same one.
- **The agent runs as root inside its container**, which is safe because the whole
  engine runs inside Colima's **Linux VM** — that VM is the isolation boundary
  from macOS.
- **Files move over HTTP (MCP)**, so containers don't need a shared filesystem —
  except the proxy's legacy outbox watcher, which is why each workspace is also
  mounted into the proxy (below).

## Two constraints baked into this example

1. **The proxy needs a pinned IP.** The MCP file-download URL handed to opencode
   is `http://<[mcp].bind>:<port>/files/<id>`, and `bind` must be an IP the
   opencode containers can reach. So the proxy gets a static IP on the `family`
   network (`172.28.0.10`), and `config.toml`'s `[mcp].bind` matches it. Keep the
   two in lockstep. (Proxy→opencode uses service DNS and needs no pin.)
2. **Workspaces are mounted into the proxy too.** The outbox watcher reads each
   slot's `workdir` by local path, so every workspace bind-mount appears in both
   the opencode container and the proxy at the same path.

```
Colima Linux VM
└── docker network "family" 172.28.0.0/24
    ├── proxy            172.28.0.10   TELOXIDE_TOKEN, /data (db), config.toml
    ├── opencode-alice   :4096         ws/alice ⇄ /workspaces/alice
    ├── opencode-bob     :4097         ws/bob   ⇄ /workspaces/bob
    └── mcp-example      :8000         optional shared stateless HTTP MCP
```

## Files

| File | Purpose |
|---|---|
| `compose.yaml` | the family stack (proxy + opencode slots + example MCP) |
| `config.toml` | proxy config, container paths, pinned `[mcp].bind` |
| `opencode-config/*.opencode.json` | per-slot provider + MCP config (`X-Slot`, stdio + HTTP MCP) |
| `proxy/Dockerfile` | multi-stage build of the proxy (context = repo root) |
| `opencode/Dockerfile` | opencode server image + agent tooling |
| `.env.example` | tokens, keys, `WS_ROOT` |
| `dockge/compose.yaml` | the Dockge web UI (run as its own stack) |
| `launchd/net.theuseless.colima.plist` | start Colima at boot |

## Prerequisites (on the Mac mini)

```sh
brew install colima docker docker-compose

# Never sleep (it already doesn't, but make it explicit + survive setting drift):
sudo pmset -a sleep 0 disablesleep 1

# Start the engine now, and enable it at boot:
colima start --cpu 4 --memory 8 --disk 60
cp launchd/net.theuseless.colima.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/net.theuseless.colima.plist
# On a headless mini, enable auto-login so the LaunchAgent runs after a reboot.
```

Add a **root `.dockerignore`** so the proxy build context (the whole repo) stays
small:

```sh
printf 'target/\n.git/\n' > ../../../.dockerignore   # from this directory
```

## Configure

```sh
cp .env.example .env
$EDITOR .env                       # TELOXIDE_TOKEN, LLM_LAN_KEY, WS_ROOT

mkdir -p "$(grep WS_ROOT .env | cut -d= -f2)"/{alice,bob}

$EDITOR config.toml                # slot names, telegram ids (optional), model ids
$EDITOR opencode-config/alice.opencode.json   # baseURL, model, X-Slot
$EDITOR opencode-config/bob.opencode.json
```

Rules that must line up:
- `[mcp].bind` in `config.toml` **==** the proxy's `ipv4_address` in `compose.yaml`.
- Each opencode.json `X-Slot` header **==** the slot `name` in `config.toml`
  (case-sensitive).
- `[model]` in `config.toml` **==** the provider/model keys in opencode.json.
- The `filesystem` stdio MCP path **==** that slot's `workdir`.

## Run

```sh
docker compose up -d --build
docker compose logs -f proxy         # look for: "advertised bot commands ... count=5"
```

Bring up the web UI (separately):

```sh
docker compose -f dockge/compose.yaml up -d
# open http://agent.lan:5001  → adopt the "family" stack
```

## Enrol users (pairing)

Each user messages the bot once, gets a 6-digit code, then you approve it:

```sh
docker exec -it family-proxy proxy pair list
docker exec -it family-proxy proxy pair approve 638942 --slot alice
docker exec -it family-proxy proxy status
```

(Or preset `telegram_id` per slot in `config.toml` to skip pairing.)

## Adding MCP servers later

- **Per-agent / sensitive** (filesystem, git, secrets) → **stdio**, like the
  `filesystem` entry in each opencode.json. Each agent gets its own instance,
  auto-scoped to its container and workspace. No network exposure.
- **Shared / stateless** (web fetch, a knowledge base) → an **HTTP container** on
  the `family` network, like `mcp-example`; point slots at `http://<service>:<port>/mcp`.
  Safe to share **only** if stateless.

> Shared HTTP MCP servers have **no per-tenant auth** (same loopback-trust model
> as the proxy). Never point one shared server at user-scoped data — give
> stateful tools a per-slot instance or run them stdio. If you outgrow this
> (untrusted/many users, shared sensitive tools, central audit), put an MCP
> gateway (e.g. agentgateway) in front — it's a URL swap in opencode.json, not a
> re-architecture.

## "Just enough" storage

`compose.yaml` sets `mem_limit` and `pids_limit` per opencode. Disk quotas are
the fiddly part on Colima's overlay — the pragmatic options:
- give `WS_ROOT` its own volume/partition and watch it, or
- back each workspace with a fixed-size disk image, or
- Colima with an xfs data disk + project quotas.

## Operating

```sh
docker compose logs -f proxy opencode-alice     # tail logs (or use Dockge)
docker compose restart opencode-alice           # bounce one slot
docker compose pull && docker compose up -d --build   # update
docker exec -it family-opencode-alice bash      # shell into an agent's box
```

State that survives everything: the `proxy-db` volume (routing, whitelist,
pairings) and the external `WS_ROOT` workspaces. Back those up.
