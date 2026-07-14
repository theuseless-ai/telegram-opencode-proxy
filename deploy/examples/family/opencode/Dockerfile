# opencode "slot" image: an opencode server plus the CLI tooling the agent uses.
#
# The agent runs as root INSIDE this container. That's acceptable here because
# the whole thing runs inside the Colima Linux VM — the VM is the real isolation
# boundary between the agent and your macOS host. Give the agent only what it
# needs, and keep each slot's workspace on its own bind-mount.

# Debian 12 "bookworm-slim" (glibc, full binary compatibility — NOT alpine/musl,
# which the prebuilt opencode/Bun binary and arbitrary agent tooling may not run).
FROM node:22-bookworm-slim

# Agent tooling — extend to taste (the agent has root and can install more).
# `unzip` is needed by the opencode installer and is NOT in bookworm-slim (it is
# in the full bookworm image); the rest mirror the tools the agent expects.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       git curl ca-certificates ripgrep jq less unzip \
    && rm -rf /var/lib/apt/lists/*

# opencode. Pin a version in production. Adjust to your preferred install method
# (this uses the official installer; it lands in /root/.opencode/bin).
RUN curl -fsSL https://opencode.ai/install | bash
ENV PATH="/root/.opencode/bin:${PATH}"

# Pre-cache the stdio MCP server(s) referenced by opencode.json so first use is
# fast (the agent can still `npx` others on demand).
RUN npm i -g @modelcontextprotocol/server-filesystem

# --- obscura: lightweight headless browser exposing browser tools via MCP ---
# obscura (h4ckf0r0day/obscura) is a Rust headless browser: a ~77MB static-ish
# binary, ~30MB RAM, real JS via V8, that ships a built-in MCP server. opencode
# launches `obscura mcp` as a stdio MCP (wired in opencode.json), so the agent gets
# browser_navigate/click/fill/evaluate/… tools with NO Chromium and NO separate
# browser service — a fraction of chromium's ~1.1GB. Pinned; arm64 + amd64 builds.
ARG OBSCURA_VERSION=v0.1.10
RUN arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      arm64) tgt=aarch64-linux ;; \
      amd64) tgt=x86_64-linux ;; \
      *) echo "unsupported arch: $arch" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/obscura.tgz \
      "https://github.com/h4ckf0r0day/obscura/releases/download/${OBSCURA_VERSION}/obscura-${tgt}.tar.gz" \
    && mkdir -p /tmp/ob && tar xzf /tmp/obscura.tgz -C /tmp/ob \
    && install -m 0755 "$(find /tmp/ob -name obscura -type f | head -1)" /usr/local/bin/obscura \
    && rm -rf /tmp/ob /tmp/obscura.tgz \
    && obscura --version
# Let the browser reach LAN/private services (searxng, metube, llm.lan). obscura
# blocks RFC1918/loopback by default (SSRF protection); opt in here per this deploy.
ENV OBSCURA_ALLOW_PRIVATE_NETWORK=1

ENTRYPOINT ["opencode"]
