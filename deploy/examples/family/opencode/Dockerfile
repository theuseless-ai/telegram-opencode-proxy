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

# --- agent-browser: a CLI the agent drives from its shell to scrape the web ---
# vercel-labs/agent-browser is a Rust CLI+daemon that drives Chrome via CDP on
# demand (no MCP server — the agent just runs `agent-browser …`, guided by the
# baked SKILL.md below). This host is ARM64, where Chrome-for-Testing has no build,
# so `agent-browser install` cannot fetch a browser; use the distro `chromium`
# package instead (it pulls in every headless dep) and point the CLI at it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends chromium fonts-liberation \
    && rm -rf /var/lib/apt/lists/*
RUN npm i -g agent-browser
# Point agent-browser at the distro Chromium, and pass the flags a root-in-container
# Chrome needs (--no-sandbox; --disable-dev-shm-usage avoids /dev/shm crashes). Baked
# as env so the agent never has to remember them.
ENV AGENT_BROWSER_EXECUTABLE_PATH=/usr/bin/chromium \
    AGENT_BROWSER_ARGS=--no-sandbox,--disable-dev-shm-usage

# --- preloaded opencode skills (deterministic; baked, not on the session volume) ---
# opencode auto-discovers <name>/SKILL.md here at session start (native Agent Skills,
# opencode >= v1.0.186). Claude-Code-format skills work unmodified.
COPY skills/ /root/.config/opencode/skills/

ENTRYPOINT ["opencode"]
