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

ENTRYPOINT ["opencode"]
