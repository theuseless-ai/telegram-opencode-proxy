# Multi-stage build of the telegram-opencode-proxy.
# Build context is the REPO ROOT (see compose.yaml). Add a root .dockerignore
# (at least `target/`) so the build context stays small — see DEPLOY.md.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --bin telegram-opencode-proxy

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/telegram-opencode-proxy /usr/local/bin/telegram-opencode-proxy
# Ergonomic alias so admin commands can be typed as `proxy …`.
RUN ln -s /usr/local/bin/telegram-opencode-proxy /usr/local/bin/proxy \
    && mkdir -p /run/topx
ENTRYPOINT ["telegram-opencode-proxy"]
