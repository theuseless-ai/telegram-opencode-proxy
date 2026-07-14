# Multi-stage build of the telegram-opencode-proxy.
# Build context is the REPO ROOT (see compose.yaml). Add a root .dockerignore
# (at least `target/`) so the build context stays small — see DEPLOY.md.

# ---- build: static musl binary on the Alpine/musl toolchain ----
FROM rust:1-alpine AS build
# build-base + musl-dev: C toolchain for rusqlite's bundled SQLite.
# openssl-dev + openssl-libs-static: static OpenSSL for reqwest/teloxide native-tls
# (OPENSSL_STATIC links it in, so the runtime image needs no libssl).
RUN apk add --no-cache build-base musl-dev openssl-dev openssl-libs-static pkgconfig
ENV OPENSSL_STATIC=1
WORKDIR /src
COPY . .
RUN cargo build --release --bin telegram-opencode-proxy

# ---- runtime: Alpine (ca-certificates for outbound TLS to Telegram / the LLM) ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /src/target/release/telegram-opencode-proxy /usr/local/bin/telegram-opencode-proxy
# Ergonomic alias so admin commands can be typed as `proxy …`.
RUN ln -s /usr/local/bin/telegram-opencode-proxy /usr/local/bin/proxy \
    && mkdir -p /run/topx
ENTRYPOINT ["telegram-opencode-proxy"]
