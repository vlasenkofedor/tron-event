# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
# The node is compiled for the musl target selected from the build platform
# (`TARGETARCH` is set automatically by BuildKit). The result is a fully static
# binary: no dynamic loader, no libc, no OpenSSL. That lets the runtime stage
# be `scratch`, so the image carries only that binary plus an empty /data dir.
#
# `musl-tools` provides the linker wrapper required by the musl target. The
# `rust:*-slim-bookworm` image already contains the Rust toolchain, and the
# protobuf compiler comes from the `protoc-bin-vendored` build dependency, so
# nothing else is needed.
FROM rust:1.99-slim-bookworm AS builder

ARG TARGETARCH=amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) triple=x86_64-unknown-linux-musl ;; \
         arm64) triple=aarch64-unknown-linux-musl ;; \
         *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && rustup target add "${triple}" \
    && printf '%s' "${triple}" > /target-triple

WORKDIR /build

# Only the workspace manifest and the node crate are needed. Examples,
# scripts and runtime state are excluded by .dockerignore.
COPY Cargo.toml Cargo.lock ./
COPY rust-node ./rust-node

# `--locked` guarantees that the dependency versions from Cargo.lock are used,
# so the image is reproducible. The release profile (fat LTO, one codegen unit,
# stripped symbols) is inherited from the workspace Cargo.toml.
RUN cargo build --release --locked --target "$(cat /target-triple)" -p rust-node \
    && mkdir -p /build/out \
    && cp "target/$(cat /target-triple)/release/rust-node" /build/out/rust-node

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
# `scratch` is an empty filesystem: the final image is the static binary plus
# a few KB of Docker metadata and the empty /data directory. There is no shell,
# no package manager and no libc, which also minimises the attack surface.
FROM scratch

# Runtime state (`height.txt`, `transactions.log`) is written to the working
# directory. Mount a volume here to keep the chain position across restarts.
WORKDIR /data

COPY --from=builder /build/out/rust-node /usr/local/bin/rust-node

# 18888 = Tron P2P (hard-coded in the node), 50051 = gRPC subscription API.
EXPOSE 18888 50051

# The binary reads its configuration from environment variables (NETWORK,
# EXTERNAL_IP, GRPC_LISTEN_ADDR, SEED_NODES, ...) and accepts `--solid-depth`.
ENTRYPOINT ["/usr/local/bin/rust-node"]
