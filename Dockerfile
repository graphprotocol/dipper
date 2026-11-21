## Rust builder
# Compile the Rust code
FROM rust:1.94.0-slim-bookworm AS rust-builder

RUN --mount=type=cache,id=apt-builder,target=/var/cache/apt,sharing=locked \
  apt-get update \
  && apt-get install -y --no-install-recommends \
      build-essential \
      clang \
      cmake \
      git \
      lld \
      pkg-config \
      libssl-dev \
      protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY ./ ./

# Set build environment variables
# - Set the C/C++ compiler to clang
ENV CC=clang CXX=clang++
# - Set the Rust flags to use lld as the linker
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"

RUN cargo build --bin dipper-service --release

## Final image
FROM debian:bookworm-slim

RUN --mount=type=cache,id=apt-runtime,target=/var/cache/apt,sharing=locked \
  apt-get update \
  && apt-get install -y --no-install-recommends \
      ca-certificates \
      libssl3 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install the dipper-service binary
COPY --from=rust-builder /src/target/release/dipper-service /usr/local/bin/dipper-service

ENTRYPOINT [ "dipper-service" ]
