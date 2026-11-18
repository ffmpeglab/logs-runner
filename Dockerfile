# syntax=docker/dockerfile:1

# ---- build stage ----
FROM rust:1.99-slim-bookworm AS builder
WORKDIR /app

# Cache dependencies: build a dummy binary first so this layer is only
# invalidated when Cargo.toml / Cargo.lock change.
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src target/release/logs-processor target/release/deps/logs_processor*

# Build the real thing
COPY src ./src
RUN cargo build --release

# ---- runtime stage ----
# distroless/cc: glibc + libgcc + CA certs, no shell, runs as non-root.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/target/release/logs-processor /usr/local/bin/logs-processor

ENV RUST_LOG=info
USER nonroot
ENTRYPOINT ["/usr/local/bin/logs-processor"]