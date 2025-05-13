FROM rust:slim-bullseye AS builder

WORKDIR /app

# Copy dependency manifests first for caching
COPY Cargo.toml Cargo.lock ./

# Build dependencies - allows caching if only src changes
# Use dummy main.rs to force dependency build if no src copied yet
RUN mkdir -p crates/yawns/src && echo "fn main() {}" > crates/yawns/src/main.rs
# Add --target if cross-compiling, e.g., x86_64-unknown-linux-gnu
RUN cargo build --release --frozen --locked --offline || true
# Remove dummy src
RUN rm -rf crates/

# Copy source code
COPY crates/ crates/
COPY xtask/ xtask/
COPY .cargo/ .cargo/

# Build application
RUN cargo build --release --bin yawns

# --- Final Stage ---
FROM debian:bullseye-slim

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/yawns ./yawns

RUN useradd -m appuser
USER appuser

# Run the application
ENTRYPOINT ["./yawns"]

