# Build stage
FROM rust:1.83-slim as builder

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml ./
COPY crates ./crates

# Build the application
RUN cargo build --release --bin pmp-mq-server

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/pmp-mq-server /usr/local/bin/pmp-mq-server

# Set the startup command
CMD ["pmp-mq-server"]
