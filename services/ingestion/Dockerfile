# --- STAGE 1: BUILDER (Compiling the Go Binary) ---
FROM golang:1.23-alpine AS builder

# Install git (required for fetching Go dependencies)
RUN apk add --no-cache git

WORKDIR /app

# 1. Cache Dependencies
# We copy only the dependency files first to leverage Docker caching.
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy Source Code
COPY . .

# FIX: Run 'go mod tidy' to sync dependencies with the actual code
# This fixes the "updates to go.mod needed" error by cleaning up the module file.
RUN go mod tidy

# 3. Build the Production Binary
# -ldflags "-s -w": Strips debug info to reduce binary size
# -o ingestion-service: Names the output binary
# We explicitly build 'main.go', so 'integration_test.go' is ignored.
RUN go build -ldflags "-s -w" -o ingestion-service main.go

# --- STAGE 2: RUNTIME (The Actual Production Image) ---
FROM alpine:latest

# 1. Install Runtime Dependencies
# ffmpeg: Core video processing
# python3: Required by yt-dlp
# curl: To download yt-dlp
# ca-certificates: For HTTPS (YouTube/NATS)
RUN apk update && apk add --no-cache \
    ffmpeg \
    python3 \
    curl \
    ca-certificates \
    bash

# 2. Install yt-dlp (Latest Release)
# Essential for resolving dynamic YouTube Live URLs
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod a+rx /usr/local/bin/yt-dlp

WORKDIR /app

# 3. Copy ONLY the compiled binary from the Builder stage
# This discards all source code, tests, and Go tools, leaving a tiny image.
COPY --from=builder /app/ingestion-service .

# 4. Expose internal ports (used by FFmpeg loopback)
EXPOSE 4000 4001

# Shared volume for chunks
RUN mkdir -p /shared/chunks

# 5. Production Entrypoint
CMD ["./ingestion-service"]