# Use the official uv image to grab the binary
FROM ghcr.io/astral-sh/uv:latest AS uv_bin

FROM nvidia/cuda:12.1-devel-ubuntu22.04 AS python-base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3.10 python3-pip \
    ffmpeg libsndfile1 sox \
    git git-lfs wget curl \
    && rm -rf /var/lib/apt/lists/*

# Copy uv binary from the first stage
COPY --from=uv_bin /uv /uvx /bin/

# uv configuration
# Tells uv to use the system python (good for Docker)
# Prevents issues with hardlinks in some container runtimes

ENV UV_SYSTEM_PYTHON=1  
ENV UV_LINK_MODE=copy   

# --- Go base image ---
FROM golang:1.21-alpine AS go-base
RUN apk add --no-cache git ffmpeg-dev