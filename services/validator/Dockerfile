# Use a slim Python image to keep it small
FROM python:3.10-slim

# 1. BUILD PHASE: Install System Dependencies
# We install ffmpeg here so it's ready forever.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# 2. BUILD PHASE: Install Python Libraries
# We install these now so we don't wait for them at runtime.
RUN pip install --no-cache-dir nats-py numpy scipy

WORKDIR /app

# 3. RUN PHASE: Execute the script
# No installation commands here. Just run.
CMD ["python", "-u", "validator.py"]