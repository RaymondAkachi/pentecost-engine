# Use a Python base image
FROM python:3.10-slim

# 1. System Deps (FFmpeg required for audio processing)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Install Python Deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy Code
COPY main.py .

# 4. The Adapter folder will be mounted by Docker Compose
# We just create the mount point here
RUN mkdir -p /app/adapter

# 5. Run
CMD ["python", "-u", "main.py"]