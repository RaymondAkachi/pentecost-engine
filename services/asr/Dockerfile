# services/asr/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# 1. Install System Dependencies
# We keep 'ffmpeg' and 'libsndfile1' as they are runtime requirements for Audio/AV.
# We REMOVE the build tools (build-essential, pkg-config) because we are now
# using pre-compiled binary wheels.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python Deps
COPY requirements.txt .

# FIX: Upgrade pip first to ensure it can detect the modern manylinux wheels.
# We strictly install from requirements.txt, which now contains the
# compatible version of faster-whisper.
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 3. Create Model Directory
RUN mkdir -p /models/whisper-large-v3-int8

# 4. Copy Code
COPY . .

# 5. Runtime
CMD ["python", "-u", "main.py"]