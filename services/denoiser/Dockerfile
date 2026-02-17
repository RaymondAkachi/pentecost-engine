# services/denoiser/Dockerfile
FROM python:3.10-slim

# 1. Install System Dependencies & Build Tools
# FIX: Added 'wget' (critical for download_model.sh) and 'pkg-config' (critical for autogen)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    autoconf \
    automake \
    libtool \
    ffmpeg \
    wget \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Compile RNNoise from Source (The Robust Way)
# We clone, download the model (now works because we have wget), and compile.
RUN git clone https://github.com/xiph/rnnoise.git && \
    cd rnnoise && \
    ./autogen.sh && \
    ./configure && \
    make && \
    # Move the compiled library to the standard location
    cp .libs/librnnoise.so /usr/lib/librnnoise.so

# 3. Install Python Dependencies
# We use numpy to manage buffers and nats-py for messaging
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    nats-py==2.6.0

# 4. Copy Code
COPY . .

CMD ["python", "-u", "main.py"]