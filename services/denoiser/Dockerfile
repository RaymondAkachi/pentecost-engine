# Use 'bookworm' for better stability
FROM python:3.10-bookworm

# Set timeout for apt-get
RUN echo 'Acquire::http::Timeout "30";' > /etc/apt/apt.conf.d/99timeout

# 1. Update first
RUN apt-get update

# 2. Install dependencies (Added 'git' here)
RUN apt-get install -y --no-install-recommends \
    git \
    libsndfile1 \
    ffmpeg \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 3. Install Python Dependencies
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    torch==2.1.2 \
    torchaudio==2.1.2 \
    nats-py==2.6.0 \
    deepfilternet==0.5.6

# 4. Cache the Model (This will now pass because 'git' exists)
RUN python -c "from df.enhance import init_df; init_df()"

COPY . .

CMD ["python", "-u", "main.py"]