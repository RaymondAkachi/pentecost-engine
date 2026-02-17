# --- STAGE 1: BUILDER (Python 3.7 + TF 2.3) ---
# We use this specific version to match the era of the DTLN research code (2020).
FROM python:3.7-slim AS builder

# 1. Install System Dependencies (libsndfile for audio I/O)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# 2. Clone the DTLN Repository
RUN git clone https://github.com/breizhn/DTLN.git .

# 3. Install Compatible Dependencies
# We pin h5py<3.0.0 because TF 2.3 crashes with newer versions.
# We quote "h5py<3.0.0" to prevent the shell from breaking
RUN pip install --no-cache-dir \
    tensorflow==2.3.0 \
    keras2onnx \
    onnx \
    soundfile \
    wavinfo \
    scipy \
    "h5py<3.0.0"

# 4. Download Weights to the expected folder
RUN mkdir -p pretrained_model && \
    curl -fL -A "Mozilla/5.0" \
    https://github.com/breizhn/DTLN/raw/master/pretrained_model/DTLN_norm_500h.h5 \
    -o pretrained_model/DTLN_norm_500h.h5

# 5. Run the Conversion Script (FIXED)
# FIX 1: We explicitly pass '-m' to tell the script which file to convert.
# FIX 2: We specify the exact path we downloaded to.
RUN python convert_weights_to_onnx.py -m pretrained_model/DTLN_norm_500h.h5


# --- STAGE 2: RUNTIME (Modern Python 3.10) ---
FROM python:3.10-slim

# 1. Runtime System Deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Runtime Python Deps (Modern & Fast)
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    onnxruntime==1.17.1 \
    nats-py==2.6.0 \
    resampy==0.4.3 \
    scipy==1.12.0

# 3. Copy the Converted Model
# The script saves the .onnx file in the same folder as the input .h5 file.
COPY --from=builder /build/pretrained_model/DTLN_norm_500h.onnx /app/model.onnx

# 4. Copy Service Code
COPY . .

CMD ["python", "-u", "main.py"]