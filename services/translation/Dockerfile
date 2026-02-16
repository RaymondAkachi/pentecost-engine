FROM python:3.10-slim

WORKDIR /app

# 1. Install system deps AND 'patchelf' (Modern replacement for execstack)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    patchelf \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 👇 THE FIX: Find the offending library files and clear the execstack flag using patchelf
# We explicitly target the ctranslate2 .so files
RUN find /usr/local/lib/python3.10/site-packages/ctranslate2 -name "*.so*" -exec patchelf --clear-execstack {} \;

# Install converter deps
RUN pip install torch transformers[sentencepiece]

COPY download_model.py .
# Now this should run without the ImportError
RUN python download_model.py

COPY src/ ./src/
ENV PYTHONPATH=/app

CMD ["python", "-m", "src.main"]