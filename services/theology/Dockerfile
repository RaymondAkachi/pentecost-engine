FROM python:3.10-slim

WORKDIR /app

# 1. Install System Deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python Deps (Cached Layer)
COPY requirements.txt .
# Force CPU torch to save space/time
RUN pip install --no-cache-dir -r requirements.txt

# 3. Pre-download Embedding Model
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# 4. Copy Application Code
COPY glossary.json .
COPY src/ ./src/

# 5. Set Python Path
ENV PYTHONPATH=/app

# 👇 FIXED: Run as a module (-m) to support relative imports
CMD ["python", "-m", "src.main"]