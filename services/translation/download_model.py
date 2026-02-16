import ctranslate2
import os
import shutil
from huggingface_hub import hf_hub_download

MODEL_NAME = "facebook/nllb-200-distilled-600M"
OUTPUT_DIR = "model"

print(f"⬇️  Downloading and Converting {MODEL_NAME}...")

# 1. Convert Model to CTranslate2 format (Int8 Quantization)
converter = ctranslate2.converters.TransformersConverter(MODEL_NAME)
converter.convert(OUTPUT_DIR, quantization="int8", force=True)
print("✅ Model Weights Converted.")

# 2. Download Raw SentencePiece Model
# We grab the file directly to ensure it exists for the C++ engine
print("⬇️  Downloading Tokenizer Binary...")
try:
    sp_source_path = hf_hub_download(repo_id=MODEL_NAME, filename="sentencepiece.bpe.model")
    
    # Copy it into the model folder so it gets baked into the Docker image
    sp_dest_path = os.path.join(OUTPUT_DIR, "sentencepiece.bpe.model")
    shutil.copy(sp_source_path, sp_dest_path)
    
    print(f"✅ Tokenizer Binary Saved to: {sp_dest_path}")
    
except Exception as e:
    print(f"❌ Error downloading tokenizer: {e}")
    exit(1)