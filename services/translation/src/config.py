from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Dict

class Settings(BaseSettings):
    # NATS
    NATS_URL: str = Field(default="nats://nats:4222")
    INPUT_SUBJECT: str = "livestream.transcription.enriched"
    OUTPUT_SUBJECT_PREFIX: str = "livestream.translation"
    
    # Model Config
    MODEL_PATH: str = "/app/model"
    
    # 👇 OPTIMIZED: Only 2 languages for Dev/Test speed (Cuts latency by 60%)
    TARGET_LANGUAGES: Dict[str, str] = {
        "spanish": "spa_Latn",
        "french": "fra_Latn",
        # "swahili": "swh_Latn",  <-- Commented out for speed
        # "portuguese": "por_Latn",
        # "german": "deu_Latn"
    }
    
    # Performance
    MAX_WORKERS: int = 1 # Keep at 1 to prevent CPU thrashing
    BEAM_SIZE: int = 1

    class Config:
        env_prefix = "TRANS_"

settings = Settings()