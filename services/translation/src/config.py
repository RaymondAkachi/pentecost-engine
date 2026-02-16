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
    
    # 👇 UPDATED: The Global 5 Language Stack
    TARGET_LANGUAGES: Dict[str, str] = {
        "spanish": "spa_Latn",    # Top 2 Global
        "french": "fra_Latn",     # Top 3 Global / Africa
        "swahili": "swh_Latn",    # East Africa (Kept)
        "portuguese": "por_Latn", # Brazil / Angola / Mozambique
        "german": "deu_Latn"      # Central Europe
    }
    
    # Performance
    MAX_WORKERS: int = 4
    BEAM_SIZE: int = 1

    class Config:
        env_prefix = "TRANS_"

settings = Settings()