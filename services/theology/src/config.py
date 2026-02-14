import os
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # NATS
    NATS_URL: str = Field(default="nats://nats:4222")
    INPUT_SUBJECT: str = "livestream.transcription.raw"
    OUTPUT_SUBJECT: str = "livestream.transcription.enriched"
    
    # Tuning
    MAX_WORKERS: int = Field(default=4, description="Thread pool size for CPU tasks")
    SIMILARITY_THRESHOLD: float = Field(default=1.1, description="Strictness of vector match")
    
    # Paths
    GLOSSARY_PATH: str = Field(default="glossary.json")

    class Config:
        env_prefix = "RAG_"

settings = Settings()