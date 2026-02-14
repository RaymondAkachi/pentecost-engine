import os

os.environ["ANONYMIZED_TELEMETRY"] = "False"
os.environ["CHROMA_SERVER_NOINTERACTIVE"] = "1"

import json
import time
import re
import structlog
import logging



# --- CHROMA DB SQLITE FIX ---
# Chroma requires sqlite3 >= 3.35. Standard Python images often fail this.
# We swap the system sqlite for the binary we installed.
__import__('pysqlite3')
import sys
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
# ----------------------------

import chromadb
from chromadb.config import Settings as ChromaSettings
from sentence_transformers import SentenceTransformer
from typing import Tuple, List, Optional
from .config import settings

logger = structlog.get_logger()

class TheologicalEngine:
    def __init__(self):
        self.log = logger.bind(component="engine")
        self.log.info("initializing_knowledge_base")
        
        # 1. Vector DB (In-Memory)
        self.chroma = chromadb.Client(ChromaSettings(anonymized_telemetry=False))
        try:
            self.chroma.delete_collection("apostolic_terms") # Cleanup restart
        except: pass
        self.collection = self.chroma.create_collection(name="apostolic_terms")
        
        # 2. Embedding Model
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
        
        # 3. Lookup Tables & Compiled Regex
        self.regex_map = [] # List of (Pattern, Replacement)
        self._ingest_glossary()

    def _ingest_glossary(self):
        try:
            with open(settings.GLOSSARY_PATH, "r") as f:
                terms = json.load(f)
            
            ids, documents, metadatas = [], [], []
            
            for item in terms:
                term = item["term"]
                definition = item["definition"]
                
                # A. Build Regex Correction Map (Word Boundary Safe)
                # Matches "coin on here" but not "bitcoin on here"
                for bad in item.get("misspellings", []):
                    # Escape the bad term, then wrap in \b boundaries
                    pattern = re.compile(r'\b' + re.escape(bad) + r'\b', re.IGNORECASE)
                    self.regex_map.append((pattern, term))
                
                # B. Prepare Vector Data
                ids.append(term)
                documents.append(f"{term}: {definition}")
                metadatas.append({"term": term})
            
            # C. Batch Upsert
            if ids:
                embeddings = self.embedder.encode(documents).tolist()
                self.collection.add(
                    ids=ids, 
                    documents=documents, 
                    embeddings=embeddings, 
                    metadatas=metadatas
                )
            
            self.log.info("glossary_ingested", terms=len(ids))
            
        except Exception as e:
            self.log.error("glossary_load_failed", error=str(e))
            raise e

    def process_sync(self, text: str) -> Tuple[str, Optional[str], List[str]]:
        """
        BLOCKING FUNCTION - Must run in Executor.
        """
        if not text or len(text) < 2:
            return text, None, []

        corrections = []
        
        # 1. Regex Correction (CPU Bound)
        # Apply strict word-boundary replacement
        for pattern, replacement in self.regex_map:
            if pattern.search(text):
                new_text = pattern.sub(replacement, text)
                if new_text != text:
                    corrections.append(f"Regex: -> {replacement}")
                    text = new_text

        # 2. Semantic Retrieval (CPU Bound)
        context_note = None
        try:
            # Query Vector DB
            query_emb = self.embedder.encode([text]).tolist()
            results = self.collection.query(query_embeddings=query_emb, n_results=1)
            
            if results['distances'] and results['distances'][0][0] < settings.SIMILARITY_THRESHOLD:
                # Term Validation: Ensure the ID (e.g., "Koinonia") is actually in the text
                # This prevents "Koinonia" context appearing for a sentence about "Fellowship" 
                # unless the user explicitly said the keyword.
                term_found = results['ids'][0][0]
                if term_found.lower() in text.lower():
                    context_note = results['documents'][0][0]

        except Exception as e:
            self.log.error("vector_query_failed", error=str(e))

        return text, context_note, corrections