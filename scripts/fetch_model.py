"""Save the embedding model into ./models so the Docker build can copy it instead of
downloading it. Only needed when containers can't reach huggingface.co (for example
behind a TLS-intercepting proxy); the build downloads the model itself otherwise.

    python scripts/fetch_model.py
"""
from pathlib import Path

from sentence_transformers import SentenceTransformer

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
TARGET = Path(__file__).resolve().parent.parent / "models" / "all-MiniLM-L6-v2"

if __name__ == "__main__":
    SentenceTransformer(MODEL).save(str(TARGET))
    print(f"saved {MODEL} to {TARGET}")
