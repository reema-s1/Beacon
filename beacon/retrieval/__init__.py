"""Runbook retrieval. `get_retriever()` builds the index once per process."""
from __future__ import annotations

import threading

from beacon.config import get_settings
from beacon.retrieval.crag import RunbookRetriever, SearchResult, SentenceTransformerEmbedder
from beacon.retrieval.runbooks import Runbook, load_runbooks

_lock = threading.Lock()
_retriever: RunbookRetriever | None = None


def get_retriever() -> RunbookRetriever:
    global _retriever
    with _lock:
        if _retriever is None:
            s = get_settings()
            _retriever = RunbookRetriever(load_runbooks(s.runbooks_dir), SentenceTransformerEmbedder(s.embedding_model))
        return _retriever


__all__ = ["Runbook", "RunbookRetriever", "SearchResult", "get_retriever", "load_runbooks"]
