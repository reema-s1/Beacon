"""Corrective-RAG style runbook retrieval.

1. Retrieve the top-k runbooks from a FAISS index (cosine similarity on MiniLM embeddings).
2. Grade each candidate's relevance to the incident.
3. If none passes the threshold, rewrite the query from the evidence and retrieve once more.
4. Return the candidates that pass, with their scores.

The grader is deterministic (semantic similarity blended with symptom-term overlap) rather
than an LLM call: it costs nothing against free-tier rate limits and is reproducible in tests.
"""
from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import Protocol

import faiss
import numpy as np

from beacon.retrieval.runbooks import Runbook

_STOPWORDS = set("""a an the and or of to in on for from with by is are was were be been it its this that
at as not no but if into than then there their they them we you your our task tasks run runs dag
pipeline pipelines failed fail fails failing failure error errors why what how did does do my""".split())

# Error signals that appear in logs/alerts, expanded into the vocabulary runbooks use.
# Used only when the first retrieval pass finds nothing relevant.
SIGNAL_EXPANSIONS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"keyerror|typeerror|attributeerror|nameerror|traceback", re.I),
     "Python exception traceback code bug KeyError"),
    (re.compile(r"filenotfound|no such file|not found|missing", re.I),
     "expected input file not found missing upstream data"),
    (re.compile(r"\b50[0234]\b|service unavailable|connection refused|max retries", re.I),
     "external dependency down HTTP 503 Service Unavailable"),
    (re.compile(r"timeout|timed out|slow|latency|sla", re.I),
     "external dependency slow timeout Read timed out SLA missed"),
    (re.compile(r"password|authentication|operationalerror|conn_id|credential", re.I),
     "database connection credentials password authentication failed"),
    (re.compile(r"sigkill|oom|out of memory|memoryerror|killed", re.I),
     "task killed out of memory SIGKILL"),
    (re.compile(r"heartbeat|scheduler", re.I),
     "scheduler heartbeat missing scheduler down"),
    (re.compile(r"no space|errno 28|disk|volume full", re.I),
     "disk volume full No space left on device"),
    (re.compile(r"permission denied|errno 13|forbidden|accessdenied", re.I),
     "permission denied access"),
]


def _terms(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9_]+", text.lower()) if len(t) > 2 and t not in _STOPWORDS}


class Embedder(Protocol):
    def encode(self, texts: list[str]) -> np.ndarray: ...


class SentenceTransformerEmbedder:
    def __init__(self, model_name: str):
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(model_name, device="cpu")

    def encode(self, texts: list[str]) -> np.ndarray:
        return np.asarray(self._model.encode(texts, normalize_embeddings=True, convert_to_numpy=True),
                          dtype="float32")


@dataclass
class ScoredRunbook:
    runbook: Runbook
    similarity: float
    score: float

    def as_dict(self) -> dict:
        return {"runbook_id": self.runbook.id, "title": self.runbook.title,
                "root_cause": self.runbook.root_cause, "score": round(self.score, 3)}


@dataclass
class SearchResult:
    query: str
    results: list[ScoredRunbook]
    rewritten_query: str | None = None

    def as_dict(self) -> dict:
        out: dict = {"results": [r.as_dict() for r in self.results]}
        if self.rewritten_query:
            out["rewritten_query"] = self.rewritten_query
        if not self.results:
            out["note"] = "no runbook passed the relevance threshold"
        return out


class RunbookRetriever:
    def __init__(self, runbooks: list[Runbook], embedder: Embedder, threshold: float = 0.45,
                 semantic_weight: float = 0.7):
        if not runbooks:
            raise ValueError("no runbooks to index")
        self.runbooks = runbooks
        self.by_id = {rb.id: rb for rb in runbooks}
        self.embedder = embedder
        self.threshold = threshold
        self.semantic_weight = semantic_weight
        vectors = embedder.encode([rb.search_text for rb in runbooks])
        self.index = faiss.IndexFlatIP(vectors.shape[1])
        self.index.add(vectors)
        self._terms = {rb.id: _terms(rb.title + " " + " ".join(rb.symptoms)) for rb in runbooks}
        self._lock = threading.Lock()

    def _retrieve(self, query: str, k: int) -> list[tuple[Runbook, float]]:
        vec = self.embedder.encode([query])
        with self._lock:
            sims, idx = self.index.search(vec, min(k, len(self.runbooks)))
        return [(self.runbooks[i], float(s)) for s, i in zip(sims[0], idx[0]) if i >= 0]

    def grade(self, query: str, runbook: Runbook, similarity: float) -> float:
        q = _terms(query)
        overlap = len(q & self._terms[runbook.id]) / min(len(q), 6) if q else 0.0
        return self.semantic_weight * similarity + (1 - self.semantic_weight) * min(overlap, 1.0)

    def rewrite(self, query: str, evidence: list[str] | None = None) -> str:
        text = " ".join([query, *(evidence or [])])
        expansions = [exp for pat, exp in SIGNAL_EXPANSIONS if pat.search(text)]
        exceptions = re.findall(r"\b([A-Z][A-Za-z]+(?:Error|Exception|Timeout))\b", text)
        parts = expansions + sorted(set(exceptions))
        return " ".join(parts) if parts else query

    def _graded(self, query: str, k: int) -> list[ScoredRunbook]:
        scored = [ScoredRunbook(rb, sim, self.grade(query, rb, sim)) for rb, sim in self._retrieve(query, k)]
        return sorted(scored, key=lambda s: s.score, reverse=True)

    def search(self, query: str, k: int = 3, evidence: list[str] | None = None) -> SearchResult:
        candidates = self._graded(query, k)
        passing = [c for c in candidates if c.score >= self.threshold]
        if passing:
            return SearchResult(query=query, results=passing)
        rewritten = self.rewrite(query, evidence)
        if rewritten == query:
            return SearchResult(query=query, results=[])
        candidates = self._graded(rewritten, k)
        return SearchResult(query=query, rewritten_query=rewritten,
                            results=[c for c in candidates if c.score >= self.threshold])
