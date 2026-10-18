import re

import numpy as np
import pytest

from beacon.config import get_settings
from beacon.retrieval.crag import RunbookRetriever, SentenceTransformerEmbedder
from beacon.retrieval.runbooks import load_runbooks, parse_runbook

RUNBOOKS = load_runbooks(get_settings().runbooks_dir)


@pytest.fixture(scope="module")
def retriever():
    return RunbookRetriever(RUNBOOKS, SentenceTransformerEmbedder(get_settings().embedding_model))


def test_runbook_library_is_complete():
    ids = {rb.id for rb in RUNBOOKS}
    assert len(RUNBOOKS) == 12
    assert {"rb-code-error", "rb-missing-input", "rb-upstream-down", "rb-upstream-slow", "rb-connection",
            "rb-resource", "rb-scheduler", "rb-disk-full"} <= ids
    for rb in RUNBOOKS:
        assert rb.symptoms and rb.fix_steps, rb.id


@pytest.mark.parametrize("rb", RUNBOOKS, ids=lambda rb: rb.id)
def test_each_runbook_is_found_for_its_own_symptoms(retriever, rb):
    result = retriever.search(". ".join(rb.symptoms), k=3)
    assert result.results, f"nothing passed the threshold for {rb.id}"
    assert result.results[0].runbook.id == rb.id


@pytest.mark.parametrize("rb", RUNBOOKS, ids=lambda rb: rb.id)
def test_each_symptom_retrieves_its_runbook_in_top_3(retriever, rb):
    for symptom in rb.symptoms:
        top = [c.runbook.id for c in retriever._graded(symptom, 3)]
        assert rb.id in top, f"{symptom!r} -> {top}"


@pytest.mark.parametrize("log_line,expected", [
    ("FileNotFoundError: Expected input file not found: /opt/airflow/data/incoming/sales.csv", "rb-missing-input"),
    ("requests.exceptions.HTTPError: 503 Server Error: Service Unavailable for url: http://upstream-mock:8000/events",
     "rb-upstream-down"),
    ("requests.exceptions.ReadTimeout: HTTPConnectionPool(host='upstream-mock', port=8000): Read timed out.",
     "rb-upstream-slow"),
    ('psycopg2.OperationalError: connection to server at "postgres" failed: FATAL: password authentication '
     'failed for user "warehouse"', "rb-connection"),
    ("Task exited with return code Negsignal.SIGKILL", "rb-resource"),
    ("OSError: [Errno 28] No space left on device", "rb-disk-full"),
    ("KeyError: 'unit_price_usd'", "rb-code-error"),
    ("No scheduler heartbeat for 60 seconds", "rb-scheduler"),
])
def test_real_log_lines_find_the_right_runbook(retriever, log_line, expected):
    result = retriever.search(log_line, k=3)
    assert result.results and result.results[0].runbook.id == expected


def test_irrelevant_query_returns_nothing(retriever):
    assert retriever.search("what is the best pizza topping in naples", k=3).results == []


# ── CRAG mechanics with a deterministic embedder ──────────────────────────────

class BagOfWordsEmbedder:
    """Cosine similarity on word counts: lets the test control exactly what matches."""

    def __init__(self, vocab_texts):
        words = sorted({w for t in vocab_texts for w in re.findall(r"[a-z]+", t.lower())})
        self.index = {w: i for i, w in enumerate(words)}

    def encode(self, texts):
        out = np.zeros((len(texts), len(self.index)), dtype="float32")
        for row, t in enumerate(texts):
            for w in re.findall(r"[a-z]+", t.lower()):
                if w in self.index:
                    out[row, self.index[w]] += 1
        norms = np.linalg.norm(out, axis=1, keepdims=True)
        return out / np.where(norms == 0, 1, norms)


def test_low_relevance_triggers_one_rewrite_from_evidence():
    books = [rb for rb in RUNBOOKS if rb.id in ("rb-disk-full", "rb-code-error", "rb-resource")]
    r = RunbookRetriever(books, BagOfWordsEmbedder([b.search_text for b in books]), threshold=0.3)
    # The user's words match nothing; the evidence carries the real signal.
    result = r.search("reports job is unhappy", k=3, evidence=["OSError: [Errno 28] No space left on device"])
    assert result.rewritten_query and "No space left on device" in result.rewritten_query
    assert result.results and result.results[0].runbook.id == "rb-disk-full"


def test_no_rewrite_when_first_pass_is_relevant():
    books = [rb for rb in RUNBOOKS if rb.id in ("rb-disk-full", "rb-code-error")]
    r = RunbookRetriever(books, BagOfWordsEmbedder([b.search_text for b in books]), threshold=0.3)
    result = r.search("No space left on device disk volume full", k=2)
    assert result.rewritten_query is None and result.results[0].runbook.id == "rb-disk-full"


def test_parse_runbook_requires_front_matter():
    with pytest.raises(ValueError):
        parse_runbook("# no front matter")
    with pytest.raises(ValueError, match="symptoms"):
        parse_runbook("---\nid: x\ntitle: t\nroot_cause: y\n---\nbody")
