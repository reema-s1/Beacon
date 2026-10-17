"""Classify a free-text /ask question before deciding how much work to do."""
from __future__ import annotations

import logging
import re

from beacon.agent.llm import ChatModel, LLMError
from beacon.agent.loop import extract_json
from beacon.agent.prompts import TRIAGE_PROMPT
from beacon.models import TriageCategory

log = logging.getLogger("beacon.triage")

_SLOW = re.compile(r"\b(slow|late|stuck|hang|hanging|taking (?:so )?long|sla|delayed|lagging)\b", re.I)
_FAIL = re.compile(r"\b(fail|failed|failing|failure|error|broke|broken|crash|red|why did|why is)\b", re.I)
_HOWTO = re.compile(r"^\s*(how|what should|what do i|can i|where)\b|\bhow (do|to|can)\b|\brunbook\b", re.I)


def heuristic_category(question: str) -> TriageCategory:
    if _SLOW.search(question):
        return TriageCategory.job_slow
    if _HOWTO.search(question):
        return TriageCategory.how_to
    if _FAIL.search(question):
        return TriageCategory.job_failure
    return TriageCategory.other


def find_dag_id(question: str, known_dags: list[str]) -> str | None:
    for dag in sorted(known_dags, key=len, reverse=True):
        if re.search(rf"\b{re.escape(dag)}\b", question):
            return dag
    return None


async def classify(llm: ChatModel | None, question: str, known_dags: list[str]) -> tuple[TriageCategory, str | None]:
    """LLM classification with a keyword fallback if the model is unavailable or answers badly."""
    dag_id = find_dag_id(question, known_dags)
    if llm is not None:
        try:
            reply = await llm.chat([{"role": "system", "content": TRIAGE_PROMPT},
                                    {"role": "user", "content": question}], json_mode=True)
            data = extract_json(reply.content)
            category = TriageCategory(str(data.get("category", "")).strip().lower())
            if not dag_id and data.get("dag_id") in known_dags:
                dag_id = data["dag_id"]
            return category, dag_id
        except (LLMError, ValueError) as exc:
            log.warning("LLM triage failed (%s); using keyword fallback", exc)
    return heuristic_category(question), dag_id
