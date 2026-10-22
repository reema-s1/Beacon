"""Domain models shared by the agent, API, Slack bot and evaluation."""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, field_validator


class RootCause(str, Enum):
    code_error = "code_error"
    missing_upstream_data = "missing_upstream_data"
    dependency_unavailable = "dependency_unavailable"
    dependency_latency = "dependency_latency"
    credentials_or_connection = "credentials_or_connection"
    resource_limit = "resource_limit"
    scheduler_unhealthy = "scheduler_unhealthy"
    storage_full = "storage_full"
    unknown = "unknown"


ROOT_CAUSE_DESCRIPTIONS: dict[RootCause, str] = {
    RootCause.code_error: "a bug or unhandled exception in the pipeline's own code (KeyError, TypeError, ...)",
    RootCause.missing_upstream_data: "an expected input file or upstream dataset is absent",
    RootCause.dependency_unavailable: "an external service/API is down or returning 5xx errors",
    RootCause.dependency_latency: "an external service is responding too slowly (timeouts, SLA misses)",
    RootCause.credentials_or_connection: "a database/connection is misconfigured or its credentials are rejected",
    RootCause.resource_limit: "the task ran out of memory/CPU and was killed (SIGKILL, OOM)",
    RootCause.scheduler_unhealthy: "the Airflow scheduler is not heartbeating, so nothing gets scheduled",
    RootCause.storage_full: "a disk or volume ran out of space",
    RootCause.unknown: "the evidence does not support any category",
}


class Diagnosis(BaseModel):
    summary: str = Field(description="<= 3 sentences, plain English, for a platform USER, not an SRE")
    user_impact: str = Field(description="which pipelines/data are affected")
    root_cause: RootCause
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list, description="specific log lines / metric values")
    runbook_id: str | None = None
    suggested_action: str

    @field_validator("summary", "user_impact", "suggested_action")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v.strip()

    @field_validator("evidence", mode="before")
    @classmethod
    def _coerce_evidence(cls, v):
        if isinstance(v, str):
            return [v]
        return [str(x) for x in v] if v else []

    @field_validator("runbook_id", mode="before")
    @classmethod
    def _null_runbook(cls, v):
        if isinstance(v, str) and v.strip().lower() in {"", "none", "null", "n/a"}:
            return None
        return v


class TriageCategory(str, Enum):
    job_failure = "job_failure"
    job_slow = "job_slow"
    how_to = "how_to"
    other = "other"


# Ground truth used by the chaos CLI and the evaluation: the runbook for each root cause.
CAUSE_TO_RUNBOOK: dict[RootCause, str] = {
    RootCause.code_error: "rb-code-error",
    RootCause.missing_upstream_data: "rb-missing-input",
    RootCause.dependency_unavailable: "rb-upstream-down",
    RootCause.dependency_latency: "rb-upstream-slow",
    RootCause.credentials_or_connection: "rb-connection",
    RootCause.resource_limit: "rb-resource",
    RootCause.scheduler_unhealthy: "rb-scheduler",
    RootCause.storage_full: "rb-disk-full",
}
