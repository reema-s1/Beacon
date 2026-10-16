"""SQLAlchemy 2 models and session handling for Beacon's own database."""
from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Integer, String, Text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.pool import StaticPool

from beacon.config import get_settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class UTCDateTime(DateTime):
    """Timezone-aware timestamps; SQLite (used in tests) drops tzinfo, so add it back."""

    cache_ok = True

    def __init__(self):
        super().__init__(timezone=True)

    def result_processor(self, dialect, coltype):
        parent = super().result_processor(dialect, coltype)

        def process(value):
            if parent:
                value = parent(value)
            if isinstance(value, datetime) and value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value
        return process


class FaultInjection(Base):
    __tablename__ = "fault_injections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scenario: Mapped[str] = mapped_column(String(64))
    ground_truth_cause: Mapped[str] = mapped_column(String(64))
    expected_runbook: Mapped[str] = mapped_column(String(64))
    injected_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    target_dag: Mapped[str | None] = mapped_column(String(250), nullable=True)
    reset_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)


class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    alert_name: Mapped[str] = mapped_column(String(128))
    dag_id: Mapped[str | None] = mapped_column(String(250), nullable=True)
    task_id: Mapped[str | None] = mapped_column(String(250), nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(250), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)  # open|triaged|resolved
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    triaged_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_impact: Mapped[str | None] = mapped_column(Text, nullable=True)
    root_cause: Mapped[str | None] = mapped_column(String(64), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    evidence: Mapped[list | None] = mapped_column(JSON, nullable=True)
    runbook_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    suggested_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    slack_channel: Mapped[str | None] = mapped_column(String(64), nullable=True)
    slack_ts: Mapped[str | None] = mapped_column(String(64), nullable=True)
    feedback: Mapped[str | None] = mapped_column(String(16), nullable=True)
    fault_injection_id: Mapped[int | None] = mapped_column(ForeignKey("fault_injections.id"), nullable=True)
    # Beyond the core schema: alert context, ack, and the agent's tool-call trail.
    labels: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    agent_steps: Mapped[int | None] = mapped_column(Integer, nullable=True)
    investigation: Mapped[list | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    def to_dict(self) -> dict[str, Any]:
        out = {}
        for col in self.__table__.columns:
            v = getattr(self, col.name)
            out[col.name] = v.isoformat() if isinstance(v, datetime) else v
        return out


class EvalMetric(Base):
    """Latest evaluation accuracy, persisted so the gauges survive an API restart."""
    __tablename__ = "eval_metrics"

    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    system: Mapped[str] = mapped_column(String(32), primary_key=True)
    value: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow, onupdate=utcnow)


def make_engine(url: str) -> Engine:
    if url.startswith("sqlite"):
        kw: dict[str, Any] = {"connect_args": {"check_same_thread": False}}
        if url in ("sqlite://", "sqlite:///:memory:"):
            kw["poolclass"] = StaticPool
        return create_engine(url, **kw)
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


@lru_cache
def get_engine() -> Engine:
    engine = make_engine(get_settings().database_url)
    Base.metadata.create_all(engine)
    return engine


@lru_cache
def get_sessionmaker() -> sessionmaker:
    return sessionmaker(bind=get_engine(), expire_on_commit=False)


def session():
    return get_sessionmaker()()
