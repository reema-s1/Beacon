"""Runtime configuration. Everything comes from environment variables (or .env)."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    # LLM: any OpenAI-compatible endpoint (Groq, Gemini, Ollama, ...)
    llm_base_url: str = "https://api.groq.com/openai/v1"
    llm_model: str = ""
    llm_api_key: str = ""
    llm_timeout_seconds: float = 60.0
    llm_max_retries: int = 6
    llm_temperature: float = 0.0
    llm_max_tokens: int = 4096  # thinking models count reasoning tokens against this
    llm_reasoning_effort: str = ""  # optional: low|medium|high for providers that support it

    database_url: str = Field("sqlite:///./beacon.db", validation_alias="BEACON_DATABASE_URL")

    airflow_url: str = "http://localhost:8080"
    airflow_user: str = "admin"
    airflow_password: str = "admin"
    prometheus_url: str = "http://localhost:9090"
    beacon_api_url: str = "http://localhost:8100"
    upstream_url: str = "http://localhost:8300"

    slack_bot_token: str = ""
    slack_app_token: str = ""
    slack_incident_channel: str = ""
    rerun_allowed_users: str = ""

    pulse_url: str = ""
    pulse_api_key: str = ""

    runbooks_dir: Path = REPO_ROOT / "runbooks"
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"

    agent_max_steps: int = 8
    agent_concurrency: int = 1
    tool_timeout_seconds: float = 20.0
    tool_output_max_chars: int = 2500
    promql_max_minutes: int = 360
    promql_max_series: int = 30
    promql_max_points: int = 60

    @property
    def rerun_allowlist(self) -> set[str]:
        return {u.strip() for u in self.rerun_allowed_users.split(",") if u.strip()}

    @property
    def llm_configured(self) -> bool:
        return bool(self.llm_model and self.llm_base_url)

    @property
    def secret_values(self) -> list[str]:
        """Configured secrets, so they can be masked verbatim wherever they appear."""
        vals = [self.llm_api_key, self.slack_bot_token, self.slack_app_token, self.pulse_api_key,
                self.airflow_password]
        return [v for v in vals if v and len(v) >= 6]


@lru_cache
def get_settings() -> Settings:
    return Settings()
