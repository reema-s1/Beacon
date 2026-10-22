import pytest

from beacon.baseline import classify, runbook_for
from beacon.models import Diagnosis, RootCause
from beacon.security import mask_obj, mask_secrets


@pytest.mark.parametrize("text,cause", [
    ("KeyError: 'unit_price_usd'\nTraceback (most recent call last):", RootCause.code_error),
    ("FileNotFoundError: Expected input file not found: /opt/airflow/data/incoming/sales.csv",
     RootCause.missing_upstream_data),
    ("GET http://upstream-mock:8000/events (timeout=10s)\nHTTPError: 503 Server Error: Service Unavailable",
     RootCause.dependency_unavailable),
    ("GET http://upstream-mock:8000/events (timeout=10s)\nrequests.exceptions.ReadTimeout: Read timed out.",
     RootCause.dependency_latency),
    ('OperationalError: FATAL:  password authentication failed for user "warehouse"',
     RootCause.credentials_or_connection),
    ("Task exited with return code Negsignal.SIGKILL", RootCause.resource_limit),
    ("SchedulerHeartbeatMissing", RootCause.scheduler_unhealthy),
    ("OSError: [Errno 28] No space left on device", RootCause.storage_full),
    ("everything looks fine", RootCause.unknown),
])
def test_baseline_rules(text, cause):
    assert classify(text) is cause


def test_baseline_runbook_mapping():
    assert runbook_for(RootCause.storage_full) == "rb-disk-full"
    assert runbook_for(RootCause.unknown) is None


@pytest.mark.parametrize("secret", [
    "xox" "b-123456789012-123456789012-abcdefABCDEF1234",  # split so scanners skip the fake
    "xa" "pp-1-A01234567-1234567890-abcdef0123456789",
    "gsk_abcdefghijklmnopqrstuvwxyz012345",
    "sk-proj-abcdefghijklmnopqrstuvwx",
    "ps_abcdefghijklmnopqrstuvwxyz0123",
    "AKIAABCDEFGHIJKLMNOP",
])
def test_mask_token_shapes(secret):
    assert secret not in mask_secrets(f"token in log: {secret} end")


def test_mask_key_values_uris_and_bearer():
    text = mask_secrets("password=hunter2 api_key: abc123 Authorization: Bearer eyJhbGciOi.xyz "
                        "postgresql://warehouse:s3cret@postgres:5432/warehouse")
    for leaked in ("hunter2", "abc123", "eyJhbGciOi", "s3cret"):
        assert leaked not in text
    assert "postgresql://warehouse:***@postgres" in text
    assert "password authentication failed" == mask_secrets("password authentication failed")


def test_mask_configured_values_and_nested_objects():
    out = mask_obj({"a": ["my-very-own-secret-value"], "b": 3}, ["my-very-own-secret-value"])
    assert out == {"a": ["***"], "b": 3}


def test_diagnosis_model_validation():
    d = Diagnosis.model_validate({"summary": " ok ", "user_impact": "x", "root_cause": "storage_full",
                                  "confidence": 0.5, "evidence": "one line", "runbook_id": "null",
                                  "suggested_action": "free space"})
    assert d.evidence == ["one line"] and d.runbook_id is None and d.summary == "ok"
    with pytest.raises(ValueError):
        Diagnosis.model_validate({"summary": "s", "user_impact": "x", "root_cause": "aliens",
                                  "confidence": 0.5, "suggested_action": "a"})
    with pytest.raises(ValueError):
        Diagnosis.model_validate({"summary": "s", "user_impact": "x", "root_cause": "unknown",
                                  "confidence": 1.5, "suggested_action": "a"})
