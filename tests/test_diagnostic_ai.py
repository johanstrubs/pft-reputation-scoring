import sys
import types

import pytest

from app.diagnostic_ai import generate_ai_diagnostic, _estimate_cost_cents
from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores


def make_score(public_key: str, composite_score: float, *, domain: str | None = None, isp: str | None = "Hetzner"):
    return ValidatorScore(
        public_key=public_key,
        domain=domain,
        composite_score=composite_score,
        metrics=ValidatorMetrics(
            agreement_1h=0.99,
            agreement_24h=0.98,
            agreement_30d=0.97,
            uptime_seconds=86400,
            uptime_pct=100.0,
            latency_ms=42.0,
            peer_count=12,
            poll_success_pct=99.0,
            server_version="1.0.0",
            server_state="proposing",
            asn=24940,
            isp=isp,
            country="DE",
        ),
        sub_scores=ValidatorSubScores(
            agreement_1h=0.95,
            agreement_24h=0.90,
            agreement_30d=0.85,
            uptime=1.0,
            poll_success=1.0,
            latency=0.95,
            peer_count=1.0,
            version=1.0,
            diversity=0.8,
        ),
        last_updated="2026-04-04T00:00:00+00:00",
    )


class FakeDB:
    def __init__(self, *, cached=None, latest_scores=None):
        self.cached = cached
        self.latest_scores = latest_scores or (7, "2026-04-04T12:00:00+00:00", [make_score("nHTest", 88.0, domain="test.example.com"), make_score("nHPeer", 80.0)])
        self.logged = []
        self.stored = None

    async def get_latest_scores(self):
        return self.latest_scores

    async def get_ai_diagnostic_cache(self, public_key, round_id):
        return self.cached

    async def log_ai_diagnostic_request(self, **kwargs):
        self.logged.append(kwargs)

    async def count_ai_requests_since(self, since, *, ip_address=None, statuses=("success", "cached")):
        return 0

    async def sum_ai_cost_since(self, since, *, statuses=("success",)):
        return 0.0

    async def get_validator_diagnostic_history(self, public_key, limit=2016):
        return [
            {"round_id": 1, "timestamp": "2026-03-29T00:00:00+00:00", "composite_score": 82.0, "agreement_24h": 0.98, "agreement_30d": 0.97, "uptime_pct": 95.0, "latency_ms": 50.0, "peer_count": 10, "poll_success_pct": 98.0, "server_version": "1.0.0", "server_state": "proposing"},
            {"round_id": 2, "timestamp": "2026-03-30T00:00:00+00:00", "composite_score": 84.0, "agreement_24h": 0.98, "agreement_30d": 0.97, "uptime_pct": 95.0, "latency_ms": 50.0, "peer_count": 10, "poll_success_pct": 98.0, "server_version": "1.0.0", "server_state": "proposing"},
        ]

    async def store_ai_diagnostic_cache(self, **kwargs):
        self.stored = kwargs


@pytest.mark.anyio
async def test_generate_ai_diagnostic_returns_cached_result(monkeypatch):
    db = FakeDB(cached={
        "public_key": "nHTest",
        "round_id": 7,
        "model": "claude-test",
        "ai_summary": "Cached summary",
        "generated_at": "2026-04-04T12:00:00+00:00",
        "cached": True,
    })
    monkeypatch.setattr("app.diagnostic_ai.settings.anthropic_api_key", "test-key")
    monkeypatch.setattr("app.diagnostic_ai.settings.anthropic_model", "claude-test")

    result = await generate_ai_diagnostic(db, public_key="nHTest", ip_address="127.0.0.1")

    assert result["cached"] is True
    assert result["ai_summary"] == "Cached summary"
    assert db.logged[0]["status"] == "cached"


@pytest.mark.anyio
async def test_generate_ai_diagnostic_creates_and_caches_summary(monkeypatch):
    class FakeMessages:
        async def create(self, **kwargs):
            return types.SimpleNamespace(
                content=[types.SimpleNamespace(text="Validator is improving but still needs attention.")],
                usage=types.SimpleNamespace(input_tokens=2000, output_tokens=120),
            )

    class FakeAnthropic:
        def __init__(self, *args, **kwargs):
            self.messages = FakeMessages()

    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(AsyncAnthropic=FakeAnthropic))
    monkeypatch.setattr("app.diagnostic_ai.settings.anthropic_api_key", "test-key")
    monkeypatch.setattr("app.diagnostic_ai.settings.anthropic_model", "claude-test")
    db = FakeDB()

    result = await generate_ai_diagnostic(db, public_key="nHTest", ip_address="127.0.0.1")

    assert result["cached"] is False
    assert result["ai_summary"]
    assert result["model"] == "claude-test"
    assert db.stored is not None
    assert db.logged[-1]["status"] == "success"


@pytest.mark.anyio
async def test_generate_ai_diagnostic_api_failure_returns_null(monkeypatch):
    class FakeMessages:
        async def create(self, **kwargs):
            raise RuntimeError("timeout")

    class FakeAnthropic:
        def __init__(self, *args, **kwargs):
            self.messages = FakeMessages()

    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(AsyncAnthropic=FakeAnthropic))
    monkeypatch.setattr("app.diagnostic_ai.settings.anthropic_api_key", "test-key")
    monkeypatch.setattr("app.diagnostic_ai.settings.anthropic_model", "claude-test")
    db = FakeDB()

    result = await generate_ai_diagnostic(db, public_key="nHTest", ip_address="127.0.0.1")

    assert result["ai_summary"] is None
    assert "temporarily unavailable" in result["message"]
    assert db.logged[-1]["status"] == "error"


def test_estimate_cost_cents_uses_token_pricing():
    cost = _estimate_cost_cents(2000, 500)
    assert cost > 0
