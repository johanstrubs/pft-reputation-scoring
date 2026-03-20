import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.models import ValidatorScore, ValidatorMetrics, ValidatorSubScores


@pytest.fixture
def mock_scores():
    return [
        ValidatorScore(
            public_key="nHTest1",
            domain="test1.example.com",
            composite_score=85.5,
            metrics=ValidatorMetrics(
                agreement_1h=0.99,
                agreement_24h=0.98,
                agreement_30d=0.97,
                uptime_seconds=86400,
                uptime_pct=100.0,
                latency_ms=42.0,
                peer_count=12,
                avg_ledger_interval=3.5,
                server_version="2.4.0",
                server_state="proposing",
                asn=24940,
                isp="Hetzner",
                country="DE",
            ),
            sub_scores=ValidatorSubScores(
                agreement_1h=0.95,
                agreement_24h=0.90,
                agreement_30d=0.85,
                uptime=0.80,
                poll_success=0.5,
                latency=0.91,
                peer_count=1.0,
                version=1.0,
                diversity=0.8,
            ),
            last_updated="2026-03-17T12:00:00+00:00",
        )
    ]


@pytest.mark.anyio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch.object(app.state, "__dict__", {}):
            from app.main import db
            with patch.object(db, "get_last_round_timestamp", new_callable=AsyncMock, return_value="2026-03-17T12:00:00"):
                resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


@pytest.mark.anyio
async def test_scores_no_data():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(None, None, [])):
            resp = await client.get("/api/scores")
    assert resp.status_code == 503


@pytest.mark.anyio
async def test_scores_with_data(mock_scores):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(1, "2026-03-17T12:00:00", mock_scores)):
            resp = await client.get("/api/scores")
    assert resp.status_code == 200
    data = resp.json()
    assert data["validator_count"] == 1
    assert data["validators"][0]["public_key"] == "nHTest1"
    assert data["validators"][0]["composite_score"] == 85.5


@pytest.mark.anyio
async def test_methodology():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/methodology")
    assert resp.status_code == 200
    data = resp.json()
    assert "weights" in data
    assert abs(sum(data["weights"].values()) - 1.0) < 0.001
