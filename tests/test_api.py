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
    assert data["enrichment_coverage"]["enriched"] == 1
    assert data["enrichment_coverage"]["coverage_pct"] == 100.0
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


@pytest.mark.anyio
async def test_simulator_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/simulator")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


@pytest.mark.anyio
async def test_incidents_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/incidents")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


@pytest.mark.anyio
async def test_diagnose_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/diagnose")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


@pytest.mark.anyio
async def test_readiness_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/readiness")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


@pytest.mark.anyio
async def test_network_topology_includes_version_and_strict_enrichment(mock_scores):
    partial = ValidatorScore(
        public_key="nHPartial",
        domain="partial.example.com",
        composite_score=60.0,
        metrics=ValidatorMetrics(
            latency_ms=55.0,
            uptime_seconds=7200,
            peer_count=8,
            server_version="1.0.0",
            country="US",
            asn=None,
        ),
        sub_scores=ValidatorSubScores(),
        last_updated="2026-03-17T12:00:00+00:00",
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(1, "2026-03-17T12:00:00", mock_scores + [partial])):
            resp = await client.get("/api/network/topology")

    assert resp.status_code == 200
    data = resp.json()
    assert data["enrichment_coverage"]["enriched"] == 1
    assert data["enrichment_coverage"]["unenriched"] == 1
    assert data["enrichment_coverage"]["coverage_pct"] == 50.0
    assert data["validators"][0]["server_version"] is not None
    partial_row = next(v for v in data["validators"] if v["public_key"] == "nHPartial")
    assert partial_row["server_version"] == "1.0.0"
    assert partial_row["enriched"] is False


@pytest.mark.anyio
async def test_latest_digest_endpoint():
    transport = ASGITransport(app=app)
    digest = {
        "id": 1,
        "created_at": "2026-04-04T12:00:00+00:00",
        "latest_round_id": 100,
        "comparison_round_id": 88,
        "delivery_status": "posted",
        "posted_at": "2026-04-04T12:01:00+00:00",
        "message_id": "msg-1",
        "payload": {
            "summary": {"joins_count": 1},
            "joins": [{"public_key": "nHJoin"}],
            "departures": [{"public_key": "nHDepart"}],
            "top_rank_gainers": [{"public_key": "nHGainer"}],
            "top_rank_losers": [{"public_key": "nHLoser"}],
            "score_change_alerts": [{"public_key": "nHAlert"}],
            "concentration": {"coverage": {"current": {"enriched": 5}, "comparison": {"enriched": 4}}},
        },
    }
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_digest", new_callable=AsyncMock, return_value=digest):
            resp = await client.get("/api/digest/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data["delivery_status"] == "posted"
    assert data["payload"]["joins"][0]["public_key"] == "nHJoin"


@pytest.mark.anyio
async def test_trigger_digest_endpoint():
    transport = ASGITransport(app=app)
    digest = {
        "id": 2,
        "created_at": "2026-04-04T12:00:00+00:00",
        "latest_round_id": 100,
        "comparison_round_id": 88,
        "delivery_status": "posted",
        "posted_at": "2026-04-04T12:01:00+00:00",
        "message_id": "msg-2",
        "payload": {"summary": {"joins_count": 1}},
    }
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch("app.main.generate_and_store_weekly_digest", new_callable=AsyncMock, return_value=digest):
            resp = await client.post("/api/digest/trigger")
    assert resp.status_code == 200
    assert resp.json()["message_id"] == "msg-2"


@pytest.mark.anyio
async def test_incidents_list_endpoint():
    transport = ASGITransport(app=app)
    incidents = [{
        "id": 1,
        "validator_key": "nHIncident1",
        "severity": "warning",
        "status": "open",
        "synthetic": False,
        "correlated": False,
        "summary": "Peer count collapse - nHIncident1...",
        "start_time": "2026-04-04T12:00:00+00:00",
        "end_time": None,
        "duration_seconds": None,
        "event_types": ["peer_collapse"],
        "active_event_types": ["peer_collapse"],
        "latest_round_id": 12,
        "latest_event_time": "2026-04-04T12:00:00+00:00",
        "before_values": {"peer_count": 10},
        "during_values": {"peer_count": 2},
        "after_values": None,
    }]
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_incidents", new_callable=AsyncMock, return_value=incidents):
            resp = await client.get("/api/incidents?status=open")
    assert resp.status_code == 200
    data = resp.json()
    assert data["incidents"][0]["validator_key"] == "nHIncident1"
    assert data["incidents"][0]["event_types"] == ["peer_collapse"]


@pytest.mark.anyio
async def test_incident_detail_and_synthetic_endpoint():
    transport = ASGITransport(app=app)
    incident = {
        "id": 9,
        "validator_key": "nHSynthetic1",
        "severity": "warning",
        "status": "closed",
        "synthetic": True,
        "correlated": False,
        "summary": "[Synthetic] Synthetic incident injected for verification - nHSynthetic1...",
        "start_time": "2026-04-04T12:00:00+00:00",
        "end_time": "2026-04-04T12:15:00+00:00",
        "duration_seconds": 900,
        "event_types": ["synthetic_test"],
        "active_event_types": [],
        "latest_round_id": None,
        "latest_event_time": "2026-04-04T12:15:00+00:00",
        "before_values": {"peer_count": 12},
        "during_values": {"peer_count": 2},
        "after_values": {"status": "recovered"},
    }
    events = [{
        "id": 10,
        "incident_id": 9,
        "round_id": None,
        "validator_key": "nHSynthetic1",
        "event_type": "synthetic_test",
        "severity": "warning",
        "event_phase": "triggered",
        "synthetic": True,
        "correlated": False,
        "created_at": "2026-04-04T12:00:00+00:00",
        "current_values": {"peer_count": 2},
        "previous_values": {"peer_count": 12},
    }]
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_incident", new_callable=AsyncMock, return_value=incident), \
             patch.object(db, "get_incident_events", new_callable=AsyncMock, return_value=events):
            detail_resp = await client.get("/api/incidents/9")
        with patch("app.main.inject_synthetic_incident", new_callable=AsyncMock, return_value=incident), \
             patch.object(db, "get_incident_events", new_callable=AsyncMock, return_value=events):
            synthetic_resp = await client.post("/api/incidents/test", json={"validator_key": "nHSynthetic1"})

    assert detail_resp.status_code == 200
    assert detail_resp.json()["events"][0]["event_phase"] == "triggered"
    assert synthetic_resp.status_code == 200
    assert synthetic_resp.json()["synthetic"] is True


@pytest.mark.anyio
async def test_diagnose_endpoint(mock_scores):
    transport = ASGITransport(app=app)
    weak = ValidatorScore(
        public_key="nHWeak",
        domain="weak.example.com",
        composite_score=61.2,
        metrics=ValidatorMetrics(
            agreement_1h=0.91,
            agreement_24h=0.92,
            agreement_30d=0.89,
            uptime_seconds=3600,
            uptime_pct=10.0,
            latency_ms=420.0,
            peer_count=2,
            poll_success_pct=80.0,
            server_version="1.0.0",
            server_state="syncing",
            asn=24940,
            isp="Hetzner",
            country="DE",
        ),
        sub_scores=ValidatorSubScores(
            agreement_1h=0.55,
            agreement_24h=0.60,
            agreement_30d=0.45,
            uptime=0.10,
            poll_success=0.40,
            latency=0.15,
            peer_count=0.0,
            version=1.0,
            diversity=0.2,
        ),
        last_updated="2026-03-17T12:00:00+00:00",
    )
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(5, "2026-03-17T12:00:00", mock_scores + [weak])):
            resp = await client.get("/api/diagnose/nHWeak")
    assert resp.status_code == 200
    data = resp.json()
    assert data["public_key"] == "nHWeak"
    assert data["overall_status"] in {"critical", "warning"}
    assert data["findings"]
    assert "json_report_url" in data


@pytest.mark.anyio
async def test_diagnose_endpoint_not_found():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(1, "2026-03-17T12:00:00", [])):
            resp = await client.get("/api/diagnose/nHMissing")
    assert resp.status_code == 404 or resp.status_code == 503


@pytest.mark.anyio
async def test_diagnose_ai_endpoint_success():
    transport = ASGITransport(app=app)
    payload = {
        "ai_summary": "Validator is underperforming due to recent uptime loss.",
        "model": "claude-test",
        "generated_at": "2026-04-04T12:00:00+00:00",
        "cached": False,
        "message": None,
    }
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch("app.main.generate_ai_diagnostic", new_callable=AsyncMock, return_value=payload):
            resp = await client.post("/api/diagnose/nHWeak/ai")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ai_summary"]
    assert data["model"] == "claude-test"


@pytest.mark.anyio
async def test_diagnose_ai_endpoint_limit_response():
    from app.diagnostic_ai import AIDiagnosticLimitError

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch("app.main.generate_ai_diagnostic", new_callable=AsyncMock, side_effect=AIDiagnosticLimitError("Temporarily unavailable")):
            resp = await client.post("/api/diagnose/nHWeak/ai")
    assert resp.status_code == 429
    assert "Temporarily unavailable" in resp.json()["detail"]


@pytest.mark.anyio
async def test_diagnose_ai_cached_get_endpoint(mock_scores):
    transport = ASGITransport(app=app)
    cached = {
        "public_key": "nHTest1",
        "round_id": 5,
        "model": "claude-haiku-4-5",
        "ai_summary": "Cached summary",
        "generated_at": "2026-04-05T04:22:48.546230+00:00",
        "cached": True,
    }
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(5, "2026-03-17T12:00:00", mock_scores)), \
             patch.object(db, "get_ai_diagnostic_cache", new_callable=AsyncMock, return_value=cached):
            resp = await client.get("/api/diagnose/nHTest1/ai")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cached"] is True
    assert data["ai_summary"] == "Cached summary"


@pytest.mark.anyio
async def test_diagnose_ai_cached_get_endpoint_empty(mock_scores):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(5, "2026-03-17T12:00:00", mock_scores)), \
             patch.object(db, "get_ai_diagnostic_cache", new_callable=AsyncMock, return_value=None):
            resp = await client.get("/api/diagnose/nHTest1/ai")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cached"] is False
    assert data["ai_summary"] is None


@pytest.mark.anyio
async def test_readiness_endpoint_success(mock_scores):
    transport = ASGITransport(app=app)
    payload = {
        "public_key": "nHTest1",
        "domain": "test1.example.com",
        "round_id": 7,
        "timestamp": "2026-04-10T12:00:00+00:00",
        "overall_status": "ready",
        "status_summary": "Ready",
        "json_report_url": "/api/readiness/nHTest1",
        "checks": [
            {
                "name": "Version parity",
                "category": "configuration",
                "status": "pass",
                "detected_value": "1.0.0",
                "expected_value": "1.0.0",
                "remediation": None,
                "source_timestamp": "2026-04-10T12:00:00+00:00",
            }
        ],
    }
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(7, "2026-04-10T12:00:00+00:00", mock_scores)), \
             patch("app.main.build_readiness_report", new=AsyncMock(return_value=payload)):
            resp = await client.get("/api/readiness/nHTest1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["overall_status"] == "ready"
    assert data["checks"][0]["name"] == "Version parity"


@pytest.mark.anyio
async def test_readiness_endpoint_not_found(mock_scores):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        from app.main import db
        with patch.object(db, "get_latest_scores", new_callable=AsyncMock, return_value=(7, "2026-04-10T12:00:00+00:00", mock_scores)), \
             patch("app.main.build_readiness_report", new=AsyncMock(side_effect=KeyError("nHMissing"))):
            resp = await client.get("/api/readiness/nHMissing")
    assert resp.status_code == 404
