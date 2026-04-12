from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores
from app.runbooks import classify_incident, get_runbook_library


def make_score(public_key: str, *, provider: str | None = None, version: str | None = "1.0.0"):
    return ValidatorScore(
        public_key=public_key,
        domain=None,
        composite_score=80.0,
        metrics=ValidatorMetrics(isp=provider, server_version=version),
        sub_scores=ValidatorSubScores(),
        last_updated="2026-04-12T00:00:00+00:00",
    )


def test_runbook_library_contains_expected_labels():
    library = get_runbook_library()
    assert "peer_collapse" in library
    assert "provider_outage" in library
    assert "unknown" in library


def test_classify_incident_detects_peer_collapse():
    incident = {
        "id": 1,
        "validator_key": "nHTest1",
        "event_types": ["peer_collapse"],
        "latest_round_id": 12,
        "start_time": "2026-04-12T00:00:00+00:00",
        "duration_seconds": 600,
        "during_values": {"peer_count": 2},
        "before_values": {"peer_count": 8},
        "after_values": {"peer_count": 7},
    }
    result = classify_incident(incident, related_incidents=[incident], round_scores=[make_score("nHTest1", provider="Hetzner")], latest_scores=[make_score("nHTest1", provider="Hetzner")])
    assert result["suspected_cause"] == "peer_collapse"
    assert result["confidence"] == "high"
    assert any("peer_collapse" in item for item in result["evidence"])


def test_classify_incident_detects_provider_outage():
    base_incident = {
        "id": 1,
        "validator_key": "nH1",
        "event_types": ["agreement_drop_warning"],
        "latest_round_id": 22,
        "start_time": "2026-04-12T00:00:00+00:00",
        "duration_seconds": 600,
        "during_values": {},
        "before_values": {},
        "after_values": {},
    }
    related = [{**base_incident, "id": idx, "validator_key": f"nH{idx}"} for idx in range(1, 6)]
    round_scores = [make_score(f"nH{idx}", provider="Hetzner", version="3.0.0") for idx in range(1, 6)]
    latest_scores = [make_score(f"nH{idx}", provider="Hetzner", version="3.0.0") for idx in range(1, 6)]

    result = classify_incident(related[0], related_incidents=related, round_scores=round_scores, latest_scores=latest_scores)
    assert result["suspected_cause"] == "provider_outage"
    assert result["confidence"] == "high"
