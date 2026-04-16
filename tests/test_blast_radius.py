from unittest.mock import AsyncMock

import pytest

from app.blast_radius import build_blast_radius_report, detect_and_store_correlated_events
from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores


def make_score(public_key: str, *, provider: str, asn: int, country: str, composite_score: float = 80.0) -> ValidatorScore:
    return ValidatorScore(
        public_key=public_key,
        domain=f"{public_key}.example.com",
        composite_score=composite_score,
        metrics=ValidatorMetrics(isp=provider, asn=asn, country=country),
        sub_scores=ValidatorSubScores(),
        last_updated="2026-04-16T12:00:00+00:00",
    )


@pytest.mark.anyio
async def test_detect_and_store_correlated_events_creates_provider_event():
    db = AsyncMock()
    db.get_round_summary.return_value = {"id": 10, "timestamp": "2026-04-16T12:00:00+00:00"}
    db.get_recent_round_summaries.return_value = [
        {"id": 10, "timestamp": "2026-04-16T12:00:00+00:00"},
        {"id": 9, "timestamp": "2026-04-16T11:55:00+00:00"},
    ]
    current_scores = [
        make_score("nH1", provider="Hetzner", asn=24940, country="FI", composite_score=70.0),
        make_score("nH2", provider="Hetzner", asn=24940, country="FI", composite_score=71.0),
        make_score("nH3", provider="Hetzner", asn=24940, country="FI", composite_score=72.0),
        make_score("nH4", provider="Other", asn=64500, country="US", composite_score=90.0),
    ]
    previous_scores = [
        make_score("nH1", provider="Hetzner", asn=24940, country="FI", composite_score=80.0),
        make_score("nH2", provider="Hetzner", asn=24940, country="FI", composite_score=81.0),
        make_score("nH3", provider="Hetzner", asn=24940, country="FI", composite_score=82.0),
        make_score("nH4", provider="Other", asn=64500, country="US", composite_score=90.0),
    ]
    db.get_scores_for_round.side_effect = [current_scores, previous_scores]
    db.get_incidents_for_round.return_value = [
        {"id": 1, "validator_key": "nH1", "status": "open", "latest_round_id": 10},
        {"id": 2, "validator_key": "nH2", "status": "open", "latest_round_id": 10},
        {"id": 3, "validator_key": "nH3", "status": "open", "latest_round_id": 10},
    ]
    db.get_open_correlated_event_by_key.return_value = None
    db.get_open_correlated_events.return_value = []

    await detect_and_store_correlated_events(db, 10)

    assert db.create_correlated_event.await_count >= 1
    kwargs = db.create_correlated_event.await_args_list[0].kwargs
    assert kwargs["correlation_type"] == "provider"
    assert kwargs["dependency_value"] == "Hetzner"
    assert kwargs["affected_count"] == 3
    assert kwargs["avg_score_drop"] == 10.0


@pytest.mark.anyio
async def test_detect_and_store_correlated_events_closes_missing_open_event():
    db = AsyncMock()
    db.get_round_summary.return_value = {"id": 12, "timestamp": "2026-04-16T12:10:00+00:00"}
    db.get_recent_round_summaries.return_value = [
        {"id": 12, "timestamp": "2026-04-16T12:10:00+00:00"},
        {"id": 11, "timestamp": "2026-04-16T12:05:00+00:00"},
    ]
    db.get_scores_for_round.side_effect = [[make_score("nH4", provider="Other", asn=64500, country="US")], []]
    db.get_incidents_for_round.return_value = []
    db.get_open_correlated_events.return_value = [{
        "id": 99,
        "correlation_type": "provider",
        "dependency_value": "Hetzner",
        "severity": "warning",
        "status": "open",
        "affected_validators": ["nH1", "nH2", "nH3"],
        "triggering_incident_ids": [1, 2, 3],
        "affected_count": 3,
        "network_pct": 15.0,
        "consensus_risk": False,
        "avg_score_drop": 8.0,
        "peak_affected_count": 3,
        "peak_network_pct": 15.0,
        "remaining_validators_if_failed": 10,
        "mitigation_guidance": "guidance",
        "suspected_cause": "Shared provider dependency: Hetzner",
    }]

    await detect_and_store_correlated_events(db, 12)

    assert db.update_correlated_event.await_count == 1
    kwargs = db.update_correlated_event.await_args.kwargs
    assert kwargs["status"] == "closed"
    assert kwargs["end_timestamp"] == "2026-04-16T12:10:00+00:00"


@pytest.mark.anyio
async def test_build_blast_radius_report_includes_risks_and_events():
    db = AsyncMock()
    scores = [
        make_score("nH1", provider="Hetzner", asn=24940, country="FI"),
        make_score("nH2", provider="Hetzner", asn=24940, country="FI"),
        make_score("nH3", provider="Hetzner", asn=24940, country="FI"),
        make_score("nH4", provider="Other", asn=64500, country="US"),
    ]
    db.get_latest_scores.return_value = (20, "2026-04-16T12:20:00+00:00", scores)
    db.get_correlated_events.side_effect = [
        [{
            "id": 1,
            "correlation_type": "provider",
            "dependency_value": "Hetzner",
            "severity": "critical",
            "status": "open",
            "synthetic": False,
            "start_round_id": 20,
            "latest_round_id": 20,
            "start_timestamp": "2026-04-16T12:20:00+00:00",
            "latest_timestamp": "2026-04-16T12:20:00+00:00",
            "end_timestamp": None,
            "duration_seconds": None,
            "affected_validators": ["nH1", "nH2", "nH3"],
            "triggering_incident_ids": [1, 2, 3],
            "affected_count": 3,
            "network_pct": 75.0,
            "consensus_risk": True,
            "avg_score_drop": 8.0,
            "peak_affected_count": 3,
            "peak_network_pct": 75.0,
            "remaining_validators_if_failed": 1,
            "mitigation_guidance": "guidance",
            "suspected_cause": "Shared provider dependency: Hetzner",
        }],
        [],
    ]

    report = await build_blast_radius_report(db)

    assert report["round_id"] == 20
    assert report["active_correlations"][0]["dependency_value"] == "Hetzner"
    assert report["concentration_risks"][0]["affected_validators"] >= 3
