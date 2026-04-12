from unittest.mock import AsyncMock, patch

import pytest

from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores
from app.remediation import build_remediation_report


def make_score(
    public_key: str,
    *,
    domain: str | None = None,
    composite_score: float = 80.0,
    agreement_24h: float | None = 0.98,
    peer_count: int | None = 8,
    server_version: str | None = "1.0.0",
    server_state: str | None = "proposing",
    node_ip: str | None = "203.0.113.10",
) -> ValidatorScore:
    return ValidatorScore(
        public_key=public_key,
        domain=domain,
        composite_score=composite_score,
        metrics=ValidatorMetrics(
            agreement_24h=agreement_24h,
            peer_count=peer_count,
            server_version=server_version,
            server_state=server_state,
            node_ip=node_ip,
        ),
        sub_scores=ValidatorSubScores(diversity=0.4),
        last_updated="2026-04-12T12:00:00+00:00",
    )


@pytest.mark.anyio
async def test_remediation_report_deduplicates_shared_version_issue():
    scores = [
        make_score("nHTest1", domain="test1.example.com", server_version="1.0.0", peer_count=3, agreement_24h=0.91, server_state="syncing"),
        make_score("nHPeer", domain="peer.example.com", server_version="3.0.0", node_ip="203.0.113.11"),
    ]

    readiness_payload = {
        "public_key": "nHTest1",
        "domain": "test1.example.com",
        "round_id": 4,
        "timestamp": "2026-04-12T12:00:00+00:00",
        "overall_status": "not_ready",
        "status_summary": "Not Ready",
        "json_report_url": "/api/readiness/nHTest1",
        "checks": [
            {
                "name": "Version parity",
                "category": "configuration",
                "status": "fail",
                "detected_value": "1.0.0",
                "expected_value": "3.0.0",
                "remediation": "Upgrade the validator.",
                "source_timestamp": "2026-04-12T12:00:00+00:00",
            }
        ],
    }
    diagnose_payload = {
        "public_key": "nHTest1",
        "domain": "test1.example.com",
        "round_id": 4,
        "timestamp": "2026-04-12T12:00:00+00:00",
        "composite_score": 80.0,
        "rank": 2,
        "validator_count": 2,
        "overall_status": "critical",
        "status_summary": "Critical",
        "json_report_url": "/api/diagnose/nHTest1",
        "findings": [
            {
                "category": "fault",
                "metric": "version",
                "severity": "warning",
                "title": "Validator version is behind the current cohort majority",
                "current_value": "1.0.0",
                "threshold_value": "3.0.0",
                "likely_cause": "lagging",
                "recommended_action": "Upgrade to the current recommended version.",
            }
        ],
        "strengths": [],
    }
    peers_payload = {
        "public_key": "nHTest1",
        "domain": "test1.example.com",
        "mode": "candidate_only",
        "mode_banner": "Candidate-only",
        "json_report_url": "/api/peers/nHTest1",
        "disclaimer": "heuristic",
        "observable_node": None,
        "summary": {
            "total_nodes_analyzed": 2,
            "current_peer_count": 0,
            "good_count": 1,
            "acceptable_count": 1,
            "risky_count": 0,
            "projected_composite_score": 80.0,
            "projected_rank": 2,
            "projected_rank_delta": 0,
        },
        "risk_findings": [],
        "table_title": "Observed nodes",
        "node_rows": [],
        "add_recommendations": [],
        "drop_recommendations": [],
    }

    db = AsyncMock()
    db.get_incidents = AsyncMock(return_value=[])

    with patch("app.remediation.build_readiness_report", new=AsyncMock(return_value=readiness_payload)), \
         patch("app.remediation.build_diagnostic_report", return_value=diagnose_payload), \
         patch("app.remediation.build_peer_report", new=AsyncMock(return_value=peers_payload)):
        report = await build_remediation_report(db, 4, "2026-04-12T12:00:00+00:00", scores, "nHTest1")

    version_items = [item for item in report["actionable_findings"] if item["metric"] == "version"]
    assert len(version_items) == 1
    assert sorted(version_items[0]["sources"]) == ["diagnose", "readiness"]
    assert version_items[0]["estimated_score_impact"] == 10.0


@pytest.mark.anyio
async def test_remediation_report_separates_actionable_and_advisory():
    scores = [make_score("nHTest1", domain="test1.example.com"), make_score("nHPeer", domain="peer.example.com", node_ip="203.0.113.11")]

    readiness_payload = {
        "public_key": "nHTest1",
        "domain": "test1.example.com",
        "round_id": 4,
        "timestamp": "2026-04-12T12:00:00+00:00",
        "overall_status": "ready",
        "status_summary": "Ready",
        "json_report_url": "/api/readiness/nHTest1",
        "checks": [],
    }
    diagnose_payload = {
        "public_key": "nHTest1",
        "domain": "test1.example.com",
        "round_id": 4,
        "timestamp": "2026-04-12T12:00:00+00:00",
        "composite_score": 80.0,
        "rank": 1,
        "validator_count": 2,
        "overall_status": "healthy",
        "status_summary": "Healthy",
        "json_report_url": "/api/diagnose/nHTest1",
        "findings": [
            {
                "category": "advisory",
                "metric": "diversity",
                "severity": "advisory",
                "title": "Hosting concentration is limiting your diversity score",
                "current_value": "40.0%",
                "threshold_value": "> 50.0% diversity sub-score",
                "likely_cause": "concentration",
                "recommended_action": "Consider moving over time.",
            }
        ],
        "strengths": [],
    }
    peers_payload = {
        "public_key": "nHTest1",
        "domain": "test1.example.com",
        "mode": "candidate_only",
        "mode_banner": "Candidate-only",
        "json_report_url": "/api/peers/nHTest1",
        "disclaimer": "heuristic",
        "observable_node": None,
        "summary": {
            "total_nodes_analyzed": 2,
            "current_peer_count": 0,
            "good_count": 1,
            "acceptable_count": 1,
            "risky_count": 0,
            "projected_composite_score": 80.0,
            "projected_rank": 1,
            "projected_rank_delta": 0,
        },
        "risk_findings": [],
        "table_title": "Observed nodes",
        "node_rows": [],
        "add_recommendations": [],
        "drop_recommendations": [],
    }
    db = AsyncMock()
    db.get_incidents = AsyncMock(return_value=[])

    with patch("app.remediation.build_readiness_report", new=AsyncMock(return_value=readiness_payload)), \
         patch("app.remediation.build_diagnostic_report", return_value=diagnose_payload), \
         patch("app.remediation.build_peer_report", new=AsyncMock(return_value=peers_payload)):
        report = await build_remediation_report(db, 4, "2026-04-12T12:00:00+00:00", scores, "nHTest1")

    assert report["actionable_findings"] == []
    assert len(report["advisories"]) == 1
    assert report["status_summary"] == "No action needed"
