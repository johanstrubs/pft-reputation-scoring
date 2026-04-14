from unittest.mock import AsyncMock, patch

import pytest

from app.improvements import _compute_tracking_state, build_improvement_report
from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores


def make_score(public_key: str, *, composite_score: float, version: float = 0.5) -> ValidatorScore:
    return ValidatorScore(
        public_key=public_key,
        domain=f"{public_key}.example.com",
        composite_score=composite_score,
        metrics=ValidatorMetrics(server_version="1.0.0"),
        sub_scores=ValidatorSubScores(version=version),
        last_updated="2026-04-14T12:00:00+00:00",
    )


def test_compute_tracking_state_requires_two_absent_snapshots():
    runs = [
        {"public_key": "nHTest1", "snapshot_date": "2026-04-10", "round_id": 10},
        {"public_key": "nHTest1", "snapshot_date": "2026-04-11", "round_id": 11},
        {"public_key": "nHTest1", "snapshot_date": "2026-04-12", "round_id": 12},
        {"public_key": "nHTest1", "snapshot_date": "2026-04-13", "round_id": 13},
    ]
    rows = [
        {
            "public_key": "nHTest1",
            "snapshot_date": "2026-04-10",
            "round_id": 10,
            "finding_key": "version::version::3.0.0",
            "title": "Version parity",
            "category": "version",
            "metric": "version",
            "severity": "critical",
            "detected_value": "1.0.0",
            "expected_value": "3.0.0",
            "estimated_impact": 10.0,
            "impact_confidence": "direct",
        },
        {
            "public_key": "nHTest1",
            "snapshot_date": "2026-04-11",
            "round_id": 11,
            "finding_key": "version::version::3.0.0",
            "title": "Version parity",
            "category": "version",
            "metric": "version",
            "severity": "critical",
            "detected_value": "1.0.0",
            "expected_value": "3.0.0",
            "estimated_impact": 10.0,
            "impact_confidence": "direct",
        },
    ]

    resolutions, open_findings = _compute_tracking_state(runs, rows)

    assert len(resolutions) == 1
    assert resolutions[0]["opened_date"] == "2026-04-10"
    assert resolutions[0]["resolved_date"] == "2026-04-13"
    assert open_findings == []


@pytest.mark.anyio
async def test_build_improvement_report_uses_demo_seed_when_no_real_resolutions():
    scores = [
        make_score("nHTest1", composite_score=82.0, version=0.5),
        make_score("nHPeer", composite_score=90.0, version=1.0),
    ]
    round_scores = {
        1: scores,
    }

    db = AsyncMock()
    db.has_improvement_snapshots = AsyncMock(return_value=True)
    db.get_latest_scores = AsyncMock(return_value=(1, "2026-04-14T12:00:00+00:00", scores))
    db.get_improvement_snapshot_runs = AsyncMock(return_value=[{"public_key": "nHTest1", "snapshot_date": "2026-04-14", "round_id": 1}])
    db.get_improvement_snapshot_rows = AsyncMock(return_value=[])
    db.get_demo_improvement_resolutions = AsyncMock(return_value=[{
        "public_key": "nHTest1",
        "finding_key": "version::version::3.0.0",
        "title": "Version parity (Demo)",
        "category": "version",
        "metric": "version",
        "severity": "warning",
        "opened_date": "2026-04-11",
        "resolved_date": "2026-04-14",
        "detected_value": "1.0.0",
        "expected_value": "3.0.0",
        "score_before": 77.0,
        "score_after": 82.0,
        "rank_before": 4,
        "rank_after": 2,
        "estimated_impact": 5.0,
        "impact_confidence": "direct",
    }])
    db.get_improvement_tracking_since = AsyncMock(return_value="2026-04-14")
    db.get_scores_for_round = AsyncMock(side_effect=lambda round_id: round_scores[round_id])

    with patch("app.improvements._build_current_non_passing_findings", new=AsyncMock(return_value=[])), \
         patch("app.improvements._all_real_tracking_state", new=AsyncMock(return_value=([], []))):
        report = await build_improvement_report(db, "nHTest1")

    assert report["demo_mode"] is True
    assert report["resolved_findings"][0]["synthetic"] is True
    assert report["resolved_findings"][0]["score_delta"] == 5.0
