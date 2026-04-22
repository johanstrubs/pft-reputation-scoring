from unittest.mock import AsyncMock

import pytest

from app.dataset import (
    build_dataset_diff,
    build_dataset_timeseries,
    build_latest_dataset_snapshot,
    build_risk_report,
)


def make_round(round_id: int, snapshot_date: str, validator_count: int = 2) -> dict:
    return {
        "id": round_id,
        "timestamp": f"{snapshot_date}T23:55:00+00:00",
        "validator_count": validator_count,
        "avg_score": 80.0,
        "min_score": 70.0,
        "max_score": 90.0,
        "snapshot_date": snapshot_date,
    }


def make_score_row(
    round_id: int,
    snapshot_date: str,
    public_key: str,
    score: float,
    version: str,
    provider: str,
    country: str,
    asn: int,
    rank_hint: int,
) -> dict:
    return {
        "round_id": round_id,
        "round_timestamp": f"{snapshot_date}T23:55:00+00:00",
        "validator_count": 2,
        "avg_score": 80.0,
        "min_score": 70.0,
        "max_score": 90.0,
        "snapshot_date": snapshot_date,
        "public_key": public_key,
        "domain": f"{public_key}.example.com",
        "composite_score": score,
        "agreement_1h": 0.99,
        "agreement_1h_total": 10,
        "agreement_24h": 0.98,
        "agreement_24h_total": 20,
        "agreement_30d": 0.97,
        "agreement_30d_total": 30,
        "poll_success_pct": 99.0,
        "uptime_seconds": 86400,
        "uptime_pct": 100.0,
        "latency_ms": 40.0 + rank_hint,
        "peer_count": 10 + rank_hint,
        "avg_ledger_interval": 3.6,
        "validated_ledger_age": 2.0,
        "server_version": version,
        "server_state": "proposing",
        "asn": asn,
        "isp": provider,
        "country": country,
        "node_ip": f"10.0.0.{rank_hint}",
        "agreement_1h_score": 0.95,
        "agreement_24h_score": 0.94,
        "agreement_30d_score": 0.93,
        "uptime_score": 0.92,
        "poll_success_score": 0.91,
        "latency_score": 0.90,
        "peer_count_score": 0.89,
        "version_score": 1.0,
        "diversity_score": 0.75,
        "timestamp": f"{snapshot_date}T23:55:00+00:00",
    }


@pytest.mark.anyio
async def test_build_latest_dataset_snapshot_includes_metadata_and_health_formula():
    db = AsyncMock()
    daily_rounds = [make_round(10, "2026-04-20"), make_round(11, "2026-04-21")]
    db.get_daily_snapshot_rounds.return_value = daily_rounds
    db.get_validator_score_rows_for_round_ids.return_value = [
        make_score_row(10, "2026-04-20", "nHA", 88.0, "1.0.0", "Hetzner", "DE", 24940, 1),
        make_score_row(10, "2026-04-20", "nHB", 79.0, "1.0.0", "Vultr", "US", 20473, 2),
        make_score_row(11, "2026-04-21", "nHA", 90.0, "1.1.0", "Hetzner", "DE", 24940, 1),
        make_score_row(11, "2026-04-21", "nHB", 81.0, "1.0.0", "Vultr", "US", 20473, 2),
    ]
    db.get_incidents_open_as_of.side_effect = [
        [],
        [{"id": 1, "validator_key": "nHB", "severity": "warning", "status": "open", "summary": "peer drop", "start_time": "2026-04-21T22:00:00+00:00", "end_time": None, "latest_round_id": 11, "latest_event_time": "2026-04-21T23:55:00+00:00", "event_types": ["peer_count_low"], "active_event_types": ["peer_count_low"], "synthetic": False, "correlated": False}],
    ]

    snapshot = await build_latest_dataset_snapshot(db)

    assert snapshot["snapshot_date"] == "2026-04-21"
    assert snapshot["dataset_metadata"]["total_daily_snapshots"] == 2
    assert snapshot["dataset_metadata"]["total_validator_day_score_records"] == 4
    assert snapshot["network_health_index"]["formula_version"] == "v1"
    assert "provider_concentration" in snapshot["network_health_index"]["components"]
    assert snapshot["incidents"]["open_incident_count"] == 1


@pytest.mark.anyio
async def test_build_dataset_diff_returns_expected_sections():
    db = AsyncMock()
    daily_rounds = [make_round(20, "2026-04-20"), make_round(21, "2026-04-21")]
    db.get_daily_snapshot_rounds.return_value = daily_rounds
    db.get_validator_score_rows_for_round_ids.return_value = [
        make_score_row(20, "2026-04-20", "nHA", 88.0, "1.0.0", "Hetzner", "DE", 24940, 1),
        make_score_row(20, "2026-04-20", "nHB", 79.0, "1.0.0", "Vultr", "US", 20473, 2),
        make_score_row(21, "2026-04-21", "nHA", 85.0, "1.1.0", "Hetzner", "DE", 24940, 1),
        make_score_row(21, "2026-04-21", "nHC", 83.0, "1.1.0", "Cherry", "FI", 212317, 2),
    ]
    db.get_incidents_open_as_of.side_effect = [
        [{"id": 1, "validator_key": "nHB", "severity": "warning", "status": "open", "summary": "peer drop", "start_time": "2026-04-20T22:00:00+00:00", "end_time": None, "latest_round_id": 20, "latest_event_time": "2026-04-20T23:55:00+00:00", "event_types": ["peer_count_low"], "active_event_types": ["peer_count_low"], "synthetic": False, "correlated": False}],
        [{"id": 2, "validator_key": "nHC", "severity": "critical", "status": "open", "summary": "disappeared", "start_time": "2026-04-21T22:00:00+00:00", "end_time": None, "latest_round_id": 21, "latest_event_time": "2026-04-21T23:55:00+00:00", "event_types": ["validator_disappearance"], "active_event_types": ["validator_disappearance"], "synthetic": False, "correlated": False}],
        [{"id": 1, "validator_key": "nHB", "severity": "warning", "status": "open", "summary": "peer drop", "start_time": "2026-04-20T22:00:00+00:00", "end_time": None, "latest_round_id": 20, "latest_event_time": "2026-04-20T23:55:00+00:00", "event_types": ["peer_count_low"], "active_event_types": ["peer_count_low"], "synthetic": False, "correlated": False}],
        [{"id": 2, "validator_key": "nHC", "severity": "critical", "status": "open", "summary": "disappeared", "start_time": "2026-04-21T22:00:00+00:00", "end_time": None, "latest_round_id": 21, "latest_event_time": "2026-04-21T23:55:00+00:00", "event_types": ["validator_disappearance"], "active_event_types": ["validator_disappearance"], "synthetic": False, "correlated": False}],
    ]

    diff = await build_dataset_diff(db, "2026-04-20", "2026-04-21")

    assert diff["validators_added"] == ["nHC"]
    assert diff["validators_removed"] == ["nHB"]
    assert diff["incidents_opened"][0]["id"] == 2
    assert diff["incidents_closed"][0]["id"] == 1
    assert any(change["public_key"] == "nHA" for change in diff["score_changes"])
    assert "providers" in diff["concentration_deltas"]


@pytest.mark.anyio
async def test_build_dataset_timeseries_caps_days_and_returns_history():
    db = AsyncMock()
    daily_rounds = [make_round(30, "2026-04-20"), make_round(31, "2026-04-21")]
    db.get_daily_snapshot_rounds.return_value = daily_rounds
    db.get_validator_score_rows_for_round_ids.return_value = [
        make_score_row(30, "2026-04-20", "nHA", 88.0, "1.0.0", "Hetzner", "DE", 24940, 1),
        make_score_row(31, "2026-04-21", "nHA", 90.0, "1.1.0", "Hetzner", "DE", 24940, 1),
    ]
    db.get_incidents_open_as_of.side_effect = [[], []]

    report = await build_dataset_timeseries(db, "nHA", days=999)

    assert report["days"] == 365
    assert len(report["history"]) == 2
    assert report["history"][0]["date"] == "2026-04-20"


@pytest.mark.anyio
async def test_build_risk_report_returns_trend():
    db = AsyncMock()
    daily_rounds = [make_round(40, "2026-04-20"), make_round(41, "2026-04-21")]
    db.get_daily_snapshot_rounds.return_value = daily_rounds
    db.get_validator_score_rows_for_round_ids.return_value = [
        make_score_row(40, "2026-04-20", "nHA", 88.0, "1.0.0", "Hetzner", "DE", 24940, 1),
        make_score_row(40, "2026-04-20", "nHB", 84.0, "1.0.0", "Vultr", "US", 20473, 2),
        make_score_row(41, "2026-04-21", "nHA", 90.0, "1.1.0", "Hetzner", "DE", 24940, 1),
        make_score_row(41, "2026-04-21", "nHB", 87.0, "1.1.0", "Vultr", "US", 20473, 2),
    ]
    db.get_incidents_open_as_of.side_effect = [[], []]

    report = await build_risk_report(db)

    assert report["snapshot_date"] == "2026-04-21"
    assert len(report["trend_7d"]) == 2
    assert "version_adoption" in report["components"]
