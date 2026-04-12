import pytest

from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores
from app.upgrades import build_upgrade_report


def make_score(public_key: str, version: str | None, domain: str | None = None) -> ValidatorScore:
    return ValidatorScore(
        public_key=public_key,
        domain=domain,
        composite_score=80.0,
        metrics=ValidatorMetrics(server_version=version),
        sub_scores=ValidatorSubScores(),
        last_updated="2026-04-12T12:00:00+00:00",
    )


def test_upgrade_report_uses_highest_semver_not_majority():
    scores = [
        make_score("nHA", "1.0.0"),
        make_score("nHB", "1.0.0"),
        make_score("nHC", "v1.1.0"),
    ]
    history_rows = [
        {"round_id": 1, "round_timestamp": "2026-04-10T00:00:00+00:00", "validator_count": 3, "public_key": "nHA", "domain": None, "server_version": "1.0.0"},
        {"round_id": 1, "round_timestamp": "2026-04-10T00:00:00+00:00", "validator_count": 3, "public_key": "nHB", "domain": None, "server_version": "1.0.0"},
        {"round_id": 1, "round_timestamp": "2026-04-10T00:00:00+00:00", "validator_count": 3, "public_key": "nHC", "domain": None, "server_version": "v1.1.0"},
    ]

    report = build_upgrade_report(10, "2026-04-12T12:00:00+00:00", scores, history_rows)

    assert report["latest_version"] == "1.1.0"
    assert report["upgraded_count"] == 1
    assert len(report["lagging_validators"]) == 2


def test_upgrade_report_builds_days_behind_and_daily_history():
    scores = [
        make_score("nHA", "1.2.0", "alpha.example.com"),
        make_score("nHB", "1.1.0", "beta.example.com"),
    ]
    history_rows = [
        {"round_id": 1, "round_timestamp": "2026-04-09T08:00:00+00:00", "validator_count": 2, "public_key": "nHA", "domain": "alpha.example.com", "server_version": "1.2.0"},
        {"round_id": 1, "round_timestamp": "2026-04-09T08:00:00+00:00", "validator_count": 2, "public_key": "nHB", "domain": "beta.example.com", "server_version": "1.1.0"},
        {"round_id": 2, "round_timestamp": "2026-04-10T08:00:00+00:00", "validator_count": 2, "public_key": "nHA", "domain": "alpha.example.com", "server_version": "1.2.0"},
        {"round_id": 2, "round_timestamp": "2026-04-10T08:00:00+00:00", "validator_count": 2, "public_key": "nHB", "domain": "beta.example.com", "server_version": "1.1.0"},
        {"round_id": 3, "round_timestamp": "2026-04-11T08:00:00+00:00", "validator_count": 2, "public_key": "nHA", "domain": "alpha.example.com", "server_version": "1.2.0"},
        {"round_id": 3, "round_timestamp": "2026-04-11T08:00:00+00:00", "validator_count": 2, "public_key": "nHB", "domain": "beta.example.com", "server_version": "1.2.0"},
    ]

    report = build_upgrade_report(3, "2026-04-12T12:00:00+00:00", scores, history_rows)

    assert report["latest_version"] == "1.2.0"
    assert report["lagging_validators"][0]["public_key"] == "nHB"
    assert report["lagging_validators"][0]["days_behind"] >= 3
    assert report["adoption_history"][0]["date"] == "2026-04-09"
    assert report["adoption_history"][0]["percentage"] == 50.0
    assert report["adoption_history"][-1]["percentage"] == 100.0


def test_upgrade_report_handles_unknown_versions_gracefully():
    scores = [make_score("nHA", None), make_score("nHB", "garbled"), make_score("nHC", "1.0.0")]
    history_rows = [
        {"round_id": 1, "round_timestamp": "2026-04-12T00:00:00+00:00", "validator_count": 3, "public_key": "nHA", "domain": None, "server_version": None},
        {"round_id": 1, "round_timestamp": "2026-04-12T00:00:00+00:00", "validator_count": 3, "public_key": "nHB", "domain": None, "server_version": "garbled"},
        {"round_id": 1, "round_timestamp": "2026-04-12T00:00:00+00:00", "validator_count": 3, "public_key": "nHC", "domain": None, "server_version": "1.0.0"},
    ]

    report = build_upgrade_report(1, "2026-04-12T12:00:00+00:00", scores, history_rows)

    assert report["latest_version"] == "1.0.0"
    assert any(entry["version"] == "unknown" for entry in report["version_distribution"])
    assert any(entry["version"] == "garbled" for entry in report["version_distribution"])
