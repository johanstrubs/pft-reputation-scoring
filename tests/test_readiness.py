from unittest.mock import AsyncMock, patch

import pytest

from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores
from app.readiness import build_readiness_report


def make_score(
    public_key: str,
    *,
    domain: str | None = "validator.example.com",
    composite_score: float = 80.0,
    agreement_24h: float | None = 0.98,
    peer_count: int | None = 8,
    server_version: str | None = "1.0.0",
    server_state: str | None = "proposing",
    avg_ledger_interval: float | None = 4.0,
    validated_ledger_age: float | None = None,
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
            avg_ledger_interval=avg_ledger_interval,
            validated_ledger_age=validated_ledger_age,
            node_ip=node_ip,
        ),
        sub_scores=ValidatorSubScores(),
        last_updated="2026-04-10T12:00:00+00:00",
    )


@pytest.mark.anyio
async def test_readiness_prefers_validated_ledger_age():
    scores = [
        make_score("nHReady", validated_ledger_age=6.0, avg_ledger_interval=22.0),
        make_score("nHPeer", domain="peer.example.com"),
    ]

    with patch("app.readiness._resolve_domain_ips", new=AsyncMock(return_value=["203.0.113.10"])), \
         patch("app.readiness._fetch_well_known", new=AsyncMock(return_value=('[[VALIDATORS]]\npublic_key = "nHReady"\n', None))):
        report = await build_readiness_report(12, "2026-04-10T12:00:00+00:00", scores, "nHReady")

    ledger_check = next(check for check in report["checks"] if check["name"] == "Ledger freshness")
    assert ledger_check["status"] == "pass"
    assert "validated_ledger_age" in ledger_check["detected_value"]
    assert report["overall_status"] == "ready"


@pytest.mark.anyio
async def test_readiness_falls_back_to_avg_ledger_interval_and_warns():
    scores = [
        make_score(
            "nHWarn",
            validated_ledger_age=None,
            avg_ledger_interval=16.0,
            peer_count=4,
            agreement_24h=0.93,
            server_state="syncing",
        ),
        make_score("nHPeer", domain="peer.example.com"),
    ]

    with patch("app.readiness._resolve_domain_ips", new=AsyncMock(return_value=["203.0.113.10"])), \
         patch("app.readiness._fetch_well_known", new=AsyncMock(return_value=('[[VALIDATORS]]\npublic_key = "nHWarn"\n', None))):
        report = await build_readiness_report(12, "2026-04-10T12:00:00+00:00", scores, "nHWarn")

    ledger_check = next(check for check in report["checks"] if check["name"] == "Ledger freshness")
    assert ledger_check["status"] == "warn"
    assert "avg_ledger_interval" in ledger_check["detected_value"]
    assert report["overall_status"] == "not_ready"


@pytest.mark.anyio
async def test_readiness_warns_when_domain_missing():
    scores = [make_score("nHNoDomain", domain=None), make_score("nHPeer", domain="peer.example.com")]

    report = await build_readiness_report(12, "2026-04-10T12:00:00+00:00", scores, "nHNoDomain")

    domain_check = next(check for check in report["checks"] if check["name"] == "Domain configured")
    dns_check = next(check for check in report["checks"] if check["name"] == "Domain DNS match")
    well_known_check = next(check for check in report["checks"] if check["name"] == "Well-known attestation")

    assert domain_check["status"] == "warn"
    assert dns_check["detected_value"].startswith("skipped")
    assert well_known_check["detected_value"].startswith("skipped")


@pytest.mark.anyio
async def test_readiness_warns_on_well_known_mismatch():
    scores = [make_score("nHMismatch"), make_score("nHPeer", domain="peer.example.com")]

    with patch("app.readiness._resolve_domain_ips", new=AsyncMock(return_value=["203.0.113.10"])), \
         patch("app.readiness._fetch_well_known", new=AsyncMock(return_value=('[[VALIDATORS]]\npublic_key = "nHOther"\n', None))):
        report = await build_readiness_report(12, "2026-04-10T12:00:00+00:00", scores, "nHMismatch")

    well_known_check = next(check for check in report["checks"] if check["name"] == "Well-known attestation")
    assert well_known_check["status"] == "warn"
    assert "mismatched" in well_known_check["detected_value"]
