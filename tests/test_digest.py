import aiosqlite
import pytest

from app.database import Database
from app.digest import build_weekly_digest, generate_and_store_weekly_digest
from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores


def make_score(public_key, score, *, domain=None, isp="Hetzner", country="DE", asn=24940):
    return ValidatorScore(
        public_key=public_key,
        domain=domain,
        composite_score=score,
        metrics=ValidatorMetrics(
            agreement_1h=0.99,
            agreement_1h_total=100,
            agreement_24h=0.98,
            agreement_24h_total=1000,
            agreement_30d=0.97,
            agreement_30d_total=5000,
            uptime_seconds=86400,
            uptime_pct=100.0,
            latency_ms=42.0,
            peer_count=12,
            poll_success_pct=99.0,
            server_version="1.0.0",
            isp=isp,
            country=country,
            asn=asn,
        ),
        sub_scores=ValidatorSubScores(
            agreement_1h=0.95,
            agreement_24h=0.90,
            agreement_30d=0.85,
            uptime=1.0,
            poll_success=1.0,
            latency=0.91,
            peer_count=1.0,
            version=1.0,
            diversity=0.8,
        ),
        last_updated="2026-04-04T00:00:00+00:00",
    )


@pytest.fixture
async def digest_db(tmp_path):
    db = Database(str(tmp_path / "digest.db"))
    await db.init()

    previous_scores = [
        make_score("nHA", 90.0, domain="alpha.example.com", isp="Hetzner", country="DE", asn=24940),
        make_score("nHC", 85.0, domain="charlie.example.com", isp="Vultr", country="US", asn=20473),
        make_score("nHB", 80.0, domain="bravo.example.com", isp="Vultr", country="US", asn=20473),
    ]
    latest_scores = [
        make_score("nHD", 95.0, domain="delta.example.com", isp="Cherry", country="FI", asn=204770),
        make_score("nHB", 88.5, domain="bravo.example.com", isp="Vultr", country="US", asn=20473),
        make_score("nHA", 82.0, domain="alpha.example.com", isp="Hetzner", country="DE", asn=24940),
    ]

    old_round_id = await db.store_round(previous_scores)
    latest_round_id = await db.store_round(latest_scores)

    async with aiosqlite.connect(db.db_path) as conn:
        await conn.execute(
            "UPDATE scoring_rounds SET timestamp = ? WHERE id = ?",
            ("2026-03-25T12:00:00+00:00", old_round_id),
        )
        await conn.execute(
            "UPDATE scoring_rounds SET timestamp = ? WHERE id = ?",
            ("2026-04-01T12:00:00+00:00", latest_round_id),
        )
        await conn.commit()

    return db


@pytest.mark.anyio
async def test_build_weekly_digest(digest_db):
    payload = await build_weekly_digest(digest_db)

    assert payload["window"]["latest_round"]["id"] > payload["window"]["comparison_round"]["id"]
    assert payload["summary"]["joins_count"] == 1
    assert payload["summary"]["departures_count"] == 1
    assert payload["joins"][0]["public_key"] == "nHD"
    assert payload["departures"][0]["public_key"] == "nHC"
    assert payload["top_rank_gainers"][0]["public_key"] == "nHB"
    assert payload["top_rank_losers"][0]["public_key"] == "nHA"
    assert {row["public_key"] for row in payload["score_change_alerts"]} == {"nHA", "nHB"}
    assert payload["concentration"]["coverage"]["current"]["enriched"] == 3
    assert payload["concentration"]["providers"]
    assert payload["concentration"]["countries"]
    assert payload["concentration"]["asns"]


@pytest.mark.anyio
async def test_generate_and_store_weekly_digest(digest_db, monkeypatch):
    async def fake_send(_webhook_url, _embed):
        return {
            "delivery_status": "posted",
            "posted_at": "2026-04-01T12:05:00+00:00",
            "message_id": "1234567890",
        }

    monkeypatch.setattr("app.digest.send_weekly_digest_to_discord", fake_send)

    stored = await generate_and_store_weekly_digest(digest_db, webhook_url="https://discord.com/api/webhooks/test")
    latest = await digest_db.get_latest_digest()

    assert stored["delivery_status"] == "posted"
    assert stored["message_id"] == "1234567890"
    assert latest is not None
    assert latest["payload"]["summary"]["joins_count"] == 1
