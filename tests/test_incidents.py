import aiosqlite
import pytest

from app.database import Database
from app.incidents import detect_and_store_incidents, inject_synthetic_incident
from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores


def make_score(
    public_key: str,
    composite_score: float,
    *,
    agreement_1h: float = 0.99,
    agreement_24h: float = 0.99,
    agreement_30d: float = 0.99,
    peer_count: int = 12,
    server_version: str = "1.0.0",
    server_state: str = "proposing",
):
    return ValidatorScore(
        public_key=public_key,
        domain="validator.example.com",
        composite_score=composite_score,
        metrics=ValidatorMetrics(
            agreement_1h=agreement_1h,
            agreement_24h=agreement_24h,
            agreement_30d=agreement_30d,
            peer_count=peer_count,
            server_version=server_version,
            server_state=server_state,
        ),
        sub_scores=ValidatorSubScores(),
        last_updated="2026-04-04T00:00:00+00:00",
    )


@pytest.fixture
async def incident_db(tmp_path):
    db = Database(str(tmp_path / "incidents.db"))
    await db.init()
    return db


async def set_round_timestamp(db: Database, round_id: int, timestamp: str):
    async with aiosqlite.connect(db.db_path) as conn:
        await conn.execute(
            "UPDATE scoring_rounds SET timestamp = ? WHERE id = ?",
            (timestamp, round_id),
        )
        await conn.commit()


@pytest.mark.anyio
async def test_agreement_incident_opens_and_closes_after_two_recovery_rounds(incident_db):
    round_1 = await incident_db.store_round([make_score("nHA", 90.0)])
    round_2 = await incident_db.store_round([make_score("nHA", 70.0, agreement_1h=0.89)])

    await set_round_timestamp(incident_db, round_1, "2026-04-01T00:00:00+00:00")
    await set_round_timestamp(incident_db, round_2, "2026-04-01T00:05:00+00:00")

    await detect_and_store_incidents(incident_db, round_2)
    incidents = await incident_db.get_incidents()
    assert len(incidents) == 1
    assert incidents[0]["status"] == "open"
    assert incidents[0]["event_types"] == ["agreement_drop_critical"]

    round_3 = await incident_db.store_round([make_score("nHA", 88.0, agreement_1h=0.97)])
    await set_round_timestamp(incident_db, round_3, "2026-04-01T00:10:00+00:00")
    await detect_and_store_incidents(incident_db, round_3)
    incident = await incident_db.get_incident(incidents[0]["id"])
    assert incident["status"] == "open"

    round_4 = await incident_db.store_round([make_score("nHA", 89.0, agreement_1h=0.98)])
    await set_round_timestamp(incident_db, round_4, "2026-04-01T00:15:00+00:00")
    await detect_and_store_incidents(incident_db, round_4)
    incident = await incident_db.get_incident(incidents[0]["id"])
    events = await incident_db.get_incident_events(incident["id"])
    assert incident["status"] == "closed"
    assert incident["end_time"] == "2026-04-01T00:15:00+00:00"
    assert [event["event_phase"] for event in events] == ["triggered", "recovered"]


@pytest.mark.anyio
async def test_peer_collapse_requires_two_rounds(incident_db):
    round_1 = await incident_db.store_round([make_score("nHA", 90.0, peer_count=10)])
    round_2 = await incident_db.store_round([make_score("nHA", 85.0, peer_count=3)])

    await set_round_timestamp(incident_db, round_1, "2026-04-01T00:00:00+00:00")
    await set_round_timestamp(incident_db, round_2, "2026-04-01T00:05:00+00:00")

    await detect_and_store_incidents(incident_db, round_2)
    assert await incident_db.get_incidents() == []

    round_3 = await incident_db.store_round([make_score("nHA", 84.0, peer_count=2)])
    await set_round_timestamp(incident_db, round_3, "2026-04-01T00:10:00+00:00")
    await detect_and_store_incidents(incident_db, round_3)
    incidents = await incident_db.get_incidents()
    assert len(incidents) == 1
    assert incidents[0]["event_types"] == ["peer_collapse"]
    assert incidents[0]["status"] == "open"


@pytest.mark.anyio
async def test_version_change_creates_closed_info_incident(incident_db):
    round_1 = await incident_db.store_round([make_score("nHA", 90.0, server_version="1.0.0")])
    round_2 = await incident_db.store_round([make_score("nHA", 90.0, server_version="1.1.0")])

    await set_round_timestamp(incident_db, round_1, "2026-04-01T00:00:00+00:00")
    await set_round_timestamp(incident_db, round_2, "2026-04-01T00:05:00+00:00")

    await detect_and_store_incidents(incident_db, round_2)
    incidents = await incident_db.get_incidents()
    assert len(incidents) == 1
    assert incidents[0]["severity"] == "info"
    assert incidents[0]["status"] == "closed"
    assert incidents[0]["event_types"] == ["version_change"]


@pytest.mark.anyio
async def test_inject_synthetic_incident_creates_demo_record(incident_db):
    incident = await inject_synthetic_incident(incident_db, "nHSyntheticValidator")
    events = await incident_db.get_incident_events(incident["id"])

    assert incident["synthetic"] is True
    assert incident["status"] == "closed"
    assert incident["event_types"] == ["synthetic_test"]
    assert len(events) == 2
    assert {event["event_phase"] for event in events} == {"triggered", "recovered"}
