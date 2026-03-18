import aiosqlite
import os
from datetime import datetime, timezone

from app.config import settings
from app.models import ValidatorScore, RoundSummary

SCHEMA = """
CREATE TABLE IF NOT EXISTS scoring_rounds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    validator_count INTEGER NOT NULL,
    avg_score REAL,
    min_score REAL,
    max_score REAL
);

CREATE TABLE IF NOT EXISTS validator_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id INTEGER NOT NULL,
    public_key TEXT NOT NULL,
    domain TEXT,
    composite_score REAL NOT NULL,
    agreement_1h REAL,
    agreement_24h REAL,
    agreement_30d REAL,
    uptime_seconds INTEGER,
    latency_ms REAL,
    peer_count INTEGER,
    server_version TEXT,
    server_state TEXT,
    asn INTEGER,
    isp TEXT,
    country TEXT,
    agreement_1h_score REAL,
    agreement_24h_score REAL,
    agreement_30d_score REAL,
    uptime_score REAL,
    latency_score REAL,
    peer_count_score REAL,
    version_score REAL,
    diversity_score REAL,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES scoring_rounds(id)
);

CREATE INDEX IF NOT EXISTS idx_validator_scores_pubkey ON validator_scores(public_key);
CREATE INDEX IF NOT EXISTS idx_validator_scores_round ON validator_scores(round_id);
"""


class Database:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or settings.database_path
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA)
            await db.commit()

    async def store_round(self, scores: list[ValidatorScore]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        composite_scores = [s.composite_score for s in scores]
        avg_score = sum(composite_scores) / len(composite_scores) if composite_scores else None
        min_score = min(composite_scores) if composite_scores else None
        max_score = max(composite_scores) if composite_scores else None

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "INSERT INTO scoring_rounds (timestamp, validator_count, avg_score, min_score, max_score) VALUES (?, ?, ?, ?, ?)",
                (now, len(scores), avg_score, min_score, max_score),
            )
            round_id = cursor.lastrowid

            for s in scores:
                await db.execute(
                    """INSERT INTO validator_scores
                    (round_id, public_key, domain, composite_score,
                     agreement_1h, agreement_24h, agreement_30d,
                     uptime_seconds, latency_ms, peer_count,
                     server_version, server_state, asn, isp, country,
                     agreement_1h_score, agreement_24h_score, agreement_30d_score,
                     uptime_score, latency_score, peer_count_score,
                     version_score, diversity_score, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        round_id, s.public_key, s.domain, s.composite_score,
                        s.metrics.agreement_1h, s.metrics.agreement_24h, s.metrics.agreement_30d,
                        s.metrics.uptime_seconds, s.metrics.latency_ms, s.metrics.peer_count,
                        s.metrics.server_version, s.metrics.server_state,
                        s.metrics.asn, s.metrics.isp, s.metrics.country,
                        s.sub_scores.agreement_1h, s.sub_scores.agreement_24h, s.sub_scores.agreement_30d,
                        s.sub_scores.uptime, s.sub_scores.latency, s.sub_scores.peer_count,
                        s.sub_scores.version, s.sub_scores.diversity, s.last_updated,
                    ),
                )
            await db.commit()
            return round_id

    async def get_latest_scores(self) -> tuple[int | None, str | None, list[ValidatorScore]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, timestamp FROM scoring_rounds ORDER BY id DESC LIMIT 1"
            )
            row = await cursor.fetchone()
            if not row:
                return None, None, []

            round_id = row["id"]
            round_ts = row["timestamp"]

            cursor = await db.execute(
                "SELECT * FROM validator_scores WHERE round_id = ? ORDER BY composite_score DESC",
                (round_id,),
            )
            rows = await cursor.fetchall()

            scores = []
            for r in rows:
                from app.models import ValidatorMetrics, ValidatorSubScores
                scores.append(ValidatorScore(
                    public_key=r["public_key"],
                    domain=r["domain"],
                    composite_score=r["composite_score"],
                    metrics=ValidatorMetrics(
                        agreement_1h=r["agreement_1h"],
                        agreement_24h=r["agreement_24h"],
                        agreement_30d=r["agreement_30d"],
                        uptime_seconds=r["uptime_seconds"],
                        latency_ms=r["latency_ms"],
                        peer_count=r["peer_count"],
                        server_version=r["server_version"],
                        server_state=r["server_state"],
                        asn=r["asn"],
                        isp=r["isp"],
                        country=r["country"],
                    ),
                    sub_scores=ValidatorSubScores(
                        agreement_1h=r["agreement_1h_score"] or 0.0,
                        agreement_24h=r["agreement_24h_score"] or 0.0,
                        agreement_30d=r["agreement_30d_score"] or 0.0,
                        uptime=r["uptime_score"] or 0.0,
                        latency=r["latency_score"] or 0.0,
                        peer_count=r["peer_count_score"] or 0.0,
                        version=r["version_score"] or 0.0,
                        diversity=r["diversity_score"] or 0.0,
                    ),
                    last_updated=r["timestamp"],
                ))
            return round_id, round_ts, scores

    async def get_validator_history(self, public_key: str, hours: int = 24) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT vs.composite_score, vs.timestamp, sr.id as round_id
                   FROM validator_scores vs
                   JOIN scoring_rounds sr ON vs.round_id = sr.id
                   WHERE vs.public_key = ?
                   ORDER BY sr.id DESC
                   LIMIT ?""",
                (public_key, (hours * 3600) // settings.poll_interval_seconds),
            )
            rows = await cursor.fetchall()
            return [{"round_id": r["round_id"], "composite_score": r["composite_score"], "timestamp": r["timestamp"]} for r in rows]

    async def get_round_history(self, limit: int = 10) -> list[RoundSummary]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM scoring_rounds ORDER BY id DESC LIMIT ?", (limit,)
            )
            rows = await cursor.fetchall()
            return [
                RoundSummary(
                    round_id=r["id"],
                    timestamp=r["timestamp"],
                    validator_count=r["validator_count"],
                    avg_score=r["avg_score"],
                    min_score=r["min_score"],
                    max_score=r["max_score"],
                )
                for r in rows
            ]

    async def get_last_round_timestamp(self) -> str | None:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT timestamp FROM scoring_rounds ORDER BY id DESC LIMIT 1"
            )
            row = await cursor.fetchone()
            return row[0] if row else None
