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
    uptime_pct REAL,
    latency_ms REAL,
    peer_count INTEGER,
    avg_ledger_interval REAL,
    server_version TEXT,
    server_state TEXT,
    asn INTEGER,
    isp TEXT,
    country TEXT,
    agreement_1h_score REAL,
    agreement_24h_score REAL,
    agreement_30d_score REAL,
    poll_success_pct REAL,
    uptime_score REAL,
    poll_success_score REAL,
    latency_score REAL,
    peer_count_score REAL,
    version_score REAL,
    diversity_score REAL,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES scoring_rounds(id)
);

CREATE INDEX IF NOT EXISTS idx_validator_scores_pubkey ON validator_scores(public_key);
CREATE INDEX IF NOT EXISTS idx_validator_scores_round ON validator_scores(round_id);

CREATE TABLE IF NOT EXISTS poll_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id INTEGER NOT NULL,
    public_key TEXT NOT NULL,
    poll_successful BOOLEAN NOT NULL,
    latency_ms REAL,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES scoring_rounds(id)
);

CREATE INDEX IF NOT EXISTS idx_poll_results_pubkey ON poll_results(public_key);
CREATE INDEX IF NOT EXISTS idx_poll_results_round ON poll_results(round_id);

CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_key TEXT NOT NULL,
    webhook_url TEXT NOT NULL,
    created_at TEXT NOT NULL,
    active BOOLEAN DEFAULT 1,
    UNIQUE(public_key, webhook_url)
);

CREATE TABLE IF NOT EXISTS alert_cooldowns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_key TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    fired_at TEXT NOT NULL,
    UNIQUE(public_key, alert_type)
);
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
                     uptime_seconds, uptime_pct, latency_ms, peer_count,
                     avg_ledger_interval, poll_success_pct,
                     server_version, server_state, asn, isp, country,
                     agreement_1h_score, agreement_24h_score, agreement_30d_score,
                     uptime_score, poll_success_score, latency_score, peer_count_score,
                     version_score, diversity_score, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        round_id, s.public_key, s.domain, s.composite_score,
                        s.metrics.agreement_1h, s.metrics.agreement_24h, s.metrics.agreement_30d,
                        s.metrics.uptime_seconds, s.metrics.uptime_pct,
                        s.metrics.latency_ms, s.metrics.peer_count,
                        s.metrics.avg_ledger_interval, s.metrics.poll_success_pct,
                        s.metrics.server_version, s.metrics.server_state,
                        s.metrics.asn, s.metrics.isp, s.metrics.country,
                        s.sub_scores.agreement_1h, s.sub_scores.agreement_24h, s.sub_scores.agreement_30d,
                        s.sub_scores.uptime, s.sub_scores.poll_success,
                        s.sub_scores.latency, s.sub_scores.peer_count,
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
                        uptime_pct=r["uptime_pct"],
                        latency_ms=r["latency_ms"],
                        peer_count=r["peer_count"],
                        avg_ledger_interval=r["avg_ledger_interval"],
                        poll_success_pct=r["poll_success_pct"],
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
                        poll_success=r["poll_success_score"] or 0.0,
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

    async def get_all_validator_trends(self, hours: int = 168) -> dict[str, list[dict]]:
        """Get composite score history for all validators over the given hours."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            max_rows = (hours * 3600) // settings.poll_interval_seconds
            cursor = await db.execute(
                """SELECT vs.public_key, vs.composite_score, vs.timestamp
                   FROM validator_scores vs
                   JOIN scoring_rounds sr ON vs.round_id = sr.id
                   ORDER BY sr.id DESC
                   LIMIT ?""",
                (max_rows * 100,),  # rough upper bound: max_rows * max_validators
            )
            rows = await cursor.fetchall()
            trends: dict[str, list[dict]] = {}
            for r in rows:
                pk = r["public_key"]
                if pk not in trends:
                    trends[pk] = []
                trends[pk].append({
                    "composite_score": r["composite_score"],
                    "timestamp": r["timestamp"],
                })
            # Reverse each list so oldest is first (for sparklines)
            for pk in trends:
                trends[pk].reverse()
            return trends

    async def store_poll_results(self, round_id: int, results: list[dict]):
        """Store poll success/failure for each validator in a round."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            for r in results:
                await db.execute(
                    """INSERT INTO poll_results (round_id, public_key, poll_successful, latency_ms, timestamp)
                       VALUES (?, ?, ?, ?, ?)""",
                    (round_id, r["public_key"], r["successful"], r.get("latency_ms"), now),
                )
            await db.commit()

    async def get_poll_success_pct(self, public_key: str, hours: int = 24) -> float | None:
        """Compute % of successful polls in the last N hours for a validator."""
        async with aiosqlite.connect(self.db_path) as db:
            max_polls = (hours * 3600) // settings.poll_interval_seconds
            cursor = await db.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN poll_successful THEN 1 ELSE 0 END) as successes
                   FROM (
                       SELECT poll_successful FROM poll_results
                       WHERE public_key = ?
                       ORDER BY id DESC
                       LIMIT ?
                   )""",
                (public_key, max_polls),
            )
            row = await cursor.fetchone()
            if not row or row[0] == 0:
                return None
            return round(100.0 * row[1] / row[0], 2)

    async def get_all_poll_success_pcts(self, hours: int = 24) -> dict[str, float]:
        """Compute poll success % for all validators in one query."""
        async with aiosqlite.connect(self.db_path) as db:
            max_polls = (hours * 3600) // settings.poll_interval_seconds
            # Get the cutoff round_id
            cursor = await db.execute(
                "SELECT id FROM scoring_rounds ORDER BY id DESC LIMIT 1 OFFSET ?",
                (max_polls,),
            )
            row = await cursor.fetchone()
            min_round_id = row[0] if row else 0

            cursor = await db.execute(
                """SELECT public_key,
                          COUNT(*) as total,
                          SUM(CASE WHEN poll_successful THEN 1 ELSE 0 END) as successes
                   FROM poll_results
                   WHERE round_id > ?
                   GROUP BY public_key""",
                (min_round_id,),
            )
            rows = await cursor.fetchall()
            return {
                r[0]: round(100.0 * r[2] / r[1], 2)
                for r in rows if r[1] > 0
            }

    # --- Subscription methods ---

    async def add_subscription(self, public_key: str, webhook_url: str) -> bool:
        """Add a subscription. Returns True if new, False if already exists."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute(
                    "INSERT INTO subscriptions (public_key, webhook_url, created_at) VALUES (?, ?, ?)",
                    (public_key, webhook_url, now),
                )
                await db.commit()
                return True
            except Exception:
                # UNIQUE constraint — already subscribed
                return False

    async def get_active_subscriptions(self) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT public_key, webhook_url FROM subscriptions WHERE active = 1"
            )
            rows = await cursor.fetchall()
            return [{"public_key": r["public_key"], "webhook_url": r["webhook_url"]} for r in rows]

    async def get_subscription(self, public_key: str) -> dict | None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT public_key, webhook_url, created_at, active FROM subscriptions WHERE public_key = ?",
                (public_key,),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return {"public_key": row["public_key"], "webhook_url": row["webhook_url"],
                    "created_at": row["created_at"], "active": bool(row["active"])}

    async def unsubscribe(self, public_key: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "UPDATE subscriptions SET active = 0 WHERE public_key = ? AND active = 1",
                (public_key,),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def get_previous_scores(self, public_key: str) -> dict | None:
        """Get the validator's score from ~24h ago for delta calculation."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            target_rounds = (24 * 3600) // settings.poll_interval_seconds
            cursor = await db.execute(
                """SELECT composite_score, timestamp FROM validator_scores
                   WHERE public_key = ?
                   ORDER BY id DESC
                   LIMIT 1 OFFSET ?""",
                (public_key, target_rounds),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return {"composite_score": row["composite_score"], "timestamp": row["timestamp"]}

    async def get_previous_rank(self, public_key: str) -> int | None:
        """Get the validator's rank from ~24h ago."""
        async with aiosqlite.connect(self.db_path) as db:
            target_rounds = (24 * 3600) // settings.poll_interval_seconds
            cursor = await db.execute(
                "SELECT id FROM scoring_rounds ORDER BY id DESC LIMIT 1 OFFSET ?",
                (target_rounds,),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            old_round_id = row[0]
            cursor = await db.execute(
                "SELECT public_key FROM validator_scores WHERE round_id = ? ORDER BY composite_score DESC",
                (old_round_id,),
            )
            rows = await cursor.fetchall()
            for i, r in enumerate(rows):
                if r[0] == public_key:
                    return i + 1
            return None

    async def check_alert_cooldown(self, public_key: str, alert_type: str, hours: int = 6) -> bool:
        """Returns True if alert is on cooldown (was fired recently)."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT fired_at FROM alert_cooldowns WHERE public_key = ? AND alert_type = ?",
                (public_key, alert_type),
            )
            row = await cursor.fetchone()
            if not row:
                return False
            fired_at = datetime.fromisoformat(row[0])
            elapsed = (datetime.now(timezone.utc) - fired_at).total_seconds()
            return elapsed < hours * 3600

    async def set_alert_cooldown(self, public_key: str, alert_type: str):
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO alert_cooldowns (public_key, alert_type, fired_at) VALUES (?, ?, ?)
                   ON CONFLICT(public_key, alert_type) DO UPDATE SET fired_at = ?""",
                (public_key, alert_type, now, now),
            )
            await db.commit()

    async def get_top_movers(self) -> dict:
        """Get biggest rank gainers and losers in last 24h."""
        async with aiosqlite.connect(self.db_path) as db:
            # Get latest round scores
            cursor = await db.execute("SELECT id FROM scoring_rounds ORDER BY id DESC LIMIT 1")
            row = await cursor.fetchone()
            if not row:
                return {"gainer": None, "loser": None}
            latest_round = row[0]

            # Get round from ~24h ago
            target_offset = (24 * 3600) // settings.poll_interval_seconds
            cursor = await db.execute(
                "SELECT id FROM scoring_rounds ORDER BY id DESC LIMIT 1 OFFSET ?",
                (target_offset,),
            )
            row = await cursor.fetchone()
            if not row:
                return {"gainer": None, "loser": None}
            old_round = row[0]

            # Get rankings for both rounds
            async def get_rankings(round_id):
                cursor = await db.execute(
                    "SELECT public_key, domain, composite_score FROM validator_scores WHERE round_id = ? ORDER BY composite_score DESC",
                    (round_id,),
                )
                rows = await cursor.fetchall()
                return {r[0]: {"rank": i + 1, "domain": r[1], "score": r[2]} for i, r in enumerate(rows)}

            old_ranks = await get_rankings(old_round)
            new_ranks = await get_rankings(latest_round)

            best_gain = 0
            best_gainer = None
            worst_loss = 0
            worst_loser = None

            for pk, new_data in new_ranks.items():
                if pk in old_ranks:
                    rank_change = old_ranks[pk]["rank"] - new_data["rank"]  # positive = improved
                    if rank_change > best_gain:
                        best_gain = rank_change
                        best_gainer = {"public_key": pk, "domain": new_data["domain"], "rank_change": rank_change, "score": new_data["score"]}
                    if rank_change < worst_loss:
                        worst_loss = rank_change
                        worst_loser = {"public_key": pk, "domain": new_data["domain"], "rank_change": rank_change, "score": new_data["score"]}

            return {"gainer": best_gainer, "loser": worst_loser}

    async def get_last_round_timestamp(self) -> str | None:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT timestamp FROM scoring_rounds ORDER BY id DESC LIMIT 1"
            )
            row = await cursor.fetchone()
            return row[0] if row else None
