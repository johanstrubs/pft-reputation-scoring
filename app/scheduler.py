import asyncio
import logging

from app.collector import DataCollector
from app.scorer import ReputationScorer
from app.database import Database
from app.config import settings

logger = logging.getLogger(__name__)


async def run_scoring_round(collector: DataCollector, scorer: ReputationScorer, db: Database) -> int | None:
    try:
        logger.info("Starting scoring round...")
        snapshots = await collector.collect()
        if not snapshots:
            logger.warning("No validator data collected, skipping scoring round")
            return None

        scores = scorer.score(snapshots)
        if not scores:
            logger.warning("No scores computed, skipping storage")
            return None

        round_id = await db.store_round(scores)
        logger.info("Scoring round %d complete: %d validators scored", round_id, len(scores))
        return round_id
    except Exception:
        logger.exception("Scoring round failed")
        return None


async def start_scheduler(collector: DataCollector, scorer: ReputationScorer, db: Database):
    # Run one immediate round on startup
    await run_scoring_round(collector, scorer, db)

    # Then run on interval
    while True:
        await asyncio.sleep(settings.poll_interval_seconds)
        await run_scoring_round(collector, scorer, db)
