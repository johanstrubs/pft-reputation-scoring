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
        snapshots, poll_results = await collector.collect()
        if not snapshots:
            logger.warning("No validator data collected, skipping scoring round")
            return None

        # Enrich snapshots with poll-success percentages from history
        poll_pcts = await db.get_all_poll_success_pcts(hours=24)
        for snap in snapshots:
            pct = poll_pcts.get(snap.public_key)
            if pct is not None:
                snap.metrics.poll_success_pct = pct

        scores = scorer.score(snapshots)
        if not scores:
            logger.warning("No scores computed, skipping storage")
            return None

        round_id = await db.store_round(scores)

        # Store poll results for this round
        if poll_results:
            await db.store_poll_results(round_id, poll_results)

        logger.info("Scoring round %d complete: %d validators scored, %d poll results", round_id, len(scores), len(poll_results))
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
