import asyncio
import logging
from datetime import datetime, timezone

from app.collector import DataCollector
from app.scorer import ReputationScorer
from app.database import Database
from app.config import settings

logger = logging.getLogger(__name__)

DAILY_REPORT_HOUR = 12  # UTC hour to send daily reports


async def run_scoring_round(collector: DataCollector, scorer: ReputationScorer, db: Database) -> int | None:
    try:
        logger.info("Starting scoring round...")

        # Load subscriber-provided node key mappings to pass into collect()
        subscriber_mappings = await db.get_subscriber_key_mappings()
        if subscriber_mappings:
            logger.info("Found %d verified subscriber node key mappings", len(subscriber_mappings))

        snapshots, poll_results = await collector.collect(subscriber_mappings=subscriber_mappings)
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

        # Check critical alerts after each scoring round
        try:
            from app.alerts import check_critical_alerts
            await check_critical_alerts(db, scores)
        except Exception:
            logger.exception("Critical alert check failed")

        return round_id
    except Exception:
        logger.exception("Scoring round failed")
        return None


async def daily_report_loop(db: Database):
    """Send daily report cards at DAILY_REPORT_HOUR UTC."""
    last_sent_date = None
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == DAILY_REPORT_HOUR and now.date() != last_sent_date:
            try:
                from app.alerts import send_daily_reports
                logger.info("Triggering daily report cards...")
                await send_daily_reports(db)
                last_sent_date = now.date()
            except Exception:
                logger.exception("Daily report generation failed")
        await asyncio.sleep(60)  # Check every minute


async def start_scheduler(collector: DataCollector, scorer: ReputationScorer, db: Database):
    # Run one immediate round on startup
    await run_scoring_round(collector, scorer, db)

    # Start daily report loop in background
    asyncio.create_task(daily_report_loop(db))

    # Then run scoring on interval
    while True:
        await asyncio.sleep(settings.poll_interval_seconds)
        await run_scoring_round(collector, scorer, db)
