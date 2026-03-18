import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.collector import DataCollector
from app.scorer import ReputationScorer
from app.database import Database
from app.scheduler import start_scheduler
from app.models import (
    ScoresResponse,
    HealthResponse,
    HistoryResponse,
    MethodologyResponse,
)

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

db = Database()
collector = DataCollector()
scorer = ReputationScorer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init()
    task = asyncio.create_task(start_scheduler(collector, scorer, db))
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="PFT Reputation Scoring API",
    description="Multi-validator reputation scoring engine for Post Fiat network",
    version=settings.methodology_version,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
async def health():
    last_round = await db.get_last_round_timestamp()
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc).isoformat(),
        last_scoring_round=last_round,
    )


@app.get("/api/scores", response_model=ScoresResponse)
async def get_scores():
    round_id, round_ts, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    return ScoresResponse(
        round_id=round_id,
        timestamp=round_ts,
        methodology_version=settings.methodology_version,
        validator_count=len(scores),
        validators=scores,
    )


@app.get("/api/scores/history", response_model=HistoryResponse)
async def get_history(limit: int = 10):
    rounds = await db.get_round_history(limit=min(limit, 100))
    return HistoryResponse(rounds=rounds)


@app.get("/api/scores/{public_key}")
async def get_validator_score(public_key: str):
    round_id, round_ts, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")

    validator = next((s for s in scores if s.public_key == public_key), None)
    if not validator:
        raise HTTPException(status_code=404, detail="Validator not found")

    history = await db.get_validator_history(public_key)
    return {
        "round_id": round_id,
        "timestamp": round_ts,
        "validator": validator.model_dump(),
        "history": history,
    }


@app.get("/api/methodology", response_model=MethodologyResponse)
async def get_methodology():
    return MethodologyResponse(
        version=settings.methodology_version,
        description="Weighted composite reputation score for Post Fiat validators. "
                    "Each metric is normalized to 0.0-1.0, multiplied by its weight, "
                    "and the sum is scaled to 0-100.",
        weights={
            "agreement_1h": 0.10,
            "agreement_24h": 0.15,
            "agreement_30d": 0.20,
            "uptime": 0.15,
            "latency": 0.10,
            "peer_count": 0.10,
            "version": 0.10,
            "diversity": 0.10,
        },
        thresholds={
            "agreement": {"min": 0.8, "max": 1.0, "scoring": "linear, <0.8 = 0"},
            "uptime": {"scoring": "normalized against max observed uptime in cohort"},
            "latency": {"full_marks_ms": 50, "zero_ms": 500, "scoring": "linear between"},
            "peer_count": {"full_marks": 10, "zero": 3, "scoring": "linear between"},
            "version": {"latest": 1.0, "one_behind": 0.8, "older": 0.5},
            "diversity": {"penalty_threshold": 0.30, "scoring": "penalty if >30% share same ASN"},
        },
    )
