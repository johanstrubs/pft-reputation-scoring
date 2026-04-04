import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import settings
from app.collector import DataCollector
from app.scorer import ReputationScorer
from app.database import Database
from app.digest import generate_and_store_weekly_digest
from app.scheduler import start_scheduler
from app.models import (
    ScoresResponse,
    HealthResponse,
    HistoryResponse,
    MethodologyResponse,
    WeeklyDigestResponse,
    WeeklyDigestHistoryResponse,
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
    enriched = [s for s in scores if s.metrics.latency_ms is not None and s.metrics.uptime_seconds is not None
                and s.metrics.peer_count is not None and s.metrics.country is not None and s.metrics.asn is not None]
    return ScoresResponse(
        round_id=round_id,
        timestamp=round_ts,
        methodology_version=settings.methodology_version,
        validator_count=len(scores),
        enrichment_coverage={
            "total_validators": len(scores),
            "enriched": len(enriched),
            "unenriched": len(scores) - len(enriched),
            "coverage_pct": round(100 * len(enriched) / len(scores), 1) if scores else 0,
        },
        validators=scores,
    )


@app.get("/api/scores/history", response_model=HistoryResponse)
async def get_history(limit: int = 10):
    rounds = await db.get_round_history(limit=min(limit, 100))
    return HistoryResponse(rounds=rounds)


@app.get("/api/scores/trends")
async def get_trends(hours: int = 168):
    """Get composite score trends for all validators (default 7 days)."""
    trends = await db.get_all_validator_trends(hours=min(hours, 168))
    return {"hours": hours, "trends": trends}


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
            "uptime": 0.08,
            "poll_success": 0.07,
            "latency": 0.10,
            "peer_count": 0.10,
            "version": 0.10,
            "diversity": 0.10,
        },
        thresholds={
            "agreement": {"min": 0.8, "max": 1.0, "scoring": "linear, <0.8 = 0; total=0 treated as neutral 0.5"},
            "uptime": {"scoring": "normalized against max observed uptime in cohort", "unit": "seconds, also reported as percentage"},
            "poll_success": {"full_marks_pct": 95, "zero_pct": 70, "scoring": "linear between; our own reachability tracking"},
            "latency": {"full_marks_ms": 50, "zero_ms": 500, "scoring": "linear between"},
            "peer_count": {"full_marks": 10, "zero": 3, "scoring": "linear between"},
            "avg_ledger_interval": {"unit": "seconds per ledger", "description": "computed from complete_ledgers range / uptime"},
            "version": {"latest": 1.0, "one_behind": 0.8, "older": 0.5},
            "diversity": {"penalty_threshold": 0.30, "scoring": "penalty if >30% share same ASN"},
        },
    )


@app.get("/api/digest/latest", response_model=WeeklyDigestResponse)
async def get_latest_digest():
    digest = await db.get_latest_digest()
    if not digest:
        raise HTTPException(status_code=503, detail="No weekly digest available yet")
    return WeeklyDigestResponse(**digest)


@app.get("/api/digest/history", response_model=WeeklyDigestHistoryResponse)
async def get_digest_history(limit: int = 10):
    digests = await db.get_digest_history(limit=min(limit, 52))
    return WeeklyDigestHistoryResponse(digests=[WeeklyDigestResponse(**digest) for digest in digests])


@app.post("/api/digest/trigger", response_model=WeeklyDigestResponse)
async def trigger_weekly_digest():
    try:
        digest = await generate_and_store_weekly_digest(db)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return WeeklyDigestResponse(**digest)


# --- Alerts & Subscriptions ---

class SubscribeRequest(BaseModel):
    public_key: str
    webhook_url: str
    node_public_key: str | None = None


class UnsubscribeRequest(BaseModel):
    public_key: str


class UpdateNodeKeyRequest(BaseModel):
    node_public_key: str


async def _validate_node_key(node_key: str, validator_key: str | None = None):
    """Validate a node key: must exist in topology, must not be claimed by another validator."""
    # Check it's not already claimed by someone else
    if await db.is_node_key_claimed(node_key, exclude_validator=validator_key):
        raise HTTPException(status_code=409, detail="This node key is already claimed by another validator.")

    # Check it exists in the current VHS topology
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{settings.vhs_base_url}/v1/network/topology/nodes")
            if resp.status_code == 200:
                data = resp.json()
                nodes = data.get("nodes", data if isinstance(data, list) else [])
                topology_keys = {n.get("node_public_key") for n in nodes if n.get("node_public_key")}
                if node_key not in topology_keys:
                    raise HTTPException(
                        status_code=400,
                        detail="This node key was not found in the current network topology. "
                               "Make sure your node is running and connected to the network, then try again."
                    )
    except HTTPException:
        raise
    except Exception:
        pass  # If topology check fails, allow it through — don't block on VHS errors


@app.get("/alerts")
async def alerts_page():
    return FileResponse(os.path.join(STATIC_DIR, "alerts.html"))


@app.post("/api/alerts/subscribe")
async def subscribe(req: SubscribeRequest):
    from app.alerts import send_confirmation

    if not req.public_key or len(req.public_key) < 10:
        raise HTTPException(status_code=400, detail="Invalid public key")
    if not req.webhook_url.startswith("https://discord.com/api/webhooks/"):
        raise HTTPException(status_code=400, detail="Invalid Discord webhook URL")
    if req.node_public_key and not req.node_public_key.startswith("n9"):
        raise HTTPException(status_code=400, detail="Invalid node key format. Must start with n9...")
    if req.node_public_key:
        await _validate_node_key(req.node_public_key, req.public_key)

    is_new = await db.add_subscription(req.public_key, req.webhook_url, req.node_public_key)
    if not is_new:
        raise HTTPException(status_code=409, detail="Already subscribed with this key and webhook")

    sent = await send_confirmation(req.webhook_url, req.public_key)
    if not sent:
        raise HTTPException(status_code=502, detail="Subscription saved but failed to send confirmation to Discord. Check your webhook URL.")

    return {"message": "Subscribed! A confirmation message was sent to your Discord channel.", "public_key": req.public_key}


@app.get("/api/alerts/status/{public_key}")
async def subscription_status(public_key: str):
    sub = await db.get_subscription(public_key)
    if not sub:
        raise HTTPException(status_code=404, detail="No subscription found")
    return {"subscription": sub}


@app.post("/api/alerts/unsubscribe")
async def unsubscribe(req: UnsubscribeRequest):
    removed = await db.unsubscribe(req.public_key)
    if not removed:
        raise HTTPException(status_code=404, detail="No active subscription found for this key")
    return {"message": "Unsubscribed successfully."}


class VerifyNodeRequest(BaseModel):
    validator_key: str
    node_key: str


@app.post("/api/alerts/verify-node")
async def verify_node(req: VerifyNodeRequest, request: Request):
    """Verify node key ownership by checking the request comes from the node's IP."""
    if not req.validator_key.startswith("nH"):
        raise HTTPException(status_code=400, detail="Invalid validator key format. Must start with nH...")
    if not req.node_key.startswith("n9"):
        raise HTTPException(status_code=400, detail="Invalid node key format. Must start with n9...")

    # Check subscription exists
    sub = await db.get_subscription(req.validator_key)
    if not sub:
        raise HTTPException(status_code=404, detail="No subscription found for this validator key. Subscribe at /alerts first.")

    # Check node key not claimed by someone else
    if await db.is_node_key_claimed(req.node_key, exclude_validator=req.validator_key):
        raise HTTPException(status_code=409, detail="This node key is already verified by another validator.")

    # Get the caller's IP
    caller_ip = request.headers.get("x-real-ip") or request.headers.get("x-forwarded-for", "").split(",")[0].strip() or request.client.host
    logger.info("Node verification attempt: validator=%s node=%s caller_ip=%s", req.validator_key[:16], req.node_key[:16], caller_ip)

    # Look up the expected IP for this node key from VHS topology
    expected_ip = None
    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{settings.vhs_base_url}/v1/network/topology/nodes")
            if resp.status_code == 200:
                data = resp.json()
                nodes = data.get("nodes", data if isinstance(data, list) else [])
                for node in nodes:
                    if node.get("node_public_key") == req.node_key:
                        expected_ip = node.get("ip")
                        break
    except Exception as e:
        logger.error("Failed to fetch topology for verification: %s", e)
        raise HTTPException(status_code=503, detail="Could not verify — topology service unavailable. Try again later.")

    if not expected_ip:
        raise HTTPException(status_code=400, detail="This node key was not found in the current network topology. Make sure your node is running.")

    # Compare caller IP with topology IP
    if caller_ip != expected_ip:
        logger.warning("Node verification FAILED: caller=%s expected=%s for node=%s", caller_ip, expected_ip, req.node_key[:16])
        raise HTTPException(
            status_code=403,
            detail=f"Verification failed. This request came from {caller_ip} but the network topology shows "
                   f"node {req.node_key[:16]}... at {expected_ip}. Run this command from your validator server, not your local machine."
        )

    # Verified! Save the node key
    await db.update_node_key(req.validator_key, req.node_key, verified=True)
    logger.info("Node verification PASSED: validator=%s node=%s ip=%s", req.validator_key[:16], req.node_key[:16], caller_ip)
    return {
        "message": "Verified! Your node key has been linked to your validator. Full topology metrics will appear in your next daily report.",
        "validator_key": req.validator_key,
        "node_key": req.node_key,
        "verified_ip": caller_ip,
    }


@app.patch("/api/alerts/subscriptions/{public_key}")
async def update_subscription(public_key: str, req: UpdateNodeKeyRequest):
    """Update the node key for an existing subscription (unverified — use verify-node for verified linking)."""
    if not req.node_public_key.startswith("n9"):
        raise HTTPException(status_code=400, detail="Invalid node key format. Must start with n9...")
    await _validate_node_key(req.node_public_key, public_key)
    updated = await db.update_node_key(public_key, req.node_public_key, verified=False)
    if not updated:
        raise HTTPException(status_code=404, detail="No active subscription found for this key")
    return {"message": "Node key updated! Full metrics will be available after the next scoring round."}


@app.post("/api/alerts/send-daily")
async def trigger_daily_reports():
    """Manual trigger for daily reports (for testing)."""
    from app.alerts import send_daily_reports
    await send_daily_reports(db)
    return {"message": "Daily reports sent."}


# --- Network Topology ---


@app.get("/api/network/topology")
async def network_topology():
    """Public network topology API showing enrichment coverage and concentration."""
    round_id, round_ts, scores = await db.get_latest_scores()
    if not scores:
        raise HTTPException(status_code=503, detail="No scoring data available yet")

    total = len(scores)

    def is_fully_enriched(s):
        m = s.metrics
        return (m.latency_ms is not None and m.uptime_seconds is not None
                and m.peer_count is not None and m.country is not None and m.asn is not None)

    enriched = [s for s in scores if is_fully_enriched(s)]
    unenriched = [s for s in scores if not is_fully_enriched(s)]

    # Build validator list
    validators = []
    for s in scores:
        validators.append({
            "public_key": s.public_key,
            "domain": s.domain,
            "server_version": s.metrics.server_version,
            "enriched": is_fully_enriched(s),
            "asn": s.metrics.asn,
            "isp": s.metrics.isp,
            "country": s.metrics.country,
            "latency_ms": s.metrics.latency_ms,
            "uptime_seconds": s.metrics.uptime_seconds,
            "peer_count": s.metrics.peer_count,
        })

    # Concentration stats (only from enriched validators)
    enriched_count = len(enriched)
    from collections import Counter

    asn_counts = Counter((s.metrics.asn, s.metrics.isp) for s in enriched if s.metrics.asn is not None)
    country_counts = Counter(s.metrics.country for s in enriched if s.metrics.country is not None)
    provider_counts = Counter(s.metrics.isp for s in enriched if s.metrics.isp is not None)

    by_asn = [
        {"asn": asn, "isp": isp or "Unknown", "count": count, "pct": round(100 * count / enriched_count, 1) if enriched_count else 0}
        for (asn, isp), count in asn_counts.most_common()
    ]
    by_country = [
        {"country": country, "count": count, "pct": round(100 * count / enriched_count, 1) if enriched_count else 0}
        for country, count in country_counts.most_common()
    ]
    by_provider = [
        {"provider": provider, "count": count, "pct": round(100 * count / enriched_count, 1) if enriched_count else 0}
        for provider, count in provider_counts.most_common()
    ]

    # Concentration warnings (deduplicated — provider covers ASN for same entity)
    warnings = []
    warned_providers = set()
    for entry in by_provider:
        if entry["pct"] > 33:
            warnings.append(f"{entry['provider']} hosts {entry['pct']}% of enriched validators (threshold: 33%)")
            warned_providers.add(entry["provider"])
    for entry in by_country:
        if entry["pct"] > 33:
            warnings.append(f"{entry['pct']}% of enriched validators are in {entry['country']} (threshold: 33%)")
    for entry in by_asn:
        if entry["pct"] > 33 and entry.get("isp") not in warned_providers:
            warnings.append(f"ASN {entry['asn']} ({entry['isp']}) has {entry['pct']}% of enriched validators (threshold: 33%)")

    return {
        "timestamp": round_ts,
        "enrichment_coverage": {
            "total_validators": total,
            "enriched": enriched_count,
            "unenriched": len(unenriched),
            "coverage_pct": round(100 * enriched_count / total, 1) if total else 0,
        },
        "concentration": {
            "by_asn": by_asn,
            "by_country": by_country,
            "by_provider": by_provider,
        },
        "warnings": warnings,
        "validators": validators,
    }


@app.get("/network")
async def network_page():
    return FileResponse(os.path.join(STATIC_DIR, "network.html"))


# --- Static files & leaderboard ---
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "static")


@app.get("/leaderboard")
async def leaderboard():
    return FileResponse(os.path.join(STATIC_DIR, "leaderboard.html"))


@app.get("/simulator")
async def simulator():
    return FileResponse(os.path.join(STATIC_DIR, "simulator.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
