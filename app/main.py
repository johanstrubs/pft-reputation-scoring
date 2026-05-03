import asyncio
import hashlib
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import settings
from app.collector import DataCollector
from app.diagnostic_ai import generate_ai_diagnostic, AIDiagnosticLimitError, AIDiagnosticUnavailableError
from app.readiness import build_readiness_report
from app.upgrades import build_upgrade_report
from app.diversity import build_diversity_report
from app.peers import build_peer_report
from app.remediation import build_remediation_report
from app.improvements import build_improvement_report, seed_demo_improvement_resolution
from app.blast_radius import build_blast_radius_report, inject_synthetic_correlated_event
from app.dataset import (
    build_dataset_diff,
    build_dataset_export_csv_zip,
    build_dataset_export_json,
    build_dataset_schema,
    build_dataset_timeseries,
    build_daily_snapshot,
    build_latest_dataset_snapshot,
    build_risk_report,
)
from app.methodology_card import build_methodology_card, build_methodology_summary
from app.scorer import ReputationScorer
from app.database import Database
from app.diagnostics import build_diagnostic_report
from app.digest import generate_and_store_weekly_digest
from app.incidents import inject_synthetic_incident
from app.runbooks import classify_incident, get_runbook_library
from app.scheduler import start_scheduler
from app.models import (
    ScoresResponse,
    HealthResponse,
    HistoryResponse,
    MethodologyResponse,
    WeeklyDigestResponse,
    WeeklyDigestHistoryResponse,
    IncidentResponse,
    IncidentListResponse,
    RunbookLibraryResponse,
    DiagnosticReportResponse,
    AIDiagnosticResponse,
    ReadinessReportResponse,
    UpgradesResponse,
    DiversityReportResponse,
    PeerReportResponse,
    RemediationReportResponse,
    ImprovementReportResponse,
    BlastRadiusReportResponse,
    BlastRadiusEventResponse,
    DatasetSnapshotResponse,
    DatasetTimeseriesResponse,
    DatasetDiffResponse,
    DatasetSchemaResponse,
    RiskReportResponse,
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
        methodology_url=f"{settings.public_base_url.rstrip('/')}/api/methodology",
        methodology_card_url=f"{settings.public_base_url.rstrip('/')}/methodology-card",
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
    return MethodologyResponse(**build_methodology_summary())


@app.get("/api/methodology-card")
async def get_methodology_card():
    return build_methodology_card()


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


class IncidentTestRequest(BaseModel):
    validator_key: str


async def _attach_incident_rca(incident: dict) -> dict:
    incident["events"] = await db.get_incident_events(incident["id"])
    round_scores = await db.get_scores_for_round(incident["latest_round_id"]) if incident.get("latest_round_id") else []
    _latest_round_id, _latest_round_ts, latest_scores = await db.get_latest_scores()
    related_incidents = await db.get_incidents(validator_key=incident["validator_key"], limit=100)
    incident["rca"] = classify_incident(
        incident,
        related_incidents=related_incidents,
        round_scores=round_scores,
        latest_scores=latest_scores,
    )
    return incident


@app.get("/api/incidents", response_model=IncidentListResponse)
async def get_incidents(
    validator_key: str | None = None,
    severity: str | None = None,
    event_type: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 100,
):
    incidents = await db.get_incidents(
        validator_key=validator_key,
        severity=severity,
        event_type=event_type,
        status=status,
        date_from=date_from,
        date_to=date_to,
        limit=min(limit, 200),
    )
    return IncidentListResponse(incidents=[IncidentResponse(**incident) for incident in incidents])


@app.get("/api/incidents/{incident_id}", response_model=IncidentResponse)
async def get_incident(incident_id: int):
    incident = await db.get_incident(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    incident = await _attach_incident_rca(incident)
    return IncidentResponse(**incident)


@app.post("/api/incidents/test", response_model=IncidentResponse)
async def create_synthetic_incident(req: IncidentTestRequest):
    if not req.validator_key or len(req.validator_key) < 10:
        raise HTTPException(status_code=400, detail="Invalid validator key")
    incident = await inject_synthetic_incident(db, req.validator_key)
    incident = await _attach_incident_rca(incident)
    return IncidentResponse(**incident)


@app.get("/api/runbooks", response_model=RunbookLibraryResponse)
async def get_runbooks():
    library = get_runbook_library()
    return RunbookLibraryResponse(runbooks=list(library.values()))


@app.get("/api/diagnose/{public_key}", response_model=DiagnosticReportResponse)
async def diagnose_validator(public_key: str):
    round_id, round_ts, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    try:
        report = build_diagnostic_report(round_id, round_ts, scores, public_key)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    return DiagnosticReportResponse(**report)


@app.post("/api/diagnose/{public_key}/ai", response_model=AIDiagnosticResponse)
async def diagnose_validator_ai(public_key: str, request: Request):
    caller_ip = request.headers.get("x-real-ip") or request.headers.get("x-forwarded-for", "").split(",")[0].strip() or request.client.host
    try:
        result = await generate_ai_diagnostic(db, public_key=public_key, ip_address=caller_ip)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    except AIDiagnosticLimitError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    except AIDiagnosticUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return AIDiagnosticResponse(**result)


@app.get("/api/diagnose/{public_key}/ai", response_model=AIDiagnosticResponse)
async def get_cached_diagnose_validator_ai(public_key: str):
    round_id, _, scores = await db.get_latest_scores()
    if round_id is None or not scores:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    if not any(score.public_key == public_key for score in scores):
        raise HTTPException(status_code=404, detail="Validator not found")
    cached = await db.get_ai_diagnostic_cache(public_key, round_id)
    if not cached:
        return AIDiagnosticResponse(
            ai_summary=None,
            model=settings.anthropic_model or None,
            generated_at=None,
            cached=False,
            message="No cached AI analysis exists for the current scoring round yet. Use POST or the page button to generate one.",
        )
    return AIDiagnosticResponse(
        ai_summary=cached["ai_summary"],
        model=cached["model"],
        generated_at=cached["generated_at"],
        cached=True,
        message="Cached AI analysis reused for the current scoring round.",
    )


@app.get("/api/readiness/{public_key}", response_model=ReadinessReportResponse)
async def get_validator_readiness(public_key: str):
    round_id, round_ts, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    try:
        report = await build_readiness_report(round_id, round_ts, scores, public_key)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    return ReadinessReportResponse(**report)


@app.get("/api/upgrades", response_model=UpgradesResponse)
async def get_upgrades():
    round_id, round_ts, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    history_rows = await db.get_upgrade_history_rows()
    try:
        report = build_upgrade_report(round_id, round_ts, scores, history_rows)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return UpgradesResponse(**report)


@app.get("/api/diversity/{public_key}", response_model=DiversityReportResponse)
async def get_diversity(public_key: str):
    round_id, _, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    try:
        report = build_diversity_report(scores, public_key)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    return DiversityReportResponse(**report)


@app.get("/api/peers/{public_key}", response_model=PeerReportResponse)
async def get_peers(public_key: str):
    round_id, _, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    try:
        report = await build_peer_report(scores, public_key)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    return PeerReportResponse(**report)


@app.get("/api/remediate/{public_key}", response_model=RemediationReportResponse)
async def get_remediation(public_key: str):
    round_id, round_ts, scores = await db.get_latest_scores()
    if round_id is None:
        raise HTTPException(status_code=503, detail="No scoring data available yet")
    try:
        report = await build_remediation_report(db, round_id, round_ts, scores, public_key)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    return RemediationReportResponse(**report)


class ImprovementSeedRequest(BaseModel):
    validator_key: str | None = None


@app.get("/api/improvements/{public_key}", response_model=ImprovementReportResponse)
async def get_improvements(public_key: str):
    try:
        report = await build_improvement_report(db, public_key)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    return ImprovementReportResponse(**report)


@app.post("/api/improvements/seed-demo", response_model=ImprovementReportResponse)
async def seed_improvement_demo(req: ImprovementSeedRequest):
    try:
        result = await seed_demo_improvement_resolution(db, public_key=req.validator_key)
        report = await build_improvement_report(db, result["public_key"])
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found") from exc
    return ImprovementReportResponse(**report)


class BlastRadiusTestRequest(BaseModel):
    provider: str | None = None


@app.get("/api/blast-radius", response_model=BlastRadiusReportResponse)
async def get_blast_radius():
    try:
        report = await build_blast_radius_report(db)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return BlastRadiusReportResponse(**report)


@app.post("/api/blast-radius/test", response_model=BlastRadiusEventResponse)
async def create_blast_radius_test(req: BlastRadiusTestRequest):
    try:
        event = await inject_synthetic_correlated_event(db, provider=req.provider)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return BlastRadiusEventResponse(**event)


@app.get("/api/dataset/latest", response_model=DatasetSnapshotResponse)
async def get_dataset_latest():
    try:
        snapshot = await build_latest_dataset_snapshot(db)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return DatasetSnapshotResponse(**snapshot)


@app.get("/api/dataset/snapshot/{snapshot_date}", response_model=DatasetSnapshotResponse)
async def get_dataset_snapshot(snapshot_date: str):
    try:
        snapshot = await build_daily_snapshot(db, snapshot_date)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Snapshot not found for that date") from exc
    return DatasetSnapshotResponse(**snapshot)


@app.get("/api/dataset/timeseries/{public_key}", response_model=DatasetTimeseriesResponse)
async def get_dataset_timeseries(public_key: str, days: int = 30):
    try:
        report = await build_dataset_timeseries(db, public_key, days=days)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Validator not found in daily snapshots") from exc
    return DatasetTimeseriesResponse(**report)


@app.get("/api/dataset/diff/{date1}/{date2}", response_model=DatasetDiffResponse)
async def get_dataset_diff(date1: str, date2: str):
    try:
        diff = await build_dataset_diff(db, date1, date2)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Snapshot not found for date: {exc.args[0]}") from exc
    return DatasetDiffResponse(**diff)


@app.get("/api/dataset/schema", response_model=DatasetSchemaResponse)
async def get_dataset_schema():
    return DatasetSchemaResponse(**build_dataset_schema())


@app.api_route("/api/dataset/export", methods=["GET", "HEAD"])
async def get_dataset_export(request: Request, format: str = "json"):
    export_format = (format or "json").lower()
    if export_format not in {"json", "csv"}:
        raise HTTPException(status_code=400, detail="format must be json or csv")

    try:
        if export_format == "json":
            document = await build_dataset_export_json(db)
            payload = JSONResponse(content=document)
            body = payload.body
            sha256 = hashlib.sha256(body).hexdigest()
            headers = {
                "X-Content-SHA256": sha256,
                "Content-Disposition": 'attachment; filename="pft-ground-truth-dataset.json"',
            }
            if request.method == "HEAD":
                return Response(status_code=200, media_type="application/json", headers=headers)
            return Response(content=body, media_type="application/json", headers=headers)

        archive_bytes, sha256 = await build_dataset_export_csv_zip(db)
        headers = {
            "X-Content-SHA256": sha256,
            "Content-Disposition": 'attachment; filename="pft-ground-truth-dataset.zip"',
        }
        if request.method == "HEAD":
            return Response(status_code=200, media_type="application/zip", headers=headers)
        return Response(content=archive_bytes, media_type="application/zip", headers=headers)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/risk", response_model=RiskReportResponse)
async def get_risk():
    try:
        report = await build_risk_report(db)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return RiskReportResponse(**report)


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


@app.get("/incidents")
async def incidents_page():
    return FileResponse(os.path.join(STATIC_DIR, "incidents.html"))


@app.get("/runbooks")
async def runbooks_page():
    return FileResponse(os.path.join(STATIC_DIR, "runbooks.html"))


@app.get("/diagnose")
async def diagnose_page():
    return FileResponse(os.path.join(STATIC_DIR, "diagnose.html"))


@app.get("/readiness")
async def readiness_page():
    return FileResponse(os.path.join(STATIC_DIR, "readiness.html"))


@app.get("/upgrades")
async def upgrades_page():
    return FileResponse(os.path.join(STATIC_DIR, "upgrades.html"))


@app.get("/diversity")
async def diversity_page():
    return FileResponse(os.path.join(STATIC_DIR, "diversity.html"))


@app.get("/peers")
async def peers_page():
    return FileResponse(os.path.join(STATIC_DIR, "peers.html"))


@app.get("/remediate")
async def remediate_page():
    return FileResponse(os.path.join(STATIC_DIR, "remediate.html"))


@app.get("/improvements")
async def improvements_page():
    return FileResponse(os.path.join(STATIC_DIR, "improvements.html"))


@app.get("/blast-radius")
async def blast_radius_page():
    return FileResponse(os.path.join(STATIC_DIR, "blast-radius.html"))


@app.get("/dataset")
async def dataset_page():
    return FileResponse(os.path.join(STATIC_DIR, "dataset.html"))


@app.get("/methodology-card")
async def methodology_card_page():
    return FileResponse(os.path.join(STATIC_DIR, "methodology-card.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
