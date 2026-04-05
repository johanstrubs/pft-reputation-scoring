import json
from datetime import datetime, timedelta, timezone

from app.config import settings
from app.diagnostics import build_diagnostic_report, build_peer_comparison


class AIDiagnosticLimitError(Exception):
    pass


class AIDiagnosticUnavailableError(Exception):
    pass


def _estimate_cost_cents(input_tokens: int | None, output_tokens: int | None) -> float:
    input_tokens = input_tokens or 0
    output_tokens = output_tokens or 0
    input_cost = (input_tokens / 1_000_000) * settings.anthropic_input_cost_per_million * 100
    output_cost = (output_tokens / 1_000_000) * settings.anthropic_output_cost_per_million * 100
    return round(input_cost + output_cost, 4)


def _friendly_limit_message() -> str:
    return "AI analysis is temporarily unavailable right now because the usage limit has been reached. Please try again later."


def _build_prompt_context(round_id: int, timestamp: str, scores: list, public_key: str) -> tuple[dict, dict]:
    report = build_diagnostic_report(round_id, timestamp, scores, public_key)
    validator = next(score for score in scores if score.public_key == public_key)
    peer_comparison = build_peer_comparison(scores, public_key)
    history = []
    return report, {
        "validator": {
            "public_key": validator.public_key,
            "domain": validator.domain,
            "composite_score": validator.composite_score,
            "rank": report["rank"],
            "validator_count": report["validator_count"],
            "metrics": validator.metrics.model_dump(),
            "sub_scores": validator.sub_scores.model_dump(),
        },
        "history": history,
        "peer_comparison": peer_comparison,
        "rule_based_findings": report["findings"],
        "strengths": report["strengths"],
        "timestamp": timestamp,
        "round_id": round_id,
    }


def _daily_snapshots(history_rows: list[dict], days: int = 7) -> list[dict]:
    snapshots = []
    seen_days = set()
    for row in reversed(history_rows):
        day = row["timestamp"][:10]
        if day in seen_days:
            continue
        seen_days.add(day)
        snapshots.append(row)
    return snapshots[-days:]


def _build_messages(context: dict) -> tuple[str, str]:
    system_prompt = (
        "You are generating advisory diagnostics for a validator operator. "
        "Use the provided structured context only. "
        "Return a concise 3-5 sentence summary that identifies the most likely root cause of any issues, "
        "whether the validator is trending better or worse, how it compares to similar infrastructure, "
        "and one prioritized recommendation. "
        "Do not repeat the entire rulebook. "
        "Treat this as advisory context and avoid claiming certainty when the data is incomplete."
    )
    user_prompt = json.dumps(context, separators=(",", ":"), ensure_ascii=True)
    return system_prompt, user_prompt


async def _enforce_ai_limits(db, public_key: str, round_id: int | None, ip_address: str | None):
    if not settings.anthropic_api_key or not settings.anthropic_model:
        raise AIDiagnosticUnavailableError("AI analysis is not configured on this service.")

    now = datetime.now(timezone.utc)
    hour_cutoff = (now - timedelta(hours=1)).isoformat()
    day_cutoff = datetime(now.year, now.month, now.day, tzinfo=timezone.utc).isoformat()

    if ip_address:
        ip_count = await db.count_ai_requests_since(hour_cutoff, ip_address=ip_address)
        if ip_count >= settings.anthropic_max_calls_per_ip_per_hour:
            await db.log_ai_diagnostic_request(
                public_key=public_key,
                round_id=round_id,
                ip_address=ip_address,
                status="limited",
                model=settings.anthropic_model or None,
                failure_reason="ip_rate_limit",
            )
            raise AIDiagnosticLimitError(_friendly_limit_message())

    global_count = await db.count_ai_requests_since(hour_cutoff, statuses=("success",))
    if global_count >= settings.anthropic_max_calls_per_hour:
        await db.log_ai_diagnostic_request(
            public_key=public_key,
            round_id=round_id,
            ip_address=ip_address,
            status="limited",
            model=settings.anthropic_model or None,
            failure_reason="global_rate_limit",
        )
        raise AIDiagnosticLimitError(_friendly_limit_message())

    daily_cost = await db.sum_ai_cost_since(day_cutoff)
    if daily_cost >= settings.anthropic_daily_budget_cents:
        await db.log_ai_diagnostic_request(
            public_key=public_key,
            round_id=round_id,
            ip_address=ip_address,
            status="limited",
            model=settings.anthropic_model or None,
            failure_reason="daily_budget_cap",
        )
        raise AIDiagnosticLimitError(_friendly_limit_message())


async def generate_ai_diagnostic(db, *, public_key: str, ip_address: str | None) -> dict:
    round_id, round_ts, scores = await db.get_latest_scores()
    if round_id is None or not scores:
        raise ValueError("No scoring data available yet")

    cached = await db.get_ai_diagnostic_cache(public_key, round_id)
    if cached:
        await db.log_ai_diagnostic_request(
            public_key=public_key,
            round_id=round_id,
            ip_address=ip_address,
            status="cached",
            model=cached["model"],
            cached=True,
            estimated_cost_cents=0.0,
        )
        return {
            "ai_summary": cached["ai_summary"],
            "model": cached["model"],
            "generated_at": cached["generated_at"],
            "cached": True,
            "message": "Cached AI analysis reused for the current scoring round.",
        }

    await _enforce_ai_limits(db, public_key, round_id, ip_address)

    report, context = _build_prompt_context(round_id, round_ts, scores, public_key)
    history_rows = await db.get_validator_diagnostic_history(public_key, limit=2016)
    context["history"] = _daily_snapshots(history_rows, days=7)
    system_prompt, user_prompt = _build_messages(context)

    try:
        from anthropic import AsyncAnthropic
    except ImportError as exc:
        raise AIDiagnosticUnavailableError("AI analysis dependencies are not installed on this service.") from exc

    client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=settings.anthropic_timeout_seconds)

    try:
        response = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=settings.anthropic_max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as exc:
        await db.log_ai_diagnostic_request(
            public_key=public_key,
            round_id=round_id,
            ip_address=ip_address,
            status="error",
            model=settings.anthropic_model,
            cached=False,
            failure_reason=str(exc),
        )
        return {
            "ai_summary": None,
            "model": settings.anthropic_model,
            "generated_at": None,
            "cached": False,
            "message": "AI analysis is temporarily unavailable. The rule-based diagnostic report is still current.",
        }

    content_blocks = getattr(response, "content", []) or []
    ai_summary = "".join(getattr(block, "text", "") for block in content_blocks).strip() or None
    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", None)
    output_tokens = getattr(usage, "output_tokens", None)
    estimated_cost_cents = _estimate_cost_cents(input_tokens, output_tokens)
    generated_at = datetime.now(timezone.utc).isoformat()

    if ai_summary:
        await db.store_ai_diagnostic_cache(
            public_key=public_key,
            round_id=round_id,
            model=settings.anthropic_model,
            ai_summary=ai_summary,
            generated_at=generated_at,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_cost_cents=estimated_cost_cents,
        )

    await db.log_ai_diagnostic_request(
        public_key=public_key,
        round_id=round_id,
        ip_address=ip_address,
        status="success" if ai_summary else "error",
        model=settings.anthropic_model,
        cached=False,
        estimated_cost_cents=estimated_cost_cents if ai_summary else 0.0,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        failure_reason=None if ai_summary else "empty_summary",
    )

    return {
        "ai_summary": ai_summary,
        "model": settings.anthropic_model,
        "generated_at": generated_at if ai_summary else None,
        "cached": False,
        "message": None if ai_summary else "AI analysis returned no summary. The rule-based diagnostic report is still available.",
    }
