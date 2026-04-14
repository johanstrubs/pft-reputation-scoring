from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

from app.diagnostics import build_diagnostic_report
from app.readiness import build_readiness_report
from app.remediation import normalize_readiness_and_diagnostic_findings
from app.scorer import WEIGHTS


METRIC_WEIGHT_POINTS = {
    "agreement_1h": WEIGHTS["agreement_1h"] * 100,
    "agreement_24h": WEIGHTS["agreement_24h"] * 100,
    "agreement_30d": WEIGHTS["agreement_30d"] * 100,
    "uptime": WEIGHTS["uptime"] * 100,
    "poll_success": WEIGHTS["poll_success"] * 100,
    "latency": WEIGHTS["latency"] * 100,
    "peer_count": WEIGHTS["peer_count"] * 100,
    "version": WEIGHTS["version"] * 100,
    "diversity": WEIGHTS["diversity"] * 100,
}


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _parse_day(value: str) -> date:
    return date.fromisoformat(value)


def _rank_map(scores: list) -> dict[str, int]:
    ordered = sorted(scores, key=lambda score: (-score.composite_score, score.public_key))
    return {score.public_key: index + 1 for index, score in enumerate(ordered)}


async def _build_current_non_passing_findings(round_id: int, timestamp: str, scores: list, public_key: str) -> list[dict]:
    readiness_task = asyncio.create_task(build_readiness_report(round_id, timestamp, scores, public_key))
    diagnose = build_diagnostic_report(round_id, timestamp, scores, public_key)
    readiness = await readiness_task
    findings = normalize_readiness_and_diagnostic_findings(readiness=readiness, diagnose=diagnose)
    return [
        {
            "finding_key": item["dedupe_key"],
            "sources": item.get("sources", [item["source"]]),
            "title": item["title"],
            "category": item["category"],
            "metric": item["metric"],
            "severity": item["severity"],
            "detected_value": item["detected_value"],
            "expected_value": item["expected_value"],
            "estimated_impact": item["estimated_score_impact"],
            "impact_confidence": item["impact_confidence"],
        }
        for item in findings
        if item["severity"] in {"critical", "warning"}
    ]


async def ensure_improvement_baseline(db) -> None:
    if await db.has_improvement_snapshots():
        return
    await snapshot_daily_findings(db, snapshot_date=_today_utc())


async def snapshot_daily_findings(db, snapshot_date: str | None = None) -> dict:
    round_id, timestamp, scores = await db.get_latest_scores()
    if round_id is None or not scores:
        raise ValueError("No scoring data available yet")

    snapshot_date = snapshot_date or _today_utc()
    stored = 0
    for score in scores:
        findings = await _build_current_non_passing_findings(round_id, timestamp, scores, score.public_key)
        await db.store_improvement_snapshot_run(
            round_id=round_id,
            public_key=score.public_key,
            snapshot_date=snapshot_date,
            findings=findings,
        )
        stored += 1
    return {"snapshot_date": snapshot_date, "validators_snapshotted": stored, "round_id": round_id}


def _compute_tracking_state(runs: list[dict], rows: list[dict]) -> tuple[list[dict], list[dict]]:
    rows_by_key_and_day: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in rows:
        rows_by_key_and_day[row["finding_key"]][row["snapshot_date"]] = row

    ordered_runs = sorted(runs, key=lambda run: run["snapshot_date"])
    resolutions: list[dict] = []
    open_findings: list[dict] = []

    for finding_key, presence_by_day in rows_by_key_and_day.items():
        segment_start = None
        last_present = None
        absent_streak = 0
        for run in ordered_runs:
            day = run["snapshot_date"]
            present = presence_by_day.get(day)
            if present:
                if segment_start is None:
                    segment_start = present
                last_present = present
                absent_streak = 0
                continue

            if segment_start is None:
                continue

            absent_streak += 1
            if absent_streak >= 2:
                resolutions.append(
                    {
                        "public_key": segment_start["public_key"],
                        "finding_key": finding_key,
                        "title": segment_start["title"],
                        "category": segment_start["category"],
                        "metric": segment_start["metric"],
                        "severity": segment_start["severity"],
                        "opened_date": segment_start["snapshot_date"],
                        "resolved_date": day,
                        "opened_round_id": segment_start["round_id"],
                        "resolved_round_id": run["round_id"],
                        "detected_value": segment_start["detected_value"],
                        "expected_value": segment_start["expected_value"],
                        "estimated_impact": segment_start["estimated_impact"],
                        "impact_confidence": segment_start["impact_confidence"],
                        "synthetic": False,
                    }
                )
                segment_start = None
                last_present = None
                absent_streak = 0

        if segment_start is not None and last_present is not None:
            latest_day = ordered_runs[-1]["snapshot_date"] if ordered_runs else segment_start["snapshot_date"]
            open_findings.append(
                {
                    "public_key": segment_start["public_key"],
                    "finding_key": finding_key,
                    "title": segment_start["title"],
                    "category": segment_start["category"],
                    "metric": segment_start["metric"],
                    "severity": segment_start["severity"],
                    "first_seen_date": segment_start["snapshot_date"],
                    "latest_snapshot_date": latest_day,
                    "detected_value": last_present["detected_value"],
                    "expected_value": last_present["expected_value"],
                }
            )

    return resolutions, open_findings


async def _round_context(db, round_id: int, cache: dict[int, dict]) -> dict:
    if round_id not in cache:
        scores = await db.get_scores_for_round(round_id)
        ranks = _rank_map(scores)
        score_map = {score.public_key: score for score in scores}
        cache[round_id] = {"scores": scores, "ranks": ranks, "score_map": score_map}
    return cache[round_id]


async def _enrich_resolution(db, resolution: dict, round_cache: dict[int, dict]) -> dict:
    before_ctx = await _round_context(db, resolution["opened_round_id"], round_cache)
    after_ctx = await _round_context(db, resolution["resolved_round_id"], round_cache)
    public_key = resolution["public_key"]
    before_score = before_ctx["score_map"].get(public_key)
    after_score = after_ctx["score_map"].get(public_key)
    score_before = before_score.composite_score if before_score else None
    score_after = after_score.composite_score if after_score else None
    rank_before = before_ctx["ranks"].get(public_key)
    rank_after = after_ctx["ranks"].get(public_key)
    score_delta = round((score_after or 0.0) - (score_before or 0.0), 2) if score_before is not None and score_after is not None else 0.0
    rank_delta = (rank_before - rank_after) if rank_before is not None and rank_after is not None else None
    days_to_resolution = (_parse_day(resolution["resolved_date"]) - _parse_day(resolution["opened_date"])).days

    estimated_impact = resolution["estimated_impact"]
    impact_confidence = resolution["impact_confidence"]
    metric = resolution["metric"]
    if metric in METRIC_WEIGHT_POINTS and before_score and after_score:
        before_sub = getattr(before_score.sub_scores, metric, None)
        after_sub = getattr(after_score.sub_scores, metric, None)
        if before_sub is not None and after_sub is not None:
            estimated_impact = round((after_sub - before_sub) * METRIC_WEIGHT_POINTS[metric], 2)
            impact_confidence = "direct"

    return {
        **resolution,
        "days_to_resolution": days_to_resolution,
        "score_before": score_before,
        "score_after": score_after,
        "score_delta": score_delta,
        "rank_before": rank_before,
        "rank_after": rank_after,
        "rank_delta": rank_delta,
        "estimated_impact": estimated_impact,
        "impact_confidence": impact_confidence,
    }


def _serialize_demo_resolution(row: dict) -> dict:
    score_before = row.get("score_before")
    score_after = row.get("score_after")
    rank_before = row.get("rank_before")
    rank_after = row.get("rank_after")
    return {
        "public_key": row["public_key"],
        "finding_key": row["finding_key"],
        "title": row["title"],
        "category": row["category"],
        "metric": row["metric"],
        "severity": row["severity"],
        "opened_date": row["opened_date"],
        "resolved_date": row["resolved_date"],
        "days_to_resolution": (_parse_day(row["resolved_date"]) - _parse_day(row["opened_date"])).days,
        "score_before": score_before,
        "score_after": score_after,
        "score_delta": round((score_after or 0.0) - (score_before or 0.0), 2) if score_before is not None and score_after is not None else 0.0,
        "rank_before": rank_before,
        "rank_after": rank_after,
        "rank_delta": (rank_before - rank_after) if rank_before is not None and rank_after is not None else None,
        "estimated_impact": row["estimated_impact"],
        "impact_confidence": row["impact_confidence"],
        "expected_value": row["expected_value"],
        "detected_value": row["detected_value"],
        "synthetic": True,
    }


async def _all_real_tracking_state(db) -> tuple[list[dict], list[dict]]:
    all_runs = await db.get_improvement_snapshot_runs()
    all_rows = await db.get_improvement_snapshot_rows()
    runs_by_validator: dict[str, list[dict]] = defaultdict(list)
    rows_by_validator: dict[str, list[dict]] = defaultdict(list)
    for run in all_runs:
        runs_by_validator[run["public_key"]].append(run)
    for row in all_rows:
        rows_by_validator[row["public_key"]].append(row)

    all_resolutions: list[dict] = []
    all_open_findings: list[dict] = []
    for public_key, validator_runs in runs_by_validator.items():
        resolutions, open_findings = _compute_tracking_state(validator_runs, rows_by_validator.get(public_key, []))
        all_resolutions.extend(resolutions)
        all_open_findings.extend(open_findings)
    return all_resolutions, all_open_findings


async def build_improvement_report(db, public_key: str) -> dict:
    await ensure_improvement_baseline(db)
    round_id, timestamp, scores = await db.get_latest_scores()
    if round_id is None or not scores:
        raise ValueError("No scoring data available yet")

    validator = next((score for score in scores if score.public_key == public_key), None)
    if not validator:
        raise KeyError(public_key)

    current_findings = await _build_current_non_passing_findings(round_id, timestamp, scores, public_key)
    validator_runs = await db.get_improvement_snapshot_runs(public_key)
    validator_rows = await db.get_improvement_snapshot_rows(public_key)
    real_resolutions, open_from_snapshots = _compute_tracking_state(validator_runs, validator_rows)

    round_cache: dict[int, dict] = {}
    resolved_findings = [await _enrich_resolution(db, resolution, round_cache) for resolution in real_resolutions]
    demo_resolutions = [_serialize_demo_resolution(row) for row in await db.get_demo_improvement_resolutions(public_key)]
    all_resolved = sorted(
        resolved_findings + demo_resolutions,
        key=lambda item: (item["resolved_date"], item["score_delta"], item["title"]),
        reverse=True,
    )

    latest_snapshot_day = validator_runs[-1]["snapshot_date"] if validator_runs else _today_utc()
    current_open_by_key = {item["finding_key"]: item for item in current_findings}
    open_findings = []
    for row in open_from_snapshots:
        if row["finding_key"] not in current_open_by_key:
            continue
        live_item = current_open_by_key[row["finding_key"]]
        days_open = (_parse_day(latest_snapshot_day) - _parse_day(row["first_seen_date"])).days
        open_findings.append(
            {
                "finding_key": row["finding_key"],
                "title": live_item["title"],
                "category": live_item["category"],
                "metric": live_item["metric"],
                "severity": live_item["severity"],
                "first_seen_date": row["first_seen_date"],
                "days_open": max(days_open, 0),
                "detected_value": live_item["detected_value"],
                "expected_value": live_item["expected_value"],
                "remediation_url": f"/remediate?validator={public_key}",
            }
        )
    open_findings.sort(key=lambda item: (-item["days_open"], item["severity"], item["title"]))

    all_real_resolutions, all_open_findings = await _all_real_tracking_state(db)
    week_cutoff = datetime.fromisoformat(timestamp) - timedelta(days=7)
    week_resolutions = [
        resolution for resolution in all_real_resolutions
        if datetime.combine(_parse_day(resolution["resolved_date"]), datetime.min.time(), tzinfo=timezone.utc) >= week_cutoff
    ]
    resolved_type = Counter(resolution["title"] for resolution in week_resolutions).most_common(1)
    ignored_type = Counter(
        finding["title"] for finding in all_open_findings
        if (_parse_day(finding["latest_snapshot_date"]) - _parse_day(finding["first_seen_date"])).days >= 7
    ).most_common(1)
    avg_days = round(sum(
        (_parse_day(resolution["resolved_date"]) - _parse_day(resolution["opened_date"])).days
        for resolution in week_resolutions
    ) / len(week_resolutions), 1) if week_resolutions else 0.0

    current_ranks = _rank_map(scores)
    tracking_since = await db.get_improvement_tracking_since(public_key)
    starting_rank = None
    if validator_runs:
        start_round_id = validator_runs[0]["round_id"]
        start_ctx = await _round_context(db, start_round_id, round_cache)
        starting_rank = start_ctx["ranks"].get(public_key)
    current_rank = current_ranks.get(public_key)

    biggest_wins = sorted(all_resolved, key=lambda item: (item["score_delta"], item["estimated_impact"], item["resolved_date"]), reverse=True)[:3]
    demo_mode = not resolved_findings and bool(demo_resolutions)

    return {
        "public_key": public_key,
        "domain": validator.domain,
        "round_id": round_id,
        "timestamp": timestamp,
        "tracking_since": tracking_since,
        "total_findings_resolved": len(all_resolved),
        "total_score_improvement": round(sum(item["score_delta"] for item in all_resolved), 2),
        "current_rank": current_rank,
        "starting_rank": starting_rank,
        "rank_delta_since_tracking": (starting_rank - current_rank) if starting_rank is not None and current_rank is not None else None,
        "resolved_findings": all_resolved,
        "biggest_wins": biggest_wins,
        "open_findings": open_findings,
        "network_summary": {
            "total_resolved_this_week": len(week_resolutions),
            "average_days_to_resolution": avg_days,
            "most_common_resolved_finding_type": resolved_type[0][0] if resolved_type else None,
            "most_common_ignored_finding_type": ignored_type[0][0] if ignored_type else None,
        },
        "demo_mode": demo_mode,
        "json_report_url": f"/api/improvements/{public_key}",
    }


async def seed_demo_improvement_resolution(db, public_key: str | None = None) -> dict:
    await ensure_improvement_baseline(db)
    real_resolutions, _ = await _all_real_tracking_state(db)
    if real_resolutions:
        raise ValueError("Demo seeding is disabled because real confirmed resolutions already exist.")

    round_id, timestamp, scores = await db.get_latest_scores()
    if round_id is None or not scores:
        raise ValueError("No scoring data available yet")

    validator = next((score for score in scores if score.public_key == public_key), None) if public_key else scores[0]
    if not validator:
        raise KeyError(public_key)

    current_rank = _rank_map(scores).get(validator.public_key)
    current_findings = await _build_current_non_passing_findings(round_id, timestamp, scores, validator.public_key)
    seed_source = current_findings[0] if current_findings else {
        "finding_key": "version::version::3.0.0",
        "title": "Demo resolved version parity issue",
        "category": "version",
        "metric": "version",
        "severity": "warning",
        "detected_value": "1.0.0",
        "expected_value": "3.0.0",
        "estimated_impact": 5.0,
        "impact_confidence": "direct",
    }

    resolved_date = _today_utc()
    opened_date = (_parse_day(resolved_date) - timedelta(days=3)).isoformat()
    estimated_impact = max(float(seed_source.get("estimated_impact", 0.0)), 1.5)
    score_after = validator.composite_score
    score_before = round(score_after - estimated_impact, 2)
    rank_after = current_rank
    rank_before = (rank_after + 2) if rank_after is not None else None

    await db.store_demo_improvement_resolution(
        public_key=validator.public_key,
        finding_key=seed_source["finding_key"],
        title=f"{seed_source['title']} (Demo)",
        category=seed_source["category"],
        metric=seed_source["metric"],
        severity=seed_source["severity"],
        opened_date=opened_date,
        resolved_date=resolved_date,
        detected_value=seed_source["detected_value"],
        expected_value=seed_source["expected_value"],
        score_before=score_before,
        score_after=score_after,
        rank_before=rank_before,
        rank_after=rank_after,
        estimated_impact=round(estimated_impact, 2),
        impact_confidence=seed_source.get("impact_confidence", "approximate"),
    )

    report = await build_improvement_report(db, validator.public_key)
    return {
        "public_key": validator.public_key,
        "resolved_findings": report["resolved_findings"],
        "demo_mode": report["demo_mode"],
    }
