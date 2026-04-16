from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone


DETECTION_THRESHOLD = 3
CONSENSUS_RISK_THRESHOLD_PCT = 20.0


def _is_enriched(score) -> bool:
    return bool(score.metrics.isp and score.metrics.asn is not None and score.metrics.country)


def _severity_for_pct(network_pct: float) -> str:
    if network_pct > 25.0:
        return "critical"
    if network_pct > 10.0:
        return "warning"
    return "info"


def _correlation_label(correlation_type: str, dependency_value: str) -> str:
    return f"Shared {correlation_type} dependency: {dependency_value}"


def _dependency_for_score(score, correlation_type: str):
    if correlation_type == "provider":
        return score.metrics.isp
    if correlation_type == "asn":
        return f"AS{score.metrics.asn}" if score.metrics.asn is not None else None
    if correlation_type == "country":
        return score.metrics.country
    return None


def _build_score_map(scores: list) -> dict:
    return {score.public_key: score for score in scores}


def _blast_radius_for_dependency(scores: list, correlation_type: str, dependency_value: str) -> tuple[int, float, int, bool]:
    total = len(scores)
    affected = [
        score for score in scores
        if _is_enriched(score) and _dependency_for_score(score, correlation_type) == dependency_value
    ]
    affected_count = len(affected)
    network_pct = round((affected_count / total) * 100, 1) if total else 0.0
    remaining = max(total - affected_count, 0)
    return affected_count, network_pct, remaining, network_pct > CONSENSUS_RISK_THRESHOLD_PCT


def _avg_score_drop(affected_validators: list[str], current_map: dict, previous_map: dict) -> float | None:
    deltas = []
    for public_key in affected_validators:
        current = current_map.get(public_key)
        previous = previous_map.get(public_key)
        if current and previous:
            deltas.append(previous.composite_score - current.composite_score)
    if not deltas:
        return None
    return round(sum(deltas) / len(deltas), 2)


def _mitigation_guidance(correlation_type: str, dependency_value: str, affected_count: int) -> str:
    return (
        f"{affected_count} validators on {dependency_value} are currently degraded. "
        f"If you depend on this {correlation_type}, check your node status and consider whether manual intervention is needed. "
        f"You can also review /diversity to reduce future single-dependency risk."
    )


async def detect_and_store_correlated_events(db, round_id: int):
    round_summary = await db.get_round_summary(round_id)
    if not round_summary:
        return

    recent_rounds = await db.get_recent_round_summaries(limit=3)
    previous_round = next((row for row in recent_rounds if row["id"] < round_id), None)
    current_scores = await db.get_scores_for_round(round_id)
    previous_scores = await db.get_scores_for_round(previous_round["id"]) if previous_round else []
    current_map = _build_score_map(current_scores)
    previous_map = _build_score_map(previous_scores)

    incidents = await db.get_incidents_for_round(round_id)
    candidate_incidents = [incident for incident in incidents if incident["status"] == "open" or incident["latest_round_id"] == round_id]

    metadata_map = {}
    for public_key in {incident["validator_key"] for incident in candidate_incidents}:
        metadata_map[public_key] = current_map.get(public_key) or previous_map.get(public_key)

    detected_keys = set()
    for correlation_type in ("provider", "asn", "country"):
        grouped: dict[str, list[dict]] = defaultdict(list)
        for incident in candidate_incidents:
            score = metadata_map.get(incident["validator_key"])
            if not score or not _is_enriched(score):
                continue
            dependency_value = _dependency_for_score(score, correlation_type)
            if dependency_value:
                grouped[dependency_value].append(incident)

        for dependency_value, matching_incidents in grouped.items():
            affected_validators = sorted({incident["validator_key"] for incident in matching_incidents})
            if len(affected_validators) < DETECTION_THRESHOLD:
                continue

            detected_keys.add((correlation_type, dependency_value))
            affected_count = len(affected_validators)
            network_pct = round((affected_count / len(current_scores)) * 100, 1) if current_scores else 0.0
            severity = _severity_for_pct(network_pct)
            consensus_risk = network_pct > CONSENSUS_RISK_THRESHOLD_PCT
            avg_drop = _avg_score_drop(affected_validators, current_map, previous_map)
            _, _, remaining_if_failed, _ = _blast_radius_for_dependency(current_scores, correlation_type, dependency_value)
            mitigation_guidance = _mitigation_guidance(correlation_type, dependency_value, affected_count)
            suspected_cause = _correlation_label(correlation_type, dependency_value)
            triggering_ids = [incident["id"] for incident in matching_incidents]

            existing = await db.get_open_correlated_event_by_key(correlation_type, dependency_value)
            if existing:
                await db.update_correlated_event(
                    existing["id"],
                    severity=severity,
                    status="open",
                    latest_round_id=round_id,
                    latest_timestamp=round_summary["timestamp"],
                    affected_validators=affected_validators,
                    triggering_incident_ids=triggering_ids,
                    affected_count=affected_count,
                    network_pct=network_pct,
                    consensus_risk=consensus_risk,
                    avg_score_drop=avg_drop,
                    peak_affected_count=max(existing["peak_affected_count"], affected_count),
                    peak_network_pct=max(existing["peak_network_pct"], network_pct),
                    remaining_validators_if_failed=remaining_if_failed,
                    mitigation_guidance=mitigation_guidance,
                    suspected_cause=suspected_cause,
                )
            else:
                await db.create_correlated_event(
                    correlation_type=correlation_type,
                    dependency_value=dependency_value,
                    severity=severity,
                    status="open",
                    synthetic=False,
                    start_round_id=round_id,
                    latest_round_id=round_id,
                    start_timestamp=round_summary["timestamp"],
                    latest_timestamp=round_summary["timestamp"],
                    affected_validators=affected_validators,
                    triggering_incident_ids=triggering_ids,
                    affected_count=affected_count,
                    network_pct=network_pct,
                    consensus_risk=consensus_risk,
                    avg_score_drop=avg_drop,
                    peak_affected_count=affected_count,
                    peak_network_pct=network_pct,
                    remaining_validators_if_failed=remaining_if_failed,
                    mitigation_guidance=mitigation_guidance,
                    suspected_cause=suspected_cause,
                )

    open_events = await db.get_open_correlated_events()
    for event in open_events:
        key = (event["correlation_type"], event["dependency_value"])
        if key in detected_keys:
            continue
        await db.update_correlated_event(
            event["id"],
            severity=event["severity"],
            status="closed",
            latest_round_id=round_id,
            latest_timestamp=round_summary["timestamp"],
            affected_validators=event["affected_validators"],
            triggering_incident_ids=event["triggering_incident_ids"],
            affected_count=event["affected_count"],
            network_pct=event["network_pct"],
            consensus_risk=event["consensus_risk"],
            avg_score_drop=event["avg_score_drop"],
            peak_affected_count=event["peak_affected_count"],
            peak_network_pct=event["peak_network_pct"],
            remaining_validators_if_failed=event["remaining_validators_if_failed"],
            mitigation_guidance=event["mitigation_guidance"],
            suspected_cause=event["suspected_cause"],
            end_timestamp=round_summary["timestamp"],
        )


def _current_concentration_risks(scores: list) -> list[dict]:
    entries = []
    for correlation_type in ("provider", "asn", "country"):
        counts = Counter(
            _dependency_for_score(score, correlation_type)
            for score in scores
            if _is_enriched(score) and _dependency_for_score(score, correlation_type)
        )
        for dependency_value, count in counts.items():
            affected_count, network_pct, remaining, consensus_risk = _blast_radius_for_dependency(scores, correlation_type, dependency_value)
            entries.append(
                {
                    "dependency_type": correlation_type,
                    "dependency_value": dependency_value,
                    "affected_validators": affected_count,
                    "network_pct": network_pct,
                    "remaining_validators_if_failed": remaining,
                    "consensus_risk": consensus_risk,
                    "mitigation_guidance": (
                        f"{dependency_value} represents a standing {correlation_type} concentration risk. "
                        f"Review /diversity to reduce single-provider dependency."
                    ),
                }
            )
    entries.sort(key=lambda item: (item["affected_validators"], item["network_pct"], item["dependency_value"]), reverse=True)
    return entries[:10]


async def build_blast_radius_report(db) -> dict:
    round_id, timestamp, scores = await db.get_latest_scores()
    if round_id is None or not scores:
        raise ValueError("No scoring data available yet")

    active = await db.get_correlated_events(status="open", limit=50)
    historical = await db.get_correlated_events(status="closed", limit=100)
    concentration_risks = _current_concentration_risks(scores)

    return {
        "round_id": round_id,
        "timestamp": timestamp,
        "total_validators": len(scores),
        "concentration_risks": concentration_risks,
        "active_correlations": active,
        "historical_correlations": historical,
        "json_report_url": "/api/blast-radius",
    }


async def inject_synthetic_correlated_event(db, provider: str | None = None) -> dict:
    round_id, timestamp, scores = await db.get_latest_scores()
    if round_id is None or not scores:
        raise ValueError("No scoring data available yet")

    provider_counts = Counter(score.metrics.isp for score in scores if score.metrics.isp)
    dependency_value = provider
    if not dependency_value or provider_counts.get(dependency_value, 0) < DETECTION_THRESHOLD:
        dependency_value = provider_counts.most_common(1)[0][0] if provider_counts else None
    if not dependency_value:
        raise ValueError("No provider data available for synthetic correlated event")

    affected_scores = [score for score in scores if score.metrics.isp == dependency_value][: max(DETECTION_THRESHOLD, 3)]
    affected_validators = sorted(score.public_key for score in affected_scores)
    affected_count = len(affected_validators)
    network_pct = round((affected_count / len(scores)) * 100, 1) if scores else 0.0
    severity = _severity_for_pct(network_pct)
    _, _, remaining_if_failed, _standing_consensus_risk = _blast_radius_for_dependency(scores, "provider", dependency_value)
    consensus_risk = network_pct > CONSENSUS_RISK_THRESHOLD_PCT
    mitigation_guidance = _mitigation_guidance("provider", dependency_value, affected_count)
    suspected_cause = _correlation_label("provider", dependency_value)
    avg_drop = 6.0

    event_id = await db.create_correlated_event(
        correlation_type="provider",
        dependency_value=dependency_value,
        severity=severity,
        status="open",
        synthetic=True,
        start_round_id=round_id,
        latest_round_id=round_id,
        start_timestamp=timestamp,
        latest_timestamp=timestamp,
        affected_validators=affected_validators,
        triggering_incident_ids=[],
        affected_count=affected_count,
        network_pct=network_pct,
        consensus_risk=consensus_risk,
        avg_score_drop=avg_drop,
        peak_affected_count=affected_count,
        peak_network_pct=network_pct,
        remaining_validators_if_failed=remaining_if_failed,
        mitigation_guidance=mitigation_guidance,
        suspected_cause=suspected_cause,
    )
    return await db.get_correlated_event(event_id)
