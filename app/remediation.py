import re
import asyncio
from collections import defaultdict

from app.diagnostics import build_diagnostic_report
from app.models import ValidatorScore
from app.peers import build_peer_report
from app.readiness import build_readiness_report
from app.scorer import WEIGHTS

SEVERITY_ORDER = {"critical": 0, "warning": 1, "advisory": 2}
CATEGORY_ORDER = {
    "version": 0,
    "docker": 1,
    "firewall": 2,
    "peer config": 3,
    "domain/attestation": 4,
    "performance": 5,
    "operations": 6,
}
METRIC_IMPACT = {
    "agreement_1h": WEIGHTS["agreement_1h"] * 100,
    "agreement_24h": WEIGHTS["agreement_24h"] * 100,
    "agreement_30d": WEIGHTS["agreement_30d"] * 100,
    "uptime": WEIGHTS["uptime"] * 100,
    "poll_success": WEIGHTS["poll_success"] * 100,
    "latency": WEIGHTS["latency"] * 100,
    "peer_count": WEIGHTS["peer_count"] * 100,
    "version": WEIGHTS["version"] * 100,
    "diversity": WEIGHTS["diversity"] * 100,
    "server_state": WEIGHTS["version"] * 100,
    "well_known_attestation": 0.0,
    "domain_dns_match": 0.0,
    "domain_configured": 0.0,
    "incident": 2.5,
    "peers": 1.5,
}


def _normalize_text(value: str | None) -> str:
    if not value:
        return "unknown"
    lowered = value.strip().lower()
    lowered = re.sub(r"`", "", lowered)
    lowered = re.sub(r"\s+", "_", lowered)
    lowered = re.sub(r"[^a-z0-9_.%-]+", "_", lowered)
    return lowered.strip("_") or "unknown"


def _slug(value: str) -> str:
    return _normalize_text(value).replace("%", "pct")


def _category_for_metric(metric: str, title: str | None = None) -> str:
    metric_key = metric.lower()
    title_key = (title or "").lower()
    if metric_key in {"version"} or "version" in title_key:
        return "version"
    if metric_key in {"peer_count"} or "peer" in title_key:
        return "peer config"
    if metric_key in {"server_state", "uptime", "poll_success"}:
        return "docker"
    if metric_key in {"agreement_1h", "agreement_24h", "agreement_30d", "latency"}:
        return "performance"
    if "domain" in title_key or "attestation" in title_key:
        return "domain/attestation"
    if "firewall" in title_key:
        return "firewall"
    return "operations"


def _commands_for(metric: str, category: str, expected: str, detected: str) -> tuple[list[str], str | None]:
    metric_key = metric.lower()
    if metric_key == "version":
        return (
            [
                "docker compose pull",
                "docker compose up -d",
                "docker compose ps",
            ],
            "If the upgrade behaves unexpectedly, inspect the recent container logs with `docker logs postfiatd --tail 50` before rolling back.",
        )
    if metric_key == "peer_count" or category == "firewall":
        return (
            [
                "ufw allow 2559/tcp",
                "ss -ltnp | grep 2559",
                "docker logs postfiatd --tail 50",
            ],
            "If opening the port changes nothing, confirm the host firewall and cloud security group match the validator's public address before reverting.",
        )
    if metric_key in {"server_state", "uptime", "poll_success"}:
        return (
            [
                "docker compose ps",
                "docker logs postfiatd --tail 100",
                "docker compose restart",
            ],
            "If the restart causes sync issues, stop and review logs before repeating the action.",
        )
    if metric_key in {"agreement_1h", "agreement_24h", "agreement_30d", "latency"}:
        return (
            [
                "chronyc tracking || ntpq -p",
                "docker logs postfiatd --tail 100",
                "ping -c 5 1.1.1.1",
            ],
            "If the node is already catching up on its own, avoid repeated restarts until you confirm the issue is persistent.",
        )
    if metric_key in {"domain_configured", "domain_dns_match", "well_known_attestation"} or category == "domain/attestation":
        return (
            [
                "dig +short your-validator-domain.example",
                "curl -i https://your-validator-domain.example/.well-known/postfiat.toml",
            ],
            "If DNS or attestation changes do not propagate immediately, wait for TTL expiration before changing records again.",
        )
    if metric_key == "diversity":
        return (
            [
                "# review the diversity and peer tools before changing hosting",
                "open https://dashboard.pftoligarchy.com/diversity",
            ],
            None,
        )
    if metric_key == "peers":
        return (
            [
                "# add candidate peers in your validator config",
                "# remove concentrated or risky peers one at a time",
                "docker compose restart",
            ],
            "After changing peers, verify connectivity and agreement before removing additional peers.",
        )
    if metric_key == "incident":
        return (
            [
                "docker logs postfiatd --tail 100",
                "docker compose ps",
            ],
            "Use the incident timeline to confirm the condition has actually recovered before closing the loop operationally.",
        )
    return (
        [
            "docker compose ps",
            "docker logs postfiatd --tail 50",
        ],
        None,
    )


def _estimate(metric: str, *, approximate: bool = False, multiplier: float = 1.0) -> tuple[float, str]:
    base = METRIC_IMPACT.get(metric, 0.0) * multiplier
    return round(base, 1), ("approximate" if approximate else "direct")


def _dedupe_key(category: str, metric: str, target: str) -> str:
    return f"{_slug(category)}::{_slug(metric)}::{_slug(target)}"


def _specificity_score(item: dict) -> tuple[int, int]:
    return (len(item.get("commands", [])), len(item.get("summary", "")))


def _make_item(
    *,
    source: str,
    source_timestamp: str,
    category: str,
    metric: str,
    severity: str,
    title: str,
    detected_value: str,
    expected_value: str,
    summary: str,
    approximate: bool,
    multiplier: float = 1.0,
) -> dict:
    commands, rollback_note = _commands_for(metric, category, expected_value, detected_value)
    impact, confidence = _estimate(metric, approximate=approximate, multiplier=multiplier)
    return {
        "source": source,
        "sources": [source],
        "source_timestamp": source_timestamp,
        "category": category,
        "metric": metric,
        "severity": severity,
        "title": title,
        "detected_value": detected_value,
        "expected_value": expected_value,
        "summary": summary,
        "commands": commands,
        "rollback_note": rollback_note,
        "estimated_score_impact": impact,
        "impact_confidence": confidence,
        "dedupe_key": _dedupe_key(category, metric, expected_value),
    }


def _normalize_diagnostic_finding(finding: dict, timestamp: str) -> dict:
    category = _category_for_metric(finding["metric"], finding["title"])
    return _make_item(
        source="diagnose",
        source_timestamp=timestamp,
        category=category,
        metric=finding["metric"],
        severity=finding["severity"],
        title=finding["title"],
        detected_value=finding["current_value"],
        expected_value=finding["threshold_value"],
        summary=finding["recommended_action"],
        approximate=finding["metric"] in {"diversity"},
    )


def _metric_from_readiness_name(name: str) -> str:
    mapping = {
        "Version parity": "version",
        "Peer floor": "peer_count",
        "Server state": "server_state",
        "24h agreement": "agreement_24h",
        "Ledger freshness": "uptime",
        "Domain configured": "domain_configured",
        "Domain DNS match": "domain_dns_match",
        "Well-known attestation": "well_known_attestation",
    }
    return mapping.get(name, _normalize_text(name))


def _severity_from_readiness_status(status: str) -> str:
    return {"fail": "critical", "warn": "warning"}.get(status, "advisory")


def _normalize_readiness_check(check: dict) -> dict:
    metric = _metric_from_readiness_name(check["name"])
    category = _category_for_metric(metric, check["name"])
    return _make_item(
        source="readiness",
        source_timestamp=check["source_timestamp"],
        category=category,
        metric=metric,
        severity=_severity_from_readiness_status(check["status"]),
        title=check["name"],
        detected_value=check["detected_value"],
        expected_value=check["expected_value"],
        summary=check.get("remediation") or "Readiness check passed no remediation details, but this was still included for normalization.",
        approximate=metric in {"domain_configured", "domain_dns_match", "well_known_attestation"},
    )


def _normalize_incident(incident: dict) -> dict:
    event_type = incident["event_types"][0] if incident.get("event_types") else "incident"
    phase = "open" if incident["status"] == "open" else "recently_resolved"
    current_values = incident.get("during_values") or {}
    detected = current_values.get("server_state") or current_values.get("peer_count") or current_values.get("composite_score") or incident["summary"]
    metric = "incident"
    if "peer" in event_type:
        metric = "peer_count"
    elif "agreement" in event_type:
        metric = "agreement_24h"
    elif "version" in event_type:
        metric = "version"
    elif "server_state" in event_type:
        metric = "server_state"
    elif "score" in event_type:
        metric = "poll_success"

    category = _category_for_metric(metric, incident["summary"])
    severity = "advisory" if phase == "recently_resolved" else incident["severity"]
    return _make_item(
        source="incidents",
        source_timestamp=incident["latest_event_time"],
        category=category,
        metric=metric if metric != "poll_success" else "incident",
        severity=severity,
        title=incident["summary"],
        detected_value=str(detected),
        expected_value="no active incident",
        summary=f"{phase.replace('_', ' ')} incident context from the incident timeline. Review before assuming the issue is fully resolved.",
        approximate=True,
        multiplier=0.6 if phase == "recently_resolved" else 1.0,
    )


def _normalize_peer_report(report: dict, timestamp: str) -> list[dict]:
    items: list[dict] = []
    for finding in report.get("risk_findings", []):
        severity = "warning" if finding["severity"] in {"warn", "risk"} else "advisory"
        items.append(_make_item(
            source="peers",
            source_timestamp=timestamp,
            category="peer config",
            metric="peers",
            severity=severity,
            title=finding["title"],
            detected_value=finding["detail"],
            expected_value="diverse, healthy peer distribution",
            summary=finding["detail"],
            approximate=True,
            multiplier=0.8,
        ))
    for recommendation in report.get("drop_recommendations", []):
        items.append(_make_item(
            source="peers",
            source_timestamp=timestamp,
            category="peer config",
            metric="peers",
            severity="warning",
            title=f"Consider dropping {_normalize_text(recommendation.get('node_public_key'))}",
            detected_value=recommendation["reason"],
            expected_value="peer set avoids risky or concentrated nodes",
            summary=recommendation["reason"],
            approximate=True,
            multiplier=0.8,
        ))
    return items


def _merge_duplicates(items: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for item in items:
        key = item["dedupe_key"]
        existing = merged.get(key)
        if not existing:
            merged[key] = item
            continue

        if SEVERITY_ORDER[item["severity"]] < SEVERITY_ORDER[existing["severity"]]:
            winner, loser = item, existing
        elif SEVERITY_ORDER[item["severity"]] > SEVERITY_ORDER[existing["severity"]]:
            winner, loser = existing, item
        else:
            winner, loser = max((existing, item), key=_specificity_score), min((existing, item), key=_specificity_score)

        merged_item = dict(winner)
        merged_item["sources"] = sorted(set(existing.get("sources", [existing["source"]])) | set(item.get("sources", [item["source"]])))
        if _specificity_score(loser) > _specificity_score(winner):
            merged_item["summary"] = loser["summary"]
        if _specificity_score(item) > _specificity_score(existing):
            merged_item["commands"] = item["commands"]
            merged_item["rollback_note"] = item["rollback_note"]
        merged_item["estimated_score_impact"] = max(existing["estimated_score_impact"], item["estimated_score_impact"])
        merged[key] = merged_item
    return list(merged.values())


def _sort_items(items: list[dict]) -> list[dict]:
    return sorted(
        items,
        key=lambda item: (
            SEVERITY_ORDER.get(item["severity"], 99),
            -item["estimated_score_impact"],
            CATEGORY_ORDER.get(item["category"], 99),
            item["title"],
        ),
    )


async def build_remediation_report(db, round_id: int, timestamp: str, scores: list[ValidatorScore], public_key: str) -> dict:
    if not scores:
        raise ValueError("No scoring data available yet")

    validator = next((score for score in scores if score.public_key == public_key), None)
    if not validator:
        raise KeyError(public_key)

    source_status = {}
    items: list[dict] = []

    readiness_task = asyncio.create_task(build_readiness_report(round_id, timestamp, scores, public_key))
    peers_task = asyncio.create_task(build_peer_report(scores, public_key))

    try:
        readiness = await readiness_task
        source_status["readiness"] = {
            "timestamp": readiness["timestamp"],
            "status": readiness["overall_status"],
            "json_report_url": readiness["json_report_url"],
        }
        items.extend(
            _normalize_readiness_check(check)
            for check in readiness["checks"]
            if check["status"] != "pass"
        )
    except Exception:
        readiness = None
        source_status["readiness"] = {"timestamp": timestamp, "status": "unavailable", "json_report_url": None}

    try:
        diagnose = build_diagnostic_report(round_id, timestamp, scores, public_key)
        source_status["diagnose"] = {
            "timestamp": diagnose["timestamp"],
            "status": diagnose["overall_status"],
            "json_report_url": diagnose["json_report_url"],
        }
        items.extend(_normalize_diagnostic_finding(finding, diagnose["timestamp"]) for finding in diagnose["findings"])
    except Exception:
        diagnose = None
        source_status["diagnose"] = {"timestamp": timestamp, "status": "unavailable", "json_report_url": None}

    try:
        peers = await peers_task
        source_status["peers"] = {
            "timestamp": timestamp,
            "status": peers["mode"],
            "json_report_url": peers["json_report_url"],
        }
        items.extend(_normalize_peer_report(peers, timestamp))
    except Exception:
        peers = None
        source_status["peers"] = {"timestamp": timestamp, "status": "unavailable", "json_report_url": None}

    open_incidents = await db.get_incidents(validator_key=public_key, status="open", limit=50)
    recent_incidents = await db.get_incidents(validator_key=public_key, status="closed", limit=5)
    source_status["incidents_open"] = {"timestamp": open_incidents[0]["latest_event_time"] if open_incidents else timestamp, "status": "ok", "count": len(open_incidents)}
    source_status["incidents_recent"] = {"timestamp": recent_incidents[0]["latest_event_time"] if recent_incidents else timestamp, "status": "ok", "count": len(recent_incidents)}

    items.extend(_normalize_incident(incident) for incident in open_incidents)
    items.extend(_normalize_incident(incident) for incident in recent_incidents)

    merged = _sort_items(_merge_duplicates(items))
    actionable = [item for item in merged if item["severity"] in {"critical", "warning"}]
    advisories = [item for item in merged if item["severity"] == "advisory"]
    summary_counts = defaultdict(int)
    for item in merged:
        summary_counts[item["severity"]] += 1

    return {
        "public_key": public_key,
        "domain": validator.domain,
        "round_id": round_id,
        "timestamp": timestamp,
        "status_summary": "No action needed" if not actionable else f"{len(actionable)} actionable remediation item{'s' if len(actionable) != 1 else ''}",
        "total_estimated_score_improvement": round(sum(item["estimated_score_impact"] for item in actionable), 1),
        "summary_counts": {
            "critical": summary_counts["critical"],
            "warning": summary_counts["warning"],
            "advisory": summary_counts["advisory"],
        },
        "source_status": source_status,
        "actionable_findings": actionable,
        "advisories": advisories,
        "json_report_url": f"/api/remediate/{public_key}",
    }
