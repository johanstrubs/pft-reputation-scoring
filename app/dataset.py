from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from app.upgrades import _display_version, _parse_semver

DATASET_SCHEMA_VERSION = "v1"
NETWORK_HEALTH_FORMULA_VERSION = "v1"
MAX_TIMESERIES_DAYS = 365

NETWORK_HEALTH_COMPONENTS = {
    "provider_concentration": {
        "weight": 0.30,
        "description": "Inverse normalized HHI across enriched validators grouped by provider. Higher score means lower provider concentration.",
    },
    "geographic_concentration": {
        "weight": 0.20,
        "description": "Inverse normalized HHI across enriched validators grouped by country. Higher score means lower country concentration.",
    },
    "version_adoption": {
        "weight": 0.20,
        "description": "Percentage of validators on the highest semantic version currently observed in the snapshot.",
    },
    "incident_freedom": {
        "weight": 0.15,
        "description": "Percentage of validators without an open incident as of the snapshot timestamp.",
    },
    "topology_enrichment_coverage": {
        "weight": 0.15,
        "description": "Percentage of validators with full enrichment fields available (provider, ASN, country, latency, peer count).",
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_fully_enriched(row: dict) -> bool:
    return all(
        row.get(field) is not None
        for field in ("isp", "asn", "country", "latency_ms", "peer_count")
    )


def _safe_float(value: Any, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _normalize_semver_for_dataset(version: str | None) -> str | None:
    if not version:
        return None
    parsed = _parse_semver(str(version).strip())
    if not parsed:
        return None
    major, minor, patch, is_final, suffix = parsed
    base = f"{major}.{minor}.{patch}"
    return base if is_final else f"{base}-{suffix}"


def _version_sort_key(version: str | None) -> tuple:
    parsed = _parse_semver(version) if version else None
    if parsed:
        return (1, parsed)
    if version:
        return (0, str(version))
    return (-1, "")


def _highest_observed_version(rows: list[dict]) -> str | None:
    versions = []
    for row in rows:
        version = row.get("server_version")
        normalized = _normalize_semver_for_dataset(version)
        if normalized:
            versions.append(normalized)
    if versions:
        return max(versions, key=_version_sort_key)

    # Fallback for non-standard strings: group raw strings and choose lexicographically for consistency.
    raw_versions = [str(row.get("server_version")).strip() for row in rows if row.get("server_version")]
    return max(raw_versions) if raw_versions else None


def _version_distribution(rows: list[dict]) -> list[dict]:
    counts = Counter(_display_version(row.get("server_version")) for row in rows)
    total = len(rows)
    return [
        {
            "version": version,
            "count": count,
            "percentage": round((count / total) * 100, 1) if total else 0.0,
        }
        for version, count in sorted(counts.items(), key=lambda item: (_version_sort_key(item[0]), item[0]), reverse=True)
    ]


def _concentration_entries(rows: list[dict], field: str, label: str) -> list[dict]:
    enriched = [row for row in rows if _is_fully_enriched(row)]
    counts = Counter(row.get(field) for row in enriched if row.get(field) is not None)
    total = sum(counts.values())
    entries = []
    for value, count in counts.most_common():
        entries.append(
            {
                label: value,
                "count": count,
                "percentage": round((count / total) * 100, 1) if total else 0.0,
            }
        )
    return entries


def _hhi_from_counts(counts: Counter) -> float:
    total = sum(counts.values())
    if total <= 0:
        return 1.0
    return sum((count / total) ** 2 for count in counts.values())


def _inverse_normalized_hhi_score(counts: Counter) -> tuple[float, float]:
    total = sum(counts.values())
    categories = len(counts)
    if total <= 0 or categories <= 1:
        raw_hhi = 1.0 if total > 0 else 0.0
        return raw_hhi, 0.0 if total > 0 else 100.0

    raw_hhi = _hhi_from_counts(counts)
    effective_count = 1.0 / raw_hhi if raw_hhi > 0 else 0.0
    normalized = ((effective_count - 1.0) / (categories - 1.0)) * 100.0
    return raw_hhi, round(max(0.0, min(100.0, normalized)), 2)


def _incident_summary(incidents: list[dict]) -> tuple[list[dict], dict[str, int]]:
    severity_counts = Counter(incident["severity"] for incident in incidents)
    items = []
    for incident in incidents:
        items.append(
            {
                "id": incident["id"],
                "validator_key": incident["validator_key"],
                "severity": incident["severity"],
                "status": incident["status"],
                "summary": incident["summary"],
                "start_time": incident["start_time"],
                "end_time": incident["end_time"],
                "latest_round_id": incident["latest_round_id"],
                "latest_event_time": incident["latest_event_time"],
                "event_types": incident["event_types"],
                "active_event_types": incident["active_event_types"],
                "synthetic": incident["synthetic"],
                "correlated": incident["correlated"],
            }
        )
    return items, dict(severity_counts)


def _topology_rows(rows: list[dict]) -> list[dict]:
    entries = []
    for row in rows:
        entries.append(
            {
                "public_key": row["public_key"],
                "domain": row.get("domain"),
                "provider": row.get("isp"),
                "asn": row.get("asn"),
                "country": row.get("country"),
                "latency_ms": row.get("latency_ms"),
                "peer_count": row.get("peer_count"),
                "server_state": row.get("server_state"),
                "server_version": row.get("server_version"),
                "node_ip": row.get("node_ip"),
                "enriched": _is_fully_enriched(row),
            }
        )
    return entries


def _score_rows(rows: list[dict]) -> list[dict]:
    entries = []
    for rank, row in enumerate(sorted(rows, key=lambda item: (-item["composite_score"], item["public_key"])), start=1):
        entries.append(
            {
                "public_key": row["public_key"],
                "domain": row.get("domain"),
                "rank": rank,
                "composite_score": row["composite_score"],
                "metrics": {
                    "agreement_1h": row.get("agreement_1h"),
                    "agreement_1h_total": row.get("agreement_1h_total"),
                    "agreement_24h": row.get("agreement_24h"),
                    "agreement_24h_total": row.get("agreement_24h_total"),
                    "agreement_30d": row.get("agreement_30d"),
                    "agreement_30d_total": row.get("agreement_30d_total"),
                    "poll_success_pct": row.get("poll_success_pct"),
                    "uptime_seconds": row.get("uptime_seconds"),
                    "uptime_pct": row.get("uptime_pct"),
                    "latency_ms": row.get("latency_ms"),
                    "peer_count": row.get("peer_count"),
                    "avg_ledger_interval": row.get("avg_ledger_interval"),
                    "validated_ledger_age": row.get("validated_ledger_age"),
                    "server_version": row.get("server_version"),
                    "server_state": row.get("server_state"),
                    "asn": row.get("asn"),
                    "provider": row.get("isp"),
                    "country": row.get("country"),
                    "node_ip": row.get("node_ip"),
                },
                "sub_scores": {
                    "agreement_1h": row.get("agreement_1h_score") or 0.0,
                    "agreement_24h": row.get("agreement_24h_score") or 0.0,
                    "agreement_30d": row.get("agreement_30d_score") or 0.0,
                    "uptime": row.get("uptime_score") or 0.0,
                    "poll_success": row.get("poll_success_score") or 0.0,
                    "latency": row.get("latency_score") or 0.0,
                    "peer_count": row.get("peer_count_score") or 0.0,
                    "version": row.get("version_score") or 0.0,
                    "diversity": row.get("diversity_score") or 0.0,
                },
            }
        )
    return entries


def _health_index(rows: list[dict], incidents: list[dict]) -> dict:
    total_validators = len(rows)
    enriched = [row for row in rows if _is_fully_enriched(row)]

    provider_counts = Counter(row.get("isp") for row in enriched if row.get("isp"))
    country_counts = Counter(row.get("country") for row in enriched if row.get("country"))

    provider_hhi, provider_score = _inverse_normalized_hhi_score(provider_counts)
    country_hhi, country_score = _inverse_normalized_hhi_score(country_counts)

    latest_version = _highest_observed_version(rows)
    on_latest = 0
    for row in rows:
        normalized = _normalize_semver_for_dataset(row.get("server_version"))
        candidate = normalized or (str(row.get("server_version")).strip() if row.get("server_version") else None)
        if latest_version and candidate == latest_version:
            on_latest += 1
    version_adoption = round((on_latest / total_validators) * 100, 2) if total_validators else 0.0

    open_validators = {incident["validator_key"] for incident in incidents}
    incident_freedom = round(((total_validators - len(open_validators)) / total_validators) * 100, 2) if total_validators else 100.0
    coverage = round((len(enriched) / total_validators) * 100, 2) if total_validators else 0.0

    component_entries = {
        "provider_concentration": {
            "weight": NETWORK_HEALTH_COMPONENTS["provider_concentration"]["weight"],
            "raw_value": round(provider_hhi, 6),
            "raw_unit": "HHI",
            "normalized_score": provider_score,
            "weighted_contribution": round(provider_score * NETWORK_HEALTH_COMPONENTS["provider_concentration"]["weight"], 2),
            "description": NETWORK_HEALTH_COMPONENTS["provider_concentration"]["description"],
        },
        "geographic_concentration": {
            "weight": NETWORK_HEALTH_COMPONENTS["geographic_concentration"]["weight"],
            "raw_value": round(country_hhi, 6),
            "raw_unit": "HHI",
            "normalized_score": country_score,
            "weighted_contribution": round(country_score * NETWORK_HEALTH_COMPONENTS["geographic_concentration"]["weight"], 2),
            "description": NETWORK_HEALTH_COMPONENTS["geographic_concentration"]["description"],
        },
        "version_adoption": {
            "weight": NETWORK_HEALTH_COMPONENTS["version_adoption"]["weight"],
            "raw_value": version_adoption,
            "raw_unit": "percent_on_latest",
            "normalized_score": version_adoption,
            "weighted_contribution": round(version_adoption * NETWORK_HEALTH_COMPONENTS["version_adoption"]["weight"], 2),
            "description": NETWORK_HEALTH_COMPONENTS["version_adoption"]["description"],
        },
        "incident_freedom": {
            "weight": NETWORK_HEALTH_COMPONENTS["incident_freedom"]["weight"],
            "raw_value": incident_freedom,
            "raw_unit": "percent_without_open_incidents",
            "normalized_score": incident_freedom,
            "weighted_contribution": round(incident_freedom * NETWORK_HEALTH_COMPONENTS["incident_freedom"]["weight"], 2),
            "description": NETWORK_HEALTH_COMPONENTS["incident_freedom"]["description"],
        },
        "topology_enrichment_coverage": {
            "weight": NETWORK_HEALTH_COMPONENTS["topology_enrichment_coverage"]["weight"],
            "raw_value": coverage,
            "raw_unit": "percent_with_full_enrichment",
            "normalized_score": coverage,
            "weighted_contribution": round(coverage * NETWORK_HEALTH_COMPONENTS["topology_enrichment_coverage"]["weight"], 2),
            "description": NETWORK_HEALTH_COMPONENTS["topology_enrichment_coverage"]["description"],
        },
    }
    composite = round(sum(component["weighted_contribution"] for component in component_entries.values()), 2)

    return {
        "formula_version": NETWORK_HEALTH_FORMULA_VERSION,
        "score": composite,
        "score_semantics": "Higher is healthier. Lower values indicate greater fragility.",
        "latest_version": latest_version,
        "formula": {
            "summary": "Weighted average of provider concentration (inverse normalized HHI), geographic concentration (inverse normalized HHI), version adoption, incident freedom, and topology enrichment coverage.",
            "components": NETWORK_HEALTH_COMPONENTS,
        },
        "components": component_entries,
    }


def _metadata_from_snapshots(snapshots: list[dict]) -> dict:
    if not snapshots:
        return {
            "dataset_schema_version": DATASET_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "date_range": {"start": None, "end": None},
            "total_daily_snapshots": 0,
            "total_validator_day_score_records": 0,
        }
    return {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "date_range": {
            "start": snapshots[0]["snapshot_date"],
            "end": snapshots[-1]["snapshot_date"],
        },
        "total_daily_snapshots": len(snapshots),
        "total_validator_day_score_records": sum(len(snapshot["validator_scores"]) for snapshot in snapshots),
    }


async def _load_snapshot_rounds_and_rows(db) -> tuple[list[dict], dict[int, list[dict]]]:
    daily_rounds = await db.get_daily_snapshot_rounds()
    round_ids = [row["id"] for row in daily_rounds]
    score_rows = await db.get_validator_score_rows_for_round_ids(round_ids)
    rows_by_round: dict[int, list[dict]] = defaultdict(list)
    for row in score_rows:
        rows_by_round[row["round_id"]].append(row)
    return daily_rounds, rows_by_round


async def build_daily_snapshot(db, snapshot_date: str) -> dict:
    daily_rounds, rows_by_round = await _load_snapshot_rounds_and_rows(db)
    target_round = next((row for row in daily_rounds if row["snapshot_date"] == snapshot_date), None)
    if not target_round:
        raise KeyError(snapshot_date)

    all_snapshots = []
    for round_row in daily_rounds:
        rows = rows_by_round.get(round_row["id"], [])
        incidents = await db.get_incidents_open_as_of(round_row["timestamp"])
        snapshot = _build_snapshot_from_round(round_row, rows, incidents)
        all_snapshots.append(snapshot)

    metadata = _metadata_from_snapshots(all_snapshots)
    for snapshot in all_snapshots:
        snapshot["dataset_metadata"] = metadata

    return next(snapshot for snapshot in all_snapshots if snapshot["snapshot_date"] == snapshot_date)


def _build_snapshot_from_round(round_row: dict, rows: list[dict], incidents: list[dict]) -> dict:
    incident_items, severity_counts = _incident_summary(incidents)
    topology = _topology_rows(rows)
    score_entries = _score_rows(rows)
    version_distribution = _version_distribution(rows)
    concentration = {
        "providers": _concentration_entries(rows, "isp", "provider"),
        "asns": _concentration_entries(rows, "asn", "asn"),
        "countries": _concentration_entries(rows, "country", "country"),
    }
    return {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "snapshot_date": round_row["snapshot_date"],
        "round_id": round_row["id"],
        "timestamp": round_row["timestamp"],
        "validator_count": round_row["validator_count"],
        "round_summary": {
            "avg_score": _safe_float(round_row.get("avg_score")),
            "min_score": _safe_float(round_row.get("min_score")),
            "max_score": _safe_float(round_row.get("max_score")),
        },
        "validator_scores": score_entries,
        "topology": topology,
        "incidents": {
            "open_incidents": incident_items,
            "severity_counts": severity_counts,
            "open_incident_count": len(incident_items),
        },
        "version_distribution": version_distribution,
        "concentration_metrics": concentration,
        "network_health_index": _health_index(rows, incidents),
    }


async def build_latest_dataset_snapshot(db) -> dict:
    daily_rounds, rows_by_round = await _load_snapshot_rounds_and_rows(db)
    if not daily_rounds:
        raise ValueError("No scoring data available yet")

    snapshots = []
    for round_row in daily_rounds:
        incidents = await db.get_incidents_open_as_of(round_row["timestamp"])
        snapshot = _build_snapshot_from_round(round_row, rows_by_round.get(round_row["id"], []), incidents)
        snapshots.append(snapshot)
    metadata = _metadata_from_snapshots(snapshots)
    latest = snapshots[-1]
    latest["dataset_metadata"] = metadata
    return latest


async def build_all_dataset_snapshots(db) -> list[dict]:
    daily_rounds, rows_by_round = await _load_snapshot_rounds_and_rows(db)
    snapshots = []
    for round_row in daily_rounds:
        incidents = await db.get_incidents_open_as_of(round_row["timestamp"])
        snapshots.append(_build_snapshot_from_round(round_row, rows_by_round.get(round_row["id"], []), incidents))
    return snapshots


def _score_map(snapshot: dict) -> dict[str, dict]:
    return {entry["public_key"]: entry for entry in snapshot["validator_scores"]}


def _incident_map(snapshot: dict) -> dict[int, dict]:
    return {entry["id"]: entry for entry in snapshot["incidents"]["open_incidents"]}


def _concentration_map(entries: list[dict], key: str) -> dict[str, int]:
    return {str(entry[key]): entry["count"] for entry in entries}


async def build_dataset_diff(db, date1: str, date2: str) -> dict:
    snapshot1 = await build_daily_snapshot(db, date1)
    snapshot2 = await build_daily_snapshot(db, date2)

    scores1 = _score_map(snapshot1)
    scores2 = _score_map(snapshot2)
    keys1 = set(scores1)
    keys2 = set(scores2)

    shared = keys1 & keys2
    score_changes = []
    rank_changes = []
    for public_key in sorted(shared):
        old = scores1[public_key]
        new = scores2[public_key]
        if old["composite_score"] != new["composite_score"]:
            score_changes.append(
                {
                    "public_key": public_key,
                    "old_score": old["composite_score"],
                    "new_score": new["composite_score"],
                    "score_delta": round(new["composite_score"] - old["composite_score"], 2),
                }
            )
        if old["rank"] != new["rank"]:
            rank_changes.append(
                {
                    "public_key": public_key,
                    "old_rank": old["rank"],
                    "new_rank": new["rank"],
                    "rank_delta": old["rank"] - new["rank"],
                }
            )

    incidents1 = _incident_map(snapshot1)
    incidents2 = _incident_map(snapshot2)
    opened_ids = sorted(set(incidents2) - set(incidents1))
    closed_ids = sorted(set(incidents1) - set(incidents2))

    concentration_deltas = {}
    for dimension, key in (("providers", "provider"), ("asns", "asn"), ("countries", "country")):
        old_map = _concentration_map(snapshot1["concentration_metrics"][dimension], key)
        new_map = _concentration_map(snapshot2["concentration_metrics"][dimension], key)
        values = sorted(set(old_map) | set(new_map))
        concentration_deltas[dimension] = [
            {
                key: value,
                "old_count": old_map.get(value, 0),
                "new_count": new_map.get(value, 0),
                "count_delta": new_map.get(value, 0) - old_map.get(value, 0),
            }
            for value in values
            if old_map.get(value, 0) != new_map.get(value, 0)
        ]

    version_old = {entry["version"]: entry["count"] for entry in snapshot1["version_distribution"]}
    version_new = {entry["version"]: entry["count"] for entry in snapshot2["version_distribution"]}
    version_distribution_delta = [
        {
            "version": version,
            "old_count": version_old.get(version, 0),
            "new_count": version_new.get(version, 0),
            "count_delta": version_new.get(version, 0) - version_old.get(version, 0),
        }
        for version in sorted(set(version_old) | set(version_new), key=_version_sort_key, reverse=True)
        if version_old.get(version, 0) != version_new.get(version, 0)
    ]

    return {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "from_snapshot_date": date1,
        "to_snapshot_date": date2,
        "validators_added": sorted(keys2 - keys1),
        "validators_removed": sorted(keys1 - keys2),
        "score_changes": score_changes,
        "rank_changes": rank_changes,
        "incidents_opened": [incidents2[incident_id] for incident_id in opened_ids],
        "incidents_closed": [incidents1[incident_id] for incident_id in closed_ids],
        "concentration_deltas": concentration_deltas,
        "version_distribution_delta": version_distribution_delta,
    }


async def build_dataset_timeseries(db, public_key: str, days: int = 30) -> dict:
    requested_days = max(1, min(days, MAX_TIMESERIES_DAYS))
    snapshots = await build_all_dataset_snapshots(db)
    if not snapshots:
        raise ValueError("No scoring data available yet")

    sliced = snapshots[-requested_days:]
    history = []
    found = False
    for snapshot in sliced:
        scores = _score_map(snapshot)
        if public_key not in scores:
            continue
        found = True
        score = scores[public_key]
        history.append(
            {
                "date": snapshot["snapshot_date"],
                "round_id": snapshot["round_id"],
                "timestamp": snapshot["timestamp"],
                "rank": score["rank"],
                "composite_score": score["composite_score"],
                "metrics": score["metrics"],
                "sub_scores": score["sub_scores"],
            }
        )
    if not found:
        raise KeyError(public_key)
    return {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "public_key": public_key,
        "days": requested_days,
        "history": history,
    }


def _schema_fields() -> dict[str, Any]:
    return {
        "snapshot": [
            {
                "name": "snapshot_date",
                "type": "string",
                "source": "scoring_rounds.timestamp",
                "units": "YYYY-MM-DD UTC",
                "caveats": "Uses the latest stored scoring round for that UTC day.",
            },
            {
                "name": "round_id",
                "type": "integer",
                "source": "scoring_rounds.id",
                "units": "round identifier",
                "caveats": "Latest stored round for the snapshot day.",
            },
            {
                "name": "timestamp",
                "type": "string",
                "source": "scoring_rounds.timestamp",
                "units": "ISO-8601 UTC timestamp",
                "caveats": "Canonical timestamp for all snapshot values.",
            },
            {
                "name": "validator_count",
                "type": "integer",
                "source": "scoring_rounds.validator_count",
                "units": "validators",
                "caveats": "Count captured by the selected daily round.",
            },
            {
                "name": "round_summary.avg_score",
                "type": "number",
                "source": "scoring_rounds.avg_score",
                "units": "0-100 composite score",
                "caveats": "Average across all validators in the selected round.",
            },
            {
                "name": "round_summary.min_score",
                "type": "number",
                "source": "scoring_rounds.min_score",
                "units": "0-100 composite score",
                "caveats": "Minimum composite score in the selected round.",
            },
            {
                "name": "round_summary.max_score",
                "type": "number",
                "source": "scoring_rounds.max_score",
                "units": "0-100 composite score",
                "caveats": "Maximum composite score in the selected round.",
            },
            {
                "name": "validator_scores",
                "type": "array",
                "source": "validator_scores",
                "units": "per validator",
                "caveats": "Contains composite score, rank, raw metrics, and sub-scores as of the selected daily round.",
            },
            {
                "name": "validator_scores[].public_key",
                "type": "string",
                "source": "validator_scores.public_key",
                "units": "validator key",
                "caveats": "Stable validator identifier.",
            },
            {
                "name": "validator_scores[].rank",
                "type": "integer",
                "source": "computed",
                "units": "rank position",
                "caveats": "Sorted by composite score descending, then public key ascending.",
            },
            {
                "name": "validator_scores[].composite_score",
                "type": "number",
                "source": "validator_scores.composite_score",
                "units": "0-100 composite score",
                "caveats": "Weighted score from the active methodology version at collection time.",
            },
            {
                "name": "validator_scores[].metrics.*",
                "type": "object",
                "source": "validator_scores raw metric columns",
                "units": "mixed",
                "caveats": "Includes agreement ratios, uptime, latency, peer count, ledger freshness, version, server state, and enrichment fields.",
            },
            {
                "name": "validator_scores[].sub_scores.*",
                "type": "object",
                "source": "validator_scores normalized sub-score columns",
                "units": "0.0-1.0 normalized score",
                "caveats": "Sub-score fields are normalized methodology components rather than the raw metrics.",
            },
            {
                "name": "topology",
                "type": "array",
                "source": "validator_scores",
                "units": "per validator",
                "caveats": "Latency is observer-dependent from VHS and scoring server vantage points.",
            },
            {
                "name": "topology[].provider",
                "type": "string",
                "source": "validator_scores.isp",
                "units": "provider name",
                "caveats": "Present only when enrichment data is available.",
            },
            {
                "name": "topology[].asn",
                "type": "integer",
                "source": "validator_scores.asn",
                "units": "ASN number",
                "caveats": "Present only when enrichment data is available.",
            },
            {
                "name": "topology[].country",
                "type": "string",
                "source": "validator_scores.country",
                "units": "ISO country code",
                "caveats": "Country reflects enrichment provider output, not legal jurisdiction.",
            },
            {
                "name": "topology[].latency_ms",
                "type": "number",
                "source": "validator_scores.latency_ms",
                "units": "milliseconds",
                "caveats": "Observer-dependent from VHS and scoring server vantage points.",
            },
            {
                "name": "topology[].peer_count",
                "type": "integer",
                "source": "validator_scores.peer_count",
                "units": "peers",
                "caveats": "Snapshot value only; does not encode historical peer adjacency.",
            },
            {
                "name": "topology[].enriched",
                "type": "boolean",
                "source": "computed",
                "units": "true/false",
                "caveats": "True only when provider, ASN, country, latency, and peer count are all present.",
            },
            {
                "name": "incidents.open_incidents",
                "type": "array",
                "source": "incidents",
                "units": "incident rows",
                "caveats": "Includes incidents whose start_time is at or before the snapshot timestamp and whose end_time is null or after the snapshot timestamp.",
            },
            {
                "name": "incidents.severity_counts",
                "type": "object",
                "source": "computed from open incidents",
                "units": "incident counts",
                "caveats": "Counts by severity as of the snapshot timestamp.",
            },
            {
                "name": "incidents.open_incident_count",
                "type": "integer",
                "source": "computed from open incidents",
                "units": "incidents",
                "caveats": "Total open incidents at the snapshot timestamp.",
            },
            {
                "name": "version_distribution",
                "type": "array",
                "source": "validator_scores.server_version",
                "units": "counts and percentages",
                "caveats": "Versions are compared by parsed semantic version where possible and fall back to raw string grouping for non-standard versions.",
            },
            {
                "name": "concentration_metrics.providers/asns/countries",
                "type": "object",
                "source": "computed from enriched topology rows",
                "units": "counts and percentages",
                "caveats": "Concentration metrics only include fully enriched validators to avoid distortion from unmapped rows.",
            },
            {
                "name": "network_health_index",
                "type": "object",
                "source": "computed",
                "units": "0-100 score",
                "caveats": "Higher is healthier. Lower values indicate greater fragility.",
            },
            {
                "name": "network_health_index.components.*.raw_value",
                "type": "number",
                "source": "computed",
                "units": "component-specific",
                "caveats": "HHI components expose raw HHI; adoption, incident freedom, and coverage expose percentages.",
            },
            {
                "name": "network_health_index.components.*.normalized_score",
                "type": "number",
                "source": "computed",
                "units": "0-100 score",
                "caveats": "Normalized component score before weighting.",
            },
            {
                "name": "network_health_index.components.*.weighted_contribution",
                "type": "number",
                "source": "computed",
                "units": "weighted points",
                "caveats": "Component normalized score multiplied by its fixed weight.",
            },
        ],
        "timeseries": [
            {
                "name": "history",
                "type": "array",
                "source": "daily snapshots",
                "units": "one entry per day",
                "caveats": f"Maximum request window is {MAX_TIMESERIES_DAYS} days.",
            },
            {
                "name": "history[].metrics",
                "type": "object",
                "source": "daily snapshots",
                "units": "mixed raw metric units",
                "caveats": "Daily raw metrics for the selected validator as of the canonical daily round.",
            },
            {
                "name": "history[].sub_scores",
                "type": "object",
                "source": "daily snapshots",
                "units": "0.0-1.0 normalized score",
                "caveats": "Daily normalized methodology component scores.",
            },
        ],
        "diff": [
            {
                "name": "score_changes",
                "type": "array",
                "source": "daily snapshots",
                "units": "per validator",
                "caveats": "Compares score values between the two selected daily snapshots.",
            },
            {
                "name": "rank_changes",
                "type": "array",
                "source": "daily snapshots",
                "units": "per validator",
                "caveats": "Only validators present in both snapshots are compared.",
            },
            {
                "name": "incidents_opened / incidents_closed",
                "type": "array",
                "source": "daily open incident sets",
                "units": "incident rows",
                "caveats": "Based on changes in which incidents are open at each snapshot timestamp.",
            },
            {
                "name": "version_distribution_delta",
                "type": "array",
                "source": "daily snapshots",
                "units": "per version count delta",
                "caveats": "Tracks counts by grouped version string between two snapshots.",
            },
        ],
        "bulk_export": [
            {
                "name": "scores.csv",
                "type": "CSV file",
                "source": "daily snapshots",
                "units": "one row per validator per day",
                "caveats": "Includes composite score plus normalized sub-scores and selected raw metrics.",
            },
            {
                "name": "topology.csv",
                "type": "CSV file",
                "source": "daily snapshots",
                "units": "one row per validator per day",
                "caveats": "Contains enrichment fields and current server identity fields as stored in daily rounds.",
            },
            {
                "name": "incidents.csv",
                "type": "CSV file",
                "source": "incidents",
                "units": "one row per incident",
                "caveats": "Contains persisted incident records across the full dataset date range.",
            },
            {
                "name": "daily_snapshots.csv",
                "type": "CSV file",
                "source": "daily snapshots",
                "units": "one row per day",
                "caveats": "Contains network-wide daily summary values and the network health score.",
            },
        ],
    }


def build_dataset_schema() -> dict:
    return {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "network_health_formula_version": NETWORK_HEALTH_FORMULA_VERSION,
        "readme": {
            "overview": "Public ground truth dataset built from historical Post Fiat validator scoring rounds.",
            "field_documentation": _schema_fields(),
            "network_health_formula": {
                "summary": "Weighted average of five 0-100 component scores.",
                "components": NETWORK_HEALTH_COMPONENTS,
                "normalization_notes": {
                    "inverse_hhi": "Inverse normalized HHI is computed as ((1/HHI)-1)/(N-1)*100 where N is the number of observed groups in the snapshot.",
                    "semantic_versions": "Parsed semantic version ordering is preferred. Non-standard version strings remain visible in distributions and fall back to raw string grouping.",
                },
            },
        },
    }


async def build_risk_report(db) -> dict:
    snapshots = await build_all_dataset_snapshots(db)
    if not snapshots:
        raise ValueError("No scoring data available yet")

    latest = snapshots[-1]
    trend = [
        {
            "date": snapshot["snapshot_date"],
            "score": snapshot["network_health_index"]["score"],
        }
        for snapshot in snapshots[-7:]
    ]
    return {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "formula_version": NETWORK_HEALTH_FORMULA_VERSION,
        "snapshot_date": latest["snapshot_date"],
        "round_id": latest["round_id"],
        "timestamp": latest["timestamp"],
        "score": latest["network_health_index"]["score"],
        "score_semantics": latest["network_health_index"]["score_semantics"],
        "formula": latest["network_health_index"]["formula"],
        "components": latest["network_health_index"]["components"],
        "trend_7d": trend,
    }


async def build_dataset_export_json(db) -> dict:
    snapshots = await build_all_dataset_snapshots(db)
    metadata = _metadata_from_snapshots(snapshots)
    return {
        "metadata": metadata,
        "schema": build_dataset_schema(),
        "snapshots": snapshots,
    }


def _csv_bytes(rows: list[dict], fieldnames: list[str]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


async def build_dataset_export_csv_zip(db) -> tuple[bytes, str]:
    snapshots = await build_all_dataset_snapshots(db)
    incidents = await db.get_all_incidents_export_rows()

    score_rows = []
    topology_rows = []
    daily_rows = []
    for snapshot in snapshots:
        daily_rows.append(
            {
                "snapshot_date": snapshot["snapshot_date"],
                "round_id": snapshot["round_id"],
                "timestamp": snapshot["timestamp"],
                "validator_count": snapshot["validator_count"],
                "avg_score": snapshot["round_summary"]["avg_score"],
                "min_score": snapshot["round_summary"]["min_score"],
                "max_score": snapshot["round_summary"]["max_score"],
                "open_incident_count": snapshot["incidents"]["open_incident_count"],
                "network_health_score": snapshot["network_health_index"]["score"],
            }
        )
        for entry in snapshot["validator_scores"]:
            score_rows.append(
                {
                    "snapshot_date": snapshot["snapshot_date"],
                    "round_id": snapshot["round_id"],
                    "public_key": entry["public_key"],
                    "domain": entry["domain"],
                    "rank": entry["rank"],
                    "composite_score": entry["composite_score"],
                    "agreement_1h": entry["metrics"]["agreement_1h"],
                    "agreement_24h": entry["metrics"]["agreement_24h"],
                    "agreement_30d": entry["metrics"]["agreement_30d"],
                    "poll_success_pct": entry["metrics"]["poll_success_pct"],
                    "uptime_seconds": entry["metrics"]["uptime_seconds"],
                    "uptime_pct": entry["metrics"]["uptime_pct"],
                    "agreement_1h_score": entry["sub_scores"]["agreement_1h"],
                    "agreement_24h_score": entry["sub_scores"]["agreement_24h"],
                    "agreement_30d_score": entry["sub_scores"]["agreement_30d"],
                    "uptime_score": entry["sub_scores"]["uptime"],
                    "poll_success_score": entry["sub_scores"]["poll_success"],
                    "latency_score": entry["sub_scores"]["latency"],
                    "peer_count_score": entry["sub_scores"]["peer_count"],
                    "version_score": entry["sub_scores"]["version"],
                    "diversity_score": entry["sub_scores"]["diversity"],
                }
            )
        for entry in snapshot["topology"]:
            topology_rows.append(
                {
                    "snapshot_date": snapshot["snapshot_date"],
                    "round_id": snapshot["round_id"],
                    "public_key": entry["public_key"],
                    "domain": entry["domain"],
                    "provider": entry["provider"],
                    "asn": entry["asn"],
                    "country": entry["country"],
                    "latency_ms": entry["latency_ms"],
                    "peer_count": entry["peer_count"],
                    "server_state": entry["server_state"],
                    "server_version": entry["server_version"],
                    "node_ip": entry["node_ip"],
                    "enriched": entry["enriched"],
                }
            )

    incident_rows = [
        {
            "id": incident["id"],
            "validator_key": incident["validator_key"],
            "severity": incident["severity"],
            "status": incident["status"],
            "summary": incident["summary"],
            "start_time": incident["start_time"],
            "end_time": incident["end_time"],
            "duration_seconds": incident["duration_seconds"],
            "latest_round_id": incident["latest_round_id"],
            "latest_event_time": incident["latest_event_time"],
            "event_types": json.dumps(incident["event_types"]),
            "active_event_types": json.dumps(incident["active_event_types"]),
            "synthetic": incident["synthetic"],
            "correlated": incident["correlated"],
        }
        for incident in incidents
    ]

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "scores.csv",
            _csv_bytes(
                score_rows,
                [
                    "snapshot_date",
                    "round_id",
                    "public_key",
                    "domain",
                    "rank",
                    "composite_score",
                    "agreement_1h",
                    "agreement_24h",
                    "agreement_30d",
                    "poll_success_pct",
                    "uptime_seconds",
                    "uptime_pct",
                    "agreement_1h_score",
                    "agreement_24h_score",
                    "agreement_30d_score",
                    "uptime_score",
                    "poll_success_score",
                    "latency_score",
                    "peer_count_score",
                    "version_score",
                    "diversity_score",
                ],
            ),
        )
        archive.writestr(
            "topology.csv",
            _csv_bytes(
                topology_rows,
                [
                    "snapshot_date",
                    "round_id",
                    "public_key",
                    "domain",
                    "provider",
                    "asn",
                    "country",
                    "latency_ms",
                    "peer_count",
                    "server_state",
                    "server_version",
                    "node_ip",
                    "enriched",
                ],
            ),
        )
        archive.writestr(
            "incidents.csv",
            _csv_bytes(
                incident_rows,
                [
                    "id",
                    "validator_key",
                    "severity",
                    "status",
                    "summary",
                    "start_time",
                    "end_time",
                    "duration_seconds",
                    "latest_round_id",
                    "latest_event_time",
                    "event_types",
                    "active_event_types",
                    "synthetic",
                    "correlated",
                ],
            ),
        )
        archive.writestr(
            "daily_snapshots.csv",
            _csv_bytes(
                daily_rows,
                [
                    "snapshot_date",
                    "round_id",
                    "timestamp",
                    "validator_count",
                    "avg_score",
                    "min_score",
                    "max_score",
                    "open_incident_count",
                    "network_health_score",
                ],
            ),
        )

    payload = zip_buffer.getvalue()
    sha256 = hashlib.sha256(payload).hexdigest()
    return payload, sha256
