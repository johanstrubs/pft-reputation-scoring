from collections import Counter, defaultdict
from datetime import datetime
import math
import re

from app.models import ValidatorScore

SEMVER_RE = re.compile(r"^[vV]?(\d+)\.(\d+)\.(\d+)(?:[-+]([0-9A-Za-z.-]+))?$")


def _parse_semver(version: str | None) -> tuple[int, int, int, int, str] | None:
    if not version:
        return None
    match = SEMVER_RE.match(version.strip())
    if not match:
        return None
    major, minor, patch, suffix = match.groups()
    is_final = 1 if not suffix else 0
    return int(major), int(minor), int(patch), is_final, suffix or ""


def _normalize_version(version: str | None) -> str | None:
    parsed = _parse_semver(version)
    if not parsed:
        return None
    major, minor, patch, is_final, suffix = parsed
    base = f"{major}.{minor}.{patch}"
    return base if is_final else f"{base}-{suffix}"


def _display_version(version: str | None) -> str:
    normalized = _normalize_version(version)
    if normalized:
        return normalized
    if version and str(version).strip():
        return str(version).strip()
    return "unknown"


def build_upgrade_report(
    round_id: int,
    timestamp: str,
    scores: list[ValidatorScore],
    history_rows: list[dict],
) -> dict:
    if not scores:
        raise ValueError("No scoring data available yet")

    current_versions = []
    for score in scores:
        parsed = _parse_semver(score.metrics.server_version)
        if parsed:
            current_versions.append((parsed, _normalize_version(score.metrics.server_version)))

    latest_version = max(current_versions, key=lambda item: item[0])[1] if current_versions else None

    distribution_counter = Counter(_display_version(score.metrics.server_version) for score in scores)
    total_validators = len(scores)
    version_distribution = [
        {
            "version": version,
            "count": count,
            "percentage": round(100 * count / total_validators, 1) if total_validators else 0.0,
        }
        for version, count in sorted(
            distribution_counter.items(),
            key=lambda item: (_parse_semver(item[0]) or (-1, -1, -1, -1, ""), item[0]),
            reverse=True,
        )
    ]

    latest_seen_at = None
    if latest_version:
        for row in history_rows:
            if _normalize_version(row.get("server_version")) == latest_version:
                latest_seen_at = row["round_timestamp"]
                break

    current_ts = datetime.fromisoformat(timestamp)
    lagging_validators = []
    upgraded_count = 0
    for score in sorted(scores, key=lambda item: (item.metrics.server_version or "", item.domain or item.public_key)):
        current_version = _display_version(score.metrics.server_version)
        if latest_version and _normalize_version(score.metrics.server_version) == latest_version:
            upgraded_count += 1
            continue
        days_behind = 0
        if latest_seen_at:
            behind_delta = current_ts - datetime.fromisoformat(latest_seen_at)
            days_behind = max(0, math.floor(behind_delta.total_seconds() / 86400))
        lagging_validators.append(
            {
                "public_key": score.public_key,
                "domain": score.domain,
                "current_version": current_version,
                "days_behind": days_behind,
            }
        )

    lagging_validators.sort(
        key=lambda validator: (-validator["days_behind"], validator["current_version"], validator["domain"] or validator["public_key"])
    )

    rounds = defaultdict(list)
    for row in history_rows:
        rounds[(row["round_id"], row["round_timestamp"], row["validator_count"])].append(row)

    adoption_by_day: dict[str, dict] = {}
    for (history_round_id, round_timestamp, validator_count), rows in sorted(rounds.items(), key=lambda item: item[0][0]):
        if not latest_version:
            continue
        upgraded = sum(1 for row in rows if _normalize_version(row.get("server_version")) == latest_version)
        date_key = datetime.fromisoformat(round_timestamp).date().isoformat()
        adoption_by_day[date_key] = {
            "date": date_key,
            "percentage": round(100 * upgraded / validator_count, 1) if validator_count else 0.0,
            "upgraded_count": upgraded,
            "total_validators": validator_count,
            "_round_id": history_round_id,
        }

    adoption_history = []
    if latest_seen_at:
        latest_seen_date = datetime.fromisoformat(latest_seen_at).date().isoformat()
        for day in sorted(adoption_by_day):
            if day >= latest_seen_date:
                entry = adoption_by_day[day]
                adoption_history.append(
                    {
                        "date": entry["date"],
                        "percentage": entry["percentage"],
                        "upgraded_count": entry["upgraded_count"],
                        "total_validators": entry["total_validators"],
                    }
                )

    return {
        "latest_version": latest_version,
        "total_validators": total_validators,
        "upgraded_count": upgraded_count,
        "upgraded_pct": round(100 * upgraded_count / total_validators, 1) if total_validators else 0.0,
        "version_distribution": version_distribution,
        "lagging_validators": lagging_validators,
        "adoption_history": adoption_history,
        "json_report_url": "/api/upgrades",
    }
