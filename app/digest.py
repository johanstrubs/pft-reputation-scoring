import logging
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

import httpx

from app.config import settings
from app.models import ValidatorScore

logger = logging.getLogger(__name__)

DIGEST_TIMEOUT = 15.0


def _is_enriched(score: ValidatorScore) -> bool:
    metrics = score.metrics
    return (
        metrics.isp is not None
        and metrics.country is not None
        and metrics.asn is not None
    )


def _rank_map(scores: list[ValidatorScore]) -> dict[str, dict]:
    return {
        score.public_key: {
            "rank": idx + 1,
            "domain": score.domain,
            "score": score.composite_score,
            "validator": score,
        }
        for idx, score in enumerate(scores)
    }


def _coverage(scores: list[ValidatorScore]) -> dict:
    enriched = [score for score in scores if _is_enriched(score)]
    total = len(scores)
    return {
        "total_validators": total,
        "enriched": len(enriched),
        "unenriched": total - len(enriched),
        "coverage_pct": round(100 * len(enriched) / total, 1) if total else 0.0,
    }


def _health_distribution(scores: list[ValidatorScore]) -> dict:
    return {
        "healthy": sum(1 for s in scores if s.composite_score >= 80),
        "degraded": sum(1 for s in scores if 60 <= s.composite_score < 80),
        "poor": sum(1 for s in scores if s.composite_score < 60),
    }


def _concentration_map(scores: list[ValidatorScore], key: str) -> tuple[Counter, int]:
    enriched = [score for score in scores if _is_enriched(score)]
    if key == "provider":
        counter = Counter(score.metrics.isp for score in enriched if score.metrics.isp)
    elif key == "country":
        counter = Counter(score.metrics.country for score in enriched if score.metrics.country)
    else:
        counter = Counter(f"AS{score.metrics.asn}" for score in enriched if score.metrics.asn is not None)
    return counter, len(enriched)


def _format_concentration_delta(current_scores: list[ValidatorScore], comparison_scores: list[ValidatorScore], key: str) -> list[dict]:
    current_counter, current_total = _concentration_map(current_scores, key)
    previous_counter, previous_total = _concentration_map(comparison_scores, key)
    labels = []
    for label, _count in current_counter.most_common(3):
        if label not in labels:
            labels.append(label)
    for label, _count in previous_counter.most_common(3):
        if label not in labels and len(labels) < 3:
            labels.append(label)

    rows = []
    for label in labels:
        current_count = current_counter.get(label, 0)
        previous_count = previous_counter.get(label, 0)
        current_pct = round(100 * current_count / current_total, 1) if current_total else 0.0
        previous_pct = round(100 * previous_count / previous_total, 1) if previous_total else 0.0
        rows.append({
            "label": label,
            "current_count": current_count,
            "current_pct": current_pct,
            "previous_count": previous_count,
            "previous_pct": previous_pct,
            "pct_change": round(current_pct - previous_pct, 1),
        })
    return rows


def _serialize_validator(score: ValidatorScore, rank: int | None = None) -> dict:
    return {
        "public_key": score.public_key,
        "domain": score.domain,
        "rank": rank,
        "composite_score": score.composite_score,
    }


async def build_weekly_digest(db) -> dict:
    latest_round = await db.get_latest_round_summary()
    if not latest_round:
        raise ValueError("No scoring rounds available yet.")

    comparison_round = await db.get_comparison_round_summary(latest_round["timestamp"])
    if not comparison_round:
        raise ValueError("No comparison round available from 6-8 days earlier yet.")

    current_scores = await db.get_scores_for_round(latest_round["id"])
    comparison_scores = await db.get_scores_for_round(comparison_round["id"])
    if not current_scores or not comparison_scores:
        raise ValueError("Digest comparison requires populated scoring rounds.")

    current_ranks = _rank_map(current_scores)
    previous_ranks = _rank_map(comparison_scores)

    current_keys = set(current_ranks)
    previous_keys = set(previous_ranks)
    joined_keys = sorted(current_keys - previous_keys)
    departed_keys = sorted(previous_keys - current_keys)

    joins = [
        _serialize_validator(current_ranks[key]["validator"], current_ranks[key]["rank"])
        for key in joined_keys
    ]
    departures = [
        _serialize_validator(previous_ranks[key]["validator"], previous_ranks[key]["rank"])
        for key in departed_keys
    ]

    movers = []
    score_changes = []
    for key in current_keys & previous_keys:
        current_entry = current_ranks[key]
        previous_entry = previous_ranks[key]
        rank_change = previous_entry["rank"] - current_entry["rank"]
        score_delta = round(current_entry["score"] - previous_entry["score"], 2)
        row = {
            "public_key": key,
            "domain": current_entry["domain"],
            "old_rank": previous_entry["rank"],
            "new_rank": current_entry["rank"],
            "rank_change": rank_change,
            "old_score": previous_entry["score"],
            "new_score": current_entry["score"],
            "score_delta": score_delta,
        }
        movers.append(row)
        if abs(score_delta) > 5:
            score_changes.append(row)

    top_gainers = sorted([row for row in movers if row["rank_change"] > 0], key=lambda row: (-row["rank_change"], -row["score_delta"]))[:3]
    top_losers = sorted([row for row in movers if row["rank_change"] < 0], key=lambda row: (row["rank_change"], row["score_delta"]))[:3]
    score_changes.sort(key=lambda row: abs(row["score_delta"]), reverse=True)

    current_avg = round(sum(score.composite_score for score in current_scores) / len(current_scores), 2)
    previous_avg = round(sum(score.composite_score for score in comparison_scores) / len(comparison_scores), 2)

    current_health = _health_distribution(current_scores)
    previous_health = _health_distribution(comparison_scores)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": {
            "latest_round": latest_round,
            "comparison_round": comparison_round,
            "days_apart": round(
                (datetime.fromisoformat(latest_round["timestamp"]) - datetime.fromisoformat(comparison_round["timestamp"])).total_seconds() / 86400,
                2,
            ),
        },
        "summary": {
            "joins_count": len(joins),
            "departures_count": len(departures),
            "score_change_alerts_count": len(score_changes),
            "top_gainer_count": len(top_gainers),
            "top_loser_count": len(top_losers),
        },
        "joins": joins,
        "departures": departures,
        "top_rank_gainers": top_gainers,
        "top_rank_losers": top_losers,
        "score_change_alerts": score_changes,
        "network_stats": {
            "current_validator_count": len(current_scores),
            "previous_validator_count": len(comparison_scores),
            "validator_count_delta": len(current_scores) - len(comparison_scores),
            "current_avg_score": current_avg,
            "previous_avg_score": previous_avg,
            "avg_score_delta": round(current_avg - previous_avg, 2),
            "current_health_distribution": current_health,
            "previous_health_distribution": previous_health,
            "health_distribution_delta": {
                key: current_health[key] - previous_health[key]
                for key in current_health
            },
        },
        "concentration": {
            "coverage": {
                "current": _coverage(current_scores),
                "comparison": _coverage(comparison_scores),
            },
            "providers": _format_concentration_delta(current_scores, comparison_scores, "provider"),
            "countries": _format_concentration_delta(current_scores, comparison_scores, "country"),
            "asns": _format_concentration_delta(current_scores, comparison_scores, "asn"),
        },
    }
    return payload


def format_weekly_digest_embed(payload: dict) -> dict:
    def summarize_validators(items: list[dict], empty_text: str) -> str:
        if not items:
            return empty_text
        return "\n".join(
            f"• {(item.get('domain') or item['public_key'][:14] + '...')} (#{item.get('rank', item.get('new_rank', '?'))}, {item.get('composite_score', item.get('new_score', 0)):.1f})"
            for item in items
        )

    def summarize_movers(items: list[dict], empty_text: str) -> str:
        if not items:
            return empty_text
        return "\n".join(
            f"• {(item.get('domain') or item['public_key'][:14] + '...')} {item['old_rank']}→{item['new_rank']} ({item['score_delta']:+.1f} pts)"
            for item in items
        )

    def summarize_concentration(items: list[dict], coverage: dict, empty_text: str) -> str:
        if not items:
            return empty_text
        current_cov = coverage["current"]["enriched"]
        return "\n".join(
            f"• {item['label']}: {item['current_pct']:.1f}% of {current_cov} enriched validators ({item['pct_change']:+.1f} pts WoW)"
            for item in items
        )

    network_stats = payload["network_stats"]
    embed = {
        "title": "📡 Weekly Validator Changefeed",
        "description": (
            f"Comparing round {payload['window']['latest_round']['id']} "
            f"to round {payload['window']['comparison_round']['id']} "
            f"({payload['window']['days_apart']} days apart)."
        ),
        "color": 0x58A6FF,
        "fields": [
            {"name": "Joins", "value": summarize_validators(payload["joins"], "No new validators this week."), "inline": False},
            {"name": "Departures", "value": summarize_validators(payload["departures"], "No departures this week."), "inline": False},
            {"name": "Top Rank Gainers", "value": summarize_movers(payload["top_rank_gainers"], "No upward movers."), "inline": False},
            {"name": "Top Rank Losers", "value": summarize_movers(payload["top_rank_losers"], "No downward movers."), "inline": False},
            {
                "name": "Network Stats",
                "value": (
                    f"Validators: {network_stats['current_validator_count']} ({network_stats['validator_count_delta']:+d})\n"
                    f"Average score: {network_stats['current_avg_score']:.1f} ({network_stats['avg_score_delta']:+.1f})\n"
                    f"Healthy/Degraded/Poor: "
                    f"{network_stats['current_health_distribution']['healthy']}/"
                    f"{network_stats['current_health_distribution']['degraded']}/"
                    f"{network_stats['current_health_distribution']['poor']}"
                ),
                "inline": False,
            },
            {
                "name": "Provider Concentration",
                "value": summarize_concentration(payload["concentration"]["providers"], payload["concentration"]["coverage"], "No enriched provider data."),
                "inline": False,
            },
        ],
        "footer": {"text": "PFT Reputation Scoring | Weekly Network Digest"},
        "timestamp": payload["generated_at"],
    }
    return embed


async def send_weekly_digest_to_discord(webhook_url: str, embed: dict) -> dict:
    if not webhook_url:
        return {"delivery_status": "skipped_no_webhook", "posted_at": None, "message_id": None}

    parts = urlsplit(webhook_url)
    query = dict(parse_qsl(parts.query))
    query["wait"] = "true"
    wait_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    try:
        async with httpx.AsyncClient(timeout=DIGEST_TIMEOUT) as client:
            response = await client.post(wait_url, json={"embeds": [embed]})
        if response.status_code in (200, 204):
            posted_at = datetime.now(timezone.utc).isoformat()
            message_id = None
            if response.status_code == 200:
                try:
                    message_id = str(response.json().get("id")) if response.json().get("id") else None
                except Exception:
                    message_id = None
            return {
                "delivery_status": "posted",
                "posted_at": posted_at,
                "message_id": message_id,
            }
        logger.warning("Weekly digest webhook returned %d: %s", response.status_code, response.text[:200])
        return {
            "delivery_status": f"error_http_{response.status_code}",
            "posted_at": None,
            "message_id": None,
        }
    except Exception as exc:
        logger.error("Failed to send weekly digest: %s", exc)
        return {
            "delivery_status": "error_exception",
            "posted_at": None,
            "message_id": None,
        }


async def generate_and_store_weekly_digest(db, webhook_url: str | None = None) -> dict:
    payload = await build_weekly_digest(db)
    embed = format_weekly_digest_embed(payload)
    delivery = await send_weekly_digest_to_discord(webhook_url or settings.weekly_digest_webhook_url, embed)
    digest_id = await db.store_weekly_digest(
        payload=payload,
        latest_round_id=payload["window"]["latest_round"]["id"],
        comparison_round_id=payload["window"]["comparison_round"]["id"],
        delivery_status=delivery["delivery_status"],
        posted_at=delivery["posted_at"],
        message_id=delivery["message_id"],
        webhook_url=webhook_url or settings.weekly_digest_webhook_url or None,
    )
    stored = await db.get_latest_digest()
    if stored:
        return stored
    raise RuntimeError(f"Weekly digest {digest_id} was stored but could not be reloaded.")
