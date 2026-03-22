import logging
from datetime import datetime, timezone

import httpx

from app.database import Database
from app.models import ValidatorScore

logger = logging.getLogger(__name__)

DISCORD_TIMEOUT = 10.0

# Metric display config: (label, metric_key, sub_score_key, max_bars)
METRIC_BARS = [
    ("Agreement 1h ", "agreement_1h", "agreement_1h", 10),
    ("Agreement 24h", "agreement_24h", "agreement_24h", 10),
    ("Agreement 30d", "agreement_30d", "agreement_30d", 10),
    ("Uptime       ", "uptime_pct", "uptime", 10),
    ("Poll Success ", "poll_success_pct", "poll_success", 10),
    ("Latency      ", "latency_ms", "latency", 10),
    ("Peer Count   ", "peer_count", "peer_count", 10),
    ("Version      ", "server_version", "version", 10),
    ("Diversity    ", None, "diversity", 10),
]

IMPROVEMENT_TIPS = {
    "agreement_1h": "Your 1-hour agreement is low. Check if your node is missing validations — ensure it's in 'proposing' state and has good peer connectivity.",
    "agreement_24h": "Your 24-hour agreement is dragging your score. Investigate recent downtime or network issues that may have caused missed validations.",
    "agreement_30d": "Your 30-day agreement needs improvement. This reflects cumulative reliability — focus on maximizing uptime and minimizing restarts.",
    "uptime": "Your uptime is below the cohort average. Avoid unnecessary restarts and ensure your server has stable power and network.",
    "poll_success": "Your node isn't consistently reachable. Check firewall rules, ensure RPC port is accessible, and verify Docker container is running.",
    "latency": "Your latency is high. Consider a server closer to other validators, or check for network congestion and CPU load on your host.",
    "peer_count": "You have fewer peers than most validators. Check your node's peering config and ensure port 2559 is accessible for inbound connections.",
    "version": "You're running an older software version. Update to the latest postfiatd release to get full version score.",
    "diversity": "Your hosting provider is overrepresented among validators. Consider migrating to a less common provider or data center region.",
}


def _make_bar(score: float, length: int = 10) -> str:
    filled = round(score * length)
    return "█" * filled + "░" * (length - filled)


def _format_metric_value(key: str, metrics: dict) -> str:
    val = metrics.get(key)
    if val is None:
        return "n/a"
    if key == "agreement_1h" or key == "agreement_24h" or key == "agreement_30d":
        return f"{val * 100:.1f}%"
    if key == "uptime_pct" or key == "poll_success_pct":
        return f"{val:.1f}%"
    if key == "latency_ms":
        return f"{val:.0f}ms"
    if key == "peer_count":
        return str(int(val))
    if key == "server_version":
        return str(val)
    return str(val)


def _find_weakest_metric(sub_scores: dict) -> tuple[str, float]:
    """Find the sub-score with the lowest value (most room for improvement)."""
    scorable = {k: v for k, v in sub_scores.items() if k not in ("agreement_1h",) or v < 0.5}
    if not scorable:
        scorable = sub_scores
    weakest_key = min(scorable, key=lambda k: scorable[k])
    return weakest_key, scorable[weakest_key]


async def send_discord_webhook(webhook_url: str, embed: dict) -> bool:
    """Send a Discord embed via webhook. Returns True on success."""
    try:
        async with httpx.AsyncClient(timeout=DISCORD_TIMEOUT) as client:
            resp = await client.post(
                webhook_url,
                json={"embeds": [embed]},
            )
            if resp.status_code in (200, 204):
                return True
            logger.warning("Discord webhook returned %d: %s", resp.status_code, resp.text[:200])
            return False
    except Exception as e:
        logger.error("Failed to send Discord webhook: %s", e)
        return False


async def send_confirmation(webhook_url: str, public_key: str) -> bool:
    """Send a confirmation test message when a user subscribes."""
    embed = {
        "title": "✅ Subscription Confirmed",
        "description": (
            f"You're now subscribed to daily report cards and critical alerts "
            f"for validator:\n`{public_key}`"
        ),
        "color": 0x00CC66,
        "fields": [
            {"name": "Daily Reports", "value": "You'll receive a scorecard every day at 12:00 UTC", "inline": True},
            {"name": "Critical Alerts", "value": "Immediate alert if agreement drops below 0.90 or your validator disappears", "inline": True},
        ],
        "footer": {"text": "PFT Reputation Scoring | dashboard.pftoligarchy.com"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return await send_discord_webhook(webhook_url, embed)


async def generate_daily_report(
    validator: ValidatorScore,
    rank: int,
    total_validators: int,
    prev_score: float | None,
    prev_rank: int | None,
    nearby_above: ValidatorScore | None,
    nearby_below: ValidatorScore | None,
    network_stats: dict,
    top_movers: dict,
) -> dict:
    """Generate a Discord embed for the daily report card."""
    metrics = validator.metrics.model_dump()
    sub_scores = validator.sub_scores.model_dump()

    # Rank and score with deltas
    rank_str = f"**#{rank}** of {total_validators}"
    if prev_rank is not None:
        rank_delta = prev_rank - rank  # positive = improved
        if rank_delta > 0:
            rank_str += f" (▲{rank_delta})"
        elif rank_delta < 0:
            rank_str += f" (▼{abs(rank_delta)})"
        else:
            rank_str += " (—)"

    score_str = f"**{validator.composite_score:.1f}**"
    if prev_score is not None:
        score_delta = validator.composite_score - prev_score
        if score_delta > 0:
            score_str += f" (+{score_delta:.1f})"
        elif score_delta < 0:
            score_str += f" ({score_delta:.1f})"

    # Metric breakdown with bars
    breakdown_lines = []
    for label, metric_key, score_key, bar_len in METRIC_BARS:
        score_val = sub_scores.get(score_key, 0)
        bar = _make_bar(score_val, bar_len)
        if metric_key:
            val_str = _format_metric_value(metric_key, metrics)
        else:
            val_str = f"{score_val:.0%}"
        breakdown_lines.append(f"`{label}` {bar} {val_str}")
    breakdown = "\n".join(breakdown_lines)

    # Weakest metric
    weakest_key, weakest_val = _find_weakest_metric(sub_scores)
    tip = IMPROVEMENT_TIPS.get(weakest_key, "Review your node configuration for improvements.")
    weakest_section = f"**Weakest: {weakest_key.replace('_', ' ').title()}** ({weakest_val:.0%})\n💡 {tip}"

    # Nearby competitors
    competitors_lines = []
    if nearby_above:
        name_above = nearby_above.domain or nearby_above.public_key[:16] + "..."
        competitors_lines.append(f"▲ #{rank - 1} **{name_above}** — {nearby_above.composite_score:.1f}")
    name_self = validator.domain or validator.public_key[:16] + "..."
    competitors_lines.append(f"➤ #{rank} **{name_self}** — {validator.composite_score:.1f}")
    if nearby_below:
        name_below = nearby_below.domain or nearby_below.public_key[:16] + "..."
        competitors_lines.append(f"▼ #{rank + 1} **{name_below}** — {nearby_below.composite_score:.1f}")
    competitors = "\n".join(competitors_lines)

    # Network summary footer
    tier_healthy = network_stats.get("healthy", 0)
    tier_degraded = network_stats.get("degraded", 0)
    tier_poor = network_stats.get("poor", 0)
    net_avg = network_stats.get("avg_score", 0)
    footer_parts = [f"{total_validators} validators", f"avg {net_avg:.1f}"]
    footer_parts.append(f"🟢 {tier_healthy} healthy · 🟡 {tier_degraded} degraded · 🔴 {tier_poor} poor")

    movers_line = ""
    if top_movers.get("gainer"):
        g = top_movers["gainer"]
        gname = g.get("domain") or g["public_key"][:12] + "..."
        movers_line += f"📈 Top gainer: {gname} (▲{g['rank_change']})"
    if top_movers.get("loser"):
        l = top_movers["loser"]
        lname = l.get("domain") or l["public_key"][:12] + "..."
        if movers_line:
            movers_line += "  "
        movers_line += f"📉 Top drop: {lname} (▼{abs(l['rank_change'])})"

    # Color based on score
    if validator.composite_score >= 80:
        color = 0x00CC66  # green
    elif validator.composite_score >= 60:
        color = 0xFFAA00  # amber
    else:
        color = 0xFF4444  # red

    embed = {
        "title": f"📊 Daily Report Card — {name_self}",
        "color": color,
        "fields": [
            {"name": "Rank", "value": rank_str, "inline": True},
            {"name": "Composite Score", "value": score_str, "inline": True},
            {"name": "\u200b", "value": "\u200b", "inline": True},  # spacer
            {"name": "Metric Breakdown", "value": breakdown, "inline": False},
            {"name": "Improvement Focus", "value": weakest_section, "inline": False},
            {"name": "Nearby Competitors", "value": competitors, "inline": False},
        ],
        "footer": {"text": " | ".join(footer_parts)},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if movers_line:
        embed["fields"].append({"name": "Top Movers (24h)", "value": movers_line, "inline": False})

    return embed


async def check_critical_alerts(db: Database, scores: list[ValidatorScore]):
    """Check for critical alert conditions and fire alerts for subscribed validators."""
    subs = await db.get_active_subscriptions()
    if not subs:
        return

    score_by_key = {s.public_key: s for s in scores}
    scored_keys = set(score_by_key.keys())

    for sub in subs:
        pk = sub["public_key"]
        webhook = sub["webhook_url"]

        # Check 1: Validator disappeared from scoring
        if pk not in scored_keys:
            if not await db.check_alert_cooldown(pk, "disappeared"):
                embed = {
                    "title": "🚨 CRITICAL: Validator Not Found",
                    "description": (
                        f"Your validator `{pk}` has **disappeared from scoring**. "
                        f"It was not found in the latest scoring round.\n\n"
                        f"**Action required:** Check if your node is running and connected to the network."
                    ),
                    "color": 0xFF0000,
                    "footer": {"text": "PFT Reputation Scoring | Critical Alert"},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                sent = await send_discord_webhook(webhook, embed)
                if sent:
                    await db.set_alert_cooldown(pk, "disappeared")
                    logger.info("Fired 'disappeared' alert for %s", pk[:16])
            continue

        # Check 2: Agreement dropped below 0.90
        v = score_by_key[pk]
        agreement_24h = v.metrics.agreement_24h
        if agreement_24h is not None and agreement_24h < 0.90:
            if not await db.check_alert_cooldown(pk, "low_agreement"):
                name = v.domain or pk[:16] + "..."
                embed = {
                    "title": "⚠️ ALERT: Agreement Score Low",
                    "description": (
                        f"**{name}**'s 24h agreement has dropped to **{agreement_24h:.4f}** (below 0.90 threshold).\n\n"
                        f"Current composite score: **{v.composite_score:.1f}**\n\n"
                        f"**Action:** Check your node's consensus participation — ensure it's in 'proposing' state "
                        f"with good peer connectivity."
                    ),
                    "color": 0xFF6600,
                    "footer": {"text": "PFT Reputation Scoring | Critical Alert"},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                sent = await send_discord_webhook(webhook, embed)
                if sent:
                    await db.set_alert_cooldown(pk, "low_agreement")
                    logger.info("Fired 'low_agreement' alert for %s (%.4f)", pk[:16], agreement_24h)


async def send_daily_reports(db: Database):
    """Generate and send daily report cards to all subscribers."""
    subs = await db.get_active_subscriptions()
    if not subs:
        logger.info("No active subscriptions for daily reports")
        return

    # Get current scores
    round_id, round_ts, scores = await db.get_latest_scores()
    if not scores:
        logger.warning("No scores available for daily reports")
        return

    total = len(scores)
    score_by_key = {s.public_key: s for s in scores}

    # Compute network stats
    all_scores = [s.composite_score for s in scores]
    avg_score = sum(all_scores) / len(all_scores)
    healthy = sum(1 for s in all_scores if s >= 80)
    degraded = sum(1 for s in all_scores if 60 <= s < 80)
    poor = sum(1 for s in all_scores if s < 60)
    network_stats = {"avg_score": avg_score, "healthy": healthy, "degraded": degraded, "poor": poor}

    top_movers = await db.get_top_movers()

    sent_count = 0
    for sub in subs:
        pk = sub["public_key"]
        webhook = sub["webhook_url"]

        validator = score_by_key.get(pk)
        if not validator:
            logger.warning("Subscribed validator %s not found in scores", pk[:16])
            continue

        # Find rank
        rank = next((i + 1 for i, s in enumerate(scores) if s.public_key == pk), None)
        if rank is None:
            continue

        # Get previous score and rank
        prev_data = await db.get_previous_scores(pk)
        prev_score = prev_data["composite_score"] if prev_data else None
        prev_rank = await db.get_previous_rank(pk)

        # Get nearby competitors
        nearby_above = scores[rank - 2] if rank > 1 else None
        nearby_below = scores[rank] if rank < total else None

        embed = await generate_daily_report(
            validator=validator,
            rank=rank,
            total_validators=total,
            prev_score=prev_score,
            prev_rank=prev_rank,
            nearby_above=nearby_above,
            nearby_below=nearby_below,
            network_stats=network_stats,
            top_movers=top_movers,
        )

        if await send_discord_webhook(webhook, embed):
            sent_count += 1

    logger.info("Sent %d/%d daily report cards", sent_count, len(subs))
