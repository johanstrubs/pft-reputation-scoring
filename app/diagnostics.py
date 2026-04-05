from collections import Counter
from statistics import median

from app.models import ValidatorScore
from app.scorer import WEIGHTS

HEALTHY_SERVER_STATES = {"proposing", "full"}
SEVERITY_ORDER = {"critical": 0, "warning": 1, "advisory": 2}
METRIC_IMPORTANCE = {
    "agreement_1h": WEIGHTS["agreement_1h"],
    "agreement_24h": WEIGHTS["agreement_24h"],
    "agreement_30d": WEIGHTS["agreement_30d"],
    "uptime": WEIGHTS["uptime"],
    "poll_success": WEIGHTS["poll_success"],
    "latency": WEIGHTS["latency"],
    "peer_count": WEIGHTS["peer_count"],
    "version": WEIGHTS["version"],
    "diversity": WEIGHTS["diversity"],
    "server_state": WEIGHTS["version"],
}


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "unknown"
    return f"{value * 100:.1f}%"


def _fmt_raw_pct(value: float | None) -> str:
    if value is None:
        return "unknown"
    return f"{value:.1f}%"


def _fmt_ms(value: float | None) -> str:
    if value is None:
        return "unknown"
    return f"{value:.0f}ms"


def _fmt_count(value: int | None) -> str:
    if value is None:
        return "unknown"
    return str(value)


def _fmt_score(value: float) -> str:
    return f"{value:.2f}"


def _ranked_scores(scores: list[ValidatorScore]) -> dict[str, int]:
    ranked = sorted(scores, key=lambda score: (-score.composite_score, score.public_key))
    return {score.public_key: idx + 1 for idx, score in enumerate(ranked)}


def _latest_version(scores: list[ValidatorScore]) -> str | None:
    versions = [score.metrics.server_version for score in scores if score.metrics.server_version]
    if not versions:
        return None
    return Counter(versions).most_common(1)[0][0]


def _median_for(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _build_cohort_context(scores: list[ValidatorScore]) -> dict:
    agreement_1h_values = [score.metrics.agreement_1h for score in scores if score.metrics.agreement_1h is not None]
    agreement_24h_values = [score.metrics.agreement_24h for score in scores if score.metrics.agreement_24h is not None]
    agreement_30d_values = [score.metrics.agreement_30d for score in scores if score.metrics.agreement_30d is not None]
    uptime_pct_values = [score.metrics.uptime_pct for score in scores if score.metrics.uptime_pct is not None]
    latency_values = [score.metrics.latency_ms for score in scores if score.metrics.latency_ms is not None]
    peer_values = [score.metrics.peer_count for score in scores if score.metrics.peer_count is not None]
    poll_success_values = [score.metrics.poll_success_pct for score in scores if score.metrics.poll_success_pct is not None]
    diversity_values = [score.sub_scores.diversity for score in scores]

    return {
        "ranks": _ranked_scores(scores),
        "latest_version": _latest_version(scores),
        "median_uptime_pct": _median_for(uptime_pct_values),
        "median_latency_ms": _median_for(latency_values),
        "median_peer_count": _median_for(peer_values),
        "median_poll_success_pct": _median_for(poll_success_values),
        "median_diversity": _median_for(diversity_values),
        "median_agreement_1h": _median_for(agreement_1h_values),
        "median_agreement_24h": _median_for(agreement_24h_values),
        "median_agreement_30d": _median_for(agreement_30d_values),
    }


def _finding(
    *,
    category: str,
    metric: str,
    severity: str,
    title: str,
    current_value: str,
    threshold_value: str,
    likely_cause: str,
    recommended_action: str,
    deviation: float,
) -> dict:
    return {
        "category": category,
        "metric": metric,
        "severity": severity,
        "title": title,
        "current_value": current_value,
        "threshold_value": threshold_value,
        "likely_cause": likely_cause,
        "recommended_action": recommended_action,
        "_sort_weight": METRIC_IMPORTANCE.get(metric, 0.0),
        "_deviation": deviation,
    }


def _strength(*, metric: str, title: str, current_value: str, benchmark: str, why_it_matters: str) -> dict:
    return {
        "metric": metric,
        "title": title,
        "current_value": current_value,
        "benchmark": benchmark,
        "why_it_matters": why_it_matters,
    }


def build_diagnostic_report(round_id: int, timestamp: str, scores: list[ValidatorScore], public_key: str) -> dict:
    if not scores:
        raise ValueError("No scoring data available yet")

    validator = next((score for score in scores if score.public_key == public_key), None)
    if not validator:
        raise KeyError(public_key)

    ranks = _ranked_scores(scores)
    rank = ranks[public_key]
    validator_count = len(scores)
    metrics = validator.metrics
    sub_scores = validator.sub_scores
    cohort = _build_cohort_context(scores)
    findings: list[dict] = []
    strengths: list[dict] = []

    peer_count = metrics.peer_count
    healthy_peers = peer_count is not None and peer_count >= 5
    latest_version = cohort["latest_version"]

    if metrics.agreement_30d is not None and metrics.agreement_30d < 0.90:
        findings.append(_finding(
            category="fault",
            metric="agreement_30d",
            severity="critical",
            title="Long-window agreement is materially below healthy range",
            current_value=_fmt_pct(metrics.agreement_30d),
            threshold_value=">= 90.0%",
            likely_cause="Your validator is disagreeing with the network often enough that this looks like a real reliability issue rather than a short blip.",
            recommended_action="Check system clock sync, confirm you are following the expected network, and inspect validator logs for consensus or ledger gaps before restarting anything.",
            deviation=(0.90 - metrics.agreement_30d) * 100,
        ))

    if metrics.agreement_24h is not None and metrics.agreement_24h < 0.95 and healthy_peers:
        findings.append(_finding(
            category="fault",
            metric="agreement_24h",
            severity="warning",
            title="24h agreement is low despite healthy peer connectivity",
            current_value=_fmt_pct(metrics.agreement_24h),
            threshold_value=">= 95.0% with peer_count >= 5",
            likely_cause="This pattern often points to clock drift, stale state, or a validator process that is online but not tracking the same view as healthy peers.",
            recommended_action="Run an NTP or chrony sync check, compare your node state against a healthy validator, and review logs for validation or ledger mismatch warnings.",
            deviation=(0.95 - metrics.agreement_24h) * 100,
        ))

    if metrics.agreement_1h is not None and metrics.agreement_1h < 0.95 and healthy_peers:
        findings.append(_finding(
            category="fault",
            metric="agreement_1h",
            severity="warning",
            title="1h agreement suggests a recent degradation",
            current_value=_fmt_pct(metrics.agreement_1h),
            threshold_value=">= 95.0% with peer_count >= 5",
            likely_cause="A short-window agreement drop with peers still present usually indicates a fresh incident such as clock skew, stalled state, or a recent bad restart.",
            recommended_action="Check recent logs first, confirm system time is correct, and avoid repeated restarts until you understand whether the validator is catching up or diverging.",
            deviation=(0.95 - metrics.agreement_1h) * 100,
        ))

    if peer_count is not None and peer_count < 3:
        findings.append(_finding(
            category="fault",
            metric="peer_count",
            severity="critical",
            title="Peer connectivity is critically low",
            current_value=_fmt_count(peer_count),
            threshold_value=">= 3 peers minimum",
            likely_cause="Your validator is barely connected to the network, which can block healthy agreement and make the node look isolated.",
            recommended_action="Check firewall and cloud security-group rules, confirm the validator port is reachable, and verify the node is advertising the correct public address. Illustrative example: `ufw allow 2559/tcp`.",
            deviation=3 - peer_count,
        ))
    elif peer_count is not None and peer_count < 5:
        findings.append(_finding(
            category="fault",
            metric="peer_count",
            severity="warning",
            title="Peer connectivity is below the healthy operating range",
            current_value=_fmt_count(peer_count),
            threshold_value=">= 5 peers",
            likely_cause="The validator is connected, but not broadly enough to be resilient against churn or regional issues.",
            recommended_action="Review peering config, confirm inbound reachability on the validator port, and double-check that the host is not rate-limiting or dropping peer sessions.",
            deviation=5 - peer_count,
        ))

    if metrics.server_state and metrics.server_state not in HEALTHY_SERVER_STATES:
        findings.append(_finding(
            category="fault",
            metric="server_state",
            severity="critical",
            title="Server state is not healthy",
            current_value=metrics.server_state,
            threshold_value="proposing or full",
            likely_cause="The validator is reporting a non-serving state, which usually means it is syncing, lagging, or otherwise not ready to participate normally.",
            recommended_action="Inspect the validator process status and recent logs, verify disk and network health, and do not assume the node is ready again until it reports `proposing` or `full` consistently.",
            deviation=1,
        ))

    if metrics.poll_success_pct is not None and metrics.poll_success_pct < 85:
        findings.append(_finding(
            category="fault",
            metric="poll_success",
            severity="warning",
            title="External poll success is low",
            current_value=_fmt_raw_pct(metrics.poll_success_pct),
            threshold_value=">= 85.0%",
            likely_cause="The scoring service is struggling to reach your validator reliably, which often indicates intermittent downtime, proxy issues, or network instability.",
            recommended_action="Check whether your validator endpoint is reachable from outside your host, verify reverse proxy or firewall behavior, and review any recent network interruptions.",
            deviation=85 - metrics.poll_success_pct,
        ))

    median_uptime_pct = cohort["median_uptime_pct"]
    if metrics.uptime_pct is not None and median_uptime_pct is not None and metrics.uptime_pct < median_uptime_pct:
        findings.append(_finding(
            category="fault",
            metric="uptime",
            severity="warning",
            title="Relative uptime is below the current cohort median",
            current_value=_fmt_raw_pct(metrics.uptime_pct),
            threshold_value=f">= {median_uptime_pct:.1f}% of cohort max uptime",
            likely_cause="This usually means the validator restarted more recently than peers or has had enough interruptions to lose uptime ground.",
            recommended_action="Confirm your process restart policy is durable, check host stability, and make sure planned maintenance is not causing unnecessary restarts. Illustrative example: `docker update --restart unless-stopped <container>`.",
            deviation=median_uptime_pct - metrics.uptime_pct,
        ))

    if metrics.latency_ms is not None and metrics.latency_ms > 300 and healthy_peers:
        findings.append(_finding(
            category="fault",
            metric="latency",
            severity="warning",
            title="Latency is high for an otherwise connected validator",
            current_value=_fmt_ms(metrics.latency_ms),
            threshold_value="<= 300ms with peer_count >= 5",
            likely_cause="This often points to geographic distance from the network center, underpowered hosting, or intermittent congestion on the uplink.",
            recommended_action="Check the host region, inspect packet loss or routing instability, and consider moving closer to the validator cohort if latency stays elevated.",
            deviation=metrics.latency_ms - 300,
        ))

    if latest_version and metrics.server_version and metrics.server_version != latest_version:
        findings.append(_finding(
            category="fault",
            metric="version",
            severity="warning",
            title="Validator version is behind the current cohort majority",
            current_value=metrics.server_version,
            threshold_value=f"match most common version {latest_version}",
            likely_cause="Running behind the current majority version can leave you with bug fixes or compatibility changes that most of the network already has.",
            recommended_action=f"Plan an upgrade to the current recommended version. Illustrative example: `docker compose pull && docker compose up -d`. Adapt the command to your own deployment setup and maintenance window.",
            deviation=1,
        ))

    if sub_scores.diversity < 0.5:
        findings.append(_finding(
            category="advisory",
            metric="diversity",
            severity="advisory",
            title="Hosting concentration is limiting your diversity score",
            current_value=_fmt_raw_pct(sub_scores.diversity * 100),
            threshold_value="> 50.0% diversity sub-score",
            likely_cause="Your validator shares ASN or hosting concentration with a large slice of the current cohort, which raises correlated-risk concerns even if the node is healthy.",
            recommended_action="Treat this as a placement optimization rather than a fault. If you want to improve resilience and score diversity, consider moving to a less common provider or ASN over time.",
            deviation=(0.5 - sub_scores.diversity) * 100,
        ))

    if metrics.latency_ms is None or metrics.peer_count is None or metrics.asn is None or metrics.country is None:
        findings.append(_finding(
            category="advisory",
            metric="latency",
            severity="advisory",
            title="Topology enrichment is incomplete for this validator",
            current_value="missing one or more topology fields",
            threshold_value="latency, peer_count, country, and ASN available",
            likely_cause="The scoring pipeline does not have a full live node-to-validator mapping for this validator yet, so some diagnostics are less precise than they could be.",
            recommended_action="If this persists, verify node-key mapping or public topology visibility so the dashboard can attach full network context to your validator.",
            deviation=1,
        ))

    if not findings and validator.composite_score < 75:
        findings.append(_finding(
            category="advisory",
            metric="agreement_30d",
            severity="advisory",
            title="No single fault stands out, but the overall score is still mid-pack",
            current_value=_fmt_score(validator.composite_score),
            threshold_value="75.00+ composite score target",
            likely_cause="This usually means several metrics are acceptable rather than excellent, so the score lags without one obvious break/fix issue.",
            recommended_action="Use the simulator and this report together: preserve your healthy metrics, then focus on whichever adjustable area is closest to a scoring threshold.",
            deviation=75 - validator.composite_score,
        ))

    if metrics.agreement_30d is not None and cohort["median_agreement_30d"] is not None and metrics.agreement_30d >= cohort["median_agreement_30d"]:
        strengths.append(_strength(
            metric="agreement_30d",
            title="Long-window agreement is above cohort median",
            current_value=_fmt_pct(metrics.agreement_30d),
            benchmark=f"median {cohort['median_agreement_30d'] * 100:.1f}%",
            why_it_matters="Strong long-window agreement is a good sign that your validator tracks the network consistently over time.",
        ))
    if metrics.peer_count is not None and cohort["median_peer_count"] is not None and metrics.peer_count >= cohort["median_peer_count"]:
        strengths.append(_strength(
            metric="peer_count",
            title="Peer connectivity is stronger than the median validator",
            current_value=_fmt_count(metrics.peer_count),
            benchmark=f"median {cohort['median_peer_count']:.1f} peers",
            why_it_matters="Healthy peer breadth improves resilience and makes other failures easier to diagnose cleanly.",
        ))
    if metrics.latency_ms is not None and cohort["median_latency_ms"] is not None and metrics.latency_ms <= cohort["median_latency_ms"]:
        strengths.append(_strength(
            metric="latency",
            title="Latency is better than the cohort median",
            current_value=_fmt_ms(metrics.latency_ms),
            benchmark=f"median {cohort['median_latency_ms']:.0f}ms",
            why_it_matters="Low latency helps the validator react quickly and keeps this dimension from dragging down the composite score.",
        ))
    if metrics.poll_success_pct is not None and cohort["median_poll_success_pct"] is not None and metrics.poll_success_pct >= cohort["median_poll_success_pct"]:
        strengths.append(_strength(
            metric="poll_success",
            title="External reachability looks strong",
            current_value=_fmt_raw_pct(metrics.poll_success_pct),
            benchmark=f"median {cohort['median_poll_success_pct']:.1f}%",
            why_it_matters="High poll success suggests the validator is consistently reachable from the scoring side.",
        ))
    if metrics.server_version and latest_version and metrics.server_version == latest_version:
        strengths.append(_strength(
            metric="version",
            title="Validator version matches the cohort majority",
            current_value=metrics.server_version,
            benchmark=f"majority version {latest_version}",
            why_it_matters="Version alignment reduces compatibility surprises and keeps you on the same baseline as most of the network.",
        ))
    if sub_scores.diversity >= 0.5:
        strengths.append(_strength(
            metric="diversity",
            title="Hosting diversity is not heavily penalized",
            current_value=_fmt_raw_pct(sub_scores.diversity * 100),
            benchmark="50.0%+ diversity sub-score",
            why_it_matters="A healthier diversity score means your validator is less exposed to correlated provider concentration.",
        ))

    findings.sort(
        key=lambda finding: (
            SEVERITY_ORDER.get(finding["severity"], 99),
            -finding["_sort_weight"],
            -finding["_deviation"],
            finding["title"],
        )
    )
    for finding in findings:
        finding.pop("_sort_weight", None)
        finding.pop("_deviation", None)

    strengths = strengths[:4]
    if any(finding["severity"] == "critical" for finding in findings):
        overall_status = "critical"
        status_summary = "Critical issues need attention first. Fix the top findings before chasing smaller optimizations."
    elif any(finding["severity"] == "warning" for finding in findings):
        overall_status = "warning"
        status_summary = "The validator is functioning, but there are meaningful issues worth correcting soon."
    elif findings:
        overall_status = "advisory"
        status_summary = "No obvious fault conditions were detected, but there are still a few ways to improve resilience or score efficiency."
    else:
        overall_status = "healthy"
        status_summary = "Clean bill of health. No current fault conditions were detected from the live scoring data."

    return {
        "public_key": validator.public_key,
        "domain": validator.domain,
        "round_id": round_id,
        "timestamp": timestamp,
        "composite_score": validator.composite_score,
        "rank": rank,
        "validator_count": validator_count,
        "overall_status": overall_status,
        "status_summary": status_summary,
        "json_report_url": f"/api/diagnose/{validator.public_key}",
        "findings": findings,
        "strengths": strengths,
    }


def build_peer_comparison(scores: list[ValidatorScore], public_key: str) -> dict:
    validator = next((score for score in scores if score.public_key == public_key), None)
    if not validator:
        raise KeyError(public_key)

    same_provider = [
        score for score in scores
        if score.public_key != public_key and score.metrics.isp and score.metrics.isp == validator.metrics.isp
    ]

    def avg(values: list[float]) -> float | None:
        if not values:
            return None
        return round(sum(values) / len(values), 2)

    return {
        "provider": validator.metrics.isp,
        "peer_count": len(same_provider),
        "avg_composite_score": avg([score.composite_score for score in same_provider]),
        "avg_latency_ms": avg([score.metrics.latency_ms for score in same_provider if score.metrics.latency_ms is not None]),
        "avg_uptime_pct": avg([score.metrics.uptime_pct for score in same_provider if score.metrics.uptime_pct is not None]),
        "avg_agreement_24h": avg([score.metrics.agreement_24h for score in same_provider if score.metrics.agreement_24h is not None]),
    }
