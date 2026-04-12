from datetime import datetime, timedelta


RUNBOOK_LIBRARY = {
    "peer_collapse": {
        "cause_label": "peer_collapse",
        "title": "Peer Collapse",
        "description": "The validator lost too many peers and became poorly connected to the network.",
        "typical_patterns": [
            "peer_count drops below healthy threshold",
            "agreement degrades after connectivity loss",
            "server remains up but becomes isolated",
        ],
        "check_first": "Confirm whether the validator is still listening on the peer port and reachable from outside the host.",
        "steps": [
            {"title": "Check listener state", "command": "ss -ltnp | grep 2559"},
            {"title": "Review validator logs", "command": "docker logs postfiatd --tail 100"},
            {"title": "Open firewall if needed", "command": "ufw allow 2559/tcp"},
            {"title": "Restart only after confirming config", "command": "docker compose restart"},
        ],
        "escalation_note": "If peer count does not recover after checking firewall, routing, and logs, escalate in Discord #validator-support with recent log excerpts.",
    },
    "provider_outage": {
        "cause_label": "provider_outage",
        "title": "Provider Outage",
        "description": "Multiple validators on the same hosting provider were affected in the same round, suggesting upstream provider trouble.",
        "typical_patterns": [
            "5 or more incidents in one round on the same provider",
            "shared provider concentration among affected validators",
            "simultaneous agreement or availability failures",
        ],
        "check_first": "Check your provider status page and compare timestamps against other validators on the same hoster.",
        "steps": [
            {"title": "Check provider status", "command": "open https://status.your-provider.example"},
            {"title": "Verify host network reachability", "command": "ping -c 5 1.1.1.1"},
            {"title": "Inspect validator logs", "command": "docker logs postfiatd --tail 100"},
            {"title": "Fail over only if needed", "command": "# prepare alternate host or recovery plan before restarting repeatedly"},
        ],
        "escalation_note": "If multiple validators on the same provider remain degraded, coordinate publicly and consider provider escalation before making repeated local changes.",
    },
    "version_drift": {
        "cause_label": "version_drift",
        "title": "Version Drift",
        "description": "The validator is running behind the highest current cohort version and the incident is consistent with upgrade lag.",
        "typical_patterns": [
            "server_version behind highest semantic version",
            "version change or degraded behavior during rollout",
            "cluster of lagging validators during upgrade window",
        ],
        "check_first": "Confirm the currently highest semantic version in the network before changing your node.",
        "steps": [
            {"title": "Pull latest images", "command": "docker compose pull"},
            {"title": "Restart the stack", "command": "docker compose up -d"},
            {"title": "Confirm running version", "command": "docker compose ps"},
            {"title": "Check startup logs", "command": "docker logs postfiatd --tail 100"},
        ],
        "escalation_note": "If the node still reports an older version after upgrade commands, verify your compose file and image tags before escalating.",
    },
    "node_restart": {
        "cause_label": "node_restart",
        "title": "Node Restart",
        "description": "The incident pattern looks like a validator restart or reboot followed by recovery.",
        "typical_patterns": [
            "agreement drop followed by rapid recovery",
            "short-lived incident window",
            "possible uptime reset or restart symptoms",
        ],
        "check_first": "Check whether the validator or host restarted intentionally or unexpectedly around the incident start time.",
        "steps": [
            {"title": "Inspect recent logs", "command": "docker logs postfiatd --tail 100"},
            {"title": "Check restart policy", "command": "docker inspect postfiatd --format '{{.HostConfig.RestartPolicy.Name}}'"},
            {"title": "Inspect service status", "command": "docker compose ps"},
            {"title": "Review host stability", "command": "journalctl -n 100 --no-pager"},
        ],
        "escalation_note": "If restarts are recurring and not operator-initiated, investigate host resource pressure or supervisor restarts before escalating.",
    },
    "ledger_stall": {
        "cause_label": "ledger_stall",
        "title": "Ledger Stall",
        "description": "Freshness signals degraded while the node remained connected, suggesting the validator stopped advancing ledgers normally.",
        "typical_patterns": [
            "validated_ledger_age or avg_ledger_interval degrades",
            "peer loss is absent or minor",
            "server remains visible but ledger progress slows",
        ],
        "check_first": "Confirm whether the validator is still advancing ledgers or has stalled despite healthy connectivity.",
        "steps": [
            {"title": "Check latest logs", "command": "docker logs postfiatd --tail 100"},
            {"title": "Confirm process state", "command": "docker compose ps"},
            {"title": "Inspect disk and resource health", "command": "df -h && free -m"},
            {"title": "Perform a controlled restart only if needed", "command": "docker compose restart"},
        ],
        "escalation_note": "If the node remains stalled after a controlled restart and resource checks look clean, escalate with logs and freshness metrics.",
    },
    "flapping": {
        "cause_label": "flapping",
        "title": "Flapping Incident",
        "description": "The same incident pattern repeated multiple times in a short window, suggesting instability rather than a single outage.",
        "typical_patterns": [
            "same incident type repeats within 24 hours",
            "open and recovery cycles repeat",
            "intermittent infrastructure or configuration issue",
        ],
        "check_first": "Compare repeated incident windows to see whether the same trigger keeps returning after temporary recovery.",
        "steps": [
            {"title": "Review repeated event history", "command": "# compare repeated incidents in the dashboard timeline"},
            {"title": "Inspect validator logs", "command": "docker logs postfiatd --tail 150"},
            {"title": "Check host/network stability", "command": "ping -c 5 1.1.1.1"},
            {"title": "Pause repeated restarts", "command": "# avoid hiding the pattern by restarting after every transient recovery"},
        ],
        "escalation_note": "If the same incident keeps returning after local fixes, escalate with the repeated timeline pattern and timestamps.",
    },
    "unknown": {
        "cause_label": "unknown",
        "title": "Unknown Cause",
        "description": "The stored evidence does not strongly match a known RCA pattern yet.",
        "typical_patterns": [
            "mixed or incomplete event evidence",
            "no single trigger dominates",
            "insufficient stored metadata for stronger classification",
        ],
        "check_first": "Start with the incident event sequence and the before/during/after snapshots to narrow down what changed first.",
        "steps": [
            {"title": "Inspect incident snapshots", "command": "# review the incident before/during/after metrics in the dashboard"},
            {"title": "Check recent validator logs", "command": "docker logs postfiatd --tail 100"},
            {"title": "Confirm process and network health", "command": "docker compose ps && ping -c 5 1.1.1.1"},
        ],
        "escalation_note": "If the evidence remains mixed, escalate with the incident ID, timestamps, and relevant logs so others can help correlate it.",
    },
}


def get_runbook_library() -> dict:
    return RUNBOOK_LIBRARY


def get_runbook(label: str) -> dict:
    return RUNBOOK_LIBRARY.get(label, RUNBOOK_LIBRARY["unknown"])


def _primary_event_type(incident: dict) -> str:
    event_types = incident.get("event_types") or []
    return event_types[0] if event_types else "unknown"


def _provider_for_incident(incident: dict, round_scores: list) -> str | None:
    score = next((row for row in round_scores if row.public_key == incident["validator_key"]), None)
    return score.metrics.isp if score else None


def classify_incident(
    incident: dict,
    *,
    related_incidents: list[dict],
    round_scores: list,
    latest_scores: list,
) -> dict:
    evidence: list[str] = []
    confidence = "low"
    cause = "unknown"
    primary_event = _primary_event_type(incident)
    during = incident.get("during_values") or {}
    before = incident.get("before_values") or {}
    after = incident.get("after_values") or {}

    provider = _provider_for_incident(incident, round_scores)
    if primary_event == "peer_collapse" or "peer_collapse" in incident.get("event_types", []):
        cause = "peer_collapse"
        confidence = "high"
        evidence.append(f"incident event_types include peer_collapse")
        if during.get("peer_count") is not None:
            evidence.append(f"during.peer_count={during.get('peer_count')}")
    else:
        same_round_provider = []
        if provider and incident.get("latest_round_id") is not None:
            for other in related_incidents:
                if other.get("latest_round_id") != incident.get("latest_round_id"):
                    continue
                if _provider_for_incident(other, round_scores) == provider:
                    same_round_provider.append(other)
        if provider and len(same_round_provider) >= 5:
            cause = "provider_outage"
            confidence = "high"
            evidence.append(f"{len(same_round_provider)} incidents in round {incident.get('latest_round_id')} on provider {provider}")
        else:
            current = next((score for score in latest_scores if score.public_key == incident["validator_key"]), None)
            versions = []
            for score in latest_scores:
                version = score.metrics.server_version
                if version:
                    parts = version.lstrip("vV").split(".")
                    if len(parts) >= 3 and all(part.split("-")[0].isdigit() for part in parts[:3]):
                        versions.append((tuple(int(part.split("-")[0]) for part in parts[:3]), version.lstrip("vV")))
            highest_version = max(versions, key=lambda item: item[0])[1] if versions else None
            current_version = current.metrics.server_version.lstrip("vV") if current and current.metrics.server_version else None
            if current_version and highest_version and current_version != highest_version:
                cause = "version_drift"
                confidence = "medium"
                evidence.append(f"validator version {current_version} behind highest cohort version {highest_version}")
            else:
                freshness_values = [
                    before.get("validated_ledger_age"), before.get("avg_ledger_interval"),
                    during.get("validated_ledger_age"), during.get("avg_ledger_interval"),
                    after.get("validated_ledger_age"), after.get("avg_ledger_interval"),
                ]
                if any(value is not None for value in freshness_values) and during.get("peer_count", 999) >= 5:
                    cause = "ledger_stall"
                    confidence = "medium"
                    evidence.append("freshness signal persisted in incident snapshots without peer collapse")
                else:
                    start_time = datetime.fromisoformat(incident["start_time"])
                    similar_recent = [
                        other for other in related_incidents
                        if other["validator_key"] == incident["validator_key"]
                        and other["id"] != incident["id"]
                        and _primary_event_type(other) == primary_event
                        and abs((datetime.fromisoformat(other["start_time"]) - start_time).total_seconds()) <= timedelta(hours=24).total_seconds()
                    ]
                    if len(similar_recent) >= 1:
                        cause = "flapping"
                        confidence = "high"
                        evidence.append(f"{len(similar_recent) + 1} incidents with primary event {primary_event} within 24 hours")
                    else:
                        rapid_recovery = (
                            primary_event in {"agreement_drop_warning", "agreement_drop_critical"}
                            and incident.get("duration_seconds") is not None
                            and incident["duration_seconds"] <= 3600
                            and after.get("agreement_24h") is not None
                            and during.get("agreement_24h") is not None
                            and after.get("agreement_24h") >= during.get("agreement_24h")
                        )
                        if rapid_recovery:
                            cause = "node_restart"
                            confidence = "low"
                            evidence.append("agreement incident recovered quickly, consistent with restart-style disruption")
                        else:
                            evidence.append("stored incident evidence did not strongly match a known RCA rule")

    runbook = get_runbook(cause)
    return {
        "suspected_cause": cause,
        "confidence": confidence,
        "evidence": evidence,
        "runbook": runbook,
    }
