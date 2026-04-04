from datetime import datetime, timedelta, timezone

from app.models import ValidatorScore

WARNING_EVENT_TYPES = {"peer_collapse", "score_shock", "server_state_anomaly", "agreement_drop_warning"}
IMMEDIATE_EVENT_TYPES = {"agreement_drop_critical", "validator_disappearance", "validator_appearance", "version_change"}
ACTIVE_EVENT_TYPES = {"agreement_drop_warning", "agreement_drop_critical", "validator_disappearance", "peer_collapse", "score_shock", "server_state_anomaly"}
HEALTHY_SERVER_STATES = {"proposing", "full"}
ROLLING_INCIDENT_WINDOW = timedelta(minutes=30)
CORRELATED_THRESHOLD = 5


def _score_map(scores: list[ValidatorScore]) -> dict[str, ValidatorScore]:
    return {score.public_key: score for score in scores}


def _rank_map(scores: list[ValidatorScore]) -> dict[str, int]:
    return {score.public_key: idx + 1 for idx, score in enumerate(scores)}


def _metric_snapshot(score: ValidatorScore | None, rank: int | None = None) -> dict:
    if not score:
        return {"rank": rank}
    metrics = score.metrics
    return {
        "rank": rank,
        "composite_score": score.composite_score,
        "agreement_1h": metrics.agreement_1h,
        "agreement_24h": metrics.agreement_24h,
        "agreement_30d": metrics.agreement_30d,
        "peer_count": metrics.peer_count,
        "server_state": metrics.server_state,
        "server_version": metrics.server_version,
    }


def _max_agreement_drop(score: ValidatorScore | None, threshold: float) -> bool:
    if not score:
        return False
    values = [score.metrics.agreement_1h, score.metrics.agreement_24h, score.metrics.agreement_30d]
    return any(value is not None and value < threshold for value in values)


def _server_state_bad(score: ValidatorScore | None) -> bool:
    if not score:
        return False
    state = score.metrics.server_state
    return state is not None and state not in HEALTHY_SERVER_STATES


def _peer_collapse(score: ValidatorScore | None) -> bool:
    return bool(score and score.metrics.peer_count is not None and score.metrics.peer_count < 5)


def _score_shock(current: ValidatorScore | None, previous: ValidatorScore | None) -> bool:
    return bool(current and previous and (previous.composite_score - current.composite_score) > 5)


def _version_changed(current: ValidatorScore | None, previous: ValidatorScore | None) -> bool:
    return bool(
        current
        and previous
        and current.metrics.server_version != previous.metrics.server_version
        and current.metrics.server_version is not None
        and previous.metrics.server_version is not None
    )


def _summary_for_event(event_type: str, validator_key: str, synthetic: bool = False) -> str:
    labels = {
        "agreement_drop_warning": "Agreement degraded",
        "agreement_drop_critical": "Agreement critically low",
        "validator_disappearance": "Validator disappeared from scoring",
        "validator_appearance": "Validator appeared in scoring",
        "peer_collapse": "Peer count collapse",
        "score_shock": "Composite score shock",
        "version_change": "Server version changed",
        "server_state_anomaly": "Server state anomaly",
        "synthetic_test": "Synthetic incident injected for verification",
    }
    prefix = "[Synthetic] " if synthetic else ""
    return f"{prefix}{labels.get(event_type, event_type)} - {validator_key[:16]}..."


def _consecutive_condition(current: bool, previous: bool) -> bool:
    return current and previous


async def detect_and_store_incidents(db, round_id: int):
    recent_rounds = await db.get_recent_round_summaries(limit=3)
    current_round = next((row for row in recent_rounds if row["id"] == round_id), None)
    if not current_round:
        return

    previous_round = next((row for row in recent_rounds if row["id"] < round_id), None)
    older_round = next((row for row in recent_rounds if previous_round and row["id"] < previous_round["id"]), None)
    if not previous_round:
        return

    current_scores = await db.get_scores_for_round(round_id)
    previous_scores = await db.get_scores_for_round(previous_round["id"])
    older_scores = await db.get_scores_for_round(older_round["id"]) if older_round else []

    current_map = _score_map(current_scores)
    previous_map = _score_map(previous_scores)
    older_map = _score_map(older_scores)
    current_ranks = _rank_map(current_scores)
    previous_ranks = _rank_map(previous_scores)
    older_ranks = _rank_map(older_scores)

    triggered_events: list[dict] = []
    all_keys = sorted(set(current_map) | set(previous_map))
    for key in all_keys:
        current = current_map.get(key)
        previous = previous_map.get(key)
        older = older_map.get(key)

        if previous and not current:
            triggered_events.append({
                "validator_key": key,
                "event_type": "validator_disappearance",
                "severity": "critical",
                "current_values": {"present": False},
                "previous_values": {"present": True, **_metric_snapshot(previous, previous_ranks.get(key))},
            })
            continue

        if current and not previous:
            triggered_events.append({
                "validator_key": key,
                "event_type": "validator_appearance",
                "severity": "info",
                "current_values": {"present": True, **_metric_snapshot(current, current_ranks.get(key))},
                "previous_values": {"present": False},
            })
            continue

        if not current or not previous:
            continue

        if _max_agreement_drop(current, 0.90):
            triggered_events.append({
                "validator_key": key,
                "event_type": "agreement_drop_critical",
                "severity": "critical",
                "current_values": _metric_snapshot(current, current_ranks.get(key)),
                "previous_values": _metric_snapshot(previous, previous_ranks.get(key)),
            })
        elif _consecutive_condition(_max_agreement_drop(current, 0.95), _max_agreement_drop(previous, 0.95)):
            triggered_events.append({
                "validator_key": key,
                "event_type": "agreement_drop_warning",
                "severity": "warning",
                "current_values": _metric_snapshot(current, current_ranks.get(key)),
                "previous_values": _metric_snapshot(previous, previous_ranks.get(key)),
            })

        if _consecutive_condition(_peer_collapse(current), _peer_collapse(previous)):
            triggered_events.append({
                "validator_key": key,
                "event_type": "peer_collapse",
                "severity": "warning",
                "current_values": _metric_snapshot(current, current_ranks.get(key)),
                "previous_values": _metric_snapshot(previous, previous_ranks.get(key)),
            })

        if older and _consecutive_condition(_score_shock(current, previous), _score_shock(previous, older)):
            triggered_events.append({
                "validator_key": key,
                "event_type": "score_shock",
                "severity": "warning",
                "current_values": _metric_snapshot(current, current_ranks.get(key)),
                "previous_values": _metric_snapshot(previous, previous_ranks.get(key)),
            })

        if _version_changed(current, previous):
            triggered_events.append({
                "validator_key": key,
                "event_type": "version_change",
                "severity": "info",
                "current_values": _metric_snapshot(current, current_ranks.get(key)),
                "previous_values": _metric_snapshot(previous, previous_ranks.get(key)),
            })

        if _consecutive_condition(_server_state_bad(current), _server_state_bad(previous)):
            triggered_events.append({
                "validator_key": key,
                "event_type": "server_state_anomaly",
                "severity": "warning",
                "current_values": _metric_snapshot(current, current_ranks.get(key)),
                "previous_values": _metric_snapshot(previous, previous_ranks.get(key)),
            })

    correlated = len({event["validator_key"] for event in triggered_events}) >= CORRELATED_THRESHOLD

    for event in triggered_events:
        event["correlated"] = correlated
        await _record_event(db, round_id, current_round["timestamp"], event)

    await _close_recovered_incidents(
        db,
        round_id=round_id,
        event_time=current_round["timestamp"],
        current_map=current_map,
        previous_map=previous_map,
        older_map=older_map,
        current_ranks=current_ranks,
        previous_ranks=previous_ranks,
        older_ranks=older_ranks,
    )


async def _record_event(db, round_id: int, event_time: str, event: dict):
    incident = await db.get_latest_active_incident_for_validator(event["validator_key"])
    use_existing = False
    if incident:
        last_event_time = datetime.fromisoformat(incident["latest_event_time"])
        use_existing = datetime.fromisoformat(event_time) - last_event_time <= ROLLING_INCIDENT_WINDOW

    if use_existing:
        event_types = sorted(set(incident["event_types"]) | {event["event_type"]})
        active_event_types = sorted(set(incident["active_event_types"]))
        if event["event_type"] in ACTIVE_EVENT_TYPES and event["event_type"] not in active_event_types:
            active_event_types.append(event["event_type"])
            active_event_types.sort()
        severity = _max_severity([incident["severity"], event["severity"]])
        await db.update_incident(
            incident["id"],
            severity=severity,
            status="open" if active_event_types else "closed",
            summary=incident["summary"],
            latest_round_id=round_id,
            latest_event_time=event_time,
            event_types=event_types,
            active_event_types=active_event_types,
            before_values=incident["before_values"] or event["previous_values"],
            during_values=incident["during_values"] or event["current_values"],
            after_values=incident["after_values"],
            correlated=incident["correlated"] or event["correlated"],
            end_time=None if active_event_types else event_time,
        )
        await db.add_incident_event(
            incident_id=incident["id"],
            round_id=round_id,
            validator_key=event["validator_key"],
            event_type=event["event_type"],
            severity=event["severity"],
            event_phase="triggered",
            current_values=event["current_values"],
            previous_values=event["previous_values"],
            correlated=event["correlated"],
            created_at=event_time,
        )
        return

    active_event_types = [event["event_type"]] if event["event_type"] in ACTIVE_EVENT_TYPES else []
    status = "open" if active_event_types else "closed"
    incident_id = await db.create_incident(
        validator_key=event["validator_key"],
        severity=event["severity"],
        status=status,
        synthetic=False,
        correlated=event["correlated"],
        summary=_summary_for_event(event["event_type"], event["validator_key"]),
        start_time=event_time,
        end_time=event_time if status == "closed" else None,
        latest_round_id=round_id,
        latest_event_time=event_time,
        event_types=[event["event_type"]],
        active_event_types=active_event_types,
        before_values=event["previous_values"],
        during_values=event["current_values"],
        after_values=event["current_values"] if status == "closed" else None,
    )
    await db.add_incident_event(
        incident_id=incident_id,
        round_id=round_id,
        validator_key=event["validator_key"],
        event_type=event["event_type"],
        severity=event["severity"],
        event_phase="triggered",
        current_values=event["current_values"],
        previous_values=event["previous_values"],
        correlated=event["correlated"],
        created_at=event_time,
    )


async def _close_recovered_incidents(
    db,
    *,
    round_id: int,
    event_time: str,
    current_map: dict[str, ValidatorScore],
    previous_map: dict[str, ValidatorScore],
    older_map: dict[str, ValidatorScore],
    current_ranks: dict[str, int],
    previous_ranks: dict[str, int],
    older_ranks: dict[str, int],
):
    open_incidents = await db.get_open_incidents()
    for incident in open_incidents:
        active_types = list(incident["active_event_types"])
        if not active_types:
            continue

        current = current_map.get(incident["validator_key"])
        previous = previous_map.get(incident["validator_key"])
        older = older_map.get(incident["validator_key"])
        recovered_types = []

        for event_type in active_types:
            if _is_recovered(event_type, current, previous, older):
                recovered_types.append(event_type)
                await db.add_incident_event(
                    incident_id=incident["id"],
                    round_id=round_id,
                    validator_key=incident["validator_key"],
                    event_type=event_type,
                    severity=incident["severity"],
                    event_phase="recovered",
                    current_values=_metric_snapshot(current, current_ranks.get(incident["validator_key"])),
                    previous_values=_metric_snapshot(previous, previous_ranks.get(incident["validator_key"])),
                    correlated=incident["correlated"],
                    created_at=event_time,
                )

        if not recovered_types:
            continue

        remaining = [event_type for event_type in active_types if event_type not in recovered_types]
        await db.update_incident(
            incident["id"],
            severity=incident["severity"],
            status="closed" if not remaining else "open",
            summary=incident["summary"],
            latest_round_id=round_id,
            latest_event_time=event_time,
            event_types=incident["event_types"],
            active_event_types=remaining,
            before_values=incident["before_values"],
            during_values=incident["during_values"],
            after_values=_metric_snapshot(current, current_ranks.get(incident["validator_key"])),
            correlated=incident["correlated"],
            end_time=event_time if not remaining else None,
        )


def _is_recovered(
    event_type: str,
    current: ValidatorScore | None,
    previous: ValidatorScore | None,
    older: ValidatorScore | None,
) -> bool:
    if event_type == "validator_disappearance":
        return current is not None and previous is not None
    if event_type in {"agreement_drop_warning", "agreement_drop_critical"}:
        return not _max_agreement_drop(current, 0.95) and not _max_agreement_drop(previous, 0.95)
    if event_type == "peer_collapse":
        return not _peer_collapse(current) and not _peer_collapse(previous)
    if event_type == "score_shock":
        return not _score_shock(current, previous) and not _score_shock(previous, older)
    if event_type == "server_state_anomaly":
        return not _server_state_bad(current) and not _server_state_bad(previous)
    return True


def _max_severity(severities: list[str]) -> str:
    ranking = {"info": 0, "warning": 1, "critical": 2}
    return max(severities, key=lambda item: ranking.get(item, 0))


async def inject_synthetic_incident(db, validator_key: str) -> dict:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(minutes=15)).isoformat()
    end = now.isoformat()
    current_values = {
        "note": "Synthetic incident for demonstration",
        "agreement_24h": 0.82,
        "peer_count": 2,
        "server_state": "syncing",
        "composite_score": 61.0,
    }
    previous_values = {
        "agreement_24h": 0.99,
        "peer_count": 12,
        "server_state": "proposing",
        "composite_score": 86.0,
    }
    incident_id = await db.create_incident(
        validator_key=validator_key,
        severity="warning",
        status="closed",
        synthetic=True,
        correlated=False,
        summary=_summary_for_event("synthetic_test", validator_key, synthetic=True),
        start_time=start,
        end_time=end,
        latest_round_id=None,
        latest_event_time=end,
        event_types=["synthetic_test"],
        active_event_types=[],
        before_values=previous_values,
        during_values=current_values,
        after_values={"status": "recovered"},
    )
    await db.add_incident_event(
        incident_id=incident_id,
        round_id=None,
        validator_key=validator_key,
        event_type="synthetic_test",
        severity="warning",
        event_phase="triggered",
        current_values=current_values,
        previous_values=previous_values,
        synthetic=True,
        correlated=False,
        created_at=start,
    )
    await db.add_incident_event(
        incident_id=incident_id,
        round_id=None,
        validator_key=validator_key,
        event_type="synthetic_test",
        severity="warning",
        event_phase="recovered",
        current_values={"status": "recovered"},
        previous_values=current_values,
        synthetic=True,
        correlated=False,
        created_at=end,
    )
    return await db.get_incident(incident_id)
