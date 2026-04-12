import asyncio
from collections import Counter

import httpx

from app.config import settings
from app.models import ValidatorScore
from app.upgrades import _display_version, _normalize_version, _parse_semver

CRAWL_TIMEOUT = 5.0
CRAWL_CONCURRENCY = 12
PEER_PORT_DEFAULT = 2559
CONCENTRATION_WARN_PCT = 50.0
NETWORK_CONCENTRATION_WARN_PCT = 33.0
HIGH_OVERLAP_WARN_PCT = 60.0
PEER_DISCLAIMER = (
    "Peer recommendations are based on observable health and decentralization heuristics. "
    "They are not a guarantee of better connectivity or performance, so operators should verify "
    "reachability and behavior after making peer changes."
)


def _truncate_key(key: str | None) -> str:
    if not key:
        return "unknown"
    return f"{key[:10]}...{key[-6:]}"


async def _fetch_topology_nodes() -> list[dict]:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(f"{settings.vhs_base_url}/v1/network/topology/nodes")
        response.raise_for_status()
        data = response.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("nodes", data.get("data", []))
    return []


async def _fetch_crawl_for_topology(topology_nodes: list[dict]) -> dict[str, dict]:
    ips = [node.get("ip") for node in topology_nodes if node.get("ip")]
    if not ips:
        return {}

    sem = asyncio.Semaphore(CRAWL_CONCURRENCY)
    results: dict[str, dict] = {}

    async def fetch_one(ip: str):
        async with sem:
            try:
                async with httpx.AsyncClient(timeout=CRAWL_TIMEOUT, verify=False) as client:
                    response = await client.get(f"https://{ip}:{settings.crawl_peer_port}/crawl")
                    response.raise_for_status()
                results[ip] = response.json()
            except Exception:
                return

    await asyncio.gather(*(fetch_one(ip) for ip in ips))
    return results


async def _fetch_single_crawl(ip: str | None) -> dict | None:
    if not ip:
        return None
    try:
        async with httpx.AsyncClient(timeout=CRAWL_TIMEOUT, verify=False) as client:
            response = await client.get(f"https://{ip}:{settings.crawl_peer_port}/crawl")
            response.raise_for_status()
        return response.json()
    except Exception:
        return None


def _normalize_peer_refs(active_peers: list[dict], topology_by_ip: dict[str, dict]) -> tuple[list[str], bool]:
    refs: list[str] = []
    adjacency_present = False
    for peer in active_peers or []:
        if not isinstance(peer, dict):
            continue
        adjacency_present = True
        node_key = peer.get("pubkey_node") or peer.get("node_public_key") or peer.get("public_key")
        if node_key:
            refs.append(node_key)
            continue
        peer_ip = peer.get("ip")
        if peer_ip:
            mapped = topology_by_ip.get(peer_ip)
            refs.append(mapped.get("node_public_key") if mapped and mapped.get("node_public_key") else peer_ip)
    return refs, adjacency_present


def _pick_target_node(candidate_records: list[dict], public_key: str) -> dict | None:
    if not candidate_records:
        return None
    exact = [record for record in candidate_records if record.get("validator_public_key") == public_key]
    if exact:
        exact.sort(key=lambda item: (item.get("node_public_key") or "", item.get("ip") or ""))
        return exact[0]
    return None


def _quality_label(record: dict, latest_version: str | None) -> tuple[str, str, int]:
    reasons: list[str] = []
    severity = 0
    agreement = record.get("agreement_24h")
    latency = record.get("latency_ms")
    version = record.get("server_version")
    mapped_validator = record.get("validator_public_key")

    if mapped_validator and agreement is not None:
        if agreement < 0.90:
            severity = max(severity, 2)
            reasons.append(f"agreement_24h is critically low at {agreement:.3f}")
        elif agreement < 0.95:
            severity = max(severity, 1)
            reasons.append(f"agreement_24h is below the healthy floor at {agreement:.3f}")

    normalized_version = _normalize_version(version)
    if latest_version and normalized_version and normalized_version != latest_version:
        severity = max(severity, 2)
        reasons.append(f"version {normalized_version} lags the latest cohort version {latest_version}")
    elif latest_version and not normalized_version and version:
        severity = max(severity, 1)
        reasons.append(f"version {version} is not semver-normalized")

    if latency is not None:
        if latency > 500:
            severity = max(severity, 2)
            reasons.append(f"latency is high at {latency:.1f}ms")
        elif latency > 250:
            severity = max(severity, 1)
            reasons.append(f"latency is elevated at {latency:.1f}ms")

    if not mapped_validator:
        reasons.append("non-validating node rated from observable topology only")
        severity = max(severity, 1 if severity == 0 else severity)

    if severity >= 2:
        return "risky", "; ".join(reasons), 2
    if severity == 1:
        return "acceptable", "; ".join(reasons), 1
    return "good", "healthy validating node with current version and normal latency", 0


def _ranked_scores(scores: list[ValidatorScore]) -> dict[str, int]:
    ranked = sorted(scores, key=lambda score: (-score.composite_score, score.public_key))
    return {score.public_key: idx + 1 for idx, score in enumerate(ranked)}


def _project_rank(scores: list[ValidatorScore], public_key: str, composite_delta: float) -> tuple[float, int, int]:
    ranked_scores = _ranked_scores(scores)
    validator = next(score for score in scores if score.public_key == public_key)
    current_rank = ranked_scores[public_key]
    projected_composite = round(validator.composite_score + composite_delta, 2)

    projected_scores = []
    for score in scores:
        composite = projected_composite if score.public_key == public_key else score.composite_score
        projected_scores.append((score.public_key, composite))
    projected_scores.sort(key=lambda item: (-item[1], item[0]))
    projected_rank = next(index + 1 for index, item in enumerate(projected_scores) if item[0] == public_key)
    return projected_composite, projected_rank, current_rank - projected_rank


def _distribution_warning(counter: Counter, total: int, label_template: str, threshold: float) -> list[dict]:
    findings = []
    for value, count in counter.most_common():
        if not value or not total:
            continue
        pct = round(100 * count / total, 1)
        if pct >= threshold:
            findings.append(
                {
                    "title": label_template.format(value=value, count=count, total=total, pct=pct),
                    "severity": "warn" if pct < 75 else "risk",
                    "detail": f"{count} of {total} observed nodes ({pct}%) fall into this grouping.",
                }
            )
    return findings


def _overlap_finding(target_peer_keys: set[str], validator_peer_sets: dict[str, set[str]], public_key: str) -> dict | None:
    if not target_peer_keys:
        return None
    other_sets = [peer_set for validator_key, peer_set in validator_peer_sets.items() if validator_key != public_key and peer_set]
    if not other_sets:
        return None

    overlap_fractions = []
    for peer_key in target_peer_keys:
        shared = sum(1 for peer_set in other_sets if peer_key in peer_set)
        overlap_fractions.append(shared / len(other_sets))
    avg_overlap = round(100 * sum(overlap_fractions) / len(overlap_fractions), 1)
    if avg_overlap < HIGH_OVERLAP_WARN_PCT:
        return None
    return {
        "title": f"Neighborhood overlap is elevated at {avg_overlap}%",
        "severity": "warn",
        "detail": "This is the average fraction of other validators whose peer sets also include each of your peers.",
    }


def _build_base_node_records(scores: list[ValidatorScore], topology_nodes: list[dict]) -> list[dict]:
    score_by_key = {score.public_key: score for score in scores}
    score_by_ip = {score.metrics.node_ip: score for score in scores if score.metrics.node_ip}

    versions = []
    for score in scores:
        parsed = _parse_semver(score.metrics.server_version)
        if parsed:
            versions.append((parsed, _normalize_version(score.metrics.server_version)))
    latest_version = max(versions, key=lambda item: item[0])[1] if versions else None

    records = []
    for node in topology_nodes:
        ip = node.get("ip")
        score = score_by_ip.get(ip) if ip else None
        validator_key = score.public_key if score else None

        record = {
            "node_public_key": node.get("node_public_key"),
            "validator_public_key": validator_key if validator_key in score_by_key else None,
            "domain": score.domain if score else None,
            "ip": ip,
            "port": settings.crawl_peer_port or PEER_PORT_DEFAULT,
            "provider": score.metrics.isp if score else None,
            "asn": score.metrics.asn if score else None,
            "country": (score.metrics.country if score and score.metrics.country else node.get("country_code")),
            "server_version": score.metrics.server_version if score else None,
            "latency_ms": score.metrics.latency_ms if score and score.metrics.latency_ms is not None else node.get("io_latency_ms"),
            "agreement_24h": score.metrics.agreement_24h if score else None,
            "peer_count": score.metrics.peer_count if score else None,
            "peer_refs": [],
            "has_adjacency": False,
            "non_validating": score is None,
        }
        quality, reason, severity_score = _quality_label(record, latest_version)
        record["quality_rating"] = quality
        record["quality_reason"] = reason
        record["_quality_score"] = severity_score
        records.append(record)

    return records


def _recommend_additions(
    *,
    validator: ValidatorScore,
    scores: list[ValidatorScore],
    node_records: list[dict],
    current_peer_keys: set[str],
    candidate_only: bool,
) -> list[dict]:
    provider_counts = Counter()
    asn_counts = Counter()
    country_counts = Counter()

    relevant_records = [record for record in node_records if record["node_public_key"] in current_peer_keys] if not candidate_only else node_records
    for record in relevant_records:
        if record.get("provider"):
            provider_counts[record["provider"]] += 1
        if record.get("asn") is not None:
            asn_counts[record["asn"]] += 1
        if record.get("country"):
            country_counts[record["country"]] += 1

    candidates = []
    for record in node_records:
        node_key = record.get("node_public_key")
        if not node_key or record.get("validator_public_key") == validator.public_key:
            continue
        if node_key in current_peer_keys:
            continue

        quality_bonus = {"good": 3.0, "acceptable": 1.5, "risky": -2.0}[record["quality_rating"]]
        provider_score = 1.0 / (provider_counts.get(record.get("provider"), 0) + 1) if record.get("provider") else 0.0
        asn_score = 1.0 / (asn_counts.get(record.get("asn"), 0) + 1) if record.get("asn") is not None else 0.0
        country_score = 1.0 / (country_counts.get(record.get("country"), 0) + 1) if record.get("country") else 0.0
        total_score = round(provider_score + asn_score + country_score + quality_bonus, 3)

        reasons = []
        if record.get("provider") and provider_counts.get(record["provider"], 0) == 0:
            reasons.append(f"introduces a new provider ({record['provider']})")
        if record.get("asn") is not None and asn_counts.get(record["asn"], 0) == 0:
            reasons.append(f"adds an underrepresented ASN (AS{record['asn']})")
        if record.get("country") and country_counts.get(record["country"], 0) == 0:
            reasons.append(f"broadens geography into {record['country']}")
        reasons.append(record["quality_reason"])

        candidates.append(
            {
                "node_public_key": node_key,
                "validator_public_key": record.get("validator_public_key"),
                "ip": record.get("ip"),
                "port": record.get("port") or PEER_PORT_DEFAULT,
                "provider": record.get("provider"),
                "asn": record.get("asn"),
                "country": record.get("country"),
                "quality_rating": record.get("quality_rating"),
                "reason": "; ".join(reasons),
                "_score": total_score,
            }
        )

    candidates.sort(key=lambda item: (-item["_score"], item["quality_rating"], item["provider"] or "", item["node_public_key"]))
    return [{key: value for key, value in candidate.items() if not key.startswith("_")} for candidate in candidates[:5]]


def _recommend_drops(peer_records: list[dict], dominant_provider: str | None, dominant_country: str | None) -> list[dict]:
    recommendations = []
    for record in peer_records:
        score = record["_quality_score"] * 10
        reasons = [record["quality_reason"]]
        if dominant_provider and record.get("provider") == dominant_provider:
            score += 3
            reasons.append(f"belongs to the dominant provider cluster ({dominant_provider})")
        if dominant_country and record.get("country") == dominant_country:
            score += 2
            reasons.append(f"belongs to the dominant geography cluster ({dominant_country})")
        if score <= 0:
            continue
        recommendations.append(
            {
                "node_public_key": record.get("node_public_key"),
                "validator_public_key": record.get("validator_public_key"),
                "ip": record.get("ip"),
                "port": record.get("port") or PEER_PORT_DEFAULT,
                "provider": record.get("provider"),
                "asn": record.get("asn"),
                "country": record.get("country"),
                "quality_rating": record.get("quality_rating"),
                "reason": "; ".join(reasons),
                "_score": score,
            }
        )
    recommendations.sort(key=lambda item: (-item["_score"], item["node_public_key"] or ""))
    return [{key: value for key, value in candidate.items() if not key.startswith("_")} for candidate in recommendations[:3]]


async def build_peer_report(scores: list[ValidatorScore], public_key: str) -> dict:
    if not scores:
        raise ValueError("No scoring data available yet")

    validator = next((score for score in scores if score.public_key == public_key), None)
    if not validator:
        raise KeyError(public_key)

    topology_nodes = await _fetch_topology_nodes()
    if not topology_nodes:
        raise ValueError("No topology data available yet")

    node_records = _build_base_node_records(scores, topology_nodes)

    target_candidates = [
        record for record in node_records
        if record.get("validator_public_key") == public_key or (validator.metrics.node_ip and record.get("ip") == validator.metrics.node_ip)
    ]
    target_node = _pick_target_node(target_candidates, public_key)

    node_by_key = {record["node_public_key"]: record for record in node_records if record.get("node_public_key")}
    topology_by_ip = {node.get("ip"): node for node in topology_nodes if node.get("ip")}
    adjacency_available = False

    if target_node and target_node.get("ip"):
        target_crawl = await _fetch_single_crawl(target_node["ip"])
        if isinstance(target_crawl, dict):
            overlay = target_crawl.get("overlay", {})
            peer_refs, has_adjacency = _normalize_peer_refs(overlay.get("active", []), topology_by_ip)
            target_node["peer_refs"] = [ref for ref in peer_refs if ref in node_by_key]
            target_node["has_adjacency"] = has_adjacency
            adjacency_available = has_adjacency

    target_has_adjacency = bool(target_node and target_node.get("has_adjacency") and adjacency_available)
    mode = "adjacency" if target_has_adjacency else "candidate_only"
    mode_banner = (
        "Full peer analysis mode: crawl adjacency is available for this validator, so the page is analyzing the current peer set directly."
        if mode == "adjacency"
        else "Candidate-only mode: crawl adjacency was unavailable or could not be mapped for this validator, so recommendations are based on observed network nodes."
    )

    validator_peer_sets = {}
    if mode == "adjacency":
        crawl_results = await _fetch_crawl_for_topology(topology_nodes)
        for record in node_records:
            ip = record.get("ip")
            crawl = crawl_results.get(ip or "", {})
            if not isinstance(crawl, dict):
                continue
            peer_refs, has_adjacency = _normalize_peer_refs(crawl.get("overlay", {}).get("active", []), topology_by_ip)
            record["peer_refs"] = [ref for ref in peer_refs if ref in node_by_key]
            record["has_adjacency"] = has_adjacency
            if record.get("validator_public_key") and has_adjacency:
                validator_peer_sets[record["validator_public_key"]] = set(record["peer_refs"])

    current_peer_keys = set(ref for ref in (target_node.get("peer_refs") if target_node else []) if ref in node_by_key) if mode == "adjacency" else set()
    peer_records = [node_by_key[key] for key in current_peer_keys if key in node_by_key]

    if mode == "adjacency" and not peer_records:
        mode = "candidate_only"
        mode_banner = (
            "Candidate-only mode: the validator node was found, but its current peer set could not be resolved into observable nodes."
        )

    displayed_rows = peer_records if mode == "adjacency" else sorted(node_records, key=lambda item: (item["_quality_score"], item["provider"] or "", item["node_public_key"] or ""))[:30]

    summary_rows = displayed_rows
    good_count = sum(1 for row in summary_rows if row["quality_rating"] == "good")
    acceptable_count = sum(1 for row in summary_rows if row["quality_rating"] == "acceptable")
    risky_count = sum(1 for row in summary_rows if row["quality_rating"] == "risky")

    risk_findings = []
    if mode == "adjacency":
        provider_counts = Counter(row.get("provider") for row in peer_records if row.get("provider"))
        country_counts = Counter(row.get("country") for row in peer_records if row.get("country"))
        version_counts = Counter(_display_version(row.get("server_version")) for row in peer_records if row.get("server_version"))

        risk_findings.extend(_distribution_warning(
            provider_counts,
            len(peer_records),
            "{value} makes up {pct}% of the current peer set",
            CONCENTRATION_WARN_PCT,
        ))
        risk_findings.extend(_distribution_warning(
            country_counts,
            len(peer_records),
            "{value} makes up {pct}% of the current peer geography",
            CONCENTRATION_WARN_PCT,
        ))
        outdated_count = sum(1 for row in peer_records if row["quality_rating"] == "risky" and "lags the latest cohort version" in row["quality_reason"])
        if outdated_count:
            risk_findings.append(
                {
                    "title": f"{outdated_count} current peers are behind the latest observed validator version",
                    "severity": "warn",
                    "detail": "Lagging peers can reduce network resilience during upgrade rollouts.",
                }
            )
        overlap = _overlap_finding(current_peer_keys, validator_peer_sets, public_key)
        if overlap:
            risk_findings.append(overlap)
        dominant_provider = provider_counts.most_common(1)[0][0] if provider_counts else None
        dominant_country = country_counts.most_common(1)[0][0] if country_counts else None
        drop_recommendations = _recommend_drops(peer_records, dominant_provider, dominant_country)
    else:
        provider_counts = Counter(row.get("provider") for row in node_records if row.get("provider"))
        country_counts = Counter(row.get("country") for row in node_records if row.get("country"))
        risk_findings.extend(_distribution_warning(
            provider_counts,
            sum(provider_counts.values()),
            "{value} accounts for {pct}% of observed node providers",
            NETWORK_CONCENTRATION_WARN_PCT,
        ))
        risk_findings.extend(_distribution_warning(
            country_counts,
            sum(country_counts.values()),
            "{value} accounts for {pct}% of observed node geographies",
            NETWORK_CONCENTRATION_WARN_PCT,
        ))
        dominant_provider = provider_counts.most_common(1)[0][0] if provider_counts else None
        dominant_country = country_counts.most_common(1)[0][0] if country_counts else None
        drop_recommendations = []

    add_recommendations = _recommend_additions(
        validator=validator,
        scores=scores,
        node_records=node_records,
        current_peer_keys=current_peer_keys,
        candidate_only=mode != "adjacency",
    )

    composite_delta_per_better_peer = 0.0
    if add_recommendations:
        high_quality = sum(1 for recommendation in add_recommendations[:3] if recommendation["quality_rating"] == "good")
        composite_delta_per_better_peer = round(high_quality * 0.15, 2)
    projected_composite, projected_rank, rank_delta = _project_rank(scores, public_key, composite_delta_per_better_peer)

    table_title = "Current Peer Set" if mode == "adjacency" else "Observed Network Node Candidates"
    normalized_rows = [
        {
            "node_public_key": row.get("node_public_key"),
            "validator_public_key": row.get("validator_public_key"),
            "domain": row.get("domain"),
            "ip": row.get("ip"),
            "port": row.get("port") or PEER_PORT_DEFAULT,
            "provider": row.get("provider"),
            "asn": row.get("asn"),
            "country": row.get("country"),
            "server_version": _display_version(row.get("server_version")),
            "latency_ms": row.get("latency_ms"),
            "agreement_24h": row.get("agreement_24h"),
            "quality_rating": row.get("quality_rating"),
            "quality_reason": row.get("quality_reason"),
            "non_validating": row.get("non_validating", False),
        }
        for row in displayed_rows
    ]

    observable_node = None
    if target_node:
        observable_node = {
            "node_public_key": target_node.get("node_public_key"),
            "validator_public_key": public_key,
            "domain": validator.domain,
            "ip": target_node.get("ip"),
            "port": target_node.get("port") or PEER_PORT_DEFAULT,
            "provider": target_node.get("provider"),
            "asn": target_node.get("asn"),
            "country": target_node.get("country"),
            "server_version": _display_version(target_node.get("server_version")),
            "latency_ms": target_node.get("latency_ms"),
            "agreement_24h": target_node.get("agreement_24h"),
            "quality_rating": target_node.get("quality_rating"),
            "quality_reason": target_node.get("quality_reason"),
            "non_validating": False,
        }

    return {
        "public_key": public_key,
        "domain": validator.domain,
        "mode": mode,
        "mode_banner": mode_banner,
        "json_report_url": f"/api/peers/{public_key}",
        "disclaimer": PEER_DISCLAIMER,
        "observable_node": observable_node,
        "summary": {
            "total_nodes_analyzed": len(node_records),
            "current_peer_count": len(peer_records) if mode == "adjacency" else 0,
            "good_count": good_count,
            "acceptable_count": acceptable_count,
            "risky_count": risky_count,
            "projected_composite_score": projected_composite,
            "projected_rank": projected_rank,
            "projected_rank_delta": rank_delta,
        },
        "risk_findings": risk_findings,
        "table_title": table_title,
        "node_rows": normalized_rows,
        "add_recommendations": add_recommendations,
        "drop_recommendations": drop_recommendations,
    }
