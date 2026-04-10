import asyncio
import socket
import tomllib
from collections import Counter

import httpx

from app.models import ValidatorScore

HEALTHY_SERVER_STATES = {"proposing", "full"}
DOMAIN_ATTESTATION_URL = "https://github.com/johanstrubs/pft-reputation-scoring/blob/master/DomainAttestation.md"
WELL_KNOWN_TIMEOUT = 5.0


def _latest_version(scores: list[ValidatorScore]) -> str | None:
    versions = [score.metrics.server_version for score in scores if score.metrics.server_version]
    if not versions:
        return None
    return Counter(versions).most_common(1)[0][0]


def _check(name: str, category: str, status: str, detected_value: str, expected_value: str, remediation: str | None, timestamp: str) -> dict:
    return {
        "name": name,
        "category": category,
        "status": status,
        "detected_value": detected_value,
        "expected_value": expected_value,
        "remediation": remediation,
        "source_timestamp": timestamp,
    }


async def _resolve_domain_ips(domain: str) -> list[str]:
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(domain, 443, type=socket.SOCK_STREAM)
    return sorted({info[4][0] for info in infos if info[4] and info[4][0]})


async def _fetch_well_known(domain: str) -> tuple[str | None, str | None]:
    url = f"https://{domain}/.well-known/postfiat.toml"
    try:
        async with httpx.AsyncClient(timeout=WELL_KNOWN_TIMEOUT, follow_redirects=True) as client:
            response = await client.get(url)
            if response.status_code != 200:
                return None, f"Endpoint returned HTTP {response.status_code}"
            return response.text, None
    except Exception as exc:
        return None, str(exc)


def _validator_in_toml(doc_text: str, public_key: str) -> bool:
    try:
        parsed = tomllib.loads(doc_text)
    except Exception:
        return False
    validators = parsed.get("VALIDATORS", [])
    if not isinstance(validators, list):
        return False
    for entry in validators:
        if isinstance(entry, dict):
            for key in ("public_key", "validator_public_key", "pubkey"):
                if entry.get(key) == public_key:
                    return True
    return False


def _attestation_template(public_key: str) -> str:
    return (
        "Host `/.well-known/postfiat.toml` over HTTPS and include your validator key. "
        "Template:\n\n"
        "[[VALIDATORS]]\n"
        f'public_key = "{public_key}"\n\n'
        f"See {DOMAIN_ATTESTATION_URL} for hosting instructions."
    )


async def build_readiness_report(round_id: int, timestamp: str, scores: list[ValidatorScore], public_key: str) -> dict:
    if not scores:
        raise ValueError("No scoring data available yet")

    validator = next((score for score in scores if score.public_key == public_key), None)
    if not validator:
        raise KeyError(public_key)

    metrics = validator.metrics
    checks: list[dict] = []
    latest_version = _latest_version(scores)
    known_topology_ips = {score.metrics.node_ip for score in scores if score.metrics.node_ip}

    if latest_version and metrics.server_version == latest_version:
        checks.append(_check("Version parity", "configuration", "pass", metrics.server_version or "unknown", latest_version, None, timestamp))
    else:
        checks.append(_check(
            "Version parity",
            "configuration",
            "fail",
            metrics.server_version or "unknown",
            latest_version or "most common cohort version",
            "Upgrade to the current cohort majority version. Illustrative example: `docker compose pull && docker compose up -d`.",
            timestamp,
        ))

    peer_count = metrics.peer_count
    peer_status = "pass" if peer_count is not None and peer_count >= 5 else "fail"
    checks.append(_check(
        "Peer floor",
        "configuration",
        peer_status,
        str(peer_count) if peer_count is not None else "unknown",
        ">= 5 peers",
        None if peer_status == "pass" else "Check validator firewall and routing for port 2559. Illustrative examples: `ufw allow 2559/tcp` and confirm cloud security-group ingress is open.",
        timestamp,
    ))

    state_status = "pass" if metrics.server_state in HEALTHY_SERVER_STATES else "fail"
    checks.append(_check(
        "Server state",
        "operational",
        state_status,
        metrics.server_state or "unknown",
        "proposing or full",
        None if state_status == "pass" else "Inspect validator logs and process health until the node reports `proposing` or `full` consistently.",
        timestamp,
    ))

    agreement_status = "pass" if metrics.agreement_24h is not None and metrics.agreement_24h >= 0.95 else "warn"
    checks.append(_check(
        "24h agreement",
        "operational",
        agreement_status,
        f"{metrics.agreement_24h * 100:.1f}%" if metrics.agreement_24h is not None else "unknown",
        ">= 95.0%",
        None if agreement_status == "pass" else "Agreement is degraded. Review clock sync, network reachability, and validator logs before the issue cascades into score deterioration.",
        timestamp,
    ))

    freshness_value = metrics.validated_ledger_age if metrics.validated_ledger_age is not None else metrics.avg_ledger_interval
    freshness_source = "validated_ledger_age" if metrics.validated_ledger_age is not None else "avg_ledger_interval"
    if freshness_value is None:
        checks.append(_check(
            "Ledger freshness",
            "operational",
            "warn",
            "no stored freshness signal",
            "freshness signal <= 10s",
            "No stored freshness metric was available for this validator in the latest round, so readiness cannot confirm ledger freshness yet.",
            timestamp,
        ))
    else:
        freshness_status = "pass" if float(freshness_value) <= 10 else "warn"
        checks.append(_check(
            "Ledger freshness",
            "operational",
            freshness_status,
            f"{float(freshness_value):.1f}s via {freshness_source}",
            "<= 10.0s",
            None if freshness_status == "pass" else "Freshness looks stale in stored data. Check node health and consider a controlled restart only after reviewing sync/log status.",
            timestamp,
        ))

    if validator.domain:
        checks.append(_check("Domain configured", "attestation", "pass", validator.domain, "configured domain in validator data", None, timestamp))
    else:
        checks.append(_check(
            "Domain configured",
            "attestation",
            "warn",
            "missing",
            f"configured domain in validator data; see {DOMAIN_ATTESTATION_URL}",
            f"Configure a validator domain and publish domain attestation guidance from {DOMAIN_ATTESTATION_URL}.",
            timestamp,
        ))

    if not validator.domain:
        checks.append(_check("Domain DNS match", "attestation", "warn", "skipped: no domain configured", "domain resolves to the validator's known topology IP", "Add a domain first before DNS readiness can be verified.", timestamp))
        checks.append(_check("Well-known attestation", "attestation", "warn", "skipped: no domain configured", "https://<domain>/.well-known/postfiat.toml includes the validator key", _attestation_template(public_key), timestamp))
    else:
        try:
            dns_ips = await _resolve_domain_ips(validator.domain)
            if metrics.node_ip and metrics.node_ip in dns_ips:
                checks.append(_check("Domain DNS match", "attestation", "pass", ", ".join(dns_ips), f"include validator topology IP {metrics.node_ip}", None, timestamp))
            elif any(ip in known_topology_ips for ip in dns_ips):
                checks.append(_check(
                    "Domain DNS match",
                    "attestation",
                    "warn",
                    ", ".join(dns_ips),
                    f"include validator topology IP {metrics.node_ip or 'known validator node IP'}",
                    "Domain resolves to a known topology IP, but not the stored IP for this validator. This may be a proxy/CDN setup; verify it still fronts the correct validator host.",
                    timestamp,
                ))
            else:
                checks.append(_check(
                    "Domain DNS match",
                    "attestation",
                    "warn",
                    ", ".join(dns_ips) if dns_ips else "no A/AAAA records",
                    f"include validator topology IP {metrics.node_ip or 'known topology IP'}",
                    "Update DNS so the domain resolves to the validator service or its intended proxy target.",
                    timestamp,
                ))
        except Exception as exc:
            checks.append(_check(
                "Domain DNS match",
                "attestation",
                "warn",
                f"DNS lookup failed: {exc}",
                "domain resolves successfully",
                "DNS lookup timed out or failed. Re-check the domain records and try again.",
                timestamp,
            ))

        doc_text, fetch_error = await _fetch_well_known(validator.domain)
        if fetch_error:
            checks.append(_check(
                "Well-known attestation",
                "attestation",
                "warn",
                f"unreachable: {fetch_error}",
                "HTTPS /.well-known/postfiat.toml reachable and contains the validator key",
                _attestation_template(public_key),
                timestamp,
            ))
        elif _validator_in_toml(doc_text or "", public_key):
            checks.append(_check(
                "Well-known attestation",
                "attestation",
                "pass",
                "matching validator key found",
                f"[[VALIDATORS]] contains {public_key}",
                None,
                timestamp,
            ))
        else:
            checks.append(_check(
                "Well-known attestation",
                "attestation",
                "warn",
                "file present but validator key missing or mismatched",
                f"[[VALIDATORS]] contains {public_key}",
                _attestation_template(public_key),
                timestamp,
            ))

    if any(check["status"] == "fail" for check in checks):
        overall_status = "not_ready"
        status_summary = "Not Ready"
    elif any(check["status"] == "warn" for check in checks):
        overall_status = "needs_attention"
        status_summary = "Needs Attention"
    else:
        overall_status = "ready"
        status_summary = "Ready"

    return {
        "public_key": validator.public_key,
        "domain": validator.domain,
        "round_id": round_id,
        "timestamp": timestamp,
        "overall_status": overall_status,
        "status_summary": status_summary,
        "json_report_url": f"/api/readiness/{validator.public_key}",
        "checks": checks,
    }
