import asyncio
import logging
import time
import subprocess

import httpx

from app.config import settings
from app.models import ValidatorSnapshot, ValidatorMetrics

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 10.0


class DataCollector:
    def __init__(self):
        self._asn_cache: dict[str, dict] = {}

    async def collect(self) -> list[ValidatorSnapshot]:
        snapshots: dict[str, ValidatorSnapshot] = {}

        # Fetch VHS data and direct RPC data concurrently
        vhs_validators, vhs_topology, rpc_results = await asyncio.gather(
            self._fetch_vhs_validators(),
            self._fetch_vhs_topology(),
            self._query_rpc_endpoints(),
            return_exceptions=True,
        )

        # Process VHS validators
        if isinstance(vhs_validators, list):
            for v in vhs_validators:
                key = v.get("master_key") or v.get("signing_key")
                if not key:
                    continue
                snapshots[key] = ValidatorSnapshot(
                    public_key=key,
                    domain=v.get("domain"),
                    unl=v.get("unl", False),
                    metrics=ValidatorMetrics(
                        agreement_1h=self._parse_agreement(v.get("agreement_1h")),
                        agreement_24h=self._parse_agreement(v.get("agreement_24h")),
                        agreement_30d=self._parse_agreement(v.get("agreement_30d")),
                        server_version=v.get("server_version"),
                    ),
                )
            logger.info("Collected %d validators from VHS", len(snapshots))
        else:
            logger.error("Failed to fetch VHS validators: %s", vhs_validators)

        # Enrich with topology data (uptime, latency, peer count, IP)
        topology_ips: dict[str, str] = {}
        if isinstance(vhs_topology, list):
            for node in vhs_topology:
                node_key = node.get("node_public_key")
                if not node_key:
                    continue
                ip = node.get("ip")
                if ip:
                    topology_ips[node_key] = ip

                if node_key in snapshots:
                    s = snapshots[node_key]
                    s.metrics.uptime_seconds = node.get("uptime")
                    s.metrics.latency_ms = node.get("latency")
                    inbound = node.get("inbound_count") or 0
                    outbound = node.get("outbound_count") or 0
                    s.metrics.peer_count = inbound + outbound
            logger.info("Enriched with topology data for %d nodes", len(vhs_topology))
        else:
            logger.error("Failed to fetch VHS topology: %s", vhs_topology)

        # Enrich with direct RPC results
        if isinstance(rpc_results, list):
            for result in rpc_results:
                if not result:
                    continue
                # Try to match RPC results to known validators
                # RPC server_info doesn't always give us the validator public key,
                # so we use the results to supplement existing data
                pubkey = result.get("pubkey_node")
                if pubkey and pubkey in snapshots:
                    s = snapshots[pubkey]
                    if result.get("latency_ms") is not None:
                        # Average VHS latency with direct measurement if both exist
                        if s.metrics.latency_ms is not None:
                            s.metrics.latency_ms = (s.metrics.latency_ms + result["latency_ms"]) / 2
                        else:
                            s.metrics.latency_ms = result["latency_ms"]
                    if result.get("peers") is not None and s.metrics.peer_count is None:
                        s.metrics.peer_count = result["peers"]
                    if result.get("uptime") is not None and s.metrics.uptime_seconds is None:
                        s.metrics.uptime_seconds = result["uptime"]
                    if result.get("server_state"):
                        s.metrics.server_state = result["server_state"]
        else:
            logger.error("Failed RPC queries: %s", rpc_results)

        # ASN lookup for validators with known IPs
        await self._enrich_asn(snapshots, topology_ips)

        return list(snapshots.values())

    async def _fetch_vhs_validators(self) -> list[dict]:
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                resp = await client.get(f"{settings.vhs_base_url}/v1/network/validators")
                resp.raise_for_status()
                data = resp.json()
                # VHS may return list directly or nested
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    return data.get("validators", data.get("data", []))
                return []
        except Exception as e:
            logger.error("VHS validators fetch failed: %s", e)
            return []

    async def _fetch_vhs_topology(self) -> list[dict]:
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                resp = await client.get(f"{settings.vhs_base_url}/v1/network/topology/nodes")
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    return data.get("nodes", data.get("data", []))
                return []
        except Exception as e:
            logger.error("VHS topology fetch failed: %s", e)
            return []

    async def _query_rpc_endpoints(self) -> list[dict | None]:
        endpoints = [settings.local_node_rpc] + settings.extra_rpc_list
        tasks = [self._query_single_rpc(url) for url in endpoints]
        return await asyncio.gather(*tasks, return_exceptions=False)

    async def _query_single_rpc(self, url: str) -> dict | None:
        try:
            start = time.monotonic()
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                resp = await client.post(
                    url,
                    json={"method": "server_info", "params": [{}]},
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
            elapsed_ms = (time.monotonic() - start) * 1000
            data = resp.json()
            info = data.get("result", {}).get("info", {})
            return {
                "url": url,
                "latency_ms": round(elapsed_ms, 2),
                "pubkey_node": info.get("pubkey_node"),
                "server_state": info.get("server_state"),
                "peers": info.get("peers"),
                "uptime": info.get("uptime"),
                "server_version": info.get("build_version"),
                "validated_ledger_seq": info.get("validated_ledger", {}).get("seq"),
            }
        except Exception as e:
            logger.warning("RPC query to %s failed: %s", url, e)
            return None

    async def _enrich_asn(self, snapshots: dict[str, ValidatorSnapshot], topology_ips: dict[str, str]):
        for key, snap in snapshots.items():
            ip = topology_ips.get(key)
            if not ip:
                continue
            asn_info = await self._lookup_asn(ip)
            if asn_info:
                snap.metrics.asn = asn_info.get("asn")
                snap.metrics.isp = asn_info.get("isp")
                snap.metrics.country = asn_info.get("country")

    async def _lookup_asn(self, ip: str) -> dict | None:
        if ip in self._asn_cache:
            return self._asn_cache[ip]

        try:
            # Use Team Cymru DNS lookup
            reversed_ip = ".".join(reversed(ip.split(".")))
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._cymru_lookup, reversed_ip
            )
            if result:
                self._asn_cache[ip] = result
                return result
        except Exception as e:
            logger.warning("ASN lookup failed for %s: %s", ip, e)

        # Fallback: try ipinfo.io
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"https://ipinfo.io/{ip}/json")
                if resp.status_code == 200:
                    data = resp.json()
                    org = data.get("org", "")
                    asn = None
                    isp = org
                    if org.startswith("AS"):
                        parts = org.split(" ", 1)
                        try:
                            asn = int(parts[0][2:])
                        except ValueError:
                            pass
                        isp = parts[1] if len(parts) > 1 else org
                    result = {
                        "asn": asn,
                        "isp": isp,
                        "country": data.get("country"),
                    }
                    self._asn_cache[ip] = result
                    return result
        except Exception as e:
            logger.warning("ipinfo.io lookup failed for %s: %s", ip, e)

        return None

    def _cymru_lookup(self, reversed_ip: str) -> dict | None:
        try:
            import subprocess
            result = subprocess.run(
                ["dig", "+short", f"{reversed_ip}.origin.asn.cymru.com", "TXT"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                # Response format: "ASN | prefix | CC | registry | date"
                line = result.stdout.strip().strip('"')
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 3:
                    try:
                        asn = int(parts[0])
                    except ValueError:
                        asn = None
                    return {"asn": asn, "isp": None, "country": parts[2] if len(parts) > 2 else None}
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_agreement(agreement_obj) -> float | None:
        if agreement_obj is None:
            return None
        if isinstance(agreement_obj, (int, float)):
            return float(agreement_obj)
        if isinstance(agreement_obj, dict):
            score = agreement_obj.get("score")
            if score is not None:
                try:
                    return float(score)
                except (ValueError, TypeError):
                    return None
        if isinstance(agreement_obj, str):
            try:
                return float(agreement_obj)
            except ValueError:
                return None
        return None
