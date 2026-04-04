import asyncio
import logging
import socket
import time
import subprocess
from datetime import datetime, timezone

import httpx

from app.config import settings
from app.models import ValidatorSnapshot, ValidatorMetrics

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 10.0
PROBE_TIMEOUT = 5.0
PROBE_PORTS = [51234, 5005, 5006]
# How often to re-probe topology nodes for key mapping (seconds)
MAPPING_CACHE_TTL = 3600  # 1 hour
CRAWL_TIMEOUT = 8.0
CRAWL_CONCURRENCY = 15


class NodeValidatorMap:
    """Persistent cache mapping node_public_key -> master_key."""

    def __init__(self):
        # node_public_key -> {master_key, signing_key, source, discovered_at}
        self._cache: dict[str, dict] = {}
        self._last_probe_time: float = 0

    def get_master_key(self, node_public_key: str) -> str | None:
        entry = self._cache.get(node_public_key)
        return entry["master_key"] if entry else None

    def add(self, node_public_key: str, master_key: str, signing_key: str | None = None, source: str = "unknown"):
        self._cache[node_public_key] = {
            "master_key": master_key,
            "signing_key": signing_key,
            "source": source,
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }

    def needs_probe(self) -> bool:
        return (time.monotonic() - self._last_probe_time) > MAPPING_CACHE_TTL

    def mark_probed(self):
        self._last_probe_time = time.monotonic()

    @property
    def size(self) -> int:
        return len(self._cache)


class DataCollector:
    def __init__(self):
        self._asn_cache: dict[str, dict] = {}
        self._node_map = NodeValidatorMap()

    async def collect(self, subscriber_mappings: dict[str, str] | None = None) -> tuple[list[ValidatorSnapshot], list[dict]]:
        """Collect validator data. Returns (snapshots, poll_results).

        poll_results is a list of {public_key, successful, latency_ms} dicts
        tracking whether each validator was reachable this round.

        subscriber_mappings: verified node_key -> master_key pairs from the
        subscription system, inserted between crawl and DNS in the priority chain.
        """
        snapshots: dict[str, ValidatorSnapshot] = {}
        poll_results: list[dict] = []

        # Fetch VHS data and direct RPC data concurrently
        vhs_validators, vhs_topology, rpc_results = await asyncio.gather(
            self._fetch_vhs_validators(),
            self._fetch_vhs_topology(),
            self._query_rpc_endpoints(),
            return_exceptions=True,
        )

        # Process VHS validators — build lookup tables
        signing_to_master: dict[str, str] = {}
        if isinstance(vhs_validators, list):
            for v in vhs_validators:
                master = v.get("master_key") or v.get("validation_public_key")
                signing = v.get("signing_key")
                if not master:
                    continue
                a1h_score, a1h_total = self._parse_agreement(v.get("agreement_1h"))
                a24h_score, a24h_total = self._parse_agreement(v.get("agreement_24h"))
                a30d_score, a30d_total = self._parse_agreement(v.get("agreement_30day") or v.get("agreement_30d"))
                snapshots[master] = ValidatorSnapshot(
                    public_key=master,
                    domain=v.get("domain"),
                    unl=bool(v.get("unl")),
                    metrics=ValidatorMetrics(
                        agreement_1h=a1h_score,
                        agreement_1h_total=a1h_total,
                        agreement_24h=a24h_score,
                        agreement_24h_total=a24h_total,
                        agreement_30d=a30d_score,
                        agreement_30d_total=a30d_total,
                        server_version=v.get("server_version"),
                    ),
                )
                if signing:
                    signing_to_master[signing] = master
            logger.info("Collected %d validators from VHS", len(snapshots))
        else:
            logger.error("Failed to fetch VHS validators: %s", vhs_validators)

        # Build node-to-validator mapping if needed
        topology_nodes: list[dict] = []
        if isinstance(vhs_topology, list):
            topology_nodes = vhs_topology
        else:
            logger.error("Failed to fetch VHS topology: %s", vhs_topology)

        # Correlation Step 0 (primary): Crawl network peers for pubkey_validator
        if self._node_map.needs_probe():
            seed_ips = settings.crawl_seed_list
            if not seed_ips and topology_nodes:
                seed_ips = [n["ip"] for n in topology_nodes if n.get("ip")]
            if seed_ips:
                await self._crawl_network(seed_ips, snapshots)

        # Correlation Step 1: Use RPC results to map pubkey_node -> pubkey_validator -> master_key
        if isinstance(rpc_results, list):
            for result in rpc_results:
                if not result:
                    continue
                node_key = result.get("pubkey_node")
                val_key = result.get("pubkey_validator")
                if node_key and val_key and val_key != "none":
                    # val_key is a signing key — look up master key
                    master = signing_to_master.get(val_key) or (val_key if val_key in snapshots else None)
                    if master and not self._node_map.get_master_key(node_key):
                        self._node_map.add(node_key, master, val_key, source="rpc_config")
                        logger.debug("Mapped %s -> %s via configured RPC", node_key[:12], master[:12])

        # Correlation Step 2: Probe topology node IPs for server_info (periodically)
        if topology_nodes and self._node_map.needs_probe():
            await self._probe_topology_nodes(topology_nodes, signing_to_master, snapshots)
            self._node_map.mark_probed()

        # Correlation Step 3: Subscriber-provided verified node keys (fills gaps from crawl/RPC)
        if subscriber_mappings:
            loaded = 0
            for node_key, master_key in subscriber_mappings.items():
                if not self._node_map.get_master_key(node_key):
                    self._node_map.add(node_key, master_key, source="subscriber")
                    loaded += 1
            if loaded:
                logger.info("Loaded %d subscriber node key mappings (filling gaps)", loaded)

        # Correlation Step 4: DNS resolution of validator domains -> match topology IPs
        if topology_nodes:
            await self._resolve_domains_to_topology(snapshots, topology_nodes)

        # Correlation Step 5: Manual key mappings (last-resort fallback, only fills gaps)
        for node_key, master_key in settings.key_mapping_pairs.items():
            if not self._node_map.get_master_key(node_key):
                self._node_map.add(node_key, master_key, source="manual_config")

        # Now enrich validators with topology data using the mapping
        ip_by_master: dict[str, str] = {}
        enriched_count = 0
        for node in topology_nodes:
            node_key = node.get("node_public_key")
            if not node_key:
                continue
            ip = node.get("ip")

            master = self._node_map.get_master_key(node_key)
            if master and master in snapshots:
                s = snapshots[master]
                s.metrics.uptime_seconds = node.get("uptime")
                s.metrics.latency_ms = node.get("io_latency_ms")
                inbound = node.get("inbound_count") or 0
                outbound = node.get("outbound_count") or 0
                s.metrics.peer_count = inbound + outbound
                s.metrics.server_state = node.get("server_state")
                # Compute average ledger interval from complete_ledgers + uptime
                s.metrics.avg_ledger_interval = self._compute_ledger_interval(
                    node.get("complete_ledgers"), node.get("uptime")
                )
                if node.get("country_code"):
                    s.metrics.country = node["country_code"]
                if ip:
                    ip_by_master[master] = ip
                enriched_count += 1

        logger.info(
            "Enriched %d/%d validators with topology data (mapping cache: %d entries)",
            enriched_count, len(snapshots), self._node_map.size,
        )

        # Enrich with direct RPC results (latency, peer count, uptime, server_state, ledger interval)
        if isinstance(rpc_results, list):
            for result in rpc_results:
                if not result:
                    continue
                node_key = result.get("pubkey_node")
                if not node_key:
                    continue
                master = self._node_map.get_master_key(node_key)
                if master and master in snapshots:
                    s = snapshots[master]
                else:
                    # RPC node not mapped to a VHS validator — create an entry
                    # so every directly-queried node appears in output with full metrics
                    if node_key not in snapshots:
                        snapshots[node_key] = ValidatorSnapshot(
                            public_key=node_key,
                            metrics=ValidatorMetrics(),
                        )
                    s = snapshots[node_key]

                if result.get("latency_ms") is not None:
                    if s.metrics.latency_ms is not None:
                        s.metrics.latency_ms = (s.metrics.latency_ms + result["latency_ms"]) / 2
                    else:
                        s.metrics.latency_ms = result["latency_ms"]
                if result.get("peers") is not None:
                    s.metrics.peer_count = result["peers"]
                if result.get("uptime") is not None:
                    s.metrics.uptime_seconds = result["uptime"]
                if result.get("server_state"):
                    s.metrics.server_state = result["server_state"]
                if result.get("server_version"):
                    s.metrics.server_version = result["server_version"]
                if result.get("server_domain") and not s.domain:
                    s.domain = result["server_domain"]
                # RPC validated_ledger.age is the most accurate ledger interval
                if result.get("validated_ledger_age") is not None:
                    s.metrics.avg_ledger_interval = float(result["validated_ledger_age"])
                # Compute uptime-based ledger interval from RPC complete_ledgers
                if s.metrics.avg_ledger_interval is None and result.get("complete_ledgers") and result.get("uptime"):
                    s.metrics.avg_ledger_interval = self._compute_ledger_interval(
                        result["complete_ledgers"], result["uptime"]
                    )

        # ASN lookup for validators with known IPs
        await self._enrich_asn(snapshots, ip_by_master)

        # Build poll results: every VHS validator was "seen" (VHS responded),
        # and RPC-queried nodes have direct reachability data
        vhs_keys = set()
        if isinstance(vhs_validators, list):
            for v in vhs_validators:
                master = v.get("master_key") or v.get("validation_public_key")
                if master:
                    vhs_keys.add(master)
                    poll_results.append({
                        "public_key": master,
                        "successful": True,  # VHS reported this validator
                        "latency_ms": None,
                    })

        # RPC-queried nodes: successful if we got a response
        if isinstance(rpc_results, list):
            rpc_seen = set()
            for result in rpc_results:
                if not result:
                    continue
                node_key = result.get("pubkey_node")
                if not node_key:
                    continue
                master = self._node_map.get_master_key(node_key)
                key = master or node_key
                if key not in vhs_keys and key not in rpc_seen:
                    rpc_seen.add(key)
                    poll_results.append({
                        "public_key": key,
                        "successful": True,
                        "latency_ms": result.get("latency_ms"),
                    })

        return list(snapshots.values()), poll_results

    async def _probe_topology_nodes(
        self,
        topology_nodes: list[dict],
        signing_to_master: dict[str, str],
        snapshots: dict[str, ValidatorSnapshot],
    ):
        """Probe topology node IPs via RPC to discover node_key -> validator_key mappings."""
        # Collect IPs to probe (skip already-mapped nodes)
        to_probe: list[tuple[str, str]] = []  # (node_key, ip)
        for node in topology_nodes:
            node_key = node.get("node_public_key")
            ip = node.get("ip")
            if node_key and ip and not self._node_map.get_master_key(node_key):
                to_probe.append((node_key, ip))

        if not to_probe:
            return

        logger.info("Probing %d topology node IPs for key mapping...", len(to_probe))

        async def probe_one(node_key: str, ip: str) -> tuple[str, str | None]:
            """Try RPC on multiple ports, return (node_key, master_key or None)."""
            for port in PROBE_PORTS:
                try:
                    url = f"http://{ip}:{port}"
                    async with httpx.AsyncClient(timeout=PROBE_TIMEOUT) as client:
                        resp = await client.post(
                            url,
                            json={"method": "server_info", "params": [{}]},
                            headers={"Content-Type": "application/json"},
                        )
                        resp.raise_for_status()
                    info = resp.json().get("result", {}).get("info", {})
                    resp_node_key = info.get("pubkey_node")
                    val_key = info.get("pubkey_validator")

                    if val_key and val_key != "none":
                        master = signing_to_master.get(val_key) or (val_key if val_key in snapshots else None)
                        if master:
                            # Use the node_key from the response if available (more reliable)
                            actual_node_key = resp_node_key or node_key
                            return actual_node_key, master
                except Exception:
                    continue
            return node_key, None

        # Run probes concurrently with a semaphore to limit parallelism
        sem = asyncio.Semaphore(10)

        async def limited_probe(nk, ip):
            async with sem:
                return await probe_one(nk, ip)

        results = await asyncio.gather(
            *(limited_probe(nk, ip) for nk, ip in to_probe),
            return_exceptions=True,
        )

        discovered = 0
        for result in results:
            if isinstance(result, tuple):
                nk, master = result
                if master:
                    self._node_map.add(nk, master, source="rpc_probe")
                    discovered += 1

        logger.info("RPC probing discovered %d new node->validator mappings", discovered)

    async def _resolve_domains_to_topology(
        self,
        snapshots: dict[str, ValidatorSnapshot],
        topology_nodes: list[dict],
    ):
        """Resolve validator domains to IPs and match against topology node IPs."""
        # Build IP -> node_key lookup from topology
        ip_to_node_keys: dict[str, list[str]] = {}
        for node in topology_nodes:
            ip = node.get("ip")
            nk = node.get("node_public_key")
            if ip and nk:
                ip_to_node_keys.setdefault(ip, []).append(nk)

        # Collect domains to resolve (only for validators not already mapped)
        already_mapped_masters = set(
            self._node_map.get_master_key(nk)
            for node in topology_nodes
            if (nk := node.get("node_public_key")) and self._node_map.get_master_key(nk)
        )

        domains_to_resolve: dict[str, list[str]] = {}  # domain -> [master_keys]
        for master, snap in snapshots.items():
            if snap.domain and master not in already_mapped_masters:
                domains_to_resolve.setdefault(snap.domain, []).append(master)

        if not domains_to_resolve:
            return

        discovered = 0
        for domain, master_keys in domains_to_resolve.items():
            # Skip ambiguous domains (multiple validators share same domain)
            if len(master_keys) > 1:
                logger.debug("Skipping ambiguous domain %s (%d validators)", domain, len(master_keys))
                continue

            master = master_keys[0]
            try:
                resolved_ips = await asyncio.get_event_loop().run_in_executor(
                    None, self._resolve_domain, domain
                )
            except Exception:
                continue

            for ip in resolved_ips:
                node_keys = ip_to_node_keys.get(ip, [])
                if len(node_keys) == 1:
                    # Unambiguous match
                    nk = node_keys[0]
                    if not self._node_map.get_master_key(nk):
                        self._node_map.add(nk, master, source="dns_resolution")
                        discovered += 1
                        break

        if discovered:
            logger.info("DNS resolution discovered %d new node->validator mappings", discovered)

    async def _crawl_network(
        self,
        seed_ips: list[str],
        snapshots: dict[str, ValidatorSnapshot],
    ):
        """Crawl /crawl endpoints to discover node_key -> validator_key mappings.

        Each node's /crawl response includes its own pubkey_node and pubkey_validator
        in the server section (postfiatd v1.0.0+). Peer entries provide IPs for
        recursive discovery.
        """
        port = settings.crawl_peer_port
        visited: set[str] = set()
        to_crawl: set[str] = set(seed_ips)
        discovered = 0

        # Three passes: seeds, then discovered peers for broader coverage
        for pass_num in range(3):
            batch = [ip for ip in to_crawl if ip not in visited]
            if not batch:
                break

            logger.info("Crawl pass %d: querying %d peers on port %d", pass_num + 1, len(batch), port)

            sem = asyncio.Semaphore(CRAWL_CONCURRENCY)

            async def crawl_one(ip: str) -> dict | None:
                async with sem:
                    try:
                        async with httpx.AsyncClient(timeout=CRAWL_TIMEOUT, verify=False) as client:
                            resp = await client.get(f"https://{ip}:{port}/crawl")
                            resp.raise_for_status()
                        return resp.json()
                    except Exception:
                        return None

            results = await asyncio.gather(
                *(crawl_one(ip) for ip in batch),
                return_exceptions=True,
            )

            for ip, result in zip(batch, results):
                visited.add(ip)
                if not isinstance(result, dict):
                    continue

                # Extract this node's own mapping from server section
                server = result.get("server", {})
                node_key = server.get("pubkey_node")
                val_key = server.get("pubkey_validator")
                if node_key and val_key and val_key != "none":
                    # pubkey_validator from /crawl is the master key directly (nH...)
                    if val_key in snapshots and not self._node_map.get_master_key(node_key):
                        self._node_map.add(node_key, val_key, source="crawl")
                        discovered += 1
                        logger.debug("Crawl mapped %s -> %s from %s", node_key[:12], val_key[:12], ip)

                # Collect peer IPs for next pass
                for peer in result.get("overlay", {}).get("active", []):
                    peer_ip = peer.get("ip")
                    if peer_ip and peer_ip not in visited:
                        to_crawl.add(peer_ip)

        logger.info("Crawl discovery complete: %d new mappings from %d nodes queried", discovered, len(visited))

    @staticmethod
    def _compute_ledger_interval(complete_ledgers: str | None, uptime: int | None) -> float | None:
        """Compute average seconds per ledger from complete_ledgers range and uptime.

        Only reliable when the node has a large enough ledger range relative to uptime.
        For nodes with a small local range (recently synced), the result will be
        inflated. We require at least 10000 ledgers for a meaningful average.
        """
        if not complete_ledgers or not uptime or uptime <= 0:
            return None
        try:
            parts = complete_ledgers.split("-")
            if len(parts) == 2:
                start, end = int(parts[0]), int(parts[1])
                ledger_count = end - start
                if ledger_count >= 10000:
                    return round(uptime / ledger_count, 3)
        except (ValueError, IndexError):
            pass
        return None

    @staticmethod
    def _resolve_domain(domain: str) -> list[str]:
        """Resolve a domain to a list of IP addresses."""
        try:
            results = socket.getaddrinfo(domain, None, socket.AF_INET)
            return list(set(r[4][0] for r in results))
        except socket.gaierror:
            return []

    async def _fetch_vhs_validators(self) -> list[dict]:
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                resp = await client.get(f"{settings.vhs_base_url}/v1/network/validators")
                resp.raise_for_status()
                data = resp.json()
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
            validated_ledger = info.get("validated_ledger", {})
            return {
                "url": url,
                "latency_ms": round(elapsed_ms, 2),
                "pubkey_node": info.get("pubkey_node"),
                "pubkey_validator": info.get("pubkey_validator"),
                "server_state": info.get("server_state"),
                "server_domain": info.get("server_domain"),
                "peers": info.get("peers"),
                "uptime": info.get("uptime"),
                "server_version": info.get("build_version"),
                "validated_ledger_seq": validated_ledger.get("seq"),
                "validated_ledger_age": validated_ledger.get("age"),
                "complete_ledgers": info.get("complete_ledgers"),
            }
        except Exception as e:
            logger.warning("RPC query to %s failed: %s", url, e)
            return None

    async def _enrich_asn(self, snapshots: dict[str, ValidatorSnapshot], ip_by_master: dict[str, str]):
        for master, snap in snapshots.items():
            ip = ip_by_master.get(master)
            if not ip:
                continue
            asn_info = await self._lookup_asn(ip)
            if asn_info:
                snap.metrics.asn = asn_info.get("asn")
                snap.metrics.isp = asn_info.get("isp")
                if not snap.metrics.country:
                    snap.metrics.country = asn_info.get("country")

    async def _lookup_asn(self, ip: str) -> dict | None:
        if ip in self._asn_cache:
            return self._asn_cache[ip]

        try:
            reversed_ip = ".".join(reversed(ip.split(".")))
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._cymru_lookup, reversed_ip
            )
            if result:
                self._asn_cache[ip] = result
                return result
        except Exception as e:
            logger.warning("ASN lookup failed for %s: %s", ip, e)

        # Fallback: ipinfo.io
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
            result = subprocess.run(
                ["dig", "+short", f"{reversed_ip}.origin.asn.cymru.com", "TXT"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
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
    def _parse_agreement(agreement_obj) -> tuple[float | None, int | None]:
        """Parse a VHS agreement object, returning (score, total_validations).

        The total is needed to distinguish "no data in window" (total=0)
        from "bad performance" (total>0, score<0.8). The VHS 1h window
        has been known to return total=0 for all validators when the
        aggregation is broken.
        """
        if agreement_obj is None:
            return None, None
        if isinstance(agreement_obj, (int, float)):
            return float(agreement_obj), None
        if isinstance(agreement_obj, dict):
            total = agreement_obj.get("total")
            score = agreement_obj.get("score")
            if score is not None:
                try:
                    return float(score), total
                except (ValueError, TypeError):
                    return None, total
            return None, total
        if isinstance(agreement_obj, str):
            try:
                return float(agreement_obj), None
            except ValueError:
                return None, None
        return None, None
