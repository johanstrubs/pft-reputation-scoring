# PFT Reputation Scoring — Key Learnings & Hurdles

## Project Summary

Built and deployed a standalone reputation scoring service for Post Fiat validators. The service polls the VHS API and direct RPC endpoints every 5 minutes, computes a weighted composite score (0-100) per validator, and serves results via a public REST API.

**Live endpoint:** https://dashboard.pftoligarchy.com/api/scores
**GitHub:** https://github.com/johanstrubs/pft-reputation-scoring

---

## Hurdles Overcome

### 1. VHS API Discovery

The task spec listed `http://rpc.testnet.postfiat.org:3000` as the VHS base URL. This endpoint is unreachable. The actual working URL is `https://vhs.testnet.postfiat.org`. Discovered by testing both URLs from the server.

### 2. VHS Field Name Differences

The VHS API uses `agreement_30day` (not `agreement_30d` as documented in the task spec). The agreement scores are also nested objects with a `score` field that's a string, not a float — e.g., `{"missed": 0, "total": 26874, "score": "1.00000", "incomplete": false}`. Required careful parsing with type checking.

### 3. The Node-to-Validator Key Correlation Problem (Biggest Hurdle)

This was the most significant technical challenge. XRPL (and therefore Post Fiat) uses three completely separate key types:

- **Node key** (`pubkey_node` / `node_public_key`): Identifies a peer-to-peer network node. Used in topology data. Starts with `n9...`.
- **Signing key** (`signing_key`): Used to sign validation messages. Rotatable. Starts with `n9...`.
- **Master key** (`master_key` / `validation_public_key`): Long-lived validator identity. Starts with `nH...`.

The VHS stores validators (keyed by master/signing key) and topology nodes (keyed by node key) in completely separate tables with **no join between them**. Zero overlap exists between any of these key sets.

**What we tried:**
- **Direct key matching**: Zero overlap. Node keys are a fundamentally different key space from validator keys.
- **RPC probing**: In standard XRPL, `server_info` returns both `pubkey_node` and `pubkey_validator`, which bridges the gap. However, the Post Fiat fork **strips `pubkey_validator` from all non-admin RPC responses**. The field only appears when querying from inside the validator's own Docker container on localhost. Every external RPC query (even to nodes that respond on port 5005) returns `pubkey_validator: missing`.
- **Manifest lookups**: The `manifest` RPC maps master_key ↔ signing_key, but NOT to node_key. These are separate key hierarchies.
- **VHS API exploration**: Checked `/v1/network/topology/validators`, `/v1/network/topology/nodes`, `/v1/network/manifests`, `/v1/network/validator_report` — none provide the node-to-validator mapping.
- **Crawl endpoint**: Previously unavailable (`/crawl` returned empty or 404). **Now available as of postfiatd v1.0.0** — see Section "Crawl Endpoint Resolution" below.
- **Peers RPC**: Returns `public_key` (node key type) and IP address, but requires admin access. Our Docker deployment uses port mapping which strips admin fields.

**What works:**
- **DNS resolution** (7/42 mapped): Resolve validator domains to IPs, match against topology node IPs. Only works for validators with unique domains that resolve directly to their server IP (not behind Cloudflare/CDN/GitHub Pages).
- **Manual key mappings** (1/42): Configured via `MANUAL_KEY_MAPPINGS` env var for validators whose node-to-master key relationship is known out-of-band.

**Current coverage:** 8/42 validators enriched with topology data (uptime, latency, peer count, geographic data). The remaining 34 get accurate agreement scores (45% of the composite weight) but neutral scores (0.5) for topology-dependent metrics.

### 4. Docker Networking for Admin RPC

The `postfiatd` container exposes port 5005 via Docker port mapping. This provides non-admin access. Admin-level access (which includes `pubkey_validator` and the `peers` command) is only available from within the container's own localhost. Our scoring container runs with `network_mode: host`, which connects to the Docker-mapped port — not the container's internal network.

### 5. Domain Resolution Limitations

Of 20 validators with domains:
- 7 resolve directly to their server IP (matched successfully)
- 5 share `postfiat.org` which resolves to GitHub Pages IPs (185.199.x.x)
- 8 resolve to Cloudflare or CDN IPs (104.21.x, 172.67.x)

Validators host websites at their domains but run their nodes at different IPs.

---

## Architecture Decisions

1. **SQLite over PostgreSQL**: Lightweight, zero-config, sufficient for the scoring use case. Persisted via Docker volume mount.
2. **Neutral scores for missing data**: When topology data is unavailable, validators get 0.5 (neutral) rather than 0.0 (penalty). This avoids punishing validators for the observability gap.
3. **Agreement scores weighted highest (45%)**: These are the most objective and reliable metrics — they directly measure consensus participation quality and come from VHS regardless of key correlation.
4. **Observer-dependent metrics weighted lower (20% for latency + peer count)**: VHS observes from a single vantage point; these metrics are less reliable.
5. **In-memory mapping cache with 1-hour TTL**: Avoids re-probing 43 IPs every 5 minutes. DNS resolution and manual mappings are re-applied each round.

---

## What Would Improve Coverage

The single biggest improvement would be **validators sharing their node-to-master key mapping**. Each validator operator can find this by running:

```bash
docker exec <container_name> curl -s -X POST http://127.0.0.1:5005 \
  -H 'Content-Type: application/json' \
  -d '{"method":"server_info","params":[{}]}' | python3 -c "
import sys,json
d=json.load(sys.stdin)
info=d['result']['info']
print('Node key:', info.get('pubkey_node'))
print('Validator key:', info.get('pubkey_validator'))
"
```

With these mappings, every validator could be fully enriched with topology data.

---

## Crawl Endpoint Resolution (postfiatd v1.0.0)

**Date:** 2026-03-27 (v1.0.0 announced by Post Fiat core team)

The biggest hurdle above — the node-to-validator key correlation problem — now has a programmatic solution. The `postfiatd v1.0.0` release added `pubkey_validator` to the `/crawl` endpoint response, specifically to enable the Dynamic UNL scoring pipeline.

### Key Facts

- **Endpoint**: `https://<node-ip>:2559/crawl` (peer port, HTTPS with self-signed cert)
- **Auth**: None required — publicly accessible, no admin credentials needed
- **Response structure**:
  - `server.pubkey_node`: The node's own node key (`n9...`)
  - `server.pubkey_validator`: The node's own validator/master key (`nH...`) — **this is the new field**
  - `server.server_domain`: The node's configured domain
  - `overlay.active[]`: List of connected peers with IP, port, base64 public key, version, uptime
- **Confirmed working**: Tested against our own validator (`87.99.136.128:2559/crawl`) — returns both `pubkey_node` and `pubkey_validator` in the `server` section

### How It Enables Full Enrichment

Each node you crawl gives you its own `pubkey_node` → `pubkey_validator` mapping plus the IPs of all its peers. By recursively crawling peers starting from known seed nodes, you can build a complete mapping table for every v1.0.0 node on the network.

From a 3-hop recursive crawl starting at our node, 40 nodes are reachable, yielding 20 validator mappings (all confirmed as direct master_key matches in VHS). The remaining unreachable nodes are either on older `postfiatd-3.0.0` (which does not expose `pubkey_validator`) or have port 2559 firewalled.

### Dynamic UNL Context

The Post Fiat team is building toward **Dynamic UNL** in three phases:
1. **Phase 1 (in progress):** Foundation builds an automated scoring pipeline using an open-weight LLM to score validators and generate a signed validator list. Validators don't need to do anything different.
2. **Phase 2 (later):** Validators run a GPU sidecar to independently reproduce scoring and publish results on-chain via commit-reveal.
3. **Phase 3 (later):** Foundation steps back; converged validator results become the authoritative UNL.

The v1.0.0 `/crawl` change is a prerequisite for Phase 1 — without it, the scoring pipeline can't resolve which nodes are validators or map them to IPs for geographic diversity scoring. Validators not on v1.0.0 show as `"ip: null"` in the scoring data.

### Recommended Enrichment Priority Order

1. **Crawl endpoint** (primary) — automatic, no operator action needed for v1.0.0 nodes
2. **Subscriber-provided node keys** (verified) — for operators who submitted and verified via `/alerts`
3. **DNS resolution** — for validators with unique domains resolving to their server IP
4. **Manual key mappings** — for known out-of-band correlations
