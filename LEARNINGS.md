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
- **Crawl endpoint**: Not available in this fork (`/crawl` returns empty or 404).
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
