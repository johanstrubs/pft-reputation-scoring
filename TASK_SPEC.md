# Task Spec: Multi-Validator Reputation Scoring Engine and API

## SETUP INSTRUCTIONS (DO FIRST)

```bash
# 1. Create directory structure under ~/documents
mkdir -p ~/documents/pft-reputation-scoring

# 2. Initialize git repo
cd ~/documents/pft-reputation-scoring
git init
gh repo create pft-reputation-scoring --public --source=. --remote=origin

# 3. All work happens in this directory
cd ~/documents/pft-reputation-scoring
```

---

## PROJECT OVERVIEW

Build a **reputation scoring service** that queries multiple Post Fiat validators' public RPC endpoints, aggregates performance metrics, computes a composite reputation score per validator, and exposes the results via a public REST API. This is the foundational scoring layer referenced in the official `dynamic-unl-scoring` project (Phase 1, Milestones 1.2-1.3 scope — data collection + scoring methodology only, NOT the full dUNL pipeline).

### What this IS:
- A standalone FastAPI service that polls validators and the VHS API on a schedule
- A documented scoring methodology with weighted metrics
- A public JSON API at `https://pftoligarchy.com/api/scores` (or subdomain)
- Historical score storage in SQLite (lightweight, no Postgres dependency needed for this scope)
- Deployable as a Docker container on the existing Hetzner server (87.99.136.128)

### What this is NOT:
- NOT the full dUNL scoring pipeline (no LLM scoring, no VL generation, no IPFS publishing)
- NOT a replacement for the official `dynamic-unl-scoring` service
- NOT modifying the validator node itself

---

## ARCHITECTURE

```
┌────────────────────────────────────────────────────────┐
│              pft-reputation-scoring                     │
│                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │  Collector    │───►│  Scorer      │───►│  FastAPI │ │
│  │  (scheduled)  │    │  (compute)   │    │  (serve) │ │
│  └──────┬───────┘    └──────────────┘    └────┬─────┘ │
│         │                                      │       │
│  ┌──────▼───────┐                       ┌─────▼─────┐ │
│  │  Data Sources │                       │  SQLite   │ │
│  │  - VHS API    │                       │  (scores  │ │
│  │  - Node RPCs  │                       │   + hist) │ │
│  │  - ASN lookup │                       └───────────┘ │
│  └──────────────┘                                      │
└────────────────────────────────────────────────────────┘
         │                                      │
         ▼                                      ▼
  External APIs                          Nginx reverse proxy
  (VHS, validator RPCs)                  https://pftoligarchy.com/api/
```

---

## DATA SOURCES & APIs

### 1. VHS (Validator History Service) API
The VHS is a fork of Ripple's validator-history-service, running on the Post Fiat network. It provides aggregated validator performance data.

**Base URL:** Discover by checking these (test which responds):
- `https://vhs.testnet.postfiat.org` 
- `http://rpc.testnet.postfiat.org:3000`

**Key endpoints:**
```bash
# List all known validators with agreement scores
GET /v1/network/validators
# Response: array of validator objects with:
#   - master_key (public key like "nHD3sPmh...")
#   - signing_key
#   - domain
#   - agreement_1h, agreement_24h, agreement_30d (objects with {missed, total, score, incomplete})
#   - current_index (latest ledger index validated)
#   - partial (boolean)
#   - unl (boolean - on the UNL or not)
#   - server_version
#   - last_datetime

# Single validator detail
GET /v1/network/validator/{publicKey}

# Network topology — peer connections, latencies, IPs
GET /v1/network/topology/nodes
# Response: array of node objects with:
#   - node_public_key
#   - ip (IP address)
#   - port
#   - version
#   - uptime (seconds)
#   - inbound_count, outbound_count
#   - latency (avg ms to VHS observer)
#   - io_latency

# Amendment voting
GET /v1/network/amendments/vote/main
```

### 2. Direct Node RPC (XRPL-style JSON-RPC)
Each validator runs an XRPL-compatible RPC. Query them for real-time state.

**Your node:** `http://127.0.0.1:5005` (admin, localhost only)  
**Public WebSocket:** `wss://snakespartan.pftoligarchy.com:6005` (but we need HTTP RPC)

**Known network peers with public RPC (test which are accessible on port 51234 or 5005):**
- `rpc.testnet.postfiat.org`
- `96.30.199.55`
- `144.202.24.188`
- `45.32.222.206`
- `144.202.31.74`
- `144.202.16.242`

**Key RPC methods:**
```bash
# Server info (state, version, peers, uptime, ledger info)
curl -s -X POST http://127.0.0.1:5005 \
  -H 'Content-Type: application/json' \
  -d '{"method":"server_info","params":[{}]}'

# Peer list with latencies
curl -s -X POST http://127.0.0.1:5005 \
  -H 'Content-Type: application/json' \
  -d '{"method":"peers","params":[{}]}'

# Current validators
curl -s -X POST http://127.0.0.1:5005 \
  -H 'Content-Type: application/json' \
  -d '{"method":"validators","params":[{}]}'

# Latest validated ledger
curl -s -X POST http://127.0.0.1:5005 \
  -H 'Content-Type: application/json' \
  -d '{"method":"ledger","params":[{"ledger_index":"validated"}]}'
```

### 3. ASN Lookup (for geographic/ISP diversity)
Use a free public ASN lookup to identify ISP/hosting provider per validator IP.

**Options (use whichever is simplest):**
- `pyasn` Python library with a downloaded RIB file
- Team Cymru IP-to-ASN DNS lookup: `dig +short $(echo IP | rev).origin.asn.cymru.com TXT`
- `ipinfo.io` free API (limited to 50K/month)

---

## SCORING METHODOLOGY

Design a composite score (0-100) per validator using weighted metrics. The methodology must be documented in a `METHODOLOGY.md` file.

### Metrics to collect and score:

| Metric | Source | Weight | Scoring Logic |
|--------|--------|--------|--------------|
| **Agreement 1h** | VHS `/validators` | 10% | Linear: 1.0 = full marks, <0.8 = 0 |
| **Agreement 24h** | VHS `/validators` | 15% | Linear: 1.0 = full marks, <0.8 = 0 |
| **Agreement 30d** | VHS `/validators` | 20% | Linear: 1.0 = full marks, <0.8 = 0 |
| **Uptime** | VHS `/topology/nodes` uptime field | 15% | Normalize against max observed uptime in cohort |
| **Latency** | VHS `/topology/nodes` latency field + direct RPC timing | 10% | Lower is better. <50ms = full marks, >500ms = 0, linear between |
| **Peer Count** | VHS `/topology/nodes` or direct RPC `peers` | 10% | Normalize: >=10 peers = full marks, <3 = 0 |
| **Server Version** | VHS `/validators` server_version | 10% | Latest version = full marks, 1 behind = 80%, 2+ behind = 50% |
| **ISP/Geographic Diversity** | ASN lookup on validator IP | 10% | Penalty if >30% of validators share the same ASN. Reward unique ASNs. |

### Score computation:
```
composite_score = (
    agreement_1h_score * 0.10 +
    agreement_24h_score * 0.15 +
    agreement_30d_score * 0.20 +
    uptime_score * 0.15 +
    latency_score * 0.10 +
    peer_count_score * 0.10 +
    version_score * 0.10 +
    diversity_score * 0.10
) * 100
```

Each sub-score is normalized to 0.0-1.0 before weighting.

**Important design notes from the official roadmap:**
- Give **low weight to observer-dependent metrics** (latency, peer count) relative to objective metrics (agreement scores, uptime, server version). VHS observes from a single vantage point.
- Diversity scoring should consider ASN concentration (how many validators share the same AS number).
- Agreement scores are the most important — they directly measure consensus participation quality.

---

## IMPLEMENTATION STEPS

### Step 1: Project skeleton
```
pft-reputation-scoring/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app entry point
│   ├── config.py             # Settings via pydantic-settings / env vars
│   ├── database.py           # SQLite setup + models
│   ├── collector.py          # Data collection from VHS + RPC + ASN
│   ├── scorer.py             # Scoring methodology implementation
│   ├── scheduler.py          # Background scheduled polling
│   └── models.py             # Pydantic models for API responses
├── tests/
│   ├── test_collector.py
│   ├── test_scorer.py
│   └── test_api.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── METHODOLOGY.md            # Human-readable scoring methodology doc
├── README.md
├── .env.example
└── .gitignore
```

### Step 2: Data Collector (`app/collector.py`)

Build a `DataCollector` class that:

1. **Fetches VHS validator list** — `GET /v1/network/validators`
   - Extract: `master_key`, `agreement_1h.score`, `agreement_24h.score`, `agreement_30d.score`, `server_version`, `unl`, `domain`
   - Handle missing fields gracefully (some validators may not have all data)

2. **Fetches VHS topology** — `GET /v1/network/topology/nodes`  
   - Extract: `node_public_key`, `ip`, `latency`, `uptime`, `inbound_count + outbound_count` (= peer count)
   - Match topology nodes to validators by public key

3. **Queries at least 3 validator RPC endpoints directly** for `server_info`
   - Time the RPC request (this gives direct latency measurement from your vantage point)
   - Extract: `server_state`, `complete_ledgers`, `peers`, `uptime`, `validated_ledger.seq`, `validated_ledger.age`
   - **Must include your own node** at `http://127.0.0.1:5005`
   - Try other known peers — wrap each in try/except with timeout (some may not have public HTTP RPC)

4. **Performs ASN lookup** on each validator IP found in topology
   - Get: ASN number, ISP name, country code
   - Cache results (ASN data doesn't change often)

5. Returns a unified `ValidatorSnapshot` dict/model per validator combining all sources.

**Error handling:** If VHS is down, log error and skip that collection round. If individual RPC fails, mark that validator's direct metrics as null. Never crash the service.

**Collection interval:** Every 5 minutes by default (configurable via `POLL_INTERVAL_SECONDS` env var).

### Step 3: Scorer (`app/scorer.py`)

Build a `ReputationScorer` class that:

1. Takes a list of `ValidatorSnapshot` objects
2. Computes each sub-score (0.0-1.0) per the methodology table above
3. Computes the weighted composite score (0-100)
4. Returns a list of `ValidatorScore` objects with: public_key, composite_score, all individual metric values, all individual sub-scores, timestamp

**Diversity scoring detail:**
- Count how many validators share each ASN
- If a validator's ASN has >30% of all validators → diversity penalty (e.g., score = 0.5)
- If a validator's ASN is unique → full diversity score (1.0)
- Scale linearly between

**Version scoring detail:**
- Determine the "latest" version as the mode (most common) version across all validators
- Exact match = 1.0, one minor version behind = 0.8, anything else = 0.5

### Step 4: Database (`app/database.py`)

Use **SQLite** (file: `data/scores.db`) with two tables:

```sql
CREATE TABLE IF NOT EXISTS scoring_rounds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,          -- ISO 8601
    validator_count INTEGER NOT NULL,
    avg_score REAL,
    min_score REAL,
    max_score REAL
);

CREATE TABLE IF NOT EXISTS validator_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id INTEGER NOT NULL,
    public_key TEXT NOT NULL,
    domain TEXT,
    composite_score REAL NOT NULL,
    agreement_1h REAL,
    agreement_24h REAL,
    agreement_30d REAL,
    uptime_seconds INTEGER,
    latency_ms REAL,
    peer_count INTEGER,
    server_version TEXT,
    server_state TEXT,
    asn INTEGER,
    isp TEXT,
    country TEXT,
    -- Sub-scores (0.0-1.0)
    agreement_1h_score REAL,
    agreement_24h_score REAL,
    agreement_30d_score REAL,
    uptime_score REAL,
    latency_score REAL,
    peer_count_score REAL,
    version_score REAL,
    diversity_score REAL,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES scoring_rounds(id)
);

CREATE INDEX IF NOT EXISTS idx_validator_scores_pubkey ON validator_scores(public_key);
CREATE INDEX IF NOT EXISTS idx_validator_scores_round ON validator_scores(round_id);
```

### Step 5: FastAPI Endpoints (`app/main.py`)

```python
# REQUIRED ENDPOINTS:

# Health check
GET /health
# Returns: {"status": "ok", "timestamp": "...", "last_scoring_round": "..."}

# Current scores for all validators (MAIN ENDPOINT — this is what ODV verification checks)
GET /api/scores
# Returns:
# {
#   "round_id": 42,
#   "timestamp": "2026-03-17T12:00:00Z",
#   "methodology_version": "1.0.0",
#   "validator_count": 30,
#   "validators": [
#     {
#       "public_key": "nHD3sPmh...",
#       "domain": "snakespartan.pftoligarchy.com",
#       "composite_score": 87.5,
#       "metrics": {
#         "agreement_1h": 0.98,
#         "agreement_24h": 0.97,
#         "agreement_30d": 0.95,
#         "uptime_seconds": 864000,
#         "latency_ms": 45.2,
#         "peer_count": 13,
#         "server_version": "2.4.0",
#         "server_state": "proposing",
#         "asn": 24940,
#         "isp": "Hetzner Online GmbH",
#         "country": "DE"
#       },
#       "sub_scores": {
#         "agreement_1h": 0.98,
#         "agreement_24h": 0.97,
#         "agreement_30d": 0.95,
#         "uptime": 0.85,
#         "latency": 0.91,
#         "peer_count": 1.0,
#         "version": 1.0,
#         "diversity": 0.8
#       },
#       "last_updated": "2026-03-17T12:00:00Z"
#     }
#   ]
# }

# Single validator detail
GET /api/scores/{public_key}
# Returns: single validator object from above + last 24h of historical scores

# Scoring history (last N rounds)
GET /api/scores/history?limit=10
# Returns: list of round summaries with avg/min/max scores

# Methodology documentation
GET /api/methodology
# Returns: the scoring methodology as JSON (weights, thresholds, etc.)
```

**CORS:** Enable CORS for all origins (this is a public API).  
**No authentication required** — the ODV verification explicitly states "endpoint must load without login or authentication."

### Step 6: Scheduler (`app/scheduler.py`)

Use `asyncio` background task or `APScheduler` to:
1. Run `DataCollector.collect()` every `POLL_INTERVAL_SECONDS` (default 300 = 5 min)
2. Run `ReputationScorer.score()` on the collected data
3. Store results in SQLite via `Database`
4. Log each round: timestamp, validator count, avg score

On startup, run one immediate collection + scoring round.

### Step 7: Docker + Deployment

**Dockerfile:**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /app/data
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**docker-compose.yml:**
```yaml
version: "3.8"
services:
  scoring:
    build: .
    container_name: pft-reputation-scoring
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data    # Persist SQLite DB
    env_file:
      - .env
    network_mode: host       # Needed to reach localhost:5005 (validator RPC)
```

**IMPORTANT: Use `network_mode: host`** so the container can reach the validator's admin RPC at `127.0.0.1:5005`. Alternatively, use `--add-host=host.docker.internal:host-gateway` and point to `host.docker.internal:5005`.

**Nginx config addition** (add to existing `/etc/nginx/sites-available/default`):

```nginx
# Add inside the existing server block for pftoligarchy.com or dashboard.pftoligarchy.com
location /api/ {
    proxy_pass http://127.0.0.1:8000/api/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}

location /health {
    proxy_pass http://127.0.0.1:8000/health;
}
```

Or create a new subdomain — `scores.pftoligarchy.com` — with its own server block.

---

## CONFIGURATION (.env.example)

```bash
# VHS API base URL (discover which works)
VHS_BASE_URL=http://rpc.testnet.postfiat.org:3000

# Local validator RPC
LOCAL_NODE_RPC=http://127.0.0.1:5005

# Additional validator RPCs to query (comma-separated)
# These are attempted but failures are non-fatal
EXTRA_NODE_RPCS=http://96.30.199.55:51234,http://144.202.24.188:51234

# Polling interval in seconds
POLL_INTERVAL_SECONDS=300

# SQLite database path
DATABASE_PATH=data/scores.db

# API host/port
API_HOST=0.0.0.0
API_PORT=8000

# Scoring methodology version
METHODOLOGY_VERSION=1.0.0

# Logging
LOG_LEVEL=INFO
```

---

## REQUIREMENTS.txt

```
fastapi>=0.109.0
uvicorn[standard]>=0.27.0
httpx>=0.26.0
pydantic>=2.5.0
pydantic-settings>=2.1.0
aiosqlite>=0.19.0
apscheduler>=3.10.0
```

---

## VERIFICATION CRITERIA (from ODV task)

The ODV verification will check:

> Provide a single publicly accessible API URL (e.g., `https://pftoligarchy.com/api/scores` or a subdomain) that returns a JSON response containing reputation scores for at least 3 Post Fiat validators, with each entry showing:
> - the validator public key
> - individual metric values (latency, uptime percentage, average ledger interval, peer count)
> - the composite reputation score
> - a timestamp of last measurement
>
> The endpoint must load without login or authentication.

**So the absolute minimum acceptance criteria are:**
1. `GET https://<your-domain>/api/scores` returns valid JSON
2. Response contains at least 3 validators
3. Each validator entry has: public_key, individual metrics, composite score, timestamp
4. No auth required
5. Scores update on subsequent polls (not static)

---

## REFERENCE REPOS (read for context, don't depend on them)

- **Official scoring pipeline:** https://github.com/postfiatorg/dynamic-unl-scoring
  - Phase 1 roadmap: `docs/CurrentRoadmap.md` — our work is a subset of Milestones 1.2 + 1.3
  - Uses FastAPI + PostgreSQL + Modal LLM — our version is lighter (SQLite, no LLM, just weighted metrics)
  - Their `scripts/fetch_vhs_data.py` shows how to query VHS
  - Their `scripts/lookup_asn.py` shows ASN lookup approach

- **VHS (Validator History Service):** https://github.com/postfiatorg/validator-history-service
  - Fork of Ripple's VHS
  - API on port 3000: `/v1/network/validators`, `/v1/network/topology/nodes`, etc.
  - The `.env.testnet` file shows testnet configuration

- **postfiatd (node software):** https://github.com/postfiatorg/postfiatd
  - Fork of XRPL rippled
  - RPC methods are XRPL-compatible: `server_info`, `peers`, `validators`, `ledger`

- **Health sidecar (already running on server):** https://github.com/jollydinger/pftvalidatorsuite
  - `healthcheck/monitor.py` shows how to query local RPC
  - Already running at `~/validator/sidecar/` as Docker container `pft-healthcheck`
  - Uses `--network container:postfiatd` to share network namespace
  - Logs to `~/validator/sidecar/logs/healthcheck/`

---

## SERVER CONTEXT

- **OS:** Ubuntu 22.04
- **IP:** 87.99.136.128
- **Docker:** Already installed and running
- **Existing containers:** `postfiatd` (validator node), `pft-healthcheck` (health sidecar)
- **Nginx:** Already configured with HTTPS for `dashboard.pftoligarchy.com` and `snakespartan.pftoligarchy.com`
- **Validator public key:** `nHD3sPmhNtVXTXrZAGhsfHRhxhzFXKHNNxemTtGFr69SRd9q88dZ`
- **Node admin RPC:** `http://127.0.0.1:5005` (localhost only)
- **Network ID:** 2025 (Post Fiat testnet)
- **Python 3:** Available on host

---

## IMPLEMENTATION ORDER (suggested)

1. Set up repo + skeleton (5 min)
2. Build `collector.py` — start with VHS API calls, verify data comes back
3. Build `scorer.py` — implement the weighted scoring
4. Build `database.py` — SQLite storage
5. Build `main.py` — FastAPI endpoints
6. Build `scheduler.py` — periodic collection
7. Write `METHODOLOGY.md`
8. Write `Dockerfile` + `docker-compose.yml`
9. Test locally, then deploy on server
10. Configure Nginx reverse proxy
11. Verify public endpoint works

---

## KEY PITFALLS TO WATCH FOR

1. **VHS API URL:** You'll need to discover the correct base URL. Try `http://rpc.testnet.postfiat.org:3000/v1/health` first. If that doesn't work, the VHS might be at a different port or hostname.

2. **RPC port confusion:** The validator admin RPC is HTTP on port 5005. The public port 6005 is WebSocket-only (not HTTP). Don't try to curl port 6005.

3. **Network mode for Docker:** If using `docker-compose`, the scoring container needs to reach `127.0.0.1:5005` on the host. Either use `network_mode: host` or `--network container:postfiatd`.

4. **VHS data structure:** The VHS API returns XRPL-style responses. Agreement scores are nested objects like `{"missed": 0, "total": 100, "score": "1.0000", "incomplete": false}` — the `score` field is a string, parse it to float.

5. **Validator public keys format:** Keys start with `nH...` (node public key format). Some APIs may use the master key vs signing key — use the master key consistently.

6. **Topology vs Validators:** The topology endpoint returns *nodes* (which includes non-validators). The validators endpoint returns *validators* only. You need to correlate them by public key.

7. **ASN lookup failures:** Some validator IPs may be behind NAT or not visible in topology. Handle gracefully — set diversity score to neutral (0.5) if ASN lookup fails.

8. **SQLite in Docker with volumes:** Mount `./data:/app/data` so the SQLite DB persists across container restarts.

---

## ALIGNMENT NOTE

This project aligns with the official Post Fiat dUNL roadmap as a "complementary" scoring layer. The official pipeline (Phase 1 of `dynamic-unl-scoring`) uses an LLM to do the scoring. Our approach uses deterministic weighted metrics — simpler, faster, and immediately useful as a "Source of Truth" the core team can eventually point at. The official roadmap's Milestone 1.2 (Data Collection Pipeline) and Milestone 1.3 (LLM Scoring) are the closest analogs to what we're building here, minus the LLM component.
