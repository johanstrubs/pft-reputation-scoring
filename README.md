# PFT Reputation Scoring Engine

A standalone reputation scoring service for Post Fiat network validators. Polls multiple data sources, computes weighted composite scores, and exposes results via a public REST API.

## Quick Start

```bash
# Clone and setup
git clone https://github.com/<your-user>/pft-reputation-scoring.git
cd pft-reputation-scoring
cp .env.example .env
# Edit .env with your configuration

# Run with Docker
docker compose up -d

# Or run locally
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check with last scoring round timestamp |
| `GET /api/scores` | Current scores for all validators |
| `GET /api/scores/{public_key}` | Single validator detail + 24h history |
| `GET /api/scores/history?limit=10` | Scoring round summaries |
| `GET /api/methodology` | Scoring methodology as JSON |

## Architecture

```
Collector (scheduled) → Scorer (compute) → FastAPI (serve)
     │                                          │
  Data Sources                              SQLite DB
  - VHS API                                (scores + history)
  - Node RPCs
  - ASN lookup
```

## Scoring Methodology

See [METHODOLOGY.md](METHODOLOGY.md) for full details. Composite score (0-100) based on:

- **Agreement scores** (45%): 1h, 24h, 30d consensus participation
- **Uptime** (15%): Normalized against cohort max
- **Latency** (10%): Lower is better, 50ms-500ms scale
- **Peer count** (10%): 3-10 peers scale
- **Server version** (10%): Compared to network mode version
- **ISP diversity** (10%): ASN concentration penalty

## Configuration

See [.env.example](.env.example) for all configuration options.

## Deployment

Uses Docker with `network_mode: host` to reach the local validator RPC at `127.0.0.1:5005`. Add Nginx reverse proxy for HTTPS:

```nginx
location /api/ {
    proxy_pass http://127.0.0.1:8000/api/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}
```
