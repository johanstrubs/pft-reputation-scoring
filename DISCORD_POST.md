# Discord Post — Request for Validator Node Key Mappings

---

**Post Fiat Reputation Scoring API — Now Live + Request for Validator Operators**

Hey everyone — I've built and deployed a public reputation scoring API for Post Fiat validators. It's live at:

**https://dashboard.pftoligarchy.com/api/scores**

It scores all 42 validators on a 0-100 composite scale based on agreement rates, uptime, latency, peer count, server version, and ISP diversity. Scores update every 5 minutes. No auth required.

Endpoints:
- `/api/scores` — all validators ranked by score
- `/api/scores/{public_key}` — single validator detail + 24h history
- `/api/methodology` — scoring weights and thresholds explained

GitHub: https://github.com/johanstrubs/pft-reputation-scoring

---

**I need your help to make the scores more accurate.**

Right now I can only get full topology data (uptime, latency, peer count, geographic location) for about 8 out of 42 validators. The issue is that the VHS tracks topology nodes and validators using completely different key types, and this fork strips `pubkey_validator` from non-admin RPC responses — so there's no way to automatically link a topology node to its validator identity from outside.

**If you're a validator operator, please share your node-to-validator key mapping.** You can find it by running this from your server:

```bash
docker exec <your_postfiatd_container> curl -s -X POST http://127.0.0.1:5005 \
  -H 'Content-Type: application/json' \
  -d '{"method":"server_info","params":[{}]}' | python3 -c "
import sys,json
d=json.load(sys.stdin)
info=d['result']['info']
print('Node key:', info.get('pubkey_node'))
print('Validator key:', info.get('pubkey_validator'))
"
```

Then just post the two keys here or DM me. Example output:
```
Node key: n94KDVS7g8nY3CKh6KJUvnGo3hJcUzkJdojmREY813YhabHkkvAT
Validator key: nHD3sPmhNtVXTXrZAGhsfHRhxhzFXKHNNxemTtGFr69SRd9q88dZ
```

These are public keys (not secret) — sharing them just lets me link your validator's consensus performance data with your node's network metrics for a more complete score.

Thanks!
— snakespartan
