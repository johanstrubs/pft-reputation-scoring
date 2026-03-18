# Discord Posts

## Post 1 — Public Announcement (for validators channel)

---

**Post Fiat Validator Reputation Scoring API — Now Live**

Built and deployed a public reputation scoring API for Post Fiat validators as part of the ODV scoring task. Scores all 42 active validators on a 0-100 composite scale.

**Live API:** https://dashboard.pftoligarchy.com/api/scores
**Source code:** https://github.com/johanstrubs/pft-reputation-scoring
**Methodology:** https://dashboard.pftoligarchy.com/api/methodology

Scoring is based on weighted metrics from the VHS API:
- Agreement rates (1h / 24h / 30d) — 45% weight
- Uptime — 15%
- Latency — 10%
- Peer count — 10%
- Server version — 10%
- ISP/geographic diversity — 10%

Scores auto-update every 5 minutes. No auth required.

Other endpoints:
- `/api/scores/{public_key}` — single validator detail + history
- `/api/scores/history` — scoring round summaries
- `/api/methodology` — full scoring weights and thresholds

This is meant to complement the official dUNL scoring pipeline — a lightweight, deterministic alternative to the LLM-based approach in `dynamic-unl-scoring`. Feedback welcome. Code is public if anyone wants to review the methodology or suggest improvements.

— snakespartan

---

## Post 2 — Direct Message to PFT Dev Team

---

Hey — quick question about the VHS API and validator/topology key correlation.

I've been building a reputation scoring service (https://dashboard.pftoligarchy.com/api/scores) that pulls from the VHS validators and topology endpoints. It's working well for agreement scores, but I'm hitting a wall correlating topology nodes with their validator identities.

The issue: `/v1/network/validators` uses `master_key`/`signing_key`, and `/v1/network/topology/nodes` uses `node_public_key` — completely different key spaces with no overlap. This means I can't reliably attach topology metrics (uptime, latency, peer count, geo) to specific validators.

In standard XRPL, `server_info` returns both `pubkey_node` and `pubkey_validator`, which bridges the gap. But in the PF fork, `pubkey_validator` is stripped from non-admin RPC responses (I'm guessing intentionally for security — linking IPs to validator identities enables targeted attacks).

A few questions:

1. **Does the VHS database internally correlate node keys to validator keys?** If so, would it be possible to expose that mapping via an API endpoint? Even something like `/v1/network/validator/{master_key}/node` would solve it.

2. **Is stripping `pubkey_validator` from non-admin RPC intentional?** If so, is there a recommended way to get this mapping without compromising the security boundary?

3. **Any other approach you'd suggest?** I looked at manifests, crawl endpoints, and the `dynamic-unl-scoring` scripts — the official pipeline seems to pass both datasets to the LLM separately without correlating them programmatically either.

Right now I'm using DNS resolution on validator domains to match ~7 validators, but most domains point to CDNs/GitHub Pages rather than the validator's actual IP.

The scoring service is designed to align with the dUNL roadmap (Milestones 1.2-1.3 scope). If there's a better way to approach the topology correlation, I'd rather do it the right way than hack around the security model.

Source: https://github.com/johanstrubs/pft-reputation-scoring

Thanks!
— snakespartan
