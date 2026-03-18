# Reputation Scoring Methodology v1.0.0

## Overview

This document describes the composite reputation scoring methodology used by the PFT Reputation Scoring Engine. Each Post Fiat validator receives a score from 0 to 100 based on weighted metrics collected from the Validator History Service (VHS), direct RPC queries, and ASN lookups.

## Metrics and Weights

| Metric | Weight | Source | Scoring Logic |
|--------|--------|--------|---------------|
| Agreement 1h | 10% | VHS `/v1/network/validators` | Linear: 1.0 = full marks, <0.8 = 0 |
| Agreement 24h | 15% | VHS `/v1/network/validators` | Linear: 1.0 = full marks, <0.8 = 0 |
| Agreement 30d | 20% | VHS `/v1/network/validators` | Linear: 1.0 = full marks, <0.8 = 0 |
| Uptime | 15% | VHS `/v1/network/topology/nodes` | Normalized against max observed uptime in cohort |
| Latency | 10% | VHS topology + direct RPC timing | <=50ms = 1.0, >=500ms = 0.0, linear between |
| Peer Count | 10% | VHS topology or direct RPC `peers` | >=10 = 1.0, <3 = 0.0, linear between |
| Server Version | 10% | VHS `/v1/network/validators` | Latest (mode) = 1.0, one minor behind = 0.8, older = 0.5 |
| ISP/Geographic Diversity | 10% | ASN lookup on validator IP | Penalty if >30% of validators share same ASN |

## Score Computation

Each sub-score is normalized to 0.0–1.0 before weighting:

```
composite_score = (
    agreement_1h_score  * 0.10 +
    agreement_24h_score * 0.15 +
    agreement_30d_score * 0.20 +
    uptime_score        * 0.15 +
    latency_score       * 0.10 +
    peer_count_score    * 0.10 +
    version_score       * 0.10 +
    diversity_score     * 0.10
) * 100
```

## Detailed Scoring Logic

### Agreement Scores (1h, 24h, 30d)
- These measure how often a validator's proposed validations agree with the network consensus.
- Score = 0 if agreement < 0.8 (validator is unreliable)
- Score scales linearly from 0.0 at 0.8 agreement to 1.0 at 1.0 agreement
- Agreement scores carry the highest combined weight (45%) as they directly measure consensus participation quality.

### Uptime
- Measured in seconds from the VHS topology endpoint or direct RPC.
- Normalized against the maximum observed uptime in the current validator cohort.
- A validator with the longest uptime gets 1.0; others scale proportionally.

### Latency
- Measured in milliseconds. When both VHS and direct RPC measurements exist, the average is used.
- <= 50ms: full score (1.0)
- >= 500ms: zero score (0.0)
- Between 50ms and 500ms: linear interpolation
- Given low weight (10%) because latency is observer-dependent — VHS measures from a single vantage point.

### Peer Count
- Total inbound + outbound peer connections.
- >= 10 peers: full score (1.0)
- < 3 peers: zero score (0.0)
- Between 3 and 10: linear interpolation
- Low weight (10%) as peer count varies by network topology and is observer-dependent.

### Server Version
- The "latest" version is determined as the most common (mode) version across all validators.
- Exact match with latest: 1.0
- One minor version behind: 0.8
- Two or more versions behind: 0.5
- Unknown version: 0.5 (neutral)

### ISP/Geographic Diversity
- Based on Autonomous System Number (ASN) of each validator's IP address.
- If a validator's ASN hosts >30% of all validators: penalty applied (score drops toward 0)
- If a validator's ASN is unique or low-concentration: higher score (up to 1.0)
- Unknown ASN: 0.5 (neutral)
- This metric encourages network decentralization across different hosting providers and geographies.

## Design Principles

1. **Objective metrics weighted higher**: Agreement scores (45% combined) are the most reliable indicators of validator quality.
2. **Observer-dependent metrics weighted lower**: Latency and peer count (20% combined) depend on the observation vantage point.
3. **Graceful degradation**: Missing data results in neutral scores (0.5) rather than penalties, to avoid punishing validators with incomplete observability.
4. **Diversity incentive**: The ASN diversity metric encourages geographic and infrastructure decentralization.

## Data Collection

- **Polling interval**: Every 5 minutes (configurable)
- **Sources**: VHS API, direct validator RPC endpoints, Team Cymru / ipinfo.io ASN lookups
- **Storage**: SQLite database with historical scoring rounds
