from __future__ import annotations

from app.config import settings
from app.scorer import WEIGHTS


SCHEMA_VERSION = "methodology-card.v1"


def _public_url(path: str) -> str:
    base = settings.public_base_url.rstrip("/")
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


def build_methodology_card() -> dict:
    return {
        "title": "Independent Validator Scoring Methodology Card",
        "schema_version": SCHEMA_VERSION,
        "methodology_version": settings.methodology_version,
        "urls": {
            "page": _public_url("/methodology-card"),
            "json": _public_url("/api/methodology-card"),
            "summary_json": _public_url("/api/methodology"),
        },
        "sections": [
            {
                "id": "purpose_and_intended_use",
                "title": "Purpose and Intended Use",
                "summary": (
                    "This scoring system is an independent operational measurement layer for Post Fiat validators. "
                    "It is designed to help operators understand observable validator performance, help contributors "
                    "inspect the scoring logic, and give the community a transparent reference point when discussing "
                    "network reliability."
                ),
                "fields": {
                    "system_role": "Independent measurement and transparency surface, not official validator selection.",
                    "primary_users": [
                        "Validator operators",
                        "Protocol contributors",
                        "Community members comparing public scoring outputs",
                    ],
                    "intended_uses": [
                        "Operator self-assessment",
                        "Public transparency into independent validator scoring",
                        "Calibration and comparison against other scoring systems, including the upcoming official dUNL pipeline",
                    ],
                    "not_intended_for": [
                        "Authoritative validator selection",
                        "Claiming that one disagreement proves another system is wrong",
                        "Replacing protocol governance or official dUNL policy",
                    ],
                },
            },
            {
                "id": "data_sources",
                "title": "Data Sources",
                "summary": (
                    "Each scoring round combines cohort-level validator data from VHS with direct observations from "
                    "crawl and RPC probes plus ASN/provider enrichment."
                ),
                "fields": {
                    "polling_cadence_seconds": settings.poll_interval_seconds,
                    "polling_cadence_note": "A scoring round runs immediately on service startup, then repeats on the configured interval.",
                    "sources": [
                        {
                            "source_id": "vhs_validators",
                            "name": "VHS validator list",
                            "kind": "aggregated network API",
                            "endpoints": [
                                f"{settings.vhs_base_url}/v1/network/validators",
                            ],
                            "freshness_expectation": {
                                "warning_after_seconds": 600,
                                "critical_after_seconds": 1800,
                            },
                            "used_for": [
                                "agreement_1h",
                                "agreement_24h",
                                "agreement_30d",
                                "server_version",
                                "validator identity and domain metadata",
                            ],
                        },
                        {
                            "source_id": "vhs_topology",
                            "name": "VHS topology",
                            "kind": "aggregated network API",
                            "endpoints": [
                                f"{settings.vhs_base_url}/v1/network/topology/nodes",
                            ],
                            "freshness_expectation": {
                                "warning_after_seconds": 600,
                                "critical_after_seconds": 1800,
                            },
                            "used_for": [
                                "uptime_seconds",
                                "latency_ms",
                                "peer_count",
                                "seed peer discovery for crawl",
                            ],
                        },
                        {
                            "source_id": "crawl_endpoint",
                            "name": "Peer /crawl endpoint",
                            "kind": "direct peer observation",
                            "endpoints": [
                                "https://<node-ip>:2559/crawl",
                            ],
                            "freshness_expectation": {
                                "warning_after_seconds": 900,
                                "critical_after_seconds": None,
                            },
                            "used_for": [
                                "node-to-validator mapping",
                                "peer topology enrichment",
                                "server.pubkey_validator correlation",
                            ],
                        },
                        {
                            "source_id": "local_rpc",
                            "name": "Direct RPC polling",
                            "kind": "direct validator RPC observation",
                            "endpoints": [
                                settings.local_node_rpc,
                                "Configured extra RPC endpoints when available",
                            ],
                            "freshness_expectation": {
                                "warning_rule": "warning if the latest collector run fails",
                                "critical_rule": "critical if local RPC fails for 2 consecutive collector runs",
                            },
                            "used_for": [
                                "poll_success_pct",
                                "direct latency observations",
                                "ledger freshness and server-state context",
                            ],
                        },
                        {
                            "source_id": "asn_lookup",
                            "name": "ASN/provider enrichment",
                            "kind": "third-party enrichment",
                            "endpoints": [
                                "https://ipinfo.io/<ip>/json",
                            ],
                            "freshness_expectation": {
                                "warning_after_seconds": 86400,
                                "critical_after_seconds": None,
                            },
                            "used_for": [
                                "diversity scoring",
                                "provider and country labeling",
                            ],
                        },
                    ],
                },
            },
            {
                "id": "metric_definitions_and_weights",
                "title": "Metric Definitions and Weights",
                "summary": (
                    "Scores are computed as weighted sub-scores on a 0.0 to 1.0 scale and then multiplied by 100 "
                    "to produce the composite score."
                ),
                "fields": {
                    "composite_formula": "weighted_sum(sub_scores) * 100",
                    "weights_sum": round(sum(WEIGHTS.values()), 2),
                    "metrics": [
                        {
                            "metric_id": "agreement_1h",
                            "name": "Agreement 1h",
                            "weight": WEIGHTS["agreement_1h"],
                            "source": "VHS /v1/network/validators",
                            "signal_class": "objective",
                            "normalization": "linear from 0.8 -> 0.0 to 1.0 -> 1.0",
                            "thresholds": {"min_for_nonzero": 0.8, "full_score": 1.0},
                            "missingness_rule": "0.5 when VHS reports total=0 for the window, otherwise 0.0 if the value itself is missing",
                        },
                        {
                            "metric_id": "agreement_24h",
                            "name": "Agreement 24h",
                            "weight": WEIGHTS["agreement_24h"],
                            "source": "VHS /v1/network/validators",
                            "signal_class": "objective",
                            "normalization": "linear from 0.8 -> 0.0 to 1.0 -> 1.0",
                            "thresholds": {"min_for_nonzero": 0.8, "full_score": 1.0},
                            "missingness_rule": "0.5 when VHS reports total=0 for the window, otherwise 0.0 if the value itself is missing",
                        },
                        {
                            "metric_id": "agreement_30d",
                            "name": "Agreement 30d",
                            "weight": WEIGHTS["agreement_30d"],
                            "source": "VHS /v1/network/validators",
                            "signal_class": "objective",
                            "normalization": "linear from 0.8 -> 0.0 to 1.0 -> 1.0",
                            "thresholds": {"min_for_nonzero": 0.8, "full_score": 1.0},
                            "missingness_rule": "0.5 when VHS reports total=0 for the window, otherwise 0.0 if the value itself is missing",
                        },
                        {
                            "metric_id": "uptime",
                            "name": "Uptime",
                            "weight": WEIGHTS["uptime"],
                            "source": "VHS topology / direct topology enrichment",
                            "signal_class": "semi-objective",
                            "normalization": "seconds divided by max observed uptime in the current cohort",
                            "thresholds": {"cohort_relative": True},
                            "missingness_rule": "0.0 if uptime_seconds is missing",
                        },
                        {
                            "metric_id": "poll_success",
                            "name": "Poll Success",
                            "weight": WEIGHTS["poll_success"],
                            "source": "Direct RPC polling history",
                            "signal_class": "observer-dependent",
                            "normalization": "linear from 70% -> 0.0 to 95% -> 1.0",
                            "thresholds": {"zero_below_pct": 70.0, "full_score_pct": 95.0},
                            "missingness_rule": "0.5 if there is no poll history yet",
                        },
                        {
                            "metric_id": "latency",
                            "name": "Latency",
                            "weight": WEIGHTS["latency"],
                            "source": "VHS topology and direct RPC observation",
                            "signal_class": "observer-dependent",
                            "normalization": "1.0 at <=50ms, 0.0 at >=500ms, linear in between",
                            "thresholds": {"full_score_ms": 50.0, "zero_score_ms": 500.0},
                            "missingness_rule": "0.5 if latency is unavailable",
                        },
                        {
                            "metric_id": "peer_count",
                            "name": "Peer Count",
                            "weight": WEIGHTS["peer_count"],
                            "source": "VHS topology and direct topology enrichment",
                            "signal_class": "observer-dependent",
                            "normalization": "0.0 below 3 peers, 1.0 at 10 peers, linear in between",
                            "thresholds": {"zero_below": 3, "full_score_at": 10},
                            "missingness_rule": "0.5 if peer count is unavailable",
                        },
                        {
                            "metric_id": "version",
                            "name": "Server Version",
                            "weight": WEIGHTS["version"],
                            "source": "VHS /v1/network/validators",
                            "signal_class": "objective",
                            "normalization": "1.0 on latest observed mode version, 0.8 when one minor behind, 0.5 otherwise",
                            "thresholds": {"latest_score": 1.0, "one_minor_behind_score": 0.8, "older_or_unknown_score": 0.5},
                            "missingness_rule": "0.5 if either the validator version or cohort latest version is unavailable",
                        },
                        {
                            "metric_id": "diversity",
                            "name": "ASN Diversity",
                            "weight": WEIGHTS["diversity"],
                            "source": "ASN/provider enrichment",
                            "signal_class": "cohort-relative",
                            "normalization": "linear reward below 30% concentration, penalty curve above 30% concentration",
                            "thresholds": {"penalty_threshold_concentration": 0.30},
                            "missingness_rule": "0.5 if ASN is unavailable or no enriched ASN cohort exists",
                        },
                    ],
                },
            },
            {
                "id": "snapshot_semantics",
                "title": "Snapshot Semantics",
                "summary": (
                    "A scoring round is the output of one collector run that successfully produces a cohort of validator "
                    "scores and stores them under a single round_id and UTC timestamp."
                ),
                "fields": {
                    "scoring_round_definition": "One completed collector cycle that yields stored validator scores for the current cohort.",
                    "round_timestamp": "UTC timestamp recorded when the scoring round is stored.",
                    "round_id_semantics": "Monotonic integer identifier for a stored scoring round. Use round_id as the primary temporal alignment key when comparing outputs across internal surfaces.",
                    "dataset_daily_snapshot_rule": "For dataset export, the canonical daily snapshot for a UTC date is the scoring_rounds row with MAX(id) for that date substring.",
                    "dataset_alignment_guidance": [
                        "Use round_id when comparing the leaderboard, incidents, diagnostics, or JSON score feeds from the same run.",
                        "Use snapshot_date for daily exports and timeseries built from the dataset API.",
                        "Do not assume two systems are aligned just because they were viewed on the same wall-clock day.",
                    ],
                },
            },
            {
                "id": "known_limitations",
                "title": "Known Limitations",
                "summary": (
                    "Some parts of the scoring system are robust and objective, while others are constrained by network "
                    "observability and external vantage-point limits."
                ),
                "fields": {
                    "limitations": [
                        "Topology enrichment coverage is incomplete whenever node-to-validator mapping cannot be resolved for every validator.",
                        "Latency is observer-dependent and reflects a specific observation vantage point rather than a universal network truth.",
                        "Peer count is topology-sensitive and should not be over-interpreted across different network layouts.",
                        "VHS itself is a single data service with its own collection assumptions and potential lag.",
                        "The 1-hour agreement window has a known history of VHS aggregation gaps; when VHS reports total=0 the scorer treats the window as neutral rather than punitive.",
                        "Validators lacking topology enrichment are not fully comparable on topology-derived dimensions because some related metrics fall back to neutral values.",
                    ],
                },
            },
            {
                "id": "missingness_handling",
                "title": "Missingness Handling",
                "summary": (
                    "Missing data is not treated uniformly. The scorer distinguishes between neutral fallbacks, zero scores, "
                    "and skipped operational checks depending on what the missingness means."
                ),
                "fields": {
                    "neutral_0_5_cases": [
                        "agreement windows where VHS reports total=0",
                        "poll_success when no poll history exists yet",
                        "latency when unavailable",
                        "peer_count when unavailable",
                        "version when cohort latest or validator version is unavailable",
                        "diversity when ASN data is unavailable",
                    ],
                    "zero_score_cases": [
                        "agreement when the score value itself is missing",
                        "uptime when uptime_seconds is missing",
                        "agreement values below 0.8",
                        "poll_success below 70%",
                    ],
                    "operational_check_behavior": [
                        "Readiness-style diagnostic checks may be marked skipped when the underlying signal is not available.",
                        "Collector source-health signals can be warnings or critical states without forcing every score to zero.",
                    ],
                },
            },
            {
                "id": "scale_and_comparability",
                "title": "Scale and Comparability",
                "summary": (
                    "The composite score ranges from 0 to 100, while each sub-score ranges from 0.0 to 1.0. Higher "
                    "scores indicate stronger observable performance under this methodology, not a universal measure "
                    "of validator worth."
                ),
                "fields": {
                    "composite_range": "0 to 100",
                    "sub_score_range": "0.0 to 1.0",
                    "interpretation_guidance": [
                        "Composite scores summarize observable performance under this specific weighting scheme.",
                        "Sub-scores are better for root-cause interpretation than the composite alone.",
                        "Cross-system comparisons should focus on windows, inputs, and methodology differences before judging which score is 'right'.",
                    ],
                    "plain_language_meaning": {
                        "higher_scores": "Stronger observed agreement, availability, and supporting telemetry under this system.",
                        "mid_scores": "Mixed operational signals, missingness, or partial enrichment may be involved.",
                        "lower_scores": "Substantial operational weakness, degraded consensus participation, or repeated telemetry problems under this methodology.",
                    },
                },
            },
            {
                "id": "maintenance_and_versioning",
                "title": "Maintenance and Versioning",
                "summary": (
                    "Methodology changes should be versioned alongside the scoring application so public explanations stay "
                    "tied to the actual live implementation."
                ),
                "fields": {
                    "methodology_version": settings.methodology_version,
                    "schema_version": SCHEMA_VERSION,
                    "change_policy": [
                        "Weights, thresholds, and scoring semantics should be updated in the shared methodology source used by the APIs and methodology-card page.",
                        "Public methodology references should move in lockstep with implementation changes rather than relying on separately maintained prose.",
                    ],
                    "historical_versions_location": [
                        "Git history for the scoring application",
                        "Public repository history for methodology-related code and docs",
                    ],
                },
            },
            {
                "id": "disagreement_interpretation_guide",
                "title": "Disagreement Interpretation Guide",
                "summary": (
                    "Differences between this independent scoring system and other systems, including the upcoming dUNL "
                    "pipeline, should be treated as a prompt for shared investigation rather than proof that one system failed."
                ),
                "fields": {
                    "expected_disagreement_categories": [
                        {
                            "category": "Different data windows",
                            "meaning": "Two systems may be measuring different slices of time even when viewed on the same day.",
                        },
                        {
                            "category": "Different metric weights",
                            "meaning": "Systems can agree on raw facts but disagree in rank order because they emphasize different signals.",
                        },
                        {
                            "category": "Qualitative versus quantitative signals",
                            "meaning": "A deterministic score and an LLM-assisted score may incorporate different kinds of evidence.",
                        },
                        {
                            "category": "Missing-data asymmetry",
                            "meaning": "One system may have observability for a validator or signal that another system lacks.",
                        },
                    ],
                    "interpretation_rules": [
                        "Disagreement does not imply error in either system.",
                        "Check whether the compared outputs share the same time window before drawing conclusions.",
                        "Use raw metric differences and missingness notes as the first debugging surface.",
                        "Treat persistent disagreement as a useful queue for operator and contributor investigation.",
                    ],
                },
            },
        ],
    }


def build_methodology_summary() -> dict:
    card = build_methodology_card()
    return {
        "version": settings.methodology_version,
        "description": (
            "Weighted composite reputation score for Post Fiat validators. "
            "Each metric is normalized to 0.0-1.0, multiplied by its weight, "
            "and the sum is scaled to 0-100."
        ),
        "weights": dict(WEIGHTS),
        "thresholds": {
            "agreement": {"min": 0.8, "max": 1.0, "scoring": "linear, <0.8 = 0; total=0 treated as neutral 0.5"},
            "uptime": {"scoring": "normalized against max observed uptime in cohort", "unit": "seconds, also reported as percentage"},
            "poll_success": {"full_marks_pct": 95, "zero_pct": 70, "scoring": "linear between; our own reachability tracking"},
            "latency": {"full_marks_ms": 50, "zero_ms": 500, "scoring": "linear between"},
            "peer_count": {"full_marks": 10, "zero": 3, "scoring": "linear between"},
            "avg_ledger_interval": {"unit": "seconds per ledger", "description": "computed from complete_ledgers range / uptime"},
            "version": {"latest": 1.0, "one_behind": 0.8, "older": 0.5},
            "diversity": {"penalty_threshold": 0.30, "scoring": "penalty if >30% share same ASN"},
        },
        "full_card_url": card["urls"]["page"],
        "full_card_api_url": card["urls"]["json"],
    }
