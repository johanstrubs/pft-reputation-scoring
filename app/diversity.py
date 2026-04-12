from collections import Counter

from app.models import ValidatorScore
from app.scorer import WEIGHTS, ReputationScorer

DIVERSITY_THRESHOLD_PCT = 33.0
DIVERSITY_ONLY_DISCLAIMER = (
    "These recommendations are decentralization heuristics based on diversity-scoring impact only. "
    "They do not account for latency, cost, reliability, or operational differences between providers."
)

PRESET_BUNDLES = [
    {"provider": "OVHcloud", "asn": 16276, "country": "FR", "source": "preset"},
    {"provider": "AWS", "asn": 14618, "country": "US", "source": "preset"},
    {"provider": "DigitalOcean", "asn": 14061, "country": "US", "source": "preset"},
    {"provider": "Linode", "asn": 63949, "country": "US", "source": "preset"},
    {"provider": "Google Cloud", "asn": 15169, "country": "US", "source": "preset"},
]


def _bundle_label(provider: str | None, asn: int | None, country: str | None) -> str:
    return f"{provider or 'Unknown'} / AS{asn if asn is not None else '?'} / {country or '??'}"


def _ranked_scores(scores: list[ValidatorScore]) -> dict[str, int]:
    ranked = sorted(scores, key=lambda score: (-score.composite_score, score.public_key))
    return {score.public_key: idx + 1 for idx, score in enumerate(ranked)}


def _diversity_score_for_asn(asn: int | None, asn_counts: Counter, total_with_asn: int) -> float:
    return ReputationScorer._score_diversity(asn, asn_counts, total_with_asn)


def _grouping(value: str, count: int, total: int) -> dict:
    pct = round(100 * count / total, 1) if total else 0.0
    return {
        "value": value,
        "shared_count": count,
        "concentration_pct": pct,
        "above_threshold": pct > DIVERSITY_THRESHOLD_PCT,
        "threshold_over_pct": round(max(0.0, pct - DIVERSITY_THRESHOLD_PCT), 1),
    }


def _make_bundle(provider: str, asn: int, country: str, source: str) -> dict:
    return {
        "provider": provider,
        "asn": asn,
        "country": country,
        "label": _bundle_label(provider, asn, country),
        "source": source,
    }


def build_diversity_report(scores: list[ValidatorScore], public_key: str) -> dict:
    if not scores:
        raise ValueError("No scoring data available yet")

    validator = next((score for score in scores if score.public_key == public_key), None)
    if not validator:
        raise KeyError(public_key)

    ranked_scores = _ranked_scores(scores)
    provider_counts = Counter(score.metrics.isp for score in scores if score.metrics.isp)
    asn_counts = Counter(score.metrics.asn for score in scores if score.metrics.asn is not None)
    country_counts = Counter(score.metrics.country for score in scores if score.metrics.country)
    bundle_counts = Counter(
        (score.metrics.isp, score.metrics.asn, score.metrics.country)
        for score in scores
        if score.metrics.isp and score.metrics.asn is not None and score.metrics.country
    )

    total_with_provider = sum(provider_counts.values())
    total_with_asn = sum(asn_counts.values())
    total_with_country = sum(country_counts.values())
    total_with_bundle = sum(bundle_counts.values())

    provider = validator.metrics.isp
    asn = validator.metrics.asn
    country = validator.metrics.country
    bundle_key = (provider, asn, country)

    current_context = {
        "public_key": validator.public_key,
        "domain": validator.domain,
        "provider": provider,
        "asn": asn,
        "country": country,
        "bundle_label": _bundle_label(provider, asn, country),
        "diversity_score": round(validator.sub_scores.diversity, 3),
        "composite_score": validator.composite_score,
        "rank": ranked_scores[validator.public_key],
        "validator_count": len(scores),
        "provider_group": _grouping(provider, provider_counts.get(provider, 0), total_with_provider) if provider else None,
        "asn_group": _grouping(f"AS{asn}", asn_counts.get(asn, 0), total_with_asn) if asn is not None else None,
        "country_group": _grouping(country, country_counts.get(country, 0), total_with_country) if country else None,
        "bundle_group": _grouping(_bundle_label(provider, asn, country), bundle_counts.get(bundle_key, 0), total_with_bundle) if all(v is not None for v in bundle_key) else None,
    }

    clean_bill_of_health = all(
        grouping is None or grouping["above_threshold"] is False
        for grouping in (
            current_context["provider_group"],
            current_context["asn_group"],
            current_context["country_group"],
        )
    )
    current_context["clean_bill_of_health"] = clean_bill_of_health

    concentration_summary = [
        {
            "bundle": _make_bundle(provider_name, asn_value, country_value, "observed"),
            "validator_count": count,
            "concentration_pct": round(100 * count / total_with_bundle, 1) if total_with_bundle else 0.0,
        }
        for (provider_name, asn_value, country_value), count in bundle_counts.most_common(5)
    ]

    observed_bundles = {
        (provider_name, asn_value, country_value): _make_bundle(provider_name, asn_value, country_value, "observed")
        for (provider_name, asn_value, country_value), _count in bundle_counts.items()
    }
    candidate_bundles = dict(observed_bundles)
    for preset in PRESET_BUNDLES:
        key = (preset["provider"], preset["asn"], preset["country"])
        candidate_bundles.setdefault(key, _make_bundle(preset["provider"], preset["asn"], preset["country"], preset["source"]))

    current_rank = ranked_scores[validator.public_key]
    current_diversity = validator.sub_scores.diversity
    current_composite = validator.composite_score
    diversity_weight_points = WEIGHTS["diversity"] * 100

    projections = []
    for candidate_key, bundle in candidate_bundles.items():
        if candidate_key == bundle_key:
            continue

        target_provider, target_asn, target_country = candidate_key
        source_bundle_count_before = bundle_counts.get(bundle_key, 0) if all(v is not None for v in bundle_key) else 0
        target_bundle_count_before = bundle_counts.get(candidate_key, 0)
        source_bundle_count_after = max(0, source_bundle_count_before - (1 if all(v is not None for v in bundle_key) else 0))
        target_bundle_count_after = target_bundle_count_before + 1
        source_bundle_pct_before = round(100 * source_bundle_count_before / total_with_bundle, 1) if total_with_bundle and all(v is not None for v in bundle_key) else 0.0
        source_bundle_pct_after = round(100 * source_bundle_count_after / total_with_bundle, 1) if total_with_bundle and all(v is not None for v in bundle_key) else 0.0
        target_bundle_pct_before = round(100 * target_bundle_count_before / total_with_bundle, 1) if total_with_bundle else 0.0
        target_bundle_pct_after = round(100 * target_bundle_count_after / total_with_bundle, 1) if total_with_bundle else 0.0

        projected_asn_counts = Counter(asn_counts)
        if asn is not None and projected_asn_counts.get(asn, 0) > 0:
            projected_asn_counts[asn] -= 1
            if projected_asn_counts[asn] <= 0:
                projected_asn_counts.pop(asn, None)
        projected_asn_counts[target_asn] += 1
        projected_diversity = _diversity_score_for_asn(target_asn, projected_asn_counts, total_with_asn)
        diversity_delta = round(projected_diversity - current_diversity, 3)
        projected_composite = round(current_composite + diversity_delta * diversity_weight_points, 2)

        projected_scores = []
        for score in scores:
            if score.public_key == validator.public_key:
                projected_scores.append((score.public_key, projected_composite))
            else:
                projected_scores.append((score.public_key, score.composite_score))
        projected_scores.sort(key=lambda item: (-item[1], item[0]))
        projected_rank = next(index + 1 for index, item in enumerate(projected_scores) if item[0] == validator.public_key)

        projections.append(
            {
                "target_bundle": bundle,
                "projected_diversity_score": round(projected_diversity, 3),
                "diversity_score_delta": diversity_delta,
                "projected_composite_score": projected_composite,
                "composite_score_delta": round(projected_composite - current_composite, 2),
                "projected_rank": projected_rank,
                "rank_delta": current_rank - projected_rank,
                "source_bundle_pct_before": source_bundle_pct_before,
                "source_bundle_pct_after": source_bundle_pct_after,
                "target_bundle_pct_before": target_bundle_pct_before,
                "target_bundle_pct_after": target_bundle_pct_after,
                "target_bundle_would_exceed_threshold": target_bundle_pct_after > DIVERSITY_THRESHOLD_PCT,
            }
        )

    projections.sort(
        key=lambda item: (
            item["target_bundle_would_exceed_threshold"],
            -item["diversity_score_delta"],
            -item["composite_score_delta"],
            -item["rank_delta"],
            item["target_bundle"]["label"],
        )
    )

    recommendations = [
        projection
        for projection in projections
        if not projection["target_bundle_would_exceed_threshold"]
    ][:3]

    return {
        "current_context": current_context,
        "concentration_summary": concentration_summary,
        "available_target_bundles": projections,
        "recommendations": [] if clean_bill_of_health else recommendations,
        "disclaimer": DIVERSITY_ONLY_DISCLAIMER,
        "json_report_url": f"/api/diversity/{validator.public_key}",
    }
