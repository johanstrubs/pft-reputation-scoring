from app.diversity import build_diversity_report
from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores


def make_score(
    public_key: str,
    *,
    domain: str | None = None,
    composite_score: float = 80.0,
    diversity: float = 0.5,
    provider: str | None = None,
    asn: int | None = None,
    country: str | None = None,
) -> ValidatorScore:
    return ValidatorScore(
        public_key=public_key,
        domain=domain,
        composite_score=composite_score,
        metrics=ValidatorMetrics(isp=provider, asn=asn, country=country),
        sub_scores=ValidatorSubScores(diversity=diversity),
        last_updated="2026-04-12T12:00:00+00:00",
    )


def test_diversity_report_recommends_migration_for_overrepresented_validator():
    scores = [
        make_score("nHA", domain="a.example.com", composite_score=80.0, diversity=0.2, provider="Hetzner", asn=24940, country="DE"),
        make_score("nHB", domain="b.example.com", composite_score=79.0, diversity=0.2, provider="Hetzner", asn=24940, country="DE"),
        make_score("nHC", domain="c.example.com", composite_score=78.0, diversity=0.2, provider="Hetzner", asn=24940, country="DE"),
        make_score("nHD", domain="d.example.com", composite_score=77.0, diversity=0.8, provider="Vultr", asn=20473, country="US"),
    ]

    report = build_diversity_report(scores, "nHA")

    assert report["current_context"]["provider_group"]["above_threshold"] is True
    assert report["recommendations"]
    top = report["recommendations"][0]
    assert top["diversity_score_delta"] > 0
    assert top["target_bundle"]["label"] != report["current_context"]["bundle_label"]


def test_diversity_report_clean_bill_for_underrepresented_validator():
    scores = [
        make_score("nHA", composite_score=81.0, diversity=0.9, provider="AWS", asn=14618, country="US"),
        make_score("nHB", composite_score=80.0, diversity=0.9, provider="Hetzner", asn=24940, country="DE"),
        make_score("nHC", composite_score=79.0, diversity=0.9, provider="OVHcloud", asn=16276, country="FR"),
        make_score("nHD", composite_score=78.0, diversity=0.9, provider="DigitalOcean", asn=14061, country="CA"),
    ]

    report = build_diversity_report(scores, "nHA")

    assert report["current_context"]["clean_bill_of_health"] is True
    assert report["recommendations"] == []


def test_diversity_report_includes_presets_and_simulation_fields():
    scores = [
        make_score("nHA", composite_score=80.0, diversity=0.5, provider="Hetzner", asn=24940, country="DE"),
        make_score("nHB", composite_score=79.0, diversity=0.8, provider="Vultr", asn=20473, country="US"),
    ]

    report = build_diversity_report(scores, "nHA")

    assert any(bundle["target_bundle"]["source"] == "preset" for bundle in report["available_target_bundles"])
    projection = report["available_target_bundles"][0]
    assert "projected_composite_score" in projection
    assert "rank_delta" in projection
