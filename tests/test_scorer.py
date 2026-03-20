import pytest
from app.scorer import ReputationScorer
from app.models import ValidatorSnapshot, ValidatorMetrics


def _make_snapshot(
    key="nHTest1",
    agreement_1h=1.0,
    agreement_24h=1.0,
    agreement_30d=1.0,
    uptime=86400,
    latency=30.0,
    peer_count=12,
    version="2.4.0",
    asn=24940,
    **kwargs,
):
    return ValidatorSnapshot(
        public_key=key,
        domain="test.example.com",
        metrics=ValidatorMetrics(
            agreement_1h=agreement_1h,
            agreement_1h_total=1000,
            agreement_24h=agreement_24h,
            agreement_24h_total=25000,
            agreement_30d=agreement_30d,
            agreement_30d_total=800000,
            uptime_seconds=uptime,
            latency_ms=latency,
            peer_count=peer_count,
            server_version=version,
            asn=asn,
            **kwargs,
        ),
    )


class TestAgreementScoring:
    def test_perfect_agreement(self):
        scorer = ReputationScorer()
        assert abs(scorer._score_agreement(1.0, total=1000) - 1.0) < 1e-9

    def test_threshold_agreement(self):
        scorer = ReputationScorer()
        assert scorer._score_agreement(0.8, total=1000) == 0.0

    def test_below_threshold(self):
        scorer = ReputationScorer()
        assert scorer._score_agreement(0.5, total=1000) == 0.0

    def test_mid_range(self):
        scorer = ReputationScorer()
        assert abs(scorer._score_agreement(0.9, total=1000) - 0.5) < 0.01

    def test_none(self):
        scorer = ReputationScorer()
        assert scorer._score_agreement(None) == 0.0

    def test_total_zero_returns_neutral(self):
        """VHS returns total=0 when 1h aggregation has no data — should be neutral."""
        scorer = ReputationScorer()
        assert scorer._score_agreement(0.0, total=0) == 0.5

    def test_total_none_uses_score_only(self):
        """When total is None (non-dict input), fall back to score-based logic."""
        scorer = ReputationScorer()
        assert scorer._score_agreement(0.0, total=None) == 0.0
        assert abs(scorer._score_agreement(0.95, total=None) - 0.75) < 0.01


class TestPollSuccessScoring:
    def test_high_success(self):
        assert ReputationScorer._score_poll_success(100.0) == 1.0

    def test_threshold(self):
        assert ReputationScorer._score_poll_success(95.0) == 1.0

    def test_low_success(self):
        assert ReputationScorer._score_poll_success(60.0) == 0.0

    def test_mid_success(self):
        score = ReputationScorer._score_poll_success(82.5)
        assert 0.4 < score < 0.6

    def test_none_returns_neutral(self):
        assert ReputationScorer._score_poll_success(None) == 0.5


class TestLatencyScoring:
    def test_low_latency(self):
        assert ReputationScorer._score_latency(30) == 1.0

    def test_high_latency(self):
        assert ReputationScorer._score_latency(600) == 0.0

    def test_mid_latency(self):
        score = ReputationScorer._score_latency(275)
        assert 0.4 < score < 0.6

    def test_none_latency(self):
        assert ReputationScorer._score_latency(None) == 0.5


class TestPeerCountScoring:
    def test_enough_peers(self):
        assert ReputationScorer._score_peer_count(15) == 1.0

    def test_too_few(self):
        assert ReputationScorer._score_peer_count(2) == 0.0

    def test_mid_peers(self):
        score = ReputationScorer._score_peer_count(6)
        assert abs(score - 3 / 7) < 0.01


class TestVersionScoring:
    def test_latest(self):
        assert ReputationScorer._score_version("2.4.0", "2.4.0") == 1.0

    def test_one_behind(self):
        assert ReputationScorer._score_version("2.3.0", "2.4.0") == 0.8

    def test_old(self):
        assert ReputationScorer._score_version("1.0.0", "2.4.0") == 0.5


class TestCompositeScoring:
    def test_perfect_validator(self):
        scorer = ReputationScorer()
        snap = _make_snapshot()
        results = scorer.score([snap])
        assert len(results) == 1
        assert results[0].composite_score > 80

    def test_multiple_validators_ranked(self):
        scorer = ReputationScorer()
        good = _make_snapshot(key="nHGood", agreement_30d=1.0)
        bad = _make_snapshot(key="nHBad", agreement_30d=0.7, latency=600, peer_count=1)
        results = scorer.score([bad, good])
        assert results[0].public_key == "nHGood"
        assert results[0].composite_score > results[1].composite_score

    def test_empty_input(self):
        scorer = ReputationScorer()
        assert scorer.score([]) == []
