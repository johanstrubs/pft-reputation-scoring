from app.diagnostics import build_diagnostic_report
from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores


def make_score(
    public_key: str,
    composite_score: float,
    *,
    domain: str | None = None,
    agreement_1h: float = 0.99,
    agreement_24h: float = 0.99,
    agreement_30d: float = 0.99,
    uptime_pct: float = 90.0,
    uptime_seconds: int = 90000,
    latency_ms: float = 70.0,
    peer_count: int = 10,
    poll_success_pct: float = 98.0,
    server_version: str = "1.0.0",
    server_state: str = "proposing",
    asn: int | None = 24940,
    country: str | None = "DE",
    diversity: float = 0.8,
):
    return ValidatorScore(
        public_key=public_key,
        domain=domain,
        composite_score=composite_score,
        metrics=ValidatorMetrics(
            agreement_1h=agreement_1h,
            agreement_24h=agreement_24h,
            agreement_30d=agreement_30d,
            uptime_pct=uptime_pct,
            uptime_seconds=uptime_seconds,
            latency_ms=latency_ms,
            peer_count=peer_count,
            poll_success_pct=poll_success_pct,
            server_version=server_version,
            server_state=server_state,
            asn=asn,
            country=country,
        ),
        sub_scores=ValidatorSubScores(
            agreement_1h=0.95,
            agreement_24h=0.95,
            agreement_30d=0.95,
            uptime=min(uptime_pct / 100, 1),
            poll_success=min(max((poll_success_pct - 70) / 25, 0), 1),
            latency=0.9,
            peer_count=1.0 if peer_count >= 10 else 0.4,
            version=1.0,
            diversity=diversity,
        ),
        last_updated="2026-04-04T00:00:00+00:00",
    )


def test_build_diagnostic_report_prioritizes_critical_faults():
    scores = [
        make_score("nHHealthy", 90.0, domain="healthy.example.com", latency_ms=45.0, uptime_pct=100.0, uptime_seconds=100000),
        make_score("nHWeak", 58.0, domain="weak.example.com", agreement_1h=0.88, agreement_24h=0.92, agreement_30d=0.89, uptime_pct=35.0, uptime_seconds=35000, latency_ms=410.0, peer_count=2, poll_success_pct=72.0, server_version="0.9.0", server_state="syncing", diversity=0.2),
        make_score("nHMedian", 82.0, domain="median.example.com", latency_ms=80.0, uptime_pct=75.0, uptime_seconds=75000, peer_count=7, poll_success_pct=94.0),
    ]

    report = build_diagnostic_report(10, "2026-04-04T12:00:00+00:00", scores, "nHWeak")

    assert report["overall_status"] == "critical"
    assert report["rank"] == 3
    assert report["findings"][0]["severity"] == "critical"
    assert report["findings"][0]["metric"] in {"agreement_30d", "peer_count", "server_state"}
    assert any(finding["metric"] == "version" for finding in report["findings"])
    assert any(finding["metric"] == "diversity" for finding in report["findings"])


def test_build_diagnostic_report_returns_clean_health_when_no_faults():
    scores = [
        make_score("nHStrong", 91.0, domain="strong.example.com", latency_ms=40.0, uptime_pct=100.0, uptime_seconds=110000, peer_count=12, poll_success_pct=99.0),
        make_score("nHAlsoStrong", 88.0, domain="also.example.com", latency_ms=55.0, uptime_pct=92.0, uptime_seconds=101000, peer_count=11, poll_success_pct=97.0),
        make_score("nHMedian", 79.0, domain="median.example.com", latency_ms=85.0, uptime_pct=78.0, uptime_seconds=86000, peer_count=7, poll_success_pct=94.0),
    ]

    report = build_diagnostic_report(11, "2026-04-04T12:05:00+00:00", scores, "nHStrong")

    assert report["overall_status"] == "healthy"
    assert report["findings"] == []
    assert report["strengths"]
