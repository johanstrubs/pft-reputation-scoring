import pytest

from app.models import ValidatorMetrics, ValidatorScore, ValidatorSubScores
from app.peers import build_peer_report


def make_score(
    public_key: str,
    *,
    domain: str | None = None,
    ip: str | None = None,
    agreement_24h: float | None = 0.98,
    latency_ms: float | None = 40.0,
    peer_count: int | None = 12,
    version: str | None = "3.0.0",
    provider: str | None = None,
    asn: int | None = None,
    country: str | None = None,
    composite_score: float = 85.0,
) -> ValidatorScore:
    return ValidatorScore(
        public_key=public_key,
        domain=domain,
        composite_score=composite_score,
        metrics=ValidatorMetrics(
            agreement_24h=agreement_24h,
            latency_ms=latency_ms,
            peer_count=peer_count,
            server_version=version,
            isp=provider,
            asn=asn,
            country=country,
            node_ip=ip,
        ),
        sub_scores=ValidatorSubScores(diversity=0.8),
        last_updated="2026-04-12T12:00:00+00:00",
    )


@pytest.mark.anyio
async def test_peer_report_uses_adjacency_mode(monkeypatch):
    scores = [
        make_score("nHTarget", domain="target.example.com", ip="10.0.0.1", provider="Hetzner", asn=24940, country="DE", composite_score=80.0),
        make_score("nHRisky", domain="risky.example.com", ip="10.0.0.2", agreement_24h=0.88, latency_ms=610.0, version="2.9.0", provider="Hetzner", asn=24940, country="DE", composite_score=78.0),
        make_score("nHGood", domain="good.example.com", ip="10.0.0.3", provider="Vultr", asn=20473, country="US", composite_score=77.0),
        make_score("nHAlt", domain="alt.example.com", ip="10.0.0.4", provider="OVHcloud", asn=16276, country="FR", composite_score=76.0),
    ]

    async def fake_topology():
        return [
            {"node_public_key": "n9Target", "ip": "10.0.0.1", "country_code": "DE", "io_latency_ms": 40.0},
            {"node_public_key": "n9Risky", "ip": "10.0.0.2", "country_code": "DE", "io_latency_ms": 610.0},
            {"node_public_key": "n9Good", "ip": "10.0.0.3", "country_code": "US", "io_latency_ms": 45.0},
            {"node_public_key": "n9Alt", "ip": "10.0.0.4", "country_code": "FR", "io_latency_ms": 35.0},
        ]

    async def fake_crawl(_topology_nodes):
        return {
            "10.0.0.1": {
                "server": {"pubkey_node": "n9Target", "pubkey_validator": "nHTarget"},
                "overlay": {"active": [{"ip": "10.0.0.2"}, {"ip": "10.0.0.3"}]},
            },
            "10.0.0.2": {
                "server": {"pubkey_node": "n9Risky", "pubkey_validator": "nHRisky", "build_version": "2.9.0"},
                "overlay": {"active": [{"ip": "10.0.0.1"}]},
            },
            "10.0.0.3": {
                "server": {"pubkey_node": "n9Good", "pubkey_validator": "nHGood", "build_version": "3.0.0"},
                "overlay": {"active": [{"ip": "10.0.0.1"}]},
            },
            "10.0.0.4": {
                "server": {"pubkey_node": "n9Alt", "pubkey_validator": "nHAlt", "build_version": "3.0.0"},
                "overlay": {"active": [{"ip": "10.0.0.3"}]},
            },
        }

    monkeypatch.setattr("app.peers._fetch_topology_nodes", fake_topology)
    monkeypatch.setattr("app.peers._fetch_crawl_for_topology", fake_crawl)

    report = await build_peer_report(scores, "nHTarget")

    assert report["mode"] == "adjacency"
    assert report["summary"]["current_peer_count"] == 2
    assert any("behind the latest observed validator version" in finding["title"] for finding in report["risk_findings"])
    assert report["drop_recommendations"][0]["node_public_key"] == "n9Risky"
    assert any(rec["node_public_key"] == "n9Alt" for rec in report["add_recommendations"])


@pytest.mark.anyio
async def test_peer_report_falls_back_to_candidate_only(monkeypatch):
    scores = [
        make_score("nHTarget", domain="target.example.com", ip="10.0.0.1", provider="Hetzner", asn=24940, country="DE"),
        make_score("nHGood", domain="good.example.com", ip="10.0.0.3", provider="Vultr", asn=20473, country="US"),
    ]

    async def fake_topology():
        return [
            {"node_public_key": "n9Target", "ip": "10.0.0.1", "country_code": "DE", "io_latency_ms": 40.0},
            {"node_public_key": "n9Good", "ip": "10.0.0.3", "country_code": "US", "io_latency_ms": 45.0},
        ]

    async def fake_crawl(_topology_nodes):
        return {
            "10.0.0.1": {"server": {"pubkey_node": "n9Target", "pubkey_validator": "nHTarget"}},
            "10.0.0.3": {"server": {"pubkey_node": "n9Good", "pubkey_validator": "nHGood"}},
        }

    monkeypatch.setattr("app.peers._fetch_topology_nodes", fake_topology)
    monkeypatch.setattr("app.peers._fetch_crawl_for_topology", fake_crawl)

    report = await build_peer_report(scores, "nHTarget")

    assert report["mode"] == "candidate_only"
    assert report["summary"]["current_peer_count"] == 0
    assert report["add_recommendations"]
    assert all(rec["validator_public_key"] != "nHTarget" for rec in report["add_recommendations"])
