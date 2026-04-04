import pytest
from app.collector import DataCollector, NodeValidatorMap


class TestParseAgreement:
    def test_dict_with_score_string(self):
        score, total = DataCollector._parse_agreement({"missed": 0, "total": 100, "score": "0.9500", "incomplete": False})
        assert abs(score - 0.95) < 0.001
        assert total == 100

    def test_dict_with_zero_total(self):
        score, total = DataCollector._parse_agreement({"missed": 0, "total": 0, "score": "0.00000", "incomplete": False})
        assert score == 0.0
        assert total == 0

    def test_float_value(self):
        score, total = DataCollector._parse_agreement(0.97)
        assert score == 0.97
        assert total is None

    def test_int_value(self):
        score, total = DataCollector._parse_agreement(1)
        assert score == 1.0
        assert total is None

    def test_string_value(self):
        score, total = DataCollector._parse_agreement("0.88")
        assert score == 0.88
        assert total is None

    def test_none(self):
        score, total = DataCollector._parse_agreement(None)
        assert score is None
        assert total is None

    def test_invalid_dict(self):
        score, total = DataCollector._parse_agreement({"no_score": True})
        assert score is None


class TestNodeValidatorMap:
    def test_add_and_get(self):
        m = NodeValidatorMap()
        m.add("nNode1", "nMaster1", "nSigning1", source="test")
        assert m.get_master_key("nNode1") == "nMaster1"
        assert m.size == 1

    def test_get_missing(self):
        m = NodeValidatorMap()
        assert m.get_master_key("nNotExist") is None

    def test_needs_probe_initially(self):
        m = NodeValidatorMap()
        assert m.needs_probe() is True

    def test_mark_probed(self):
        m = NodeValidatorMap()
        m.mark_probed()
        assert m.needs_probe() is False


class TestComputeLedgerInterval:
    def test_large_range(self):
        # 100000 ledgers in 350000 seconds = 3.5 sec/ledger
        result = DataCollector._compute_ledger_interval("100-100100", 350000)
        assert abs(result - 3.5) < 0.01

    def test_small_range_returns_none(self):
        # Less than 10000 ledgers — not reliable
        result = DataCollector._compute_ledger_interval("100-1100", 3500)
        assert result is None

    def test_none_inputs(self):
        assert DataCollector._compute_ledger_interval(None, 3500) is None
        assert DataCollector._compute_ledger_interval("100-1100", None) is None
        assert DataCollector._compute_ledger_interval(None, None) is None

    def test_zero_uptime(self):
        assert DataCollector._compute_ledger_interval("100-1100", 0) is None

    def test_invalid_format(self):
        assert DataCollector._compute_ledger_interval("invalid", 3500) is None

    def test_full_chain_range(self):
        # Large range = long running node, gives accurate interval
        result = DataCollector._compute_ledger_interval("1-1000000", 3500000)
        assert abs(result - 3.5) < 0.01


class TestResolveDomain:
    def test_resolve_localhost(self):
        ips = DataCollector._resolve_domain("localhost")
        assert "127.0.0.1" in ips

    def test_resolve_invalid(self):
        ips = DataCollector._resolve_domain("this.domain.definitely.does.not.exist.example")
        assert ips == []


class TestMappingPriority:
    @pytest.mark.anyio
    async def test_rpc_does_not_override_existing_crawl_mapping(self, monkeypatch):
        collector = DataCollector()

        async def fake_fetch_vhs_validators():
            return [
                {
                    "master_key": "nHMasterFromCrawl",
                    "signing_key": "n9SigningFromRpc",
                    "agreement_1h": 1.0,
                    "agreement_24h": 1.0,
                    "agreement_30day": 1.0,
                },
                {
                    "master_key": "nHMasterFromRpc",
                    "signing_key": "n9SigningFromRpc",
                    "agreement_1h": 1.0,
                    "agreement_24h": 1.0,
                    "agreement_30day": 1.0,
                },
            ]

        async def fake_fetch_vhs_topology():
            return [
                {
                    "node_public_key": "n9Node1",
                    "ip": "198.51.100.10",
                    "uptime": 100,
                    "io_latency_ms": 25.0,
                    "inbound_count": 4,
                    "outbound_count": 5,
                    "server_state": "proposing",
                    "complete_ledgers": "1-20000",
                    "country_code": "US",
                }
            ]

        async def fake_query_rpc_endpoints():
            return [
                {
                    "pubkey_node": "n9Node1",
                    "pubkey_validator": "n9SigningFromRpc",
                    "latency_ms": 30.0,
                }
            ]

        async def fake_crawl_network(seed_ips, snapshots):
            collector._node_map.add("n9Node1", "nHMasterFromCrawl", source="crawl")

        async def fake_enrich_asn(snapshots, ip_by_master):
            return None

        monkeypatch.setattr(collector, "_fetch_vhs_validators", fake_fetch_vhs_validators)
        monkeypatch.setattr(collector, "_fetch_vhs_topology", fake_fetch_vhs_topology)
        monkeypatch.setattr(collector, "_query_rpc_endpoints", fake_query_rpc_endpoints)
        monkeypatch.setattr(collector, "_crawl_network", fake_crawl_network)
        monkeypatch.setattr(collector, "_enrich_asn", fake_enrich_asn)

        snapshots, _ = await collector.collect()

        by_key = {snapshot.public_key: snapshot for snapshot in snapshots}
        assert "nHMasterFromCrawl" in by_key
        assert by_key["nHMasterFromCrawl"].metrics.latency_ms == 27.5
        assert collector._node_map.get_master_key("n9Node1") == "nHMasterFromCrawl"

    @pytest.mark.anyio
    async def test_subscriber_and_manual_only_fill_gaps(self, monkeypatch):
        collector = DataCollector()

        async def fake_fetch_vhs_validators():
            return [
                {
                    "master_key": "nHCrawl",
                    "signing_key": "n9Signing1",
                    "agreement_1h": 1.0,
                    "agreement_24h": 1.0,
                    "agreement_30day": 1.0,
                },
                {
                    "master_key": "nHSubscriber",
                    "signing_key": "n9Signing2",
                    "agreement_1h": 1.0,
                    "agreement_24h": 1.0,
                    "agreement_30day": 1.0,
                },
                {
                    "master_key": "nHManual",
                    "signing_key": "n9Signing3",
                    "agreement_1h": 1.0,
                    "agreement_24h": 1.0,
                    "agreement_30day": 1.0,
                },
            ]

        async def fake_fetch_vhs_topology():
            return [
                {"node_public_key": "n9NodeCrawl", "ip": "198.51.100.11", "uptime": 100, "io_latency_ms": 20.0, "inbound_count": 3, "outbound_count": 4, "country_code": "US"},
                {"node_public_key": "n9NodeSubscriber", "ip": "198.51.100.12", "uptime": 100, "io_latency_ms": 21.0, "inbound_count": 3, "outbound_count": 4, "country_code": "US"},
                {"node_public_key": "n9NodeManual", "ip": "198.51.100.13", "uptime": 100, "io_latency_ms": 22.0, "inbound_count": 3, "outbound_count": 4, "country_code": "US"},
            ]

        async def fake_query_rpc_endpoints():
            return []

        async def fake_crawl_network(seed_ips, snapshots):
            collector._node_map.add("n9NodeCrawl", "nHCrawl", source="crawl")

        async def fake_resolve_domains_to_topology(snapshots, topology_nodes):
            return None

        async def fake_enrich_asn(snapshots, ip_by_master):
            return None

        monkeypatch.setattr(collector, "_fetch_vhs_validators", fake_fetch_vhs_validators)
        monkeypatch.setattr(collector, "_fetch_vhs_topology", fake_fetch_vhs_topology)
        monkeypatch.setattr(collector, "_query_rpc_endpoints", fake_query_rpc_endpoints)
        monkeypatch.setattr(collector, "_crawl_network", fake_crawl_network)
        monkeypatch.setattr(collector, "_resolve_domains_to_topology", fake_resolve_domains_to_topology)
        monkeypatch.setattr(collector, "_enrich_asn", fake_enrich_asn)
        monkeypatch.setattr(
            "app.collector.settings.manual_key_mappings",
            "n9NodeCrawl:nHWrongManual,n9NodeManual:nHManual",
        )

        await collector.collect(subscriber_mappings={"n9NodeCrawl": "nHWrongSubscriber", "n9NodeSubscriber": "nHSubscriber"})

        assert collector._node_map.get_master_key("n9NodeCrawl") == "nHCrawl"
        assert collector._node_map.get_master_key("n9NodeSubscriber") == "nHSubscriber"
        assert collector._node_map.get_master_key("n9NodeManual") == "nHManual"
