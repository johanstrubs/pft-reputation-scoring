import pytest
from app.collector import DataCollector, NodeValidatorMap


class TestParseAgreement:
    def test_dict_with_score_string(self):
        result = DataCollector._parse_agreement({"missed": 0, "total": 100, "score": "0.9500", "incomplete": False})
        assert abs(result - 0.95) < 0.001

    def test_float_value(self):
        assert DataCollector._parse_agreement(0.97) == 0.97

    def test_int_value(self):
        assert DataCollector._parse_agreement(1) == 1.0

    def test_string_value(self):
        assert DataCollector._parse_agreement("0.88") == 0.88

    def test_none(self):
        assert DataCollector._parse_agreement(None) is None

    def test_invalid_dict(self):
        assert DataCollector._parse_agreement({"no_score": True}) is None


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
