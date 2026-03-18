import pytest
from app.collector import DataCollector


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
