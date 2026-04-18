"""Tests for merged_builder.py: _get_sl_group_overrides."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from merged_builder import _get_sl_group_overrides


class TestGetSlGroupOverrides:
    def test_normal_extraction(self):
        cache = {
            "张三||sl_group||5": {"adopt": "LLM2", "episodes": [{"供职单位": "A"}]},
            "张三||sl_group||10": {"adopt": "LLM1", "episodes": []},
        }
        result = _get_sl_group_overrides(cache, "张三")
        assert len(result) == 2
        assert result[5]["adopt"] == "LLM2"
        assert result[10]["adopt"] == "LLM1"

    def test_no_matching_name(self):
        cache = {
            "李四||sl_group||5": {"adopt": "LLM2", "episodes": []},
        }
        result = _get_sl_group_overrides(cache, "张三")
        assert result == {}

    def test_empty_cache(self):
        assert _get_sl_group_overrides({}, "张三") == {}

    def test_ignores_non_sl_group_keys(self):
        cache = {
            "张三||field||5": {"adopt": "LLM2"},
            "张三||sl_group||3": {"adopt": "LLM1", "episodes": []},
        }
        result = _get_sl_group_overrides(cache, "张三")
        assert len(result) == 1
        assert 3 in result

    def test_invalid_line_number(self):
        cache = {
            "张三||sl_group||abc": {"adopt": "LLM1"},
        }
        result = _get_sl_group_overrides(cache, "张三")
        assert result == {}
