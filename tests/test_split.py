"""Tests for Step1 multi-episode split: schema validation + Phase C upgrade + merger skip_sls."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from judge import _validate_field_decision, _collect_need_split_from_epbatch
from merged_builder import _apply_step1_field_overrides, build_merged_episodes_step1


# ── _validate_field_decision ────────────────────────────────────────────────

class TestValidateFieldDecision:
    def test_compliant_decision_unchanged(self):
        d = {"verdict": "采纳LLM1", "correct_value": "", "confidence": 90, "reason": "准确"}
        out = _validate_field_decision(d, "供职单位")
        assert out["verdict"] == "采纳LLM1"
        assert not out.get("_downgraded")

    def test_natural_language_recovered_to_needsplit(self):
        d = {"verdict": "自行修正", "correct_value": "需拆分提取：新疆生产建设兵团和中国新建集团公司"}
        out = _validate_field_decision(d, "供职单位")
        assert out["verdict"] == "需拆分"
        assert out["correct_value"] == ""
        assert out["_downgraded"] is True
        assert out["_downgrade_reason"] == "natural_language_recovered_to_needsplit"

    def test_multivalue_unit_separator_recovered_to_needsplit(self):
        d = {"verdict": "自行修正", "correct_value": "中共福建省委、中共福州市委", "confidence": 80}
        out = _validate_field_decision(d, "供职单位")
        assert out["verdict"] == "需拆分"
        assert out["correct_value"] == ""
        assert out["_downgraded"] is True
        assert out["_downgrade_reason"] == "multivalue_unit_recovered_to_needsplit"

    def test_invalid_separator_in_time_field_downgraded(self):
        d = {"verdict": "自行修正", "correct_value": "2010.05；2010.08"}
        out = _validate_field_decision(d, "起始时间")
        assert out["verdict"] == "两者均存疑"
        assert out["correct_value"] == ""
        assert out["_downgraded"] is True
        assert "invalid_separator" in out["_downgrade_reason"]

    def test_position_dot_separator_allowed(self):
        """职务 field allows 、 (same unit multiple roles)."""
        d = {"verdict": "自行修正", "correct_value": "副书记、政法委书记"}
        out = _validate_field_decision(d, "职务")
        assert out["verdict"] == "自行修正"
        assert out["correct_value"] == "副书记、政法委书记"
        assert not out.get("_downgraded")

    def test_position_semicolon_separator_downgraded(self):
        """职务 field disallows ；."""
        d = {"verdict": "自行修正", "correct_value": "委员；书记；主任"}
        out = _validate_field_decision(d, "职务")
        assert out["verdict"] == "两者均存疑"
        assert out["_downgraded"] is True

    def test_unit_semicolon_separator_recovered_to_needsplit(self):
        d = {"verdict": "自行修正", "correct_value": "中共中央政治局；中共江苏省委；江苏省人大常委会"}
        out = _validate_field_decision(d, "供职单位")
        assert out["verdict"] == "需拆分"
        assert out["_downgraded"] is True
        assert out["_downgrade_reason"] == "multivalue_unit_recovered_to_needsplit"


# ── _collect_need_split_from_epbatch ────────────────────────────────────────

class TestCollectNeedSplit:
    def test_finds_needsplit_entries(self):
        cache = {
            "李强||ep_batch||sl27||中共中央政治局||委员||2017.00||供职单位": {"verdict": "需拆分"},
            "习近平||ep_batch||sl9||中共福州市委||书记||1993.00||供职单位": {"verdict": "需拆分"},
            "李强||ep_batch||sl27||中共中央政治局||委员||2017.00||职务": {"verdict": "两者均存疑"},
            "张三||sl_group||5": {"adopt": "LLM1", "episodes": []},
        }
        result = _collect_need_split_from_epbatch(cache)
        assert ("李强", 27) in result
        assert ("习近平", 9) in result
        assert len(result) == 2

    def test_empty_cache(self):
        assert _collect_need_split_from_epbatch({}) == set()

    def test_no_needsplit_returns_empty(self):
        cache = {
            "张三||ep_batch||sl5||中共浙江省委||书记||2010.00||供职单位": {"verdict": "采纳LLM1"},
        }
        assert _collect_need_split_from_epbatch(cache) == set()

    def test_deduplicates_same_name_sl(self):
        """Multiple fields from same (name, sl) should produce one entry."""
        cache = {
            "车俊||ep_batch||sl17||X||Y||Z||供职单位": {"verdict": "需拆分"},
            "车俊||ep_batch||sl17||X||Y||Z||职务": {"verdict": "需拆分"},
        }
        result = _collect_need_split_from_epbatch(cache)
        assert result == {("车俊", 17)}


# ── _apply_step1_field_overrides with skip_sls ───────────────────────────────

class TestMergerSkipSls:
    def _make_ep(self, sl, unit, pos, start):
        return {"source_line": sl, "供职单位": unit, "职务": pos, "起始时间": start, "终止时间": ""}

    def test_skip_sls_prevents_field_override(self):
        ep = self._make_ep(27, "中共中央政治局", "委员", "2017.00")
        # There's a judge cache entry that would change 供职单位
        cache = {
            "李强||ep_batch||sl27||中共中央政治局||委员||2017.00||供职单位": {
                "verdict": "自行修正", "correct_value": "WRONG_VALUE"
            }
        }
        result = _apply_step1_field_overrides(
            [ep], {"episodes": []}, cache, "李强", skip_sls={27}
        )
        assert result[0]["供职单位"] == "中共中央政治局"

    def test_without_skip_sls_applies_override(self):
        ep = self._make_ep(5, "浙江省政府", "副省长", "2010.00")
        cache = {
            "张三||ep_batch||sl5||浙江省政府||副省长||2010.00||供职单位": {
                "verdict": "自行修正", "correct_value": "浙江省人民政府"
            }
        }
        result = _apply_step1_field_overrides(
            [ep], {"episodes": []}, cache, "张三"
        )
        assert result[0]["供职单位"] == "浙江省人民政府"

    def test_skip_sls_only_skips_matching_sl(self):
        ep_skip = self._make_ep(27, "A单位", "书记", "2017.00")
        ep_apply = self._make_ep(5, "B单位", "副省长", "2010.00")
        cache = {
            "李强||ep_batch||sl27||A单位||书记||2017.00||供职单位": {
                "verdict": "自行修正", "correct_value": "WRONG"
            },
            "李强||ep_batch||sl5||B单位||副省长||2010.00||供职单位": {
                "verdict": "自行修正", "correct_value": "B单位修正版"
            },
        }
        result = _apply_step1_field_overrides(
            [ep_skip, ep_apply], {"episodes": []}, cache, "李强", skip_sls={27}
        )
        assert result[0]["供职单位"] == "A单位"       # sl=27 skipped
        assert result[1]["供职单位"] == "B单位修正版"  # sl=5 applied


# ── build_merged_episodes_step1 passes skip_sls ──────────────────────────────

class TestBuildMergedEpisodesStep1SkipSls:
    def test_sl_group_overrides_not_overwritten_by_ep_batch(self):
        """If sl_group took over sl=27, ep_batch decisions for that sl are ignored."""
        ds = {"episodes": [
            {"source_line": 27, "供职单位": "中共中央政治局", "职务": "委员",
             "起始时间": "2017.00", "终止时间": ""}
        ]}
        vf = {"episodes": [
            {"source_line": 27, "供职单位": "中共江苏省委", "职务": "书记",
             "起始时间": "2017.00", "终止时间": ""}
        ]}
        # sl_group override: judge said use 3 episodes
        sl_group_decision = {
            "adopt": "LLM1",
            "episodes": [
                {"source_line": 27, "供职单位": "中共中央政治局", "职务": "委员",
                 "起始时间": "2017.00", "终止时间": ""},
                {"source_line": 27, "供职单位": "中共江苏省委", "职务": "书记",
                 "起始时间": "2017.00", "终止时间": ""},
                {"source_line": 27, "供职单位": "江苏省人大常委会", "职务": "主任",
                 "起始时间": "2017.00", "终止时间": ""},
            ]
        }
        # ep_batch that would wrongly overwrite 供职单位 if skip_sls not used
        ep_batch_decision = {
            "verdict": "自行修正",
            "correct_value": "SHOULD_NOT_APPEAR",
        }
        cache = {
            "李强||sl_group||27": sl_group_decision,
            "李强||ep_batch||sl27||中共中央政治局||委员||2017.00||供职单位": ep_batch_decision,
        }
        result = build_merged_episodes_step1("李强", ds, vf, cache)
        units = [ep["供职单位"] for ep in result]
        assert "SHOULD_NOT_APPEAR" not in units
        assert len(result) == 3
