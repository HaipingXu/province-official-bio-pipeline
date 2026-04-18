"""Tests for diff.py: group_by_source_line, _diff_single_pair, compute_verdict."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from diff import group_by_source_line, _diff_single_pair, compute_verdict


# ── compute_verdict ────────────────────────────────────────────────────────────

class TestComputeVerdict:
    def test_pass(self):
        assert compute_verdict(0, 0) == "PASS"

    def test_pass_one_medium(self):
        assert compute_verdict(0, 1) == "PASS"

    def test_needs_review_one_high(self):
        assert compute_verdict(1, 0) == "NEEDS_REVIEW"

    def test_needs_review_two_medium(self):
        assert compute_verdict(0, 2) == "NEEDS_REVIEW"

    def test_major_conflict_two_high(self):
        assert compute_verdict(2, 0) == "MAJOR_CONFLICT"

    def test_major_conflict_one_high_two_medium(self):
        assert compute_verdict(1, 2) == "MAJOR_CONFLICT"


# ── group_by_source_line ───────────────────────────────────────────────────────

class TestGroupBySourceLine:
    def test_normal_grouping(self):
        episodes = [
            {"source_line": 1, "供职单位": "A"},
            {"source_line": 1, "供职单位": "B"},
            {"source_line": 2, "供职单位": "C"},
        ]
        groups = group_by_source_line(episodes)
        assert len(groups) == 2
        assert len(groups[1]) == 2
        assert len(groups[2]) == 1

    def test_empty_list(self):
        assert group_by_source_line([]) == {}

    def test_fallback_to_position(self):
        episodes = [{"供职单位": "A"}, {"供职单位": "B"}]
        groups = group_by_source_line(episodes)
        assert 1 in groups
        assert 2 in groups

    def test_single_episode(self):
        episodes = [{"source_line": 5, "供职单位": "X"}]
        groups = group_by_source_line(episodes)
        assert list(groups.keys()) == [5]


# ── _diff_single_pair ──────────────────────────────────────────────────────────

class TestDiffSinglePair:
    def test_identical_episodes(self):
        ep = {"起始时间": "1990.01", "终止时间": "1995.06",
              "组织标签": "政府", "供职单位": "浙江省政府",
              "职务": "省长", "任职地（省）": "浙江",
              "任职地（市）": "", "中央/地方": "地方"}
        assert _diff_single_pair(ep, ep, 1) == []

    def test_date_small_diff_pass(self):
        ep1 = {"起始时间": "1990.01", "终止时间": "", "组织标签": "",
               "供职单位": "", "职务": "", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        ep2 = {"起始时间": "1990.06", "终止时间": "", "组织标签": "",
               "供职单位": "", "职务": "", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        diffs = _diff_single_pair(ep1, ep2, 1)
        # 5 months < DATE_DISCREPANCY_YEARS (1 year) -> no diff
        assert len(diffs) == 0

    def test_date_large_diff_high(self):
        ep1 = {"起始时间": "1990.01", "终止时间": "", "组织标签": "",
               "供职单位": "", "职务": "", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        ep2 = {"起始时间": "1993.01", "终止时间": "", "组织标签": "",
               "供职单位": "", "职务": "", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        diffs = _diff_single_pair(ep1, ep2, 1)
        assert len(diffs) == 1
        assert diffs[0]["level"] == "HIGH"

    def test_org_name_normalized_same(self):
        """供职单位 differs in text but normalizes to same -> no diff."""
        ep1 = {"起始时间": "", "终止时间": "", "组织标签": "",
               "供职单位": "浙江省人民政府", "职务": "", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        ep2 = {"起始时间": "", "终止时间": "", "组织标签": "",
               "供职单位": "浙江省政府", "职务": "", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        diffs = _diff_single_pair(ep1, ep2, 1)
        assert len(diffs) == 0

    def test_position_diff_medium(self):
        ep1 = {"起始时间": "", "终止时间": "", "组织标签": "",
               "供职单位": "", "职务": "省长", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        ep2 = {"起始时间": "", "终止时间": "", "组织标签": "",
               "供职单位": "", "职务": "副省长", "任职地（省）": "",
               "任职地（市）": "", "中央/地方": ""}
        diffs = _diff_single_pair(ep1, ep2, 1)
        assert len(diffs) == 1
        assert diffs[0]["level"] == "MEDIUM"
