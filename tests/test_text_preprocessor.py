"""Tests for text_preprocessor.py: _is_career_line, _extract_start_ym, _is_honor_line."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from text_preprocessor import _is_career_line, _extract_start_ym, _is_honor_line


# ── _is_career_line ────────────────────────────────────────────────────────────

class TestIsCareerLine:
    def test_dot_date_range(self):
        assert _is_career_line("1978.10-1982.07 北京工业学院光学工程系学习")

    def test_dot_date_open(self):
        assert _is_career_line("2020.03- 任浙江省省长")

    def test_dash_nian_range(self):
        assert _is_career_line("1975—1978年 在北京大学学习")

    def test_single_date(self):
        assert _is_career_line("2025年9月 任中共浙江省委书记")

    def test_nian_only(self):
        assert _is_career_line("1973年参加工作")

    def test_empty_string(self):
        assert not _is_career_line("")

    def test_honor_line_not_career(self):
        assert not _is_career_line("中共十九大代表")

    def test_plain_text_not_career(self):
        assert not _is_career_line("他是一位优秀的领导干部")

    def test_dot_comma_format(self):
        assert _is_career_line("2023.03，任国务院副总理")


# ── _extract_start_ym ─────────────────────────────────────────────────────────

class TestExtractStartYm:
    def test_dot_date_range(self):
        assert _extract_start_ym("1978.10-1982.07 北京大学") == (1978, 10)

    def test_dot_date_open(self):
        assert _extract_start_ym("2020.03- 任省长") == (2020, 3)

    def test_dash_nian(self):
        assert _extract_start_ym("1975—1978年 学习") == (1975, 0)

    def test_single_date(self):
        assert _extract_start_ym("2025年9月 任书记") == (2025, 9)

    def test_unparseable(self):
        assert _extract_start_ym("一些普通文字") is None

    def test_year_open(self):
        assert _extract_start_ym("2022-中央政治局委员") == (2022, 0)


# ── _is_honor_line ─────────────────────────────────────────────────────────────

class TestIsHonorLine:
    def test_party_congress(self):
        assert _is_honor_line("中共十九大代表")

    def test_npc_delegate(self):
        assert _is_honor_line("全国人大代表")

    def test_cppcc_member(self):
        assert _is_honor_line("全国政协委员")

    def test_normal_text(self):
        assert not _is_honor_line("1990.01-1995.06 任省长")

    def test_empty_string(self):
        assert not _is_honor_line("")
