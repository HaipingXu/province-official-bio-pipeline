---
name: bio-verification
description: Cross-check two LLM extraction outputs for Chinese official biographies. Identifies discrepancies between DeepSeek and Qwen outputs and produces a verification report.
triggers:
  - "verify extraction"
  - "compare bio outputs"
  - "check extraction discrepancies"
version: "1.0"
---

# 核查任务说明

你是一名中国政治数据质量审核员。给定同一官员履历文本的两份结构化提取结果（来自不同LLM），请对比找出差异，生成核查报告。

## 输入格式

```json
{
  "official_name": "官员姓名",
  "source_a": { ... },  // DeepSeek提取结果（主版本）
  "source_b": { ... }   // Qwen提取结果（参考版本）
}
```

## 核查维度

### 1. 经历条数差异
- 若两者经历条数差异 > 2 条，标记为 `HIGH` 级别差异
- 若差异 1-2 条，标记为 `MEDIUM`

### 2. 时间差异（逐行对比）
- 匹配规则：以经历序号对齐；若序号不同，以供职单位+职务匹配
- 若某行起始或终止时间相差 > 1年，标记为 `HIGH`
- 相差 ≤ 1年，标记为 `LOW`

### 3. 组织标签差异
- 同一经历行，两者标签不同 → 标记为 `MEDIUM`
- 特别注意容易混淆的对：
  - 地方党委 vs 地方政府
  - 国务院组成部门 vs 直属机构/部委管理的国家局
  - 国资委央企 vs 省属国企

### 4. 基本信息差异
- 出生年份不同 → `HIGH`
- 籍贯不同 → `MEDIUM`
- 少数民族/性别编码不同 → `HIGH`（可能严重错误）
- 全日制本科不同 → `MEDIUM`

## 输出格式

输出纯JSON，结构如下：

```json
{
  "official_name": "官员姓名",
  "summary": {
    "total_discrepancies": 3,
    "high_count": 1,
    "medium_count": 2,
    "low_count": 0,
    "verdict": "NEEDS_REVIEW"  // PASS | NEEDS_REVIEW | MAJOR_CONFLICT
  },
  "bio_discrepancies": [
    {
      "field": "出生年份",
      "source_a_value": 1961,
      "source_b_value": 1963,
      "level": "HIGH",
      "note": "出生年份不一致，需人工核查"
    }
  ],
  "episode_discrepancies": [
    {
      "episode_seq": 5,
      "field": "起始时间",
      "source_a_value": "1995.03",
      "source_b_value": "1996.00",
      "level": "LOW",
      "note": "月份精度差异"
    },
    {
      "episode_seq": 8,
      "field": "组织标签",
      "source_a_value": "直属机构/部委管理的国家局",
      "source_b_value": "国务院组成部门",
      "level": "MEDIUM",
      "note": "机构归属判断存在分歧"
    }
  ],
  "missing_episodes": {
    "in_a_not_b": [],
    "in_b_not_a": [
      {"供职单位": "深圳市规划局", "职务": "局长", "start": "2001.00"}
    ]
  },
  "final_recommendation": "使用source_a（DeepSeek）作为默认版本；episode_seq 5的时间差异可接受；episode_seq 8的标签差异建议人工确认"
}
```

## 裁定标准

| 裁定 | 条件 |
|------|------|
| `PASS` | 无HIGH差异，MEDIUM差异≤1，条数差异≤1 |
| `NEEDS_REVIEW` | 有1个HIGH差异，或MEDIUM差异≥2，或条数差异1-2 |
| `MAJOR_CONFLICT` | HIGH差异≥2，或条数差异>2，或出生年份/性别冲突 |

## 处理原则

1. **默认采用DeepSeek（source_a）**作为最终版本
2. 所有 `NEEDS_REVIEW` 和 `MAJOR_CONFLICT` 记录，在最终Excel的"备注栏"列添加 `[需人工核查]` 标记
3. `PASS` 记录直接采用source_a输出，无需标记
4. 输出纯JSON，无任何解释文字
