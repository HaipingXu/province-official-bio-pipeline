# 省级主官履历数据库 (v9)

> 爬虫阶段已完成。当前关注：`officials/{省}/` biography 文本 → `output/{省}/*.xlsx`

---

## 模块结构

| 文件 | 职责 |
|------|------|
| `extraction.py` | 统一 step1/2/3 提取（LLMConfig 参数化，LLM1+LLM2 共用） |
| `diff.py` | step1/2/3 diff 比较逻辑 |
| `judge.py` | 裁判调用 + judge_step1/2/3 + battle.xlsx |
| `merged_builder.py` | build_merged_episodes + source-line group helpers |
| `postprocess.py` | Phase 4 后处理 → final_rows.json |
| `export.py` | Phase 5 Excel 导出 |
| `main_province.py` | Pipeline 入口 + CLI |
| `utils.py` | llm_chat, LLMConfig, RoundRobinClientPool, TokenCounter |
| `config.py` | API keys, 路径, 列定义, 并发常量 |

---

## 规则（每窗口强制）

1. **不爬虫** — `skip_scrape=True`；需爬取时显式加 `--scrape`
2. **按省分存** — `preprocessed_texts.json` 写 `logs/{省}/`，不写顶层 `logs/`
3. **prompts 只读** — 修改提取行为只改 `prompts/*.md`，不改代码字符串
4. **数据只读** — `officials/` 文本、`data/{省}_officials.txt` 不可覆写
5. **无指令不改代码** — 没有用户明确指令，不得主动修改任何代码文件；诊断结论只报告，等待用户决策后再动手

---

## 权威来源

| 内容 | 权威文件 |
|------|---------|
| Step1/2/3 提取规则 | `prompts/step1_extraction.md` / `step2_rank.md` / `step3_labeling.md` |
| 30列定义与顺序 | `config.py → COLUMNS` |
| 官员名单 | `data/{省份}_officials.txt`（人工维护） |
| 并发上限 | `config.py → LLM1_MAX_WORKERS / LLM2_MAX_WORKERS / JUDGE_MAX_WORKERS` |

---

## Pipeline 流程（v9 interleaved）

```
Phase 0.5 → logs/{省}/preprocessed_texts.json

Phase 1 (Step1 extraction):
  LLM1 + LLM2 并行 → llm1_step1_results.json / llm2_step1_results.json
  → diff → step1_diff_report.json
  → judge → step1_judge_decisions.json + merged_episodes.json

Phase 2 (Step2 rank, 基于 merged_episodes):
  LLM1 + LLM2 并行 → llm1_step2_rank.json / llm2_step2_rank.json
  → diff → step2_diff_report.json
  → judge → step2_judge_decisions.json

Phase 3 (Step3 labels, 基于 merged_episodes):
  LLM1 + LLM2 并行 → llm1_step3_labels.json / llm2_step3_labels.json
  → diff → step3_diff_report.json
  → judge → step3_judge_decisions.json

Phase 4 → logs/{省}/final_rows.json
Phase 5 → output/{省}/{省}_officials.xlsx
```

---

## 环境

**包管理器：`uv`**（唯一权威）

```bash
uv run main_province.py --province 浙江       # 推荐方式
uv add <package>                              # 添加依赖
uv sync                                       # 同步环境
```

> 禁止直接用 `pip install`；依赖变更统一通过 `uv add / uv remove`。

---

## 入口

```bash
uv run main_province.py --province 浙江            # 默认：现有文本，不爬
uv run main_province.py --province 浙江 --skip-extract  # 复用 DS 结果
uv run main_province.py --batch                    # 31省批量
# uv run main_province.py --help                  # 完整选项
```

---

## 官员名单格式（`data/{省份}_officials.txt`）

```
省份：浙江
[省长]
张三, 2000.01-2003.06
李四（代）, 2003.07-2004.01
[省委书记]
王五, 1999.06-2002.10
```

> 技能：`.claude/skills/city-official-db.md` | Agent：`.claude/agents/official-bio-agent.md`
