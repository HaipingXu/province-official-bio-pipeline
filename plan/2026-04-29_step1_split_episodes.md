# Plan — Step1 多 Episode 拆分（ep_batch 诊断 + sl_group 接管拆分）

> 目标：解决 issue #2 — 裁判正确识别"原文一行 = 多职兼任 = 应拆为多 episode"，
> 但当前 ep_batch 只能塞 `correct_value="A；B；C"` 字符串，merger 无能力拆分。
>
> **设计：保留两路裁判分工，让职责更清晰**：
> - **ep_batch（字段级）**：只负责"诊断"——发现该拆 → 返回 `verdict="需拆分"`（不必给具体怎么拆）
> - **sl_group（整段级）**：已有的 episodes-array 输出能力直接复用，负责"怎么拆"
> - 中间编排：ep_batch 跑完后，凡返回 `需拆分` 的 sl 升级到 sl_group 二次调用
>
> 这样 ep_batch 的 schema 改动**很小**（只多一个枚举值），merger 也**几乎不动**（拆分逻辑沿用 sl_group 现成路径）。

---

## 问题速记（来自 2026-04-28 浙江跑批）

```
李强 sl=27   → "中共中央政治局；中共江苏省委；江苏省人大常委会"
习近平 sl=9  → "中共福建省委、中共福州市委、福州市人大常委会、福州军分区"
车俊 sl=17   → "需拆分提取：新疆生产建设兵团和中国新建集团公司"   ← 裁判直接写中文
万学远 sl=1  → "共青团上海交通大学委员会；上海市青年联合会"
```

根因：ep_batch 路径 schema 只允许 `correct_value: str`，裁判想表达"该拆"时无合法格式 → 用分隔符或自然语言塞字符串 → merger 写回单 episode。

---

## 数据流（改动后）

```
diff_step1
  │
  ├── len(ds_list) ≠ len(vf_list)  ──→  pending_group_calls
  │       │
  │       └─ judge_source_line_group  (返回 episodes 数组，已支持)
  │
  └── len(ds_list) == len(vf_list) ──→  pending_episode_calls
          │
          └─ judge_episode_batch  (字段级 verdict)
                │
                ├─ 普通 verdict (采纳/自行修正/两者均存疑) → 写回 ep_batch cache
                │
                └─ verdict="需拆分"  ★ 新增
                       │
                       └─ 把 (name, sl) 加进 pending_group_calls 第二轮
                              │
                              └─ judge_source_line_group 二次调用
                                     │
                                     └─ 返回 episodes 数组，写回 sl_group cache

merged_builder.build_merged_episodes_step1
  ├── _apply_sl_group_overrides   ← 接管所有"需要展开多 episode"的情况（不变）
  └── _apply_step1_field_overrides ← 仅处理字段级 verdict（不再需要懂拆分）
```

---

## 改动清单

### 1. 裁判 ep_batch prompt（`judge.py`）

**位置**：`_EPISODE_BATCH_INSTRUCTIONS` 附近 (judge.py 约 70-90 行，`_judge_system()`)

**新增 verdict 枚举值**：`需拆分`

**新约束（写进 prompt 文字）**：

> ⚠️ `correct_value` 字段级规则：
>
> | 字段 | 允许 `、` | 允许 `；` / `和` | 说明 |
> |---|---|---|---|
> | **供职单位** | ❌ 禁止 | ❌ 禁止 | 含分隔符 ⇒ 必须走 `verdict="需拆分"` |
> | **职务** | ✅ 允许 | ❌ 禁止 | 仅当**同一供职单位内**多个职务时（如"副书记、政法委书记"） |
> | **起始时间 / 终止时间** | ❌ 禁止 | ❌ 禁止 | 单值 |
>
> 触发 `需拆分` 的判定标准：**只看供职单位是否需要分成多个**。
> 若原文一行实际属于多个机构（如"任 X 省委副书记、Y 市委书记"），
> 输出 `{"verdict": "需拆分"}` 即可，**不必给 episodes 数组**——
> 后续会有专门一轮调用让你看完整上下文再决定怎么拆。
>
> 同单位多职务（如"中共浙江省委 + 副书记、政法委书记"）保持单 episode，
> `correct_value` 用 `、` 连接职务，**不要走需拆分**。

**关键**：ep_batch schema 只多 1 个枚举值，**不需要 `correct_episodes` 字段**。

---

### 2. sl_group prompt 微调（`judge.py:judge_source_line_group`）

现有 sl_group 已能返回 `episodes` 数组（看 `merged_builder._apply_sl_group_overrides` 的 `judge_eps = override.get("episodes", [])` 处理路径）。

**唯一可能要加**：在 sl_group 的 prompt 里增加一句明确说明，"在以下两种场景被调用"：
1. LLM1/LLM2 给出的 episode 数不一致
2. **新增**：ep_batch 判定该 source_line 需要拆分（即便两边都给了 1 条 episode）

如果 sl_group prompt 已经足够通用（"按原文真实情况输出 episodes 数组"），就不需要改。**先看代码再决定**。

---

### 3. Pipeline 编排：ep_batch → sl_group 升级（`judge.py:judge_step1`）

**位置**：`judge_step1` 第 458-486 行附近，`_run_judge_tasks` 调用之后。

**当前**：
```python
_run_judge_tasks(pending_group_calls, _judge_grp, ...)
_run_judge_tasks(pending_episode_calls, _judge_ep, ep_dummy, ...)
```

**新增第三步**：
```python
# Phase A: 整段不一致的 sl
_run_judge_tasks(pending_group_calls, _judge_grp, judge_cache, ...)

# Phase B: 字段级 ep_batch
_ep_dummy = {}
_run_judge_tasks(pending_episode_calls, _judge_ep, _ep_dummy, ...)

# Phase C ★ 新增：ep_batch 中判出"需拆分"的 sl 升级到 sl_group
need_split_sls = _collect_need_split_from_epbatch(judge_cache, name_sl_lookup)
secondary_group_calls = []
for (name, sl) in need_split_sls:
    cache_key = _sl_group_cache_key(name, sl)
    if cache_key in judge_cache and not _is_downgraded(judge_cache[cache_key]):
        continue   # 已经被 Phase A 判过且合法，跳过
    secondary_group_calls.append((cache_key, {
        "name": name, "line_num": sl,
        "raw_text": career_lines_by_name[name][sl],
        "ds_episodes": ds_groups_by_name[name].get(sl, []),
        "vf_episodes": vf_groups_by_name[name].get(sl, []),
    }))
if secondary_group_calls:
    logger.info(f"  Phase C: ep_batch 升级到 sl_group 共 {len(secondary_group_calls)} 个 sl")
    _run_judge_tasks(secondary_group_calls, _judge_grp, judge_cache, ...)
```

**注意**：
- Phase A 已存的 sl_group 决策**不重判**（除非已降级）
- Phase B 的 ep_batch decisions 留在 cache 里，但 merger 优先读 sl_group（见 #4）
- `name_sl_lookup`、`ds_groups_by_name`、`vf_groups_by_name` 需在 Phase A 之前就构建好供 Phase C 复用，不要重新解析 diff_report

---

### 4. Merger 优先级调整（`merged_builder.py`）

**位置**：`build_merged_episodes_step1` 第 154-187 行

**当前流程**：
```python
overrides = _get_sl_group_overrides(step1_judge_cache, official_name)
if overrides:
    episodes = _apply_sl_group_overrides(...)
episodes = _apply_step1_field_overrides(...)   # 字段级覆盖
```

**新流程**：
```python
overrides = _get_sl_group_overrides(step1_judge_cache, official_name)
if overrides:
    episodes = _apply_sl_group_overrides(...)   # 已包含 Phase C 升级后的拆分结果

# 字段级覆盖：跳过被 sl_group 接管的 sl
episodes = _apply_step1_field_overrides(
    episodes, vf_data, step1_judge_cache, official_name,
    skip_sls=set(overrides.keys()),   # ← 新增参数
)
```

`_apply_step1_field_overrides` 内部加一行：
```python
for ep in episodes:
    sl = ep.get("source_line", 0)
    if sl in skip_sls:
        result.append(ep)
        continue
    ...
```

避免 sl_group 覆盖后字段级再回写。

**关键**：merger **不需要懂"需拆分"** verdict——它只需要知道"该 sl 已被 sl_group 接管"。

---

### 5. Schema 校验 + 降级（`judge.py:_call_judge` 后置）

裁判 LLM 偶尔会忽略 schema 约束。需要 post-validate：

```python
# 按字段决定哪些分隔符是非法的
_INVALID_SEP_BY_FIELD = {
    "供职单位": ("；", ";", "、", "和"),    # 任何分隔符都说明多单位 → 必须 需拆分
    "职务":    ("；", ";", "和"),           # 、 合法（同单位多职）
    "起始时间": ("；", ";", "、"),
    "终止时间": ("；", ";", "、"),
}

def _validate_field_decision(decision: dict, field: str) -> dict:
    """规范化裁判返回的字段级决策；不合规则降级，并打 _downgraded 标记。

    `_downgraded` 是内部诊断字段：表示裁判返回的 schema 不合规，
    被代码自动降级。事后从 step1_judge_decisions.json 里 grep 出来
    可以监控裁判 prompt 遵循度（目标：占比 < 5%）。
    """
    verdict = decision.get("verdict", "")
    cv = decision.get("correct_value", "")
    invalid_seps = _INVALID_SEP_BY_FIELD.get(field, ())

    if verdict in ("自行修正", "两者均存疑") and cv:
        # 1) "需拆分" 文字塞在 correct_value 里 → 自动转成 verdict="需拆分"
        if "需拆分" in cv or "拆分提取" in cv:
            logger.warning(
                f"[judge schema] field={field} 'correct_value 含需拆分文字': {cv!r} → 转 needsplit"
            )
            decision["verdict"] = "需拆分"
            decision["correct_value"] = ""
            decision["_downgraded"] = True
            decision["_downgrade_reason"] = "natural_language_recovered_to_needsplit"

        # 2) 字段不允许的分隔符
        elif any(sep in cv for sep in invalid_seps):
            if field == "供职单位":
                # 供职单位含分隔符 = 多单位 = 该拆 → 自动转 需拆分
                logger.warning(
                    f"[judge schema] 供职单位含分隔符: {cv!r} → 转 needsplit"
                )
                decision["verdict"] = "需拆分"
                decision["correct_value"] = ""
                decision["_downgraded"] = True
                decision["_downgrade_reason"] = "multivalue_unit_recovered_to_needsplit"
            else:
                # 其他字段不该有这些分隔符 → 直接降级
                logger.warning(
                    f"[judge schema] field={field} 含非法分隔符: {cv!r} → 降级"
                )
                decision["verdict"] = "两者均存疑"
                decision["correct_value"] = ""
                decision["_downgraded"] = True
                decision["_downgrade_reason"] = f"invalid_separator_in_{field}"

    if decision.get("_downgraded"):
        from failures import FAILURES
        FAILURES.record(
            scope="judge_schema", source="judge", step="step1_validate",
            name=decision.get("_meta_key", "?"),
            error=decision.get("_downgrade_reason", "unknown"),
        )
    return decision
```

**升级语义**：原本只能"降级"（当作 `两者均存疑`），现在多一种"侧推"——把多值字符串自动转成 `需拆分`，让 Phase C 接管。这样裁判即使没遵守 schema，效果上等价于走对了路径。

调用点：插在 `judge_episode_batch` 解析完 JSON、写 cache 之前，对每个字段调一次。

---

## 实施顺序（约 2.5h）

1. **测试数据准备**（5 min）
   - 从 `logs/浙江/step1_judge_decisions.json` 找出 `correct_value` 含分隔符的 ep_batch 条目（约 10-20 条）
   - 复制到 `tests/fixtures/multi_value_decisions.json`

2. **改 ep_batch prompt**（15 min）
   - `judge.py` 加 `需拆分` 枚举 + 字段级分隔符规则
   - 强调"只诊断不必给 episodes 数组"

3. **加 schema 校验 + 升级路径**（45 min）
   - 新增 `_validate_field_decision` 和 `_INVALID_SEP_BY_FIELD`
   - 单测：4 个场景（合规 / 供职单位多值升级 / 职务多值降级 / 自然语言识别）

4. **改编排 Phase C**（45 min）
   - `judge_step1` 加 ep_batch → sl_group 升级流程
   - 复用 `pending_group_calls` 的执行函数 `_judge_grp`
   - 单测：mock 一个 ep_batch 返回 `需拆分`，验证 sl_group 被二次调用

5. **改 merger 优先级**（20 min）
   - `_apply_step1_field_overrides` 加 `skip_sls` 参数
   - `build_merged_episodes_step1` 把 `set(overrides.keys())` 传进去
   - 单测：sl_group 接管的 sl，字段级 cache 不应再写回

6. **集成测试 — 重跑浙江**（30 min）
   ```bash
   uv run main_province.py --province 浙江 --force
   ```
   预期：
   - 李强 sl=27 → 3 个独立 episode
   - 习近平 sl=9 → 4 个独立 episode
   - 车俊 sl=17 → 不再有"需拆分提取：..."
   - 万学远 sl=1 → 2 个独立 episode

---

## 风险 / 边界

- **裁判 LLM 对新 schema 的遵循度**：DeepSeek-v4-pro 一般遵循 OK，但需要在 prompt 加 ⚠️ 强调 + 明确反例。第一次跑要监控降级率
- **Phase C 调用成本**：仅在 ep_batch 判出"需拆分"时触发，预计每省 < 20 次调用，可接受
- **同 sl 在 Phase A 和 Phase C 都被处理**：Phase A 优先（因为已经是 group-level 决策），Phase C 跳过该 sl
- **拆分后 episode 时间字段空缺**：sl_group 已有逻辑允许时间字段为空，由后续 step2/3/4 处理
- **经历序号重排**：`_apply_sl_group_overrides` 末尾已重排过（第 103-104 行），无需额外处理

---

## 涉及文件

```
judge.py               — ep_batch prompt + _validate_field_decision + Phase C 编排
merged_builder.py      — _apply_step1_field_overrides 加 skip_sls 参数
tests/test_split.py    — 新建，覆盖 schema 校验 + 升级路径 + merger 跳过
tests/fixtures/multi_value_decisions.json — 测试数据
```

不动：`extraction.py`, `diff.py`, `postprocess.py`, `export.py`, `merger._apply_sl_group_overrides`（直接复用）

---

## 验收标准

- [ ] 浙江重跑：4 个已知多值 case 全部正确拆分为多 episode
- [ ] battle1.xlsx 不再有 `；/、` 出现在 correct_value 列
- [ ] Phase C 升级次数日志记录可读（每个 sl 升级原因清晰）
- [ ] final_rows 行数比改前略多（合理增量），人物列表不漏不重

---

## 与上一版 plan 的差别

上一版要求 ep_batch 直接生成 `correct_episodes` 数组、merger 直接接住拆分。
本版改为 ep_batch 仅返回 `verdict="需拆分"`、由 sl_group 第二轮接管生成 episodes。

| 维度 | 上一版 | 本版 |
|---|---|---|
| ep_batch schema 改动 | 大（新增 array 字段 + 拆分规则）| 小（仅多 1 枚举值） |
| sl_group 改动 | 无 | 极小（prompt 微调或不动）|
| merger 改动 | 大（新增拆分分支 + 字段并行拆分）| 小（仅加 skip_sls 参数）|
| 拆分质量 | ep_batch 上下文有限 | sl_group 看到完整 raw text，质量更高 |
| 多 1 次调用 | 否 | 是（仅在真要拆的少数 case）|
