这个目录专门用于模型敏感性 live test，不参与主流程，也不会被主流程导入。

当前脚本会使用 浙江 真实日志中的 习近平 样本，分别对以下模型执行 step1、step2、step3、step4 四个任务：

- deepseek-v4-pro
- deepseek-v4-flash
- doubao-seed-2-0-pro-260215
- qwen3.6-plus

运行方式：

```bash
/Users/xuhaiping/Desktop/Workflow省级官员/.venv/bin/python temp/model_sensitivity/run_xi_sensitivity.py
```

输出：

- temp/model_sensitivity/xi_model_sensitivity_report.json

报告字段说明：

- status: success / blocked / parse_error / schema_error / api_error
- content_sensitive: 是否命中内容风控或安全拦截

样本说明：

- step1 到 step4 使用真实日志中的聚焦片段，而不是整省全量回放
- 当前聚焦 source_line 13-17，也就是 1998-2007 年、从福建省长到浙江再到上海的关键区段
- step4 仍保留真实 bio_summary，但履历部分只保留上述真实区段，用于更快比较模型对该敏感人物任务的风控反应
