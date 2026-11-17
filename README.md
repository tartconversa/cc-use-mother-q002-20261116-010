# LoopLake 回收物料数据台

这是面向社区回收运营分析员的本地数据湖与数仓服务。它将智能回收箱投递、地磅入出站、转运和兑付事件保存为不可改写的原始记录，并生成带有效期维度、物料对账、付款校验和冻结发布的街道月度视图。所有示例均为合成数据。

## 使用

运行环境为 Python 3.11+；依赖版本见 `requirements.txt`。测试：`python -m unittest discover -s tests -v`；语法检查：`python -m compileall -q looplake`；完整本地演示：`python -m looplake.demo`。HTTP 服务可运行 `uvicorn looplake.api:app`，并在 `/docs` 查阅接口；CLI 为 `python -m looplake.cli --help`。设置 `LOOPLAKE_DB` 可改变 API SQLite 路径。

先登记来源和带有效期的街道、社区、设备、物料、站点与人员维度，再按来源导入事件；同一来源和外部 ID 的不同正文会拒绝，完全重复事件幂等返回。维度未就绪的输入进入隔离状态，登记维度后可按 ingestion ID 重试。批量导入支持清单指纹和检查点，防止使用不同清单误续跑。

同一物料批次的投递与地磅入站重量需在容差内相符；不同物料、出站超入站、付款与费率不符均产生可解释的差异。街道月度报表仅纳入已解决批次，不会将投递、地磅与转运三条记录相加。发布前读取预览指纹；发布时再次校验数据与不可变名称。冻结版保留聚合、对账和事实血缘，日后的新批次不会修改旧版。

## 地磅称重更正链

地磅净重更正以 `weigh_correction` 事件入湖，必须携带原单据号 `corrects_external_id`、更正后的 `weight_kg` 与单据签发时间 `corrected_at`；入湖时间 `received_at` 独立记录。系统保留两条时间线：替代称重继承原单据的 `happened_at`（发生时间），更正节点保留自己的 `corrected_at` 与 `received_at`（到达时间）。

更正不会删除或改写原事实：每次受理都插入替代事实、把前置事实置为 `superseded`，并在 `weigh_corrections` 中记录替代链（`root_fact_id`、`superseded_fact_id`、`sequence_no`）。当前有效称重即链头（`state='active'`），批次守恒、站点库存、月度集市与查询视图只统计 active 事实，因此任何时刻每条称重只计一次；已发布冻结版保留原事实 ID 与原数值，仍可解释当时的关账。

异常处理均显式化，不产生双计或静默回退：前置单据未到则判 `correction_chain_broken` 并隔离，前置到达后自动按依赖顺序治愈重放；重复送达幂等返回同一事实；引用已被替代的原单据另起分支判 `correction_chain_diverged`；更正值与当前链头相同判 `correction_unchanged`；隔离中的更正会阻断相关月份的预览与发布，与月结并行时旧预览指纹失效（`stale_preview`）。链查询见 `GET /facts/{fact_id}/corrections`，待处理隔离见 `GET /corrections/pending`，手动重放见 `POST /corrections/heal`。端到端合成验收运行 `python -m looplake.acceptance`。

## 输入约定

时间带时区，重量用 kg，金额用元；业务日期由事件时间换算为 UTC 后记录。维度的 valid_to 为开区间。来源 channel 与事件 channel 必须一致。演示代码 `looplake/demo.py` 展示完整输入和可观察输出。HTTP 错误返回 `code` 和 `detail`；422 表示输入形状不合法，409 表示版本或业务冲突。
