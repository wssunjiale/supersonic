# 按查询结果固化为 Superset 数据集

- 需求编号：`需求-12`
- 首次提出：`2026-02-05`
- 最后有效确认：`2026-03-05`
- 时效状态：`有效`
- 实现状态：`进行中`
- 需求主线：`Superset 数据集映射认知`

## 当前定义

- 不再把 Supersonic 语义模型直接同步成 Superset dataset，而是把“一次查询生成的最终 SQL 与最终输出列”固化为 Superset dataset，并由 Supersonic 负责命名、说明、tag 与复用治理。

## 用户要求

- 先验证“语义模型并不等于 Superset dataset”这一认知是否正确。
- 一旦认知成立，就删除“把语义模型直接同步到 Superset dataset”的旧实现思路。
- 一次查询产生的最终 SQL 应被固化为一个 Superset dataset。
- dataset 要命名清晰、写好说明、打好 tag，便于后续复用和治理。
- 不要重复创建 dataset；可复用时就复用。
- 复用判定可以先从“SQL 规范化 hash”这类简单规则开始。
- 若最终只涉及一张物理表，可优先按物理 dataset 处理；否则按虚拟 dataset 处理。
- Supersonic 自己要保存和管理这些 dataset 定义，并在启动时同步到 Superset。

## 需求边界

- 这条需求解决的是“Superset dataset 到底应该怎么理解和如何固化”。
- 失败重试、失败原因展示、手动同步入口和离线验收，属于 [建立 Superset 语义同步链路与离线验收运行器](建立Superset语义同步链路与离线验收runner.md)。
- 聊天绘图是否改为完全依赖 Headless 已同步数据集，属于 [基于 Headless 数据集同步链路重构聊天绘图与 Superset 嵌入方案](基于headlessdataset同步链路重构聊天绘图与Superset嵌入方案.md)。

## 验收要点

- 语义模型不再被直接等同为 Superset dataset。
- dataset 的固化对象变成“最终 SQL + 最终输出列 + 查询组合”。
- dataset 具备命名、说明、tag 和复用策略。
- 不同查询如可复用已有 dataset，不会重复创建。

## 归属的 bug / 修复

### 问题 1：把语义模型误当成 Superset dataset

- 现象：早期实现默认把 Supersonic 的语义模型或数据集直接映射为 Superset dataset。
- 根因：没有区分“语义层的查询能力边界”和“Superset 图表必须依赖单个 dataset”的技术事实。
- 处理：把 dataset 的定义对象改为一次查询最终产出的结果结构，而不是语义模型本身。

## 关系

- 替代了 [将语义模型直接同步为 Superset 数据集](将语义模型直接同步为Superset数据集.md)。
- 被 [建立 Superset 语义同步链路与离线验收运行器](建立Superset语义同步链路与离线验收runner.md) 和 [基于 Headless 数据集同步链路重构聊天绘图与 Superset 嵌入方案](基于headlessdataset同步链路重构聊天绘图与Superset嵌入方案.md) 依赖。

## 来源会话

- `2026-02-05` `019c2e68-3250-7371-917d-6139278ba574` 用户首次明确提出“按查询结果 / 最终 SQL 固化 Superset dataset”的新认知。
- `2026-03-05` `019cbe69-b847-7432-9db8-c7782d50ddc6` 用户继续围绕 Supersonic 数据集与 Superset dataset 的映射关系、saved metrics 和复用方式展开讨论。
