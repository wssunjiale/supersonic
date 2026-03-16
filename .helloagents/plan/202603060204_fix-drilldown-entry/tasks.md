# 任务清单: fix-drilldown-entry

```yaml
@feature: fix-drilldown-entry
@created: 2026-03-06
@status: paused
@mode: R2
```

<!-- LIVE_STATUS_BEGIN -->
状态: paused | 进度: 3/4 (75%) | 更新: 2026-03-06 03:20:33
当前: 2.2 待你在 UI/接口侧确认（recommendedDimensions + 下钻入口）
<!-- LIVE_STATUS_END -->

## 进度概览

| 完成 | 失败 | 跳过 | 总数 |
|------|------|------|------|
| 3 | 0 | 0 | 4 |

---

## 任务列表

### 1. Root cause & 修复（headless/server）

- [√] 1.1 修复 `SchemaServiceImpl`：构建 `MetricSchemaResp` 时合并 `metric.relateDimension` 与 `model.drillDownDimensions`，补齐 `relateDimension.drillDownDimensions` | depends_on: []
- [√] 1.2 补单测：覆盖“metric 无 drillDown、model 有 drillDown”的合并与去重逻辑 | depends_on: [1.1]

### 2. 验证与回归检查

- [√] 2.1 运行最小测试集（至少 `headless/server` 相关单测）；确认无新增失败 | depends_on: [1.2]
- [?] 2.2 端到端自检：触发一次图表查询，确认返回 `recommendedDimensions` 且前端显示下钻入口；若仍缺失，继续排查 `simpleMode/queryMode` gating | depends_on: [2.1]

---

## 执行日志

| 时间 | 任务 | 状态 | 备注 |
|------|------|------|------|
| 2026-03-06 02:48:10 | 1.1 | completed | headless/server: SchemaServiceImpl 合并模型默认下钻维度 |
| 2026-03-06 02:49:30 | 1.2 | completed | 新增 TestNG 单测覆盖 model drillDown 继承 |
| 2026-03-06 02:50:32 | 2.1 | completed | `mvn -pl headless/server -Dtest=SchemaServiceImplTest test` 通过 |
| 2026-03-06 03:20:33 | 2.2 | pending | 需要 UI/接口侧确认推荐维度与下钻入口是否恢复 |

---

## 执行备注

> 记录执行过程中的重要说明、决策变更、风险提示等
