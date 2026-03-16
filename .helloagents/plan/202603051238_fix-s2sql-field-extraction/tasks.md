# 任务清单: fix-s2sql-field-extraction

```yaml
@feature: fix-s2sql-field-extraction
@created: 2026-03-05
@status: pending
@mode: R2
```

<!-- LIVE_STATUS_BEGIN -->
状态: pending | 进度: 0/5 (0%) | 更新: 2026-03-05 12:38:00
当前: -
<!-- LIVE_STATUS_END -->

## 进度概览

| 完成 | 失败 | 跳过 | 总数 |
|------|------|------|------|
| 0 | 0 | 0 | 5 |

---

## 任务列表

### 1. common - JOIN 字段抽取

- [ ] 1.1 在 `common/src/main/java/com/tencent/supersonic/common/jsqlparser/SqlSelectHelper.java` 中把 JOIN ON/USING 字段纳入 `getAllSelectFields()` 抽取结果
- [ ] 1.2 在 `common/src/test/java/com/tencent/supersonic/common/jsqlparser/SqlSelectHelperTest.java` 增加单测覆盖 JOIN ON/USING 字段抽取
  - 依赖: 1.1

### 2. headless-core - S2SQL 解析字段修复

- [ ] 2.1 在 `headless/core/src/main/java/com/tencent/supersonic/headless/core/translator/parser/SqlQueryParser.java` 修复 alias 与字段同名导致的字段误删问题
- [ ] 2.2 在 `headless/core/src/test/java/com/tencent/supersonic/headless/core/translator/parser/` 增加单测覆盖 `SUM(x) AS x` 场景的指标识别
  - 依赖: 2.1

### 3. 验证

- [ ] 3.1 运行 `mvn -pl common,headless/core test` 确认单测通过
- [ ] 3.2 重启 Supersonic 后在浏览器“销售数据分析助手”发送“每个产品的销售额”，确认不再出现 join key 缺失/中文字段当物理列的报错

---

## 执行日志

| 时间 | 任务 | 状态 | 备注 |
|------|------|------|------|

---

## 执行备注

> 记录执行过程中的重要说明、决策变更、风险提示等
