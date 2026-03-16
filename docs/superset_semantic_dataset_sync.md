# Supersonic 语义 DataSet → Superset Dataset 同步（含 Dataset Saved Metrics）

本文档说明如何将 Supersonic 的语义 DataSet 映射并同步到 Superset 的 Dataset，并在 **Dataset 级别沉淀 saved metrics** 以供多个图表统一复用。

## 1. 映射规则（核心）

### 1.1 Dataset（Superset）
- 每个 Supersonic 语义 DataSet 会映射为 1 个 Superset Dataset（Virtual Dataset）。
- Dataset 的 `sql` 由语义层翻译得到（S2SQL → physical SQL）。

### 1.2 Columns（字段）
仅同步两类字段：
1) 该 DataSet 已配置的维度（Dimension）
2) 指标表达式依赖字段（从展开后的 metric expression 中抽取到的字段名）

> 目的：避免把“宽表全字段”全部同步到 Superset；同时保证 saved metrics 可正确计算。

### 1.3 Saved Metrics（统一复用）
Supersonic DataSet 中选中的指标，会在 Superset Dataset 上创建/更新为 **saved metrics**：
- `metric_type=SQL`
- `expression=<展开后的指标表达式>`

## 2. 同步触发（实时 + 鲁棒）

### 2.1 实时触发
语义 DataSet 创建/更新后会触发同步：
- 写入/更新 registry 记录（`s2_superset_dataset`，`sync_state=PENDING`）
- 推送一次 Superset dataset 同步任务

### 2.2 定时重试
定时器会周期性同步以下记录（避免高频无谓同步）：
- `sync_state=PENDING`
- `sync_state=FAILED` 且 `sync_error_type=RETRYABLE` 且 `next_retry_at <= now()`

Superset 不在线/网络异常会被分类为 `RETRYABLE` 并按指数退避写入 `next_retry_at`，到期后自动重试。

## 3. Registry 表结构（重要）

同步的 SSOT（单一事实来源）是元数据库表：`s2_superset_dataset`。为支持“只同步待同步/失败 + 重试 + 失败原因展示”，该表需要包含以下列：
- `source_type`
- `sync_state`
- `sync_attempt_at`
- `next_retry_at`
- `retry_count`
- `sync_error_type`
- `sync_error_msg`

### 3.1 自动迁移（推荐）
默认在 Superset sync 启用时，会在应用启动阶段 best-effort 补齐缺失列/索引：
配置项：
- `s2.superset.sync.auto-migrate-registry-schema`（默认 `true`）
- 环境变量：`S2_SUPERSET_SYNC_AUTO_MIGRATE_REGISTRY_SCHEMA`（默认 `true`）

### 3.2 手动迁移（可选）
不启动服务，仅执行 schema 迁移 CLI（可用于上线前/排障）：
- `com.tencent.supersonic.headless.server.tools.SupersetDatasetRegistrySchemaMigrateCli`

参数：
- `--dry-run`：只打印计划 DDL
- `--jdbc-url=... --username=... --password=...`：可显式指定连接

环境变量回退（与 standalone 配置一致）：
- `S2_DB_TYPE`、`S2_DB_HOST`、`S2_DB_PORT`、`S2_DB_DATABASE`、`S2_DB_USER`、`S2_DB_PASSWORD`

## 4. 管理与手动同步

前端页面：`/supersetDataset`（系统管理员可见）
- 列表默认展示“需要同步”的 registry 记录（`needSync=true`）
- 支持：单条同步 / 批量同步 / 全量同步
- 展示：`syncState`、`syncErrorMsg`、`nextRetryAt`、`retryCount`、`syncedAt`

对应后端接口：
- `POST /api/semantic/superset/datasets/query`
- `POST /api/semantic/superset/datasets/{id}/sync`
- `POST /api/semantic/superset/datasets/syncBatch`
- `POST /api/semantic/superset/sync/datasets`（语义 DataSet 全量触发）

## 5. 校验建议（Superset 侧）

同步后可在 Superset UI 里校验：
1) Dataset Columns 只包含“维度 + 指标依赖字段”
2) Dataset Metrics 中存在 saved metrics，表达式与 Supersonic 展开后指标一致

