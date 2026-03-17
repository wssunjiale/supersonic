# CURRENT_STATE.md

## 项目定位
- SuperSonic 融合 Chat BI 与 Headless BI，通过统一语义层支撑自然语言问答与开放查询接口。（状态: 已验证，来源: supersonic/README_CN.md）
- 当前仓库是基于官方 Supersonic master 的二次开发版本，不是从零自建的新产品。（状态: 已验证，来源: supersonic/pr/00-项目基线.md）

## 当前主要目标
- 推进“基于 Headless 数据集同步链路重构聊天绘图与 Superset 嵌入方案”，让聊天绘图与嵌入统一建立在已同步数据集之上。（状态: 已验证，来源: supersonic/pr/01-当前有效需求索引.md）
- 完善图表类型选择与 `form_data` 生成规则，补强 Superset 图表创建链路的可用性与一致性。（状态: 已验证，来源: supersonic/pr/01-当前有效需求索引.md）
- 提升语义查询链路稳定性，并继续推进 Superset 语义同步链路与离线验收 runner。（状态: 已验证，来源: supersonic/pr/01-当前有效需求索引.md）

## 当前重点
- 以 Headless 已同步数据集作为聊天绘图的唯一数据底座，先打通同步、认证、嵌入与前后端联动链路。（状态: 已验证，来源: supersonic/pr/01-当前有效需求索引.md）
- 人工核定 `viztype-online.json`、`form-data-schema/` 与 Superset 图表模板资产是否足够支撑当前图表生成需求。（状态: 已验证，来源: supersonic/pr/01-当前有效需求索引.md）
- 聚焦语义查询稳定性中的空指针、脏数据、字段名解析与 CLI Bean 串用等问题，避免 Chat BI 输出异常结果。（状态: 已验证，来源: supersonic/pr/01-当前有效需求索引.md）

## 最近一次可用的最小验证方式
- `bash scripts/doctor.sh`（状态: 已验证，来源: supersonic/RUNBOOK.md；2026-03-17 当前环境执行通过）
- `bash scripts/smoke.sh`（状态: 已验证，来源: supersonic/RUNBOOK.md；2026-03-17 当前环境执行通过）
- `mvn test`（状态: 未验证，来源: supersonic/AGENTS.md）
- `mvn test -Dtest=ClassName`（状态: 未验证，来源: supersonic/AGENTS.md）
- `cd webapp && pnpm test`（状态: 未验证，来源: supersonic/AGENTS.md）
- `./assembly/bin/supersonic-build.sh standalone`（状态: 未验证，来源: supersonic/AGENTS.md）
- `./assembly/bin/supersonic-daemon.sh start`（状态: 未验证，来源: supersonic/AGENTS.md）

## 已知阻塞/高风险区域
- `chat/`、`headless/`、`launchers/`、`webapp/` 是主业务与运行入口核心区域，改动会同时影响问答链路、语义层与前端体验。（状态: 已验证，来源: supersonic/AGENTS.md）
- `form-data-schema/`、`superset-spec/` 直接关联 Superset 图表模板、schema 与嵌入能力，属于当前需求主线的高敏感目录。（状态: 已验证，来源: supersonic/pr/00-项目基线.md）
- Headless 数据集同步、图表生成与语义查询稳定性三条主线正在并行推进，任何跨目录改动都可能引入联动回归。（状态: 已验证，来源: supersonic/pr/00-项目基线.md）

## 当前不建议碰的区域
- 在没有明确需求卡与联动验证链的情况下，不建议先动 `chat/`、`headless/`、`launchers/`、`webapp/` 的核心流程代码。（状态: 已验证，来源: supersonic/AGENTS.md）
- `form-data-schema/`、`superset-spec/` 与图表模板脚本资产当前用于支撑 Superset 链路，非当前任务不要顺手改 schema 或生成逻辑。（状态: 已验证，来源: supersonic/README_CN.md）

## 首读顺序
1. `supersonic/AGENTS.md`（状态: 已验证，来源: 文件存在）
2. `supersonic/README_CN.md`（状态: 已验证，来源: 文件存在）
3. `supersonic/RUNBOOK.md`（状态: 已验证，来源: 文件存在）
4. `supersonic/pr/01-当前有效需求索引.md`（状态: 已验证，来源: 文件存在）
5. `supersonic/pr/00-项目基线.md`（状态: 已验证，来源: 文件存在）
6. `supersonic/pr/04-问题与修复索引.md`（状态: 已验证，来源: 文件存在）

## 备注
- 本文件只保留项目级入口信息；命令是否实际跑过，统一按“已验证 / 未验证 / 推断”区分，不把 README 或 AGENTS 中的命令默认当成已跑通事实。（状态: 已验证，来源: AI_CONVENTIONS.md）
- 新增 `RUNBOOK.md` 与 `scripts/doctor.sh`、`scripts/smoke.sh` 后，默认排障应先从这三个入口开始，再扩大到完整构建或集成验证。（状态: 已验证，来源: 本次第 3 阶段落地结果）
