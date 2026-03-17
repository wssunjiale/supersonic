# RUNBOOK.md

## 适用范围
- `supersonic` 是主业务系统，包含 Chat BI、Headless BI、Standalone launcher 与前端 `webapp`。（状态: 已验证，来源: supersonic/AGENTS.md；supersonic/CURRENT_STATE.md）
- 本手册只回答“先跑什么、失败先看哪里、下一步怎么放大验证”，不替代 `README_CN.md` 与需求文档。（状态: 已验证，来源: AI_CONVENTIONS.md）

## 关键入口
- `bash scripts/doctor.sh`：检查 Java/Maven/Node/pnpm 与关键脚本、关键目录是否就绪。（状态: 已验证，来源: 本次第 3 阶段新增脚本；2026-03-17 当前环境执行通过）
- `bash scripts/smoke.sh`：默认执行 `DateUtilsTest`，用于确认 Maven 测试链最小可用。（状态: 已验证，来源: 本次第 3 阶段新增脚本；2026-03-17 当前环境执行通过）
- `bash scripts/smoke.sh frontend`：可选执行前端测试入口，适合前端改动后补做验证。（状态: 未验证，来源: 本次第 3 阶段新增脚本）
- `./assembly/bin/supersonic-build.sh standalone`：完整构建发布包。（状态: 未验证，来源: supersonic/AGENTS.md）
- `./assembly/bin/supersonic-daemon.sh start`：启动 Standalone 服务。（状态: 未验证，来源: supersonic/AGENTS.md）

## 最小排障顺序
1. 先执行 `bash scripts/doctor.sh`，确认基础命令、脚本和前后端目录都存在。
2. 如果只是确认仓库还能通过最小回归，执行 `bash scripts/smoke.sh`。
3. 如果要验证完整启动链，再执行 `./assembly/bin/supersonic-build.sh standalone` 与 `./assembly/bin/supersonic-daemon.sh start`。
4. 如果启动失败，优先查看 `assembly/bin/` 下脚本、`launchers/` 入口与运行生成的 `logs/error.log`。

## Doctor 重点检查
- Java、Maven、Node、pnpm 是否可执行。
- `pom.xml`、`assembly/bin/supersonic-build.sh`、`assembly/bin/supersonic-daemon.sh`、`webapp/package.json` 是否存在。
- 当前仓库是否同时具备后端构建入口与前端测试入口。

## Smoke 默认做什么
- 默认执行 `mvn -pl common -Dtest=DateUtilsTest test`。（状态: 已验证，来源: `supersonic/common/src/test/java/com/tencent/supersonic/common/DateUtilsTest.java`；2026-03-17 当前环境执行通过）
- 这个用例只覆盖公共模块，不依赖外部数据库或完整服务启动，适合作为“仓库还活着”的最小回归。
- 如果是前端改动，可改跑 `bash scripts/smoke.sh frontend`；如果是更深的后端改动，再扩大到 `mvn test -Dtest=ClassName` 或模块级回归。

## 常见失败与判断
- `java` / `mvn` / `node` / `pnpm` 缺失：先解决本机依赖，不要直接怀疑业务代码。
- `doctor` 通过但 `smoke` 失败：优先判断是公共模块单测回归，还是 Maven/JDK 版本偏差。
- `build.sh` 或 `daemon.sh` 失败：优先看 `assembly/bin/` 环境脚本、`launchers/standalone` 与 `logs/error.log`。
- 前端相关问题：优先回到 `webapp/`，不要在未确认前端依赖齐备前直接推断后端异常。

## 外部依赖与升级验证建议
- 更深层的启动验证通常还需要数据库、配置文件和发布产物，不应把它们塞进默认 `smoke`。
- 如果改动落在 `chat/`、`headless/`、`launchers/`、`webapp/`、`form-data-schema/`、`superset-spec/`，默认 `smoke` 之后应追加对应模块验证。（状态: 已验证，来源: supersonic/CURRENT_STATE.md）
- 变更完成后，记得把新的稳定入口或失败结论回填到 `CURRENT_STATE.md`、`README_AI.md` 或本手册，而不是只停留在临时聊天里。（状态: 已验证，来源: AI_CONVENTIONS.md）
