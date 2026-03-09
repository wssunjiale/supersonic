# 变更日志

## [Unreleased]

### 新增
- **[headless-superset-sync]**: 支持语义 DataSet → Superset Dataset 同步并沉淀 Dataset saved metrics（统一复用），提供 registry schema 自动迁移与管理页手动同步入口
  - 方案: `plan/202603071012_superset-semantic-dataset-sync/`
- **[chat-server]**: Superset 选图输出有序候选并生成多图响应（最多 3 个）
  - 方案: `archive/2026-02/202602040217_superset-viztype-candidates/`
  - 决策: superset-viztype-candidates#D001(后端生成候选图表供前端切换)
- **[webapp-chat-sdk]**: Superset 嵌入支持候选图表切换与按候选推送
  - 方案: `archive/2026-02/202602040217_superset-viztype-candidates/`
  - 决策: superset-viztype-candidates#D001(前端提供候选切换入口)
- **[docs]**: 生成 Superset 6.0.0 controlPanel 抽取版 `viztype.6.0.0.json`
- **[docs]**: 修正 controlPanel 抽取逻辑，补齐 mapbox 等别名配置控件的必填项解析
- **[docs]**: 合并 Superset 6.0.0 抽取结果到 docs/viztype.json
- **[chat-server]**: Superset 选图候选尽量覆盖不同类别并附加 table 兜底类型
- **[chat-server]**: 修复 viztype.json 必填字段解析与候选列表不可变导致的运行时异常
- **[docs]**: 新增语义层指标体系 JSON 规范与导入/导出说明
  - 方案: `archive/2026-02/202602100131_headless-bi-metrics-json/`
- **[docs]**: 新增指标体系 JSON 样例 headless_bi_metrics_example.json
  - 方案: `archive/2026-02/202602100131_headless-bi-metrics-json/`
- **[docs]**: 生成 Superset 6.0.0 vizType 资产 `viztype.6.0.0.json`（静态提取 + `_meta.inferred` 标注）
  - 方案: `.helloagents/archive/2026-03/202603071013_superset-viztype-6-0-0/`
- **[docs]**: 增加 vizType 资产对比脚本并输出差异报告（对比 `docs/viztype.json` vs `viztype.6.0.0.json`）
  - 脚本: `scripts/compare_viztype_assets.py`
  - 报告: `output/viztype.compare.docs_vs_generated.6.0.0.json`
- **[docs]**: 补齐 `docs/viztype.json` 缺失的 `cartodiagram`（已从 Superset 6.0.0 `plugin-chart-cartodiagram` 的 `metadata` 与 `controlPanel` 确认）
- **[docs]**: 富化 Superset 6.0.0 `viztype.6.0.0.json`（选图用途）：融合 `docs/viztype.json` 的 `name/description/vizKey`，补齐 deck.gl/legacy 的 `name/description/availability`，并剔除仅示例出现且无前后端证据的 `osm`
  - 脚本: `scripts/extract_superset_viztype.py`, `scripts/viztype_heuristics.py`
  - 报告: `output/viztype.compare.docs_vs_viztype.6.0.0.json`
- **[docs]**: 生成 Superset 6.0.0 图表 form_data 参数资产 `viztype-params.6.0.0.json`（controlPanel/sections 静态抽取 + `superset/viz.py` legacy 回退）
  - 脚本: `scripts/extract_superset_viztype_params.py`, `scripts/ts_static_extract.py`
  - 产物: `viztype-params.6.0.0.json`
- **[docs]**: 使用真实 Superset 实例（10.0.12.244:8088）验证已存在图表的 form_data keys 被 `viztype-params.6.0.0.json` 覆盖（unknownKeyKinds=0）
  - 脚本: `scripts/validate_formdata_against_superset_instance.py`, `scripts/superset_web_login_to_playwright_state.py`
  - 报告: `output/formdata.validate.superset-instance.6.0.0.json`
  - 截图: `output/playwright/superset.welcome.loggedin.png`

### 修复
- **[chat-server]**: 打包后可从 classpath 读取 docs/viztype.json，避免选图退化为 table
  - 方案: `archive/2026-02/202602040217_superset-viztype-candidates/`
- **[chat-server]**: 查询列缺失时回退语义/数据集列生成选图特征，避免固定为 table
  - 方案: `archive/2026-02/202602040217_superset-viztype-candidates/`
- **[chat-server]**: 查询结果为空时不使用 rowCount 触发饼图选择
  - 方案: `archive/2026-02/202602040217_superset-viztype-candidates/`
- **[chat-server]**: Superset 绘图场景恢复执行 SQL 查询用于选图
  - 方案: `archive/2026-02/202602040217_superset-viztype-candidates/`
- **[chat-server]**: 单图 dashboard line 类型默认高度提升至 300px，其他类型维持 260px（可用 `s2.superset.height` 覆盖）
- **[chat-server]**: 放宽 sanitizeDashboardTitle 可见性以支持单元测试复用
- **[chat-server]**: LLM form_data 校验对齐 viztype.json 必填规则，并支持 tooltip_metrics/secondary_metric/select_country/time_series_option 等关键字段
- **[chat-server]**: Superset 选图候选优先同类图表（同 profile/分类），减少不相关类型扩散
- **[chat-server]**: LLM 选图时生成中文图表名，并用于 Superset chart 命名与候选展示
- **[headless-server]**: 新增 Superset dataset 注册表（SQL 规范化 hash 去重、物理/虚拟区分）并支持增量同步
  - 方案: `plan/202602060045_superset-sql-dataset/`
- **[common]**: 新增 SQL 规范化工具用于 Superset dataset 去重
  - 方案: `plan/202602060045_superset-sql-dataset/`
- **[headless-superset-sync]**: 增加 Superset 数据集注册表管理接口（查询/删除）
  - 方案: `archive/2026-02/202602060344_superset-dataset-manage/`
- **[headless-superset-sync]**: 同步数据集时先校验 SQL，可执行失败则跳过同步，避免 Superset 侧接收错误数据集定义（SQL）
- **[supersonic-fe]**: 新增 Superset 数据集管理页面（查询/单删/批删）
  - 方案: `archive/2026-02/202602060344_superset-dataset-manage/`

### 修复
- **[chat-server]**: Superset chart form_data 基于 dataset + 语义解析生成，指标缺失时构建 adhoc metric，避免与 dataset 不一致
  - 方案: `archive/2026-02/202602041326_superset-form-data-dataset/`
- **[chat-server]**: 基于 Superset 6.0.0 controlPanel 补全 docs/viztype.json 表单字段与必填项规则，供 LLM 生成 form_data
- **[chat-server]**: 对话绘图链路不再执行 SQL，仅生成 SQL + QueryColumns 并由 Superset 执行
  - 方案: `plan/202602060045_superset-sql-dataset/`
- **[chat-server]**: Superset 绘图改为仅做 SQL 翻译，使用最终执行 SQL 注册 dataset，避免数据集名被误当表名
- **[chat-server]**: 图表创建后补齐 chart params（dashboardId/slice_id/chart_id/result_*），减少 guest payload 校验失败
- **[chat-server]**: Chart params 同步补齐 url_params(uiConfig/show_filters/expand_filters) 以匹配嵌入请求
- **[chat-server]**: Table 图表 form_data 补齐 Superset 默认字段，降低 guest payload 校验失败
- **[chat-server]**: Superset dashboard charts 端点返回 404 时跳过关联检查，避免嵌入链路阻断
- **[chat-server]**: 更新 chart params 前读取当前 params 并补齐 viz_type/datasource，避免 payload 校验失败
- **[chat-server]**: 强制覆盖 chart params 关键字段（viz_type/datasource/dashboardId/slice_id/chart_id/url_params），与嵌入请求保持一致
- **[chat-server]**: chart/dashboard 创建与 params 更新输出 debug payload，便于参数对比
- **[chat-server]**: 为所有图表补充 query_context，并补齐 metrics/columns/orderby 访问控制字段，减少 guest payload 校验失败
- **[chat-server]**: 支持配置模板 chart params/query_context 合并，优先对齐 Superset UI 生成结构
- **[chat-server]**: 模板 chart 映射支持 vizType/vizKey/name 并读取 docs/viztype.json 做键归一化
- **[headless-server]**: Superset 版本探测不再调用 /api/v1/version，避免 6.0.0 接口 404
- **[headless-server]**: 物理 dataset 解析不到表名时自动回退虚拟 dataset，避免 Superset 422 表不存在错误
- **[headless-server]**: 虚拟 dataset 缺失 SQL 时回填 normalized_sql，仍缺失则跳过同步，避免 Superset 422 表不存在错误
- **[headless-server]**: Supersonic 对话 SQL 注册到 Superset 时统一使用虚拟 dataset，并在注册后回填 Superset 解析列信息
- **[chat-server]**: Superset 表格图表在 dataset 列缺失时回退 QueryColumns，避免“vizType requires columns”错误
- **[chat-server]**: Superset 绘图响应缺少 queryResults 时补空列表，避免前端误判“数据查询失败”
- **[chat-server]**: 创建嵌入式 dashboard 后等待图表关联就绪，减少首屏 guest payload 校验失败
- **[chat-server]**: 补齐 Superset API 响应解析工具方法，修复编译错误
- **[chat-server]**: Guest token 优先解析 embedded uuid 对应的 dashboard_id 并使用该 id 生成，避免 guest payload 校验失败
- **[webapp-chat-sdk]**: 嵌入时统一调用 guest-token 接口获取新 token，避免复用旧 token 导致 payload 校验失败
- **[webapp-chat-sdk]**: Superset 嵌入保留图表控件并更新 uiConfig，支持交互控制
- **[webapp-chat-sdk]**: 聊天嵌入不再透传 urlParams，与测试页嵌入行为一致
- **[webapp-chat-sdk]**: 修复 guest token 错误信息解析与嵌入 SDK 类型不兼容导致的构建失败
- **[webapp-chat-sdk]**: 嵌入看板高度改为基于消息容器与 iframe scrollHeight 多次同步，提升自适应稳定性
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: Superset dashboard 列表支持 accessToken 拉取，避免列表为空
  - 方案: `archive/2026-02/202602081133_superset-dashboard-access-token/`
- **[chat-server]**: 按 Superset 前端 buildQuery 规则对齐各 vizType 的 query_context 关键字段，提升嵌入 chart payload 一致性
- **[chat-server]**: 补齐 legacy vizType 的 query_context 模板（mapbox/partition/rose/para 等），完善 metrics/columns/orderby/time_offsets 对齐
- **[chat-server]**: 维度解析优先映射到 dataset 实际列名（忽略空白差异），避免 groupby 与 columns 不一致导致 guest payload 校验失败
- **[chat-server]**: 规范化 adhoc metric 的 optionName（移除空白），确保 params/query_context/queryObject 一致
- **[chat-server]**: timeseries query_context columns 对齐 Superset normalizeTimeColumn（BASE_AXIS adhoc column）
- **[supersonic-fe]**: superset-embed-test.html 增加 Supersonic API Base 与错误提示，修复 dashboard 列表为空
  - 方案: `archive/2026-02/202602080832_superset-dashboard-list/`

### 新增
- **[chat-server]**: 基于 viztype.json 全量生成 form_data 模板，缺失必填字段时跳过候选并允许回退 table
  - 方案: `archive/2026-02/202602041730_superset-formdata-templates/`

### 微调
- **[supersonic-fe]**: 数据集管理页移除“同步到 Superset”入口
  - 类型: 微调（无方案包）
  - 文件: webapp/packages/supersonic-fe/src/pages/SemanticModel/View/components/DataSetTable.tsx:7-255
- **[supersonic-fe]**: superset-embed-test.html 对齐聊天嵌入配置，保留图表控件
  - 类型: 微调（无方案包）
  - 文件: webapp/packages/supersonic-fe/public/superset-embed-test.html
- **[supersonic-fe]**: superset-embed-test.html 调整看板容器为 1000x720 并支持自适应高度
  - 类型: 微调（无方案包）
  - 文件: webapp/packages/supersonic-fe/public/superset-embed-test.html:13-19
- **[webapp-chat-sdk]**: Superset 看板列表为空时仍尝试拉取，确保“推送到看板”可用
  - 类型: 微调（无方案包）
  - 文件: webapp/packages/chat-sdk/src/components/ChatMsg/SupersetChart/index.tsx:367-377; webapp/packages/chat-sdk/src/components/ChatMsg/SupersetChart/index.test.tsx:189-199
- **[supersonic-fe]**: superset-db-list-test.html 增加看板列表测试入口
  - 类型: 微调（无方案包）
  - 文件: webapp/packages/supersonic-fe/public/superset-db-list-test.html:78-245

## [0.9.10] - 2026-02-03

### 新增
- **[chat-sdk]**: 启用嵌入看板的可视化切换（Chart controls）
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`

### 修复
- **[chat-sdk]**: Superset 嵌入看板自适应尺寸并同步主题背景，避免内部滚动与黑底
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
  - 决策: superset-embed-chat#D001(容器高度+getScrollSize混合自适应策略)
- **[chat-sdk]**: 基于消息容器可用区域计算高度并加强嵌入后同步，提升 iframe 自适应稳定性
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-sdk]**: Superset 嵌入在存在 height 参数时锁定高度，贴合 ECharts 固定高度表现
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: Superset 单图 dashboard 默认高度对齐 ECharts（260px）
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: 单图 dashboard 布局补齐背景元数据并强制全宽
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: 单图 dashboard 注入全高 CSS 并隐藏图表标题
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: 单图 dashboard 布局强制关闭图表标题显示
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: 单图 dashboard CSS 隐藏 dashboard 标题栏
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: 单图 dashboard CSS 隐藏图表菜单与编辑入口
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: 单图 dashboard 标题使用查询语句并清理 supersonic 后缀
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: 单图 dashboard 恢复显示标题与菜单，保留无滚动布局
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
- **[chat-server]**: dashboard 标题清理 supersonic/superset 及数字后缀
  - 方案: `archive/2026-02/202602040218_superset-embed-chat/`
