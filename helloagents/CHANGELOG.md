# 变更日志

## [Unreleased]

### 新增
- **[chat-server]**: Superset 选图输出有序候选并生成多图响应（最多 3 个）
  - 方案: [202602040217_superset-viztype-candidates](archive/2026-02/202602040217_superset-viztype-candidates/)
  - 决策: superset-viztype-candidates#D001(后端生成候选图表供前端切换)
- **[webapp-chat-sdk]**: Superset 嵌入支持候选图表切换与按候选推送
  - 方案: [202602040217_superset-viztype-candidates](archive/2026-02/202602040217_superset-viztype-candidates/)
  - 决策: superset-viztype-candidates#D001(前端提供候选切换入口)
- **[headless-server]**: 新增 Superset dataset 注册表（SQL 规范化 hash 去重、物理/虚拟区分）并支持增量同步
  - 方案: [202602060045_superset-sql-dataset](plan/202602060045_superset-sql-dataset/)
- **[common]**: 新增 SQL 规范化工具用于 Superset dataset 去重
  - 方案: [202602060045_superset-sql-dataset](plan/202602060045_superset-sql-dataset/)
- **[headless-superset-sync]**: 增加 Superset 数据集注册表管理接口（查询/删除）
  - 方案: [202602060344_superset-dataset-manage](archive/2026-02/202602060344_superset-dataset-manage/)
- **[supersonic-fe]**: 新增 Superset 数据集管理页面（查询/单删/批删）
  - 方案: [202602060344_superset-dataset-manage](archive/2026-02/202602060344_superset-dataset-manage/)

### 修复
- **[chat-server]**: Superset chart form_data 基于 dataset + 语义解析生成，指标缺失时构建 adhoc metric，避免与 dataset 不一致
  - 方案: [202602041326_superset-form-data-dataset](archive/2026-02/202602041326_superset-form-data-dataset/)
- **[chat-server]**: 对话绘图链路不再执行 SQL，仅生成 SQL + QueryColumns 并由 Superset 执行
  - 方案: [202602060045_superset-sql-dataset](plan/202602060045_superset-sql-dataset/)
- **[chat-server]**: Superset 绘图改为仅做 SQL 翻译，使用最终执行 SQL 注册 dataset，避免数据集名被误当表名
- **[chat-server]**: 图表创建后补齐 chart params（dashboardId/slice_id/chart_id/result_*），减少 guest payload 校验失败
- **[chat-server]**: Chart params 同步补齐 url_params(uiConfig/show_filters/expand_filters) 以匹配嵌入请求
- **[chat-server]**: Table 图表 form_data 补齐 Superset 默认字段，降低 guest payload 校验失败
- **[chat-server]**: Superset dashboard charts 端点返回 404 时跳过关联检查，避免嵌入链路阻断
- **[chat-server]**: 更新 chart params 前读取当前 params 并补齐 viz_type/datasource，避免 payload 校验失败
- **[chat-server]**: 强制覆盖 chart params 关键字段（viz_type/datasource/dashboardId/slice_id/chart_id/url_params），与嵌入请求保持一致
- **[chat-server]**: chart/dashboard 创建与 params 更新输出 debug payload，便于参数对比
- **[chat-server]**: 为 table 图表补充 query_context，减少 guest payload 校验失败
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
  - 方案: [202602040218_superset-embed-chat](archive/2026-02/202602040218_superset-embed-chat/)

### 新增
- **[chat-server]**: 基于 viztype.json 全量生成 form_data 模板，缺失必填字段时跳过候选并允许回退 table
  - 方案: [202602041730_superset-formdata-templates](archive/2026-02/202602041730_superset-formdata-templates/)

### 微调
- **[supersonic-fe]**: 数据集管理页移除“同步到 Superset”入口
  - 类型: 微调（无方案包）
  - 文件: webapp/packages/supersonic-fe/src/pages/SemanticModel/View/components/DataSetTable.tsx:7-255
- **[supersonic-fe]**: superset-embed-test.html 对齐聊天嵌入配置，保留图表控件
  - 类型: 微调（无方案包）
  - 文件: webapp/packages/supersonic-fe/public/superset-embed-test.html

## [0.9.10] - 2026-02-03

### 新增
- **[chat-sdk]**: 启用嵌入看板的可视化切换（Chart controls）
  - 方案: [202602040218_superset-embed-chat](archive/2026-02/202602040218_superset-embed-chat/)

### 修复
- **[chat-sdk]**: Superset 嵌入看板自适应尺寸并同步主题背景，避免内部滚动与黑底
  - 方案: [202602040218_superset-embed-chat](archive/2026-02/202602040218_superset-embed-chat/)
  - 决策: superset-embed-chat#D001(容器高度+getScrollSize混合自适应策略)
- **[chat-sdk]**: 基于消息容器可用区域计算高度并加强嵌入后同步，提升 iframe 自适应稳定性
  - 方案: [202602040218_superset-embed-chat](archive/2026-02/202602040218_superset-embed-chat/)
