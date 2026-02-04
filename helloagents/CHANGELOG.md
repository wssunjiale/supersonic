# 变更日志

## [Unreleased]

### 新增
- **[chat-server]**: Superset 选图输出有序候选并生成多图响应（最多 3 个）
  - 方案: [202602040217_superset-viztype-candidates](archive/2026-02/202602040217_superset-viztype-candidates/)
  - 决策: superset-viztype-candidates#D001(后端生成候选图表供前端切换)
- **[webapp-chat-sdk]**: Superset 嵌入支持候选图表切换与按候选推送
  - 方案: [202602040217_superset-viztype-candidates](archive/2026-02/202602040217_superset-viztype-candidates/)
  - 决策: superset-viztype-candidates#D001(前端提供候选切换入口)

### 修复
- **[chat-server]**: Superset chart form_data 基于 dataset + 语义解析生成，指标缺失时构建 adhoc metric，避免与 dataset 不一致
  - 方案: [202602041326_superset-form-data-dataset](archive/2026-02/202602041326_superset-form-data-dataset/)
- **[webapp-chat-sdk]**: 嵌入看板高度改为基于消息容器与 iframe scrollHeight 多次同步，提升自适应稳定性
  - 方案: [202602040218_superset-embed-chat](archive/2026-02/202602040218_superset-embed-chat/)

### 新增
- **[chat-server]**: 基于 viztype.json 全量生成 form_data 模板，缺失必填字段时跳过候选并允许回退 table
  - 方案: [202602041730_superset-formdata-templates](archive/2026-02/202602041730_superset-formdata-templates/)

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
