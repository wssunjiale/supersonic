# 完善图表类型选择与 form_data 生成规则

- 需求编号：`需求-08`
- 首次提出：`2026-02-02`
- 最后有效确认：`2026-03-11`
- 时效状态：`有效`
- 实现状态：`进行中`
- 需求主线：`Superset 选图与图表参数`

## 当前定义

- 围绕 `SupersetVizTypeSelector`、`viztype-online.json`、`form-data-schema` 与 chart `form_data` 生成逻辑，建立一套可解释、可维护的选图与参数规则。

## 用户要求

- 选图不能继续靠少量硬编码规则蒙对，要真正利用 Superset 图表资产。
- 生成出来的 `form_data` 必须与 dataset 真实列一致。
- `chat model id missing` 这类问题不能静默糊过去。
- `viztype-online.json` 与 `form-data-schema` 需要人工精读判断够不够用，不能靠脚本粗抽。
- 用户允许重组现有资产，但要先说明哪些可复用、哪些还得补。

## 需求边界

- 本需求只解决“选什么图、图表参数从哪里来、如何生成这些参数”。
- 数据集同步架构与 Headless 主路径属于 [基于 Headless 数据集同步链路重构聊天绘图与 Superset 嵌入方案](基于headlessdataset同步链路重构聊天绘图与Superset嵌入方案.md)。
- 数据集注册失败、sync runner、失败重试属于 [建立 Superset 语义同步链路与离线验收运行器](建立Superset语义同步链路与离线验收runner.md)。

## 验收要点

- 常见图表类型能够基于明确规则生成可用的 `form_data`。
- `form_data` 中引用的字段与 dataset 列保持一致。
- 资产文件能解释每种 viztype 的关键字段、适用条件和失败边界。
- 能明确列出当前资产哪些足够、哪些仍不足。

## 归属的 bug / 修复

### 问题 1：`form_data` 引用的列与 dataset 真实列不一致

- 现象：生成出的图表无法正常渲染，`form_data` 中的字段和 dataset 列对不上。
- 根因：字段在 `bizName`、`nameEn`、`name` 之间的映射优先级不稳定。
- 处理：需要把字段命名优先级收敛成稳定规则，并补回归校验。

### 问题 2：`SupersetVizTypeSelector` 缺模型时行为不清晰

- 现象：日志里出现 `chat model id missing`，但链路没有给出可解释的结果。
- 根因：插件模型、助理模型和兜底策略之间的优先级不清晰。
- 处理：拿不到有效模型时要么明确失败，要么进入可解释兜底，不能继续静默糊过去。

### 问题 3：现有 `viztype` 资产是否够用仍未证实

- 现象：虽然已经整理出 `viztype-online.json` 和 `form-data-schema`，用户仍要求人工核定这些资产是否真正可用。
- 根因：资产可能只覆盖“文件存在”，还没有覆盖到“可直接生成图表参数”的粒度。
- 处理：继续人工核查 control panel、共享控件定义和 schema 文件，明确资产边界。

## 关系

- 这条需求会直接影响 [基于 Headless 数据集同步链路重构聊天绘图与 Superset 嵌入方案](基于headlessdataset同步链路重构聊天绘图与Superset嵌入方案.md) 的可行性。

## 来源会话

- `2026-02-02` `019c1eb8-f466-78e2-be91-6a6616f9e17c` 用户指出当前 Superset 画图时图表类型选得不合适。
- `2026-02-03` `019c2343-463b-7d00-80ff-5b69a948ac20` 用户指出创建 chart 时 `data` 域与 `columns` 不匹配。
- `2026-02-04` `019c2472-567e-7b30-9694-22e4fb3cfd5c` 用户继续指出 `SupersetVizTypeSelector` 缺模型问题。
- `2026-03-09` `019cd2b5-2db7-7811-8cc4-7398790ea5a4` 用户推动整理 `viztype` 资产。
- `2026-03-11` `019cda40-d2eb-7611-88c0-95c90c21de4b` 用户明确要求不能写脚本粗抽，必须人工精读控件定义。
- `2026-03-11` `019cdd7f-261b-7f11-a7ca-de0dec94c1f9` 用户要求重新审视 `viztype-online.json` 与 `form-data-schema` 是否足够支撑新链路。
