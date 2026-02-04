package com.tencent.supersonic.chat.server.processor.execute;

import com.alibaba.fastjson.JSONObject;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.chat.server.agent.Agent;
import com.tencent.supersonic.chat.server.agent.AgentToolType;
import com.tencent.supersonic.chat.server.agent.PluginTool;
import com.tencent.supersonic.chat.server.plugin.ChatPlugin;
import com.tencent.supersonic.chat.server.plugin.build.ParamOption;
import com.tencent.supersonic.chat.server.plugin.build.WebBase;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetApiClient;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartCandidate;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartInfo;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartResp;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetDashboardInfo;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetPluginConfig;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetVizTypeSelector;
import com.tencent.supersonic.chat.server.pojo.ExecuteContext;
import com.tencent.supersonic.chat.server.service.PluginService;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.response.QueryState;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class SupersetChartProcessor implements ExecuteResultProcessor {

    public static final String QUERY_MODE = "SUPERSET";

    @Override
    public boolean accept(ExecuteContext executeContext) {
        QueryResult queryResult = executeContext.getResponse();
        if (queryResult == null || executeContext.getParseInfo() == null) {
            return false;
        }
        if (!QueryState.SUCCESS.equals(queryResult.getQueryState())) {
            return false;
        }
        String queryMode = queryResult.getQueryMode();
        return !StringUtils.equalsAnyIgnoreCase(queryMode, "WEB_PAGE", "WEB_SERVICE", "PLAIN_TEXT");
    }

    @Override
    public void process(ExecuteContext executeContext) {
        QueryResult queryResult = executeContext.getResponse();
        log.debug("superset process start, queryId={}, queryMode={}",
                executeContext.getRequest().getQueryId(),
                queryResult == null ? null : queryResult.getQueryMode());
        Optional<ChatPlugin> pluginOptional = resolveSupersetPlugin(executeContext);
        if (!pluginOptional.isPresent()) {
            log.debug("superset plugin not found for queryId={}",
                    executeContext.getRequest().getQueryId());
            return;
        }
        ChatPlugin plugin = pluginOptional.get();
        SupersetPluginConfig config =
                JsonUtil.toObject(plugin.getConfig(), SupersetPluginConfig.class);
        if (config == null || !config.isEnabled()) {
            log.debug("superset config disabled, pluginId={}, queryId={}", plugin.getId(),
                    executeContext.getRequest().getQueryId());
            return;
        }
        SupersetChartResp response = new SupersetChartResp();
        response.setName(plugin.getName());
        response.setPluginId(plugin.getId());
        response.setPluginType(plugin.getType());

        String sql = resolveSql(queryResult, executeContext.getParseInfo());
        List<SupersetVizTypeSelector.VizTypeItem> vizTypeCandidates =
                resolveVizTypeCandidates(config, queryResult, executeContext);
        String vizType = resolvePrimaryVizType(vizTypeCandidates);
        response.setVizType(vizType);
        if (StringUtils.isBlank(sql) || StringUtils.isBlank(config.getBaseUrl())
                || !config.hasValidAuthConfig()) {
            response.setFallback(true);
            response.setFallbackReason("superset config invalid");
            log.debug(
                    "superset fallback: invalid config, pluginId={}, hasSql={}, baseUrl={}, authOk={}",
                    plugin.getId(), StringUtils.isNotBlank(sql), config.getBaseUrl(),
                    config.hasValidAuthConfig());
            queryResult.setQueryMode(QUERY_MODE);
            queryResult.setResponse(response);
            return;
        }
        SupersetDatasetInfo datasetInfo = resolveSupersetDataset(executeContext);
        Long datasetId = datasetInfo == null ? null : datasetInfo.getId();
        Long databaseId = datasetInfo == null ? null : datasetInfo.getDatabaseId();
        String schema = datasetInfo == null ? null : datasetInfo.getSchema();
        if (datasetId == null && databaseId == null) {
            response.setFallback(true);
            response.setFallbackReason("superset dataset unresolved");
            log.debug("superset fallback: dataset unresolved, pluginId={}, dataSetId={}",
                    plugin.getId(), executeContext.getParseInfo() == null ? null
                            : executeContext.getParseInfo().getDataSetId());
            queryResult.setQueryMode(QUERY_MODE);
            queryResult.setResponse(response);
            return;
        }
        try {
            SupersetApiClient client = new SupersetApiClient(config);
            String chartName = buildChartName(executeContext, plugin);
            log.debug("superset build chart start, pluginId={}, vizType={}, chartName={}",
                    plugin.getId(), vizType, chartName);
            List<String> dashboardTags = buildDashboardTags(executeContext);
            List<SupersetChartCandidate> chartCandidates =
                    buildChartCandidates(client, vizTypeCandidates, sql, chartName, config,
                            queryResult, datasetInfo, datasetId, databaseId, schema, dashboardTags);
            if (!chartCandidates.isEmpty()) {
                SupersetChartCandidate primary = chartCandidates.get(0);
                response.setChartId(primary.getChartId());
                response.setChartUuid(primary.getChartUuid());
                response.setGuestToken(primary.getGuestToken());
                response.setEmbeddedId(primary.getEmbeddedId());
                response.setSupersetDomain(primary.getSupersetDomain());
                response.setVizType(primary.getVizType());
                response.setVizTypeCandidates(chartCandidates);
            } else {
                throw new IllegalStateException("superset chart build failed");
            }
            response.setWebPage(buildWebPage(config));
            log.debug(
                    "superset build chart success, pluginId={}, chartId={}, chartUuid={}, guestToken={}",
                    plugin.getId(), response.getChartId(), response.getChartUuid(),
                    StringUtils.isNotBlank(response.getGuestToken()));
            List<SupersetDashboardInfo> dashboards = Collections.emptyList();
            try {
                dashboards = client.listDashboards();
            } catch (Exception ex) {
                log.warn("superset dashboard list load failed", ex);
            }
            response.setDashboards(dashboards);
            response.setFallback(false);
        } catch (Exception ex) {
            log.warn("superset chart build failed", ex);
            response.setFallback(true);
            response.setFallbackReason(ex.getMessage());
            log.debug("superset fallback: {}", ex.getMessage());
        }
        queryResult.setQueryMode(QUERY_MODE);
        queryResult.setResponse(response);
        log.debug("superset process complete, queryId={}, fallback={}",
                executeContext.getRequest().getQueryId(), response.isFallback());
    }

    private Optional<ChatPlugin> resolveSupersetPlugin(ExecuteContext executeContext) {
        PluginService pluginService = ContextUtils.getBean(PluginService.class);
        List<ChatPlugin> plugins = pluginService.getPluginList();
        if (CollectionUtils.isEmpty(plugins)) {
            return Optional.empty();
        }
        Agent agent = executeContext.getAgent();
        List<Long> pluginIds = resolvePluginIds(agent);
        if (!CollectionUtils.isEmpty(pluginIds)) {
            plugins = plugins.stream().filter(plugin -> pluginIds.contains(plugin.getId()))
                    .collect(Collectors.toList());
        }
        Long dataSetId = executeContext.getParseInfo().getDataSetId();
        return plugins.stream().filter(plugin -> "SUPERSET".equalsIgnoreCase(plugin.getType()))
                .filter(plugin -> matchDataSet(plugin, dataSetId)).findFirst();
    }

    private List<Long> resolvePluginIds(Agent agent) {
        if (agent == null) {
            return Collections.emptyList();
        }
        List<String> tools = agent.getTools(AgentToolType.PLUGIN);
        if (CollectionUtils.isEmpty(tools)) {
            return Collections.emptyList();
        }
        List<Long> pluginIds = new ArrayList<>();
        for (String tool : tools) {
            PluginTool pluginTool = JSONObject.parseObject(tool, PluginTool.class);
            if (pluginTool != null && !CollectionUtils.isEmpty(pluginTool.getPlugins())) {
                pluginIds.addAll(pluginTool.getPlugins());
            }
        }
        return pluginIds;
    }

    private boolean matchDataSet(ChatPlugin plugin, Long dataSetId) {
        if (plugin.isContainsAllDataSet()) {
            return true;
        }
        if (dataSetId == null) {
            return true;
        }
        return plugin.getDataSetList() != null && plugin.getDataSetList().contains(dataSetId);
    }

    private String resolveSql(QueryResult queryResult, SemanticParseInfo parseInfo) {
        if (queryResult != null && StringUtils.isNotBlank(queryResult.getQuerySql())) {
            return queryResult.getQuerySql();
        }
        if (parseInfo != null && parseInfo.getSqlInfo() != null) {
            return parseInfo.getSqlInfo().getCorrectedS2SQL();
        }
        return null;
    }

    private SupersetDatasetInfo resolveSupersetDataset(ExecuteContext executeContext) {
        if (executeContext == null || executeContext.getParseInfo() == null) {
            return null;
        }
        Long dataSetId = executeContext.getParseInfo().getDataSetId();
        if (dataSetId == null) {
            return null;
        }
        SupersetSyncService syncService;
        try {
            syncService = ContextUtils.getBean(SupersetSyncService.class);
        } catch (Exception ex) {
            log.debug("superset sync service missing", ex);
            return null;
        }
        SupersetDatasetInfo datasetInfo = syncService.resolveDatasetByDataSetId(dataSetId);
        if (datasetInfo == null) {
            log.debug("superset dataset resolve failed, dataSetId={}", dataSetId);
            return null;
        }
        log.debug(
                "superset dataset resolved, dataSetId={}, supersetDatasetId={}, databaseId={}, schema={}",
                dataSetId, datasetInfo.getId(), datasetInfo.getDatabaseId(),
                datasetInfo.getSchema());
        return datasetInfo;
    }

    private List<SupersetVizTypeSelector.VizTypeItem> resolveVizTypeCandidates(
            SupersetPluginConfig config, QueryResult queryResult, ExecuteContext executeContext) {
        if (StringUtils.isNotBlank(config.getVizType())
                && !"AUTO".equalsIgnoreCase(config.getVizType())) {
            SupersetVizTypeSelector.VizTypeItem configured =
                    SupersetVizTypeSelector.resolveItemByVizType(config.getVizType(), null);
            return configured == null ? Collections.emptyList()
                    : Collections.singletonList(configured);
        }
        String queryText = executeContext == null || executeContext.getRequest() == null ? null
                : executeContext.getRequest().getQueryText();
        Agent agent = executeContext == null ? null : executeContext.getAgent();
        return SupersetVizTypeSelector.selectCandidates(config, queryResult, queryText, agent);
    }

    private String resolvePrimaryVizType(List<SupersetVizTypeSelector.VizTypeItem> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return "table";
        }
        SupersetVizTypeSelector.VizTypeItem primary = candidates.get(0);
        return primary == null || StringUtils.isBlank(primary.getVizType()) ? "table"
                : primary.getVizType();
    }

    private String buildChartName(ExecuteContext executeContext, ChatPlugin plugin) {
        Long queryId = executeContext.getRequest().getQueryId();
        String suffix =
                queryId == null ? String.valueOf(System.currentTimeMillis()) : queryId.toString();
        return "supersonic_" + plugin.getName() + "_" + suffix;
    }

    private List<String> buildDashboardTags(ExecuteContext executeContext) {
        List<String> tags = new ArrayList<>();
        tags.add("supersonic");
        tags.add("supersonic-single-chart");
        SemanticParseInfo parseInfo = executeContext == null ? null : executeContext.getParseInfo();
        if (parseInfo != null && parseInfo.getDataSet() != null
                && parseInfo.getDataSet().getDataSetId() != null) {
            tags.add("supersonic-dataset-" + parseInfo.getDataSet().getDataSetId());
        }
        return tags;
    }

    private List<SupersetChartCandidate> buildChartCandidates(SupersetApiClient client,
            List<SupersetVizTypeSelector.VizTypeItem> candidates, String sql, String chartName,
            SupersetPluginConfig config, QueryResult queryResult, SupersetDatasetInfo datasetInfo,
            Long datasetId, Long databaseId, String schema, List<String> dashboardTags) {
        if (client == null || candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }
        List<SupersetChartCandidate> results = new ArrayList<>();
        Long resolvedDatasetId = datasetId;
        int limit = Math.min(3, candidates.size());
        for (int i = 0; i < limit; i++) {
            SupersetVizTypeSelector.VizTypeItem candidate = candidates.get(i);
            if (candidate == null || StringUtils.isBlank(candidate.getVizType())) {
                continue;
            }
            String vizType = candidate.getVizType();
            String candidateChartName = buildCandidateChartName(chartName, vizType, i);
            try {
                Map<String, Object> formData =
                        buildFormData(config, executeContext.getParseInfo(), datasetInfo, vizType);
                log.debug(
                        "superset chart formData prepared, vizType={}, keys={}, size={}, datasetColumns={}, datasetMetrics={}, parseMetrics={}, parseDimensions={}",
                        vizType, formData.keySet(), formData.size(),
                        datasetInfo == null || datasetInfo.getColumns() == null ? 0
                                : datasetInfo.getColumns().size(),
                        datasetInfo == null || datasetInfo.getMetrics() == null ? 0
                                : datasetInfo.getMetrics().size(),
                        executeContext.getParseInfo() == null
                                || executeContext.getParseInfo().getMetrics() == null ? 0
                                        : executeContext.getParseInfo().getMetrics().size(),
                        executeContext.getParseInfo() == null
                                || executeContext.getParseInfo().getDimensions() == null ? 0
                                        : executeContext.getParseInfo().getDimensions().size());
                SupersetChartInfo chartInfo = client.createEmbeddedChart(sql, candidateChartName,
                        vizType, formData, resolvedDatasetId, databaseId, schema, dashboardTags);
                if (resolvedDatasetId == null && chartInfo.getDatasetId() != null) {
                    resolvedDatasetId = chartInfo.getDatasetId();
                }
                SupersetChartCandidate chartCandidate = new SupersetChartCandidate();
                chartCandidate.setVizType(vizType);
                chartCandidate.setVizName(candidate.getName());
                chartCandidate.setChartId(chartInfo.getChartId());
                chartCandidate.setChartUuid(chartInfo.getChartUuid());
                chartCandidate.setGuestToken(chartInfo.getGuestToken());
                chartCandidate.setEmbeddedId(chartInfo.getEmbeddedId());
                chartCandidate.setSupersetDomain(buildSupersetDomain(config));
                results.add(chartCandidate);
            } catch (Exception ex) {
                log.warn("superset chart candidate skipped, vizType={}, reason={}", vizType,
                        ex.getMessage());
                log.debug("superset chart candidate error", ex);
            }
        }
        boolean hasTableCandidate = containsVizTypeCandidate(candidates, "table");
        if (results.isEmpty() && !hasTableCandidate) {
            String fallbackVizType = "table";
            try {
                Map<String, Object> formData = buildFormData(config, executeContext.getParseInfo(),
                        datasetInfo, fallbackVizType);
                SupersetChartInfo chartInfo =
                        client.createEmbeddedChart(sql, chartName, fallbackVizType, formData,
                                resolvedDatasetId, databaseId, schema, dashboardTags);
                SupersetChartCandidate fallback = new SupersetChartCandidate();
                fallback.setVizType(fallbackVizType);
                fallback.setVizName(fallbackVizType);
                fallback.setChartId(chartInfo.getChartId());
                fallback.setChartUuid(chartInfo.getChartUuid());
                fallback.setGuestToken(chartInfo.getGuestToken());
                fallback.setEmbeddedId(chartInfo.getEmbeddedId());
                fallback.setSupersetDomain(buildSupersetDomain(config));
                results.add(fallback);
            } catch (Exception ex) {
                log.warn("superset table fallback failed, reason={}", ex.getMessage());
                log.debug("superset table fallback error", ex);
            }
        }
        return results;
    }

    private String buildCandidateChartName(String baseName, String vizType, int index) {
        String safeVizType =
                StringUtils.defaultIfBlank(vizType, "candidate").replaceAll("[^a-zA-Z0-9_-]", "_");
        return baseName + "_" + safeVizType + "_" + (index + 1);
    }

    /**
     * 构建 Superset 图表的 formData，优先合并插件自定义配置。
     *
     * Args: config: Superset 插件配置。 parseInfo: 语义解析信息。 datasetInfo: Superset dataset 信息。 vizType:
     * 图表类型。
     *
     * Returns: 合并后的 formData。
     */
    Map<String, Object> buildFormData(SupersetPluginConfig config, SemanticParseInfo parseInfo,
            SupersetDatasetInfo datasetInfo, String vizType) {
        Map<String, Object> autoFormData = buildAutoFormData(parseInfo, datasetInfo, vizType);
        Map<String, Object> customFormData = config == null ? null : config.getFormData();
        if (customFormData == null || customFormData.isEmpty()) {
            return autoFormData;
        }
        if (autoFormData.isEmpty()) {
            return customFormData;
        }
        Map<String, Object> merged = new HashMap<>(autoFormData);
        merged.putAll(customFormData);
        return merged;
    }

    /**
     * 基于 dataset 与解析信息生成 Superset formData，确保由 Superset 计算结果。
     *
     * Args: parseInfo: 语义解析信息。 datasetInfo: Superset dataset 信息。 vizType: 图表类型。
     *
     * Returns: 自动生成的 formData。
     */
    private Map<String, Object> buildAutoFormData(SemanticParseInfo parseInfo,
            SupersetDatasetInfo datasetInfo, String vizType) {
        FormDataContext context = buildFormDataContext(parseInfo, datasetInfo);
        FormDataProfile profile = resolveFormDataProfile(vizType);
        switch (profile) {
            case TABLE:
                return buildTableFormData(context);
            case TIME_SERIES:
                return buildTimeSeriesFormData(context, false);
            case TIME_SERIES_MULTI:
                return buildTimeSeriesFormData(context, true);
            case KPI:
                return buildKpiFormData(context, false);
            case KPI_TIME:
                return buildKpiFormData(context, true);
            case PROPORTION:
                return buildProportionFormData(context);
            case RANKING:
                return buildRankingFormData(context);
            case DISTRIBUTION:
                return buildDistributionFormData(context);
            case HISTOGRAM:
                return buildHistogramFormData(context);
            case HEATMAP:
                return buildHeatmapFormData(context);
            case CALENDAR:
                return buildCalendarFormData(context);
            case BUBBLE:
                return buildBubbleFormData(context);
            case FLOW:
                return buildFlowFormData(context);
            case MAP_LATLON:
                return buildMapLatLonFormData(context);
            case MAP_REGION:
                return buildMapRegionFormData(context);
            case GANTT:
                return buildGanttFormData(context);
            case HANDLEBARS:
                return buildHandlebarsFormData(context);
            case GENERIC:
            default:
                return buildGenericFormData(context);
        }
    }

    private FormDataContext buildFormDataContext(SemanticParseInfo parseInfo,
            SupersetDatasetInfo datasetInfo) {
        List<SupersetDatasetColumn> datasetColumns =
                datasetInfo == null ? Collections.emptyList() : datasetInfo.getColumns();
        List<String> columnNames = resolveDatasetColumns(datasetColumns);
        Map<String, SupersetDatasetColumn> columnMap = toColumnMap(datasetColumns);
        List<String> dimensionColumns = resolveDimensionColumns(parseInfo, columnMap);
        List<Object> metrics = resolveMetrics(parseInfo, datasetInfo, columnMap);
        String timeColumn = resolveTimeColumn(parseInfo, datasetInfo, columnMap);
        List<String> timeColumns = resolveTimeColumns(datasetColumns, datasetInfo);
        List<String> numericColumns = resolveNumericColumns(datasetColumns);
        return new FormDataContext(datasetColumns, columnNames, dimensionColumns, metrics,
                timeColumn, timeColumns, numericColumns);
    }

    private FormDataProfile resolveFormDataProfile(String vizType) {
        String normalized = StringUtils.lowerCase(StringUtils.trimToEmpty(vizType));
        if (StringUtils.isBlank(normalized)) {
            return FormDataProfile.TABLE;
        }
        if (isTableVizType(normalized)) {
            return FormDataProfile.TABLE;
        }
        if ("mixed_timeseries".equals(normalized)) {
            return FormDataProfile.TIME_SERIES_MULTI;
        }
        if ("pop_kpi".equals(normalized)) {
            return FormDataProfile.KPI_TIME;
        }
        if ("histogram_v2".equals(normalized)) {
            return FormDataProfile.HISTOGRAM;
        }
        if ("horizon".equals(normalized)) {
            return FormDataProfile.TIME_SERIES;
        }
        if ("heatmap_v2".equals(normalized)) {
            return FormDataProfile.HEATMAP;
        }
        if ("cal_heatmap".equals(normalized)) {
            return FormDataProfile.CALENDAR;
        }
        if ("paired_ttest".equals(normalized)) {
            return FormDataProfile.DISTRIBUTION;
        }
        if ("bubble".equals(normalized) || "bubble_v2".equals(normalized)) {
            return FormDataProfile.BUBBLE;
        }
        if ("gantt_chart".equals(normalized)) {
            return FormDataProfile.GANTT;
        }
        if ("handlebars".equals(normalized)) {
            return FormDataProfile.HANDLEBARS;
        }
        if ("mapbox".equals(normalized)) {
            return FormDataProfile.MAP_LATLON;
        }
        if ("world_map".equals(normalized) || "country_map".equals(normalized)) {
            return FormDataProfile.MAP_REGION;
        }
        if ("sankey_v2".equals(normalized) || "chord".equals(normalized)
                || "graph_chart".equals(normalized)) {
            return FormDataProfile.FLOW;
        }
        String category = SupersetVizTypeSelector.resolveCategory(vizType);
        if ("Evolution".equalsIgnoreCase(category)) {
            return FormDataProfile.TIME_SERIES;
        }
        if ("KPI".equalsIgnoreCase(category)) {
            return FormDataProfile.KPI;
        }
        if ("Part of a Whole".equalsIgnoreCase(category)) {
            return FormDataProfile.PROPORTION;
        }
        if ("Ranking".equalsIgnoreCase(category)) {
            return FormDataProfile.RANKING;
        }
        if ("Flow".equalsIgnoreCase(category)) {
            return FormDataProfile.FLOW;
        }
        if ("Map".equalsIgnoreCase(category)) {
            return FormDataProfile.MAP_REGION;
        }
        if ("Distribution".equalsIgnoreCase(category)) {
            return FormDataProfile.DISTRIBUTION;
        }
        if ("Correlation".equalsIgnoreCase(category)) {
            return FormDataProfile.HEATMAP;
        }
        return FormDataProfile.GENERIC;
    }

    private Map<String, Object> buildTableFormData(FormDataContext context) {
        Map<String, Object> formData = new HashMap<>();
        if (!context.metrics.isEmpty() || !context.dimensions.isEmpty()) {
            formData.put("query_mode", "aggregate");
            if (!context.metrics.isEmpty()) {
                formData.put("metrics", context.metrics);
            }
            if (!context.dimensions.isEmpty()) {
                formData.put("groupby", context.dimensions);
            }
            if (StringUtils.isNotBlank(context.timeColumn)) {
                formData.put("granularity_sqla", context.timeColumn);
            }
            return formData;
        }
        if (!context.columns.isEmpty()) {
            formData.put("query_mode", "raw");
            formData.put("all_columns", context.columns);
            formData.put("columns", context.columns);
            return formData;
        }
        throw new IllegalStateException("vizType requires columns");
    }

    private Map<String, Object> buildTimeSeriesFormData(FormDataContext context,
            boolean requireSecondMetric) {
        Object metric = requireSingleMetric(context, "timeseries");
        if (StringUtils.isBlank(context.timeColumn)) {
            throw new IllegalStateException("vizType requires time column");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("granularity_sqla", context.timeColumn);
        if (requireSecondMetric) {
            if (context.metrics.size() < 2) {
                throw new IllegalStateException("vizType requires at least 2 metrics");
            }
            formData.put("metrics", Collections.singletonList(metric));
            formData.put("metrics_b", Collections.singletonList(context.metrics.get(1)));
        } else {
            formData.put("metrics", context.metrics);
        }
        if (!context.dimensions.isEmpty()) {
            formData.put("groupby", context.dimensions);
        }
        return formData;
    }

    private Map<String, Object> buildKpiFormData(FormDataContext context, boolean requireTime) {
        Object metric = requireSingleMetric(context, "kpi");
        if (requireTime && StringUtils.isBlank(context.timeColumn)) {
            throw new IllegalStateException("vizType requires time column");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metric", metric);
        if (StringUtils.isNotBlank(context.timeColumn)) {
            formData.put("granularity_sqla", context.timeColumn);
        }
        return formData;
    }

    private Map<String, Object> buildProportionFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "proportion");
        requireDimensions(context, 1, "proportion");
        List<String> groupby = context.dimensions;
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metric", metric);
        formData.put("groupby", groupby);
        return formData;
    }

    private Map<String, Object> buildRankingFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "ranking");
        requireDimensions(context, 1, "ranking");
        List<String> groupby = context.dimensions;
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metric", metric);
        formData.put("groupby", groupby);
        return formData;
    }

    private Map<String, Object> buildDistributionFormData(FormDataContext context) {
        requireMetrics(context, "distribution");
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metrics", context.metrics);
        if (!context.dimensions.isEmpty()) {
            formData.put("groupby", context.dimensions);
        }
        return formData;
    }

    private Map<String, Object> buildHistogramFormData(FormDataContext context) {
        String column = requireNumericColumn(context, "histogram");
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("column", column);
        if (!context.dimensions.isEmpty()) {
            formData.put("groupby", context.dimensions);
        }
        return formData;
    }

    private Map<String, Object> buildHeatmapFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "heatmap");
        List<String> groupby = requireDimensions(context, 2, "heatmap");
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metric", metric);
        formData.put("x_axis", groupby.get(0));
        formData.put("y_axis", groupby.get(1));
        formData.put("all_columns_x", groupby.get(0));
        formData.put("all_columns_y", groupby.get(1));
        return formData;
    }

    private Map<String, Object> buildCalendarFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "cal_heatmap");
        if (StringUtils.isBlank(context.timeColumn)) {
            throw new IllegalStateException("vizType requires time column");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metric", metric);
        formData.put("granularity_sqla", context.timeColumn);
        return formData;
    }

    private Map<String, Object> buildBubbleFormData(FormDataContext context) {
        List<String> numericColumns = context.numericColumns;
        if (numericColumns.size() < 2) {
            throw new IllegalStateException("vizType requires at least 2 numeric columns");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("x", numericColumns.get(0));
        formData.put("y", numericColumns.get(1));
        if (!context.metrics.isEmpty()) {
            formData.put("size", context.metrics.get(0));
            formData.put("metric", context.metrics.get(0));
        } else if (numericColumns.size() > 2) {
            formData.put("size", numericColumns.get(2));
        }
        if (!context.dimensions.isEmpty()) {
            formData.put("groupby", context.dimensions);
        }
        return formData;
    }

    private Map<String, Object> buildFlowFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "flow");
        List<String> groupby = requireDimensions(context, 2, "flow");
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("source", groupby.get(0));
        formData.put("target", groupby.get(1));
        formData.put("metric", metric);
        return formData;
    }

    private Map<String, Object> buildMapLatLonFormData(FormDataContext context) {
        String latitude = resolveLatitudeColumn(context.datasetColumns);
        String longitude = resolveLongitudeColumn(context.datasetColumns);
        if (StringUtils.isBlank(latitude) || StringUtils.isBlank(longitude)) {
            throw new IllegalStateException("vizType requires latitude/longitude columns");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("latitude", latitude);
        formData.put("longitude", longitude);
        formData.put("all_columns_x", longitude);
        formData.put("all_columns_y", latitude);
        if (!context.metrics.isEmpty()) {
            formData.put("metric", context.metrics.get(0));
        }
        return formData;
    }

    private Map<String, Object> buildMapRegionFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "map");
        requireDimensions(context, 1, "map");
        String region = resolveRegionColumn(context.dimensions);
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("entity", region);
        formData.put("metric", metric);
        return formData;
    }

    private Map<String, Object> buildGanttFormData(FormDataContext context) {
        if (context.timeColumns.size() < 2) {
            throw new IllegalStateException("vizType requires start/end time columns");
        }
        List<String> groupby = requireDimensions(context, 1, "gantt");
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("start", context.timeColumns.get(0));
        formData.put("end", context.timeColumns.get(1));
        formData.put("groupby", groupby);
        return formData;
    }

    private Map<String, Object> buildHandlebarsFormData(FormDataContext context) {
        if (context.columns.isEmpty()) {
            throw new IllegalStateException("vizType requires columns");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "raw");
        formData.put("all_columns", context.columns);
        formData.put("columns", context.columns);
        return formData;
    }

    private Map<String, Object> buildGenericFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "generic");
        requireDimensions(context, 1, "generic");
        List<String> groupby = context.dimensions;
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metric", metric);
        formData.put("groupby", groupby);
        if (StringUtils.isNotBlank(context.timeColumn)) {
            formData.put("granularity_sqla", context.timeColumn);
        }
        return formData;
    }

    private Object requireSingleMetric(FormDataContext context, String label) {
        requireMetrics(context, label);
        return context.metrics.get(0);
    }

    private void requireMetrics(FormDataContext context, String label) {
        if (context.metrics.isEmpty()) {
            throw new IllegalStateException("vizType requires metrics: " + label);
        }
    }

    private List<String> requireDimensions(FormDataContext context, int count, String label) {
        if (context.dimensions.size() < count) {
            throw new IllegalStateException("vizType requires dimensions: " + label);
        }
        if (count == 1) {
            return Collections.singletonList(context.dimensions.get(0));
        }
        return context.dimensions.subList(0, count);
    }

    private String requireNumericColumn(FormDataContext context, String label) {
        if (context.numericColumns.isEmpty()) {
            throw new IllegalStateException("vizType requires numeric column: " + label);
        }
        return context.numericColumns.get(0);
    }

    private List<String> resolveNumericColumns(List<SupersetDatasetColumn> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return Collections.emptyList();
        }
        List<String> numericColumns = new ArrayList<>();
        for (SupersetDatasetColumn column : columns) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            if (isNumericType(column.getType())) {
                numericColumns.add(column.getColumnName());
            }
        }
        return numericColumns;
    }

    private List<String> resolveTimeColumns(List<SupersetDatasetColumn> columns,
            SupersetDatasetInfo datasetInfo) {
        List<String> timeColumns = new ArrayList<>();
        if (datasetInfo != null && StringUtils.isNotBlank(datasetInfo.getMainDttmCol())) {
            timeColumns.add(datasetInfo.getMainDttmCol());
        }
        if (CollectionUtils.isEmpty(columns)) {
            return timeColumns;
        }
        for (SupersetDatasetColumn column : columns) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            if (Boolean.TRUE.equals(column.getIsDttm())
                    && !timeColumns.contains(column.getColumnName())) {
                timeColumns.add(column.getColumnName());
            }
        }
        return timeColumns;
    }

    private String resolveLatitudeColumn(List<SupersetDatasetColumn> columns) {
        return resolveGeoColumn(columns, new String[] {"lat", "latitude", "纬度"});
    }

    private String resolveLongitudeColumn(List<SupersetDatasetColumn> columns) {
        return resolveGeoColumn(columns, new String[] {"lon", "lng", "longitude", "经度"});
    }

    private String resolveGeoColumn(List<SupersetDatasetColumn> columns, String[] tokens) {
        if (CollectionUtils.isEmpty(columns)) {
            return null;
        }
        for (SupersetDatasetColumn column : columns) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            String name = normalizeName(column.getColumnName());
            for (String token : tokens) {
                String normalized = normalizeName(token);
                if (name.equals(normalized) || name.endsWith("_" + normalized)
                        || name.contains(normalized)) {
                    return column.getColumnName();
                }
            }
        }
        return null;
    }

    private String resolveRegionColumn(List<String> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) {
            return null;
        }
        for (String dimension : dimensions) {
            String name = normalizeName(dimension);
            if (name.contains("country") || name.contains("region") || name.contains("state")
                    || name.contains("province") || name.contains("city") || name.contains("nation")
                    || name.contains("国家") || name.contains("省") || name.contains("市")) {
                return dimension;
            }
        }
        return dimensions.get(0);
    }

    private boolean containsVizTypeCandidate(List<SupersetVizTypeSelector.VizTypeItem> candidates,
            String vizType) {
        if (candidates == null || candidates.isEmpty()) {
            return false;
        }
        for (SupersetVizTypeSelector.VizTypeItem candidate : candidates) {
            if (candidate != null
                    && StringUtils.equalsIgnoreCase(candidate.getVizType(), vizType)) {
                return true;
            }
        }
        return false;
    }

    private static class FormDataContext {
        private final List<SupersetDatasetColumn> datasetColumns;
        private final List<String> columns;
        private final List<String> dimensions;
        private final List<Object> metrics;
        private final String timeColumn;
        private final List<String> timeColumns;
        private final List<String> numericColumns;

        private FormDataContext(List<SupersetDatasetColumn> datasetColumns, List<String> columns,
                List<String> dimensions, List<Object> metrics, String timeColumn,
                List<String> timeColumns, List<String> numericColumns) {
            this.datasetColumns = datasetColumns;
            this.columns = columns;
            this.dimensions = dimensions;
            this.metrics = metrics;
            this.timeColumn = timeColumn;
            this.timeColumns = timeColumns;
            this.numericColumns = numericColumns;
        }
    }

    private enum FormDataProfile {
        TABLE,
        TIME_SERIES,
        TIME_SERIES_MULTI,
        KPI,
        KPI_TIME,
        PROPORTION,
        RANKING,
        DISTRIBUTION,
        HISTOGRAM,
        HEATMAP,
        CALENDAR,
        BUBBLE,
        FLOW,
        MAP_LATLON,
        MAP_REGION,
        GANTT,
        HANDLEBARS,
        GENERIC
    }

    private List<String> resolveDatasetColumns(List<SupersetDatasetColumn> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return Collections.emptyList();
        }
        return columns.stream().map(SupersetDatasetColumn::getColumnName)
                .filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
    }

    private Map<String, SupersetDatasetColumn> toColumnMap(List<SupersetDatasetColumn> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetColumn> map = new HashMap<>();
        for (SupersetDatasetColumn column : columns) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            map.put(normalizeName(column.getColumnName()), column);
        }
        return map;
    }

    private Map<String, SupersetDatasetMetric> toMetricMap(List<SupersetDatasetMetric> metrics) {
        if (CollectionUtils.isEmpty(metrics)) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetMetric> map = new HashMap<>();
        for (SupersetDatasetMetric metric : metrics) {
            if (metric == null || StringUtils.isBlank(metric.getMetricName())) {
                continue;
            }
            map.put(normalizeName(metric.getMetricName()), metric);
        }
        return map;
    }

    private List<String> resolveDimensionColumns(SemanticParseInfo parseInfo,
            Map<String, SupersetDatasetColumn> columnMap) {
        if (parseInfo != null && !CollectionUtils.isEmpty(parseInfo.getDimensions())) {
            List<String> dimensions = new ArrayList<>();
            for (SchemaElement element : parseInfo.getDimensions()) {
                String name = resolveSchemaElementName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                if (!columnMap.isEmpty() && !columnMap.containsKey(normalizeName(name))) {
                    continue;
                }
                dimensions.add(name);
            }
            if (!dimensions.isEmpty()) {
                return dimensions.stream().distinct().collect(Collectors.toList());
            }
        }
        if (columnMap.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> dimensions = new ArrayList<>();
        for (SupersetDatasetColumn column : columnMap.values()) {
            if (column != null && Boolean.TRUE.equals(column.getGroupby())
                    && StringUtils.isNotBlank(column.getColumnName())) {
                dimensions.add(column.getColumnName());
            }
        }
        return dimensions.stream().distinct().collect(Collectors.toList());
    }

    private List<Object> resolveMetrics(SemanticParseInfo parseInfo,
            SupersetDatasetInfo datasetInfo, Map<String, SupersetDatasetColumn> columnMap) {
        List<Object> metrics = new ArrayList<>();
        List<SupersetDatasetMetric> datasetMetrics =
                datasetInfo == null ? Collections.emptyList() : datasetInfo.getMetrics();
        Map<String, SupersetDatasetMetric> metricMap = toMetricMap(datasetMetrics);
        boolean hasDatasetMetrics = !metricMap.isEmpty();
        if (parseInfo != null && !CollectionUtils.isEmpty(parseInfo.getMetrics())) {
            for (SchemaElement element : parseInfo.getMetrics()) {
                String name = resolveSchemaElementName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                if (hasDatasetMetrics) {
                    SupersetDatasetMetric metric = metricMap.get(normalizeName(name));
                    if (metric != null) {
                        metrics.add(metric.getMetricName());
                    }
                    continue;
                }
                SupersetDatasetColumn column = columnMap.get(normalizeName(name));
                if (column != null) {
                    metrics.add(buildAdhocMetric(column, resolveAggregate(element)));
                }
            }
        }
        if (!metrics.isEmpty()) {
            return metrics;
        }
        if (hasDatasetMetrics) {
            metrics.add(datasetMetrics.get(0).getMetricName());
            return metrics;
        }
        SupersetDatasetColumn numeric = resolveNumericColumn(columnMap);
        if (numeric != null) {
            metrics.add(buildAdhocMetric(numeric, "SUM"));
        }
        return metrics;
    }

    private SupersetDatasetColumn resolveNumericColumn(
            Map<String, SupersetDatasetColumn> columnMap) {
        if (columnMap.isEmpty()) {
            return null;
        }
        for (SupersetDatasetColumn column : columnMap.values()) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            if (isNumericType(column.getType())) {
                return column;
            }
        }
        return null;
    }

    private String resolveTimeColumn(SemanticParseInfo parseInfo, SupersetDatasetInfo datasetInfo,
            Map<String, SupersetDatasetColumn> columnMap) {
        if (datasetInfo != null && StringUtils.isNotBlank(datasetInfo.getMainDttmCol())) {
            return datasetInfo.getMainDttmCol();
        }
        if (!columnMap.isEmpty()) {
            for (SupersetDatasetColumn column : columnMap.values()) {
                if (column != null && Boolean.TRUE.equals(column.getIsDttm())
                        && StringUtils.isNotBlank(column.getColumnName())) {
                    return column.getColumnName();
                }
            }
        }
        if (parseInfo != null && !CollectionUtils.isEmpty(parseInfo.getDimensions())) {
            for (SchemaElement element : parseInfo.getDimensions()) {
                String name = resolveSchemaElementName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                SupersetDatasetColumn column = columnMap.get(normalizeName(name));
                if (column != null) {
                    return column.getColumnName();
                }
            }
        }
        return null;
    }

    private Map<String, Object> buildAdhocMetric(SupersetDatasetColumn column, String aggregate) {
        Map<String, Object> metric = new HashMap<>();
        metric.put("expressionType", "SIMPLE");
        if (StringUtils.isNotBlank(aggregate)) {
            metric.put("aggregate", aggregate);
        }
        Map<String, Object> columnRef = new HashMap<>();
        columnRef.put("column_name", column.getColumnName());
        if (StringUtils.isNotBlank(column.getType())) {
            columnRef.put("type", column.getType());
        }
        metric.put("column", columnRef);
        metric.put("hasCustomLabel", true);
        String label =
                StringUtils.defaultIfBlank(aggregate, "SUM") + "(" + column.getColumnName() + ")";
        metric.put("label", label);
        metric.put("optionName", "metric_" + normalizeName(column.getColumnName()));
        return metric;
    }

    private String resolveSchemaElementName(SchemaElement element) {
        if (element == null) {
            return null;
        }
        if (StringUtils.isNotBlank(element.getBizName())) {
            return element.getBizName();
        }
        return element.getName();
    }

    private String resolveAggregate(SchemaElement element) {
        if (element == null || StringUtils.isBlank(element.getDefaultAgg())) {
            return "SUM";
        }
        return element.getDefaultAgg().toUpperCase();
    }

    private String normalizeName(String name) {
        return StringUtils.lowerCase(StringUtils.trimToEmpty(name));
    }

    /**
     * 判断列是否为数值类型。
     *
     * Args: column: 查询列信息。
     *
     * Returns: 是否为数值列。
     */
    private boolean isNumericType(String type) {
        String normalized = StringUtils.defaultString(type).toUpperCase();
        return normalized.contains("INT") || normalized.contains("LONG")
                || normalized.contains("DOUBLE") || normalized.contains("FLOAT")
                || normalized.contains("DECIMAL") || normalized.contains("NUMBER")
                || normalized.contains("NUMERIC") || normalized.contains("BIGINT")
                || normalized.contains("SHORT");
    }

    /**
     * 判断是否为表格类图表。
     *
     * Args: vizType: 图表类型。
     *
     * Returns: 是否为 table 类。
     */
    private boolean isTableVizType(String vizType) {
        return StringUtils.isNotBlank(vizType) && vizType.toLowerCase().contains("table");
    }

    private WebBase buildWebPage(SupersetPluginConfig config) {
        WebBase webBase = new WebBase();
        webBase.setUrl("");
        List<ParamOption> paramOptions = new ArrayList<>();
        if (config.getHeight() != null) {
            ParamOption heightOption = new ParamOption();
            heightOption.setParamType(ParamOption.ParamType.FORWARD);
            heightOption.setKey("height");
            heightOption.setValue(config.getHeight());
            paramOptions.add(heightOption);
        }
        webBase.setParamOptions(paramOptions);
        return webBase;
    }

    private String buildSupersetDomain(SupersetPluginConfig config) {
        String baseUrl = StringUtils.defaultString(config.getBaseUrl());
        if (StringUtils.isBlank(baseUrl)) {
            return null;
        }
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }
}
