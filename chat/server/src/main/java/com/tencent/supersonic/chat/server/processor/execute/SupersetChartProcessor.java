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
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartBuildRequest;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartCandidate;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartInfo;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartResp;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetPluginConfig;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetVizTypeSelector;
import com.tencent.supersonic.chat.server.pojo.ExecuteContext;
import com.tencent.supersonic.chat.server.service.PluginService;
import com.tencent.supersonic.common.config.ChatModel;
import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.ChatModelConfig;
import com.tencent.supersonic.common.pojo.DateConf;
import com.tencent.supersonic.common.pojo.Order;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.pojo.enums.DatePeriodEnum;
import com.tencent.supersonic.common.service.ChatModelService;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.response.QueryState;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.service.SupersetDatasetRegistryService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncService;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.input.Prompt;
import dev.langchain4j.model.input.PromptTemplate;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.provider.ModelProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SupersetChartProcessor implements ExecuteResultProcessor {

    public static final String QUERY_MODE = "SUPERSET";
    private static final int DEFAULT_SINGLE_CHART_HEIGHT = 260;
    private static final int LINE_CHART_HEIGHT = 300;
    private static final String FORMDATA_LLM_PROMPT = ""
            + "#Role: You are a Superset formData key fields generator.\n"
            + "#Task: Given viz_type and dataset metadata, generate key fields for formData.\n"
            + "#Rules:\n"
            + "1. For column/metric fields, ONLY use fields in #Columns and #MetricsCandidates.\n"
            + "2. Provide required keys for the viz_type; omit others.\n"
            + "3. Return JSON only, no extra text.\n"
            + "4. Output fields: query_mode, metrics, metrics_b, metric, secondary_metric, tooltip_metrics, groupby, groupbyRows, groupbyColumns,\n"
            + "   columns, column, column_collection, granularity_sqla, time_grain_sqla, x_axis, y_axis, source, target, entity, latitude, longitude,\n"
            + "   start, end, x, y, size, select_country, time_series_option, all_columns, all_columns_x, all_columns_y.\n"
            + "5. Arrays must be JSON arrays; strings must be JSON strings.\n"
            + "#UserInstruction: {{instruction}}\n" + "#VizType: {{viz_type}}\n"
            + "#RequiredKeys: {{required_keys}}\n" + "#Columns: {{columns}}\n"
            + "#MetricsCandidates: {{metrics}}\n" + "#Response:";

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
        if (queryResult == null) {
            log.debug("superset process skipped, queryResult missing, queryId={}",
                    executeContext.getRequest() == null ? null
                            : executeContext.getRequest().getQueryId());
            return;
        }
        log.debug("superset process start, queryId={}, queryMode={}",
                executeContext.getRequest().getQueryId(), queryResult.getQueryMode());
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
        log.debug("superset resolve dataset input, queryId={}, queryColumns={}",
                executeContext.getRequest() == null ? null : executeContext.getRequest().getQueryId(),
                queryResult.getQueryColumns() == null ? Collections.emptyList()
                        : queryResult.getQueryColumns().stream().map(QueryColumn::getBizName)
                                .collect(Collectors.toList()));

        String sql = resolveSql(queryResult, executeContext.getParseInfo());
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
        SupersetDatasetInfo datasetInfo = resolveSupersetDataset(executeContext, sql, queryResult);
        Long datasetId = datasetInfo == null ? null : datasetInfo.getId();
        Long databaseId = datasetInfo == null ? null : datasetInfo.getDatabaseId();
        String schema = datasetInfo == null ? null : datasetInfo.getSchema();
        if (datasetId == null) {
            response.setFallback(true);
            response.setFallbackReason("superset dataset unresolved");
            log.debug("superset fallback: dataset unresolved, pluginId={}, dataSetId={}",
                    plugin.getId(), executeContext.getParseInfo() == null ? null
                            : executeContext.getParseInfo().getDataSetId());
            queryResult.setQueryMode(QUERY_MODE);
            queryResult.setResponse(response);
            return;
        }
        ensureQueryColumns(queryResult, datasetInfo);
        List<SupersetVizTypeSelector.VizTypeItem> vizTypeCandidates =
                resolveVizTypeCandidates(config, queryResult, executeContext, datasetInfo);
        if (config.isVizTypeLlmEnabled()) {
            vizTypeCandidates = ensureFormDataCandidates(config, executeContext, queryResult,
                    datasetInfo, vizTypeCandidates);
        }
        String vizType = resolvePrimaryVizType(vizTypeCandidates);
        response.setVizType(vizType);
        try {
            SupersetApiClient client = new SupersetApiClient(config);
            String chartName = buildChartName(executeContext, plugin);
            String dashboardTitle = buildDashboardTitle(executeContext, chartName);
            log.debug("superset build chart start, pluginId={}, vizType={}, chartName={}",
                    plugin.getId(), vizType, chartName);
            List<String> dashboardTags = buildDashboardTags(executeContext);
            List<SupersetChartBuildRequest> chartRequests = buildChartRequests(vizTypeCandidates,
                    chartName, config, executeContext, queryResult, datasetInfo);
            if (chartRequests.isEmpty()) {
                throw new IllegalStateException("superset chart build failed");
            }
            List<SupersetChartCandidate> chartCandidates =
                    buildEmbeddedChartCandidates(client, dashboardTitle, chartRequests, sql,
                            datasetId, databaseId, schema, dashboardTags, config);
            populateFinalDashboardResponse(response, dashboardTitle, chartCandidates, config,
                    resolveChartHeights(chartRequests));
            log.debug(
                    "superset build chart success, pluginId={}, dashboardId={}, chartCount={}, guestToken={}",
                    plugin.getId(), response.getDashboardId(),
                    response.getVizTypeCandidates() == null ? 0
                            : response.getVizTypeCandidates().size(),
                    StringUtils.isNotBlank(response.getGuestToken()));
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
        if (parseInfo != null && parseInfo.getSqlInfo() != null
                && StringUtils.isNotBlank(parseInfo.getSqlInfo().getQuerySQL())) {
            return parseInfo.getSqlInfo().getQuerySQL();
        }
        return null;
    }

    private SupersetDatasetInfo resolveSupersetDataset(ExecuteContext executeContext, String sql,
            QueryResult queryResult) {
        if (executeContext == null || executeContext.getParseInfo() == null
                || StringUtils.isBlank(sql)) {
            return null;
        }
        SupersetSyncService syncService = resolveSupersetSyncService();
        if (syncService == null) {
            return null;
        }
        SemanticParseInfo parseInfo = executeContext.getParseInfo();
        SupersetDatasetInfo persistentDataset =
                resolvePersistentSupersetDataset(parseInfo, queryResult, syncService);
        if (persistentDataset != null) {
            return persistentDataset;
        }
        SupersetDatasetInfo datasetInfo = syncService.registerAndSyncDataset(parseInfo, sql,
                queryResult == null ? null : queryResult.getQueryColumns(),
                executeContext.getRequest() == null ? null : executeContext.getRequest().getUser());
        if (datasetInfo == null) {
            log.debug("superset dataset register failed, dataSetId={}",
                    parseInfo == null ? null : parseInfo.getDataSetId());
            return null;
        }
        log.debug(
                "superset dataset registered, dataSetId={}, supersetDatasetId={}, databaseId={}, schema={}",
                parseInfo == null ? null : parseInfo.getDataSetId(), datasetInfo.getId(),
                datasetInfo.getDatabaseId(), datasetInfo.getSchema());
        return datasetInfo;
    }

    private SupersetSyncService resolveSupersetSyncService() {
        try {
            return ContextUtils.getBean(SupersetSyncService.class);
        } catch (Exception ex) {
            log.debug("superset sync service missing", ex);
            return null;
        }
    }

    private SupersetDatasetRegistryService resolveSupersetDatasetRegistryService() {
        try {
            return ContextUtils.getBean(SupersetDatasetRegistryService.class);
        } catch (Exception ex) {
            log.debug("superset dataset registry service missing", ex);
            return null;
        }
    }

    private SupersetDatasetInfo resolvePersistentSupersetDataset(SemanticParseInfo parseInfo,
            QueryResult queryResult, SupersetSyncService syncService) {
        if (parseInfo == null || parseInfo.getDataSetId() == null || syncService == null) {
            return null;
        }
        SupersetDatasetRegistryService registryService = resolveSupersetDatasetRegistryService();
        if (registryService == null) {
            return null;
        }
        List<SupersetDatasetDO> candidates =
                registryService.listAvailablePersistentDatasets(parseInfo.getDataSetId());
        if (CollectionUtils.isEmpty(candidates)) {
            return null;
        }
        for (SupersetDatasetDO candidate : candidates) {
            SupersetDatasetInfo datasetInfo = syncService.resolveDatasetInfo(candidate);
            if (datasetInfo == null || datasetInfo.getId() == null) {
                log.debug(
                        "superset persistent dataset skipped, dataSetId={}, registryId={}, syncState={}, datasetId={}",
                        parseInfo.getDataSetId(), candidate == null ? null : candidate.getId(),
                        candidate == null ? null : candidate.getSyncState(),
                        candidate == null ? null : candidate.getSupersetDatasetId());
                continue;
            }
            List<String> missingFields =
                    resolveMissingPersistentFields(parseInfo, queryResult, datasetInfo);
            if (missingFields.isEmpty()) {
                log.debug(
                        "superset persistent dataset hit, dataSetId={}, registryId={}, sourceType={}, datasetType={}, supersetDatasetId={}",
                        parseInfo.getDataSetId(), candidate.getId(), candidate.getSourceType(),
                        candidate.getDatasetType(), candidate.getSupersetDatasetId());
                return datasetInfo;
            }
            log.debug(
                    "superset persistent dataset missing fields, dataSetId={}, registryId={}, sourceType={}, datasetType={}, missing={}",
                    parseInfo.getDataSetId(), candidate.getId(), candidate.getSourceType(),
                    candidate.getDatasetType(), missingFields);
        }
        return null;
    }


    private void ensureQueryColumns(QueryResult queryResult, SupersetDatasetInfo datasetInfo) {
        if (queryResult == null || datasetInfo == null
                || !CollectionUtils.isEmpty(queryResult.getQueryColumns())) {
            return;
        }
        if (queryResult.getQueryResults() == null) {
            queryResult.setQueryResults(Collections.emptyList());
        }
        List<SupersetDatasetColumn> datasetColumns = datasetInfo.getColumns();
        if (CollectionUtils.isEmpty(datasetColumns)) {
            return;
        }
        List<QueryColumn> queryColumns = new ArrayList<>();
        for (SupersetDatasetColumn column : datasetColumns) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            queryColumns.add(new QueryColumn(column.getColumnName(), column.getType(),
                    column.getColumnName()));
        }
        if (!queryColumns.isEmpty()) {
            queryResult.setQueryColumns(queryColumns);
        }
    }

    private List<String> resolveMissingPersistentFields(SemanticParseInfo parseInfo,
            QueryResult queryResult, SupersetDatasetInfo datasetInfo) {
        if (datasetInfo == null) {
            return Collections.singletonList("datasetInfo");
        }
        Map<String, SupersetDatasetColumn> columnMap =
                toColumnMap(datasetInfo.getColumns() == null ? Collections.emptyList()
                        : datasetInfo.getColumns());
        Map<String, SupersetDatasetColumn> relaxedColumnMap = buildRelaxedColumnMap(columnMap);
        List<String> datasetMetricNames = resolveDatasetMetrics(datasetInfo);
        List<String> missing = new ArrayList<>();
        boolean hasSemanticSelections =
                parseInfo != null && (!CollectionUtils.isEmpty(parseInfo.getMetrics())
                        || !CollectionUtils.isEmpty(parseInfo.getDimensions()));
        if (parseInfo != null && !CollectionUtils.isEmpty(parseInfo.getDimensions())) {
            for (SchemaElement dimension : parseInfo.getDimensions()) {
                String name = resolveSchemaElementName(dimension);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                if (resolveDatasetColumn(name, columnMap, relaxedColumnMap) == null) {
                    missing.add(name);
                }
            }
        }
        if (parseInfo != null && !CollectionUtils.isEmpty(parseInfo.getMetrics())) {
            for (SchemaElement metric : parseInfo.getMetrics()) {
                String name = resolveSchemaElementName(metric);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                if (resolveMetricValue(name, columnMap, datasetMetricNames) == null) {
                    missing.add(name);
                }
            }
        }
        if (!hasSemanticSelections && queryResult != null
                && !CollectionUtils.isEmpty(queryResult.getQueryColumns())) {
            for (QueryColumn queryColumn : queryResult.getQueryColumns()) {
                if (queryColumn == null) {
                    continue;
                }
                String name =
                        StringUtils.defaultIfBlank(queryColumn.getBizName(), queryColumn.getName());
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                if (resolveDatasetColumn(name, columnMap, relaxedColumnMap) != null
                        || resolveMetricValue(name, columnMap, datasetMetricNames) != null) {
                    continue;
                }
                missing.add(name);
            }
        }
        return missing.stream().filter(StringUtils::isNotBlank).distinct()
                .collect(Collectors.toList());
    }

    private String resolveDatasetColumn(String name, Map<String, SupersetDatasetColumn> columnMap,
            Map<String, SupersetDatasetColumn> relaxedColumnMap) {
        if (StringUtils.isBlank(name) || CollectionUtils.isEmpty(columnMap)) {
            return null;
        }
        SupersetDatasetColumn exact = columnMap.get(normalizeName(name));
        if (exact != null && StringUtils.isNotBlank(exact.getColumnName())) {
            return exact.getColumnName();
        }
        if (CollectionUtils.isEmpty(relaxedColumnMap)) {
            return null;
        }
        SupersetDatasetColumn relaxed = relaxedColumnMap.get(normalizeRelaxedName(name));
        return relaxed == null ? null : relaxed.getColumnName();
    }

    private List<SupersetVizTypeSelector.VizTypeItem> resolveVizTypeCandidates(
            SupersetPluginConfig config, QueryResult queryResult, ExecuteContext executeContext,
            SupersetDatasetInfo datasetInfo) {
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
        String executedSql = resolveSql(queryResult,
                executeContext == null ? null : executeContext.getParseInfo());
        QueryResult resolvedResult = resolveQueryResultForVizType(queryResult,
                executeContext == null ? null : executeContext.getParseInfo(), datasetInfo);
        return SupersetVizTypeSelector.selectCandidates(config, resolvedResult, queryText, agent,
                executedSql, executeContext == null ? null : executeContext.getParseInfo(),
                datasetInfo);
    }

    private List<SupersetVizTypeSelector.VizTypeItem> ensureFormDataCandidates(
            SupersetPluginConfig config, ExecuteContext executeContext, QueryResult queryResult,
            SupersetDatasetInfo datasetInfo, List<SupersetVizTypeSelector.VizTypeItem> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return candidates;
        }
        List<SupersetVizTypeSelector.VizTypeItem> validated = new ArrayList<>();
        SemanticParseInfo parseInfo = executeContext == null ? null : executeContext.getParseInfo();
        Agent agent = executeContext == null ? null : executeContext.getAgent();
        String queryText = executeContext == null || executeContext.getRequest() == null ? null
                : executeContext.getRequest().getQueryText();
        for (SupersetVizTypeSelector.VizTypeItem candidate : candidates) {
            if (candidate == null || StringUtils.isBlank(candidate.getVizType())) {
                continue;
            }
            String vizType = candidate.getVizType();
            try {
                buildFormData(config, parseInfo, queryResult, datasetInfo, vizType, agent,
                        queryText);
                validated.add(candidate);
            } catch (Exception ex) {
                log.warn("superset viztype candidate invalid, vizType={}, reason={}", vizType,
                        ex.getMessage());
                log.debug("superset viztype candidate formData failed", ex);
            }
        }
        if (!validated.isEmpty()) {
            return validated;
        }
        SupersetVizTypeSelector.VizTypeItem fallback =
                SupersetVizTypeSelector.resolveItemByVizType("table", candidates);
        if (fallback != null) {
            try {
                buildFormData(config, parseInfo, queryResult, datasetInfo, fallback.getVizType(),
                        agent, queryText);
                return Collections.singletonList(fallback);
            } catch (Exception ex) {
                log.warn("superset viztype fallback formData failed, reason={}", ex.getMessage());
                log.debug("superset viztype fallback formData error", ex);
            }
        }
        return candidates;
    }

    private QueryResult resolveQueryResultForVizType(QueryResult queryResult,
            SemanticParseInfo parseInfo, SupersetDatasetInfo datasetInfo) {
        if (queryResult == null) {
            return null;
        }
        List<QueryColumn> queryColumns = queryResult.getQueryColumns();
        if (queryColumns != null && !queryColumns.isEmpty()) {
            return queryResult;
        }
        List<QueryColumn> parsedColumns = buildColumnsFromParse(parseInfo);
        List<QueryColumn> resolvedColumns =
                parsedColumns.isEmpty() ? buildColumnsFromDataset(datasetInfo) : parsedColumns;
        if (resolvedColumns.isEmpty()) {
            return queryResult;
        }
        QueryResult resolved = new QueryResult();
        resolved.setQueryColumns(resolvedColumns);
        resolved.setQueryResults(queryResult.getQueryResults());
        resolved.setQueryMode(queryResult.getQueryMode());
        log.debug("superset viztype fallback columns applied, metrics={}, dimensions={}",
                parseInfo == null || parseInfo.getMetrics() == null ? 0
                        : parseInfo.getMetrics().size(),
                parseInfo == null || parseInfo.getDimensions() == null ? 0
                        : parseInfo.getDimensions().size());
        return resolved;
    }

    private List<QueryColumn> buildColumnsFromDataset(SupersetDatasetInfo datasetInfo) {
        if (datasetInfo == null) {
            return Collections.emptyList();
        }
        List<QueryColumn> columns = new ArrayList<>();
        List<SupersetDatasetMetric> metrics =
                datasetInfo.getMetrics() == null ? Collections.emptyList()
                        : datasetInfo.getMetrics();
        List<String> metricNames = metrics.stream().map(SupersetDatasetMetric::getMetricName)
                .filter(StringUtils::isNotBlank).collect(Collectors.toList());
        for (SupersetDatasetMetric metric : metrics) {
            if (metric == null || StringUtils.isBlank(metric.getMetricName())) {
                continue;
            }
            QueryColumn column = new QueryColumn();
            column.setName(metric.getMetricName());
            column.setBizName(metric.getMetricName());
            column.setShowType("NUMBER");
            column.setType("DOUBLE");
            columns.add(column);
        }
        List<SupersetDatasetColumn> datasetColumns =
                datasetInfo.getColumns() == null ? Collections.emptyList()
                        : datasetInfo.getColumns();
        for (SupersetDatasetColumn datasetColumn : datasetColumns) {
            if (datasetColumn == null || StringUtils.isBlank(datasetColumn.getColumnName())) {
                continue;
            }
            if (metricNames.contains(datasetColumn.getColumnName())) {
                continue;
            }
            boolean isTime = Boolean.TRUE.equals(datasetColumn.getIsDttm());
            boolean isGroupBy = Boolean.TRUE.equals(datasetColumn.getGroupby());
            if (!isTime && !isGroupBy) {
                continue;
            }
            QueryColumn column = new QueryColumn();
            column.setName(datasetColumn.getColumnName());
            column.setBizName(datasetColumn.getColumnName());
            column.setShowType(isTime ? "DATE" : "CATEGORY");
            column.setType(isTime ? "DATE"
                    : StringUtils.defaultIfBlank(datasetColumn.getType(), "STRING"));
            columns.add(column);
        }
        return columns;
    }

    private List<QueryColumn> buildColumnsFromParse(SemanticParseInfo parseInfo) {
        if (parseInfo == null) {
            return Collections.emptyList();
        }
        List<QueryColumn> columns = new ArrayList<>();
        if (parseInfo.getMetrics() != null) {
            parseInfo.getMetrics().forEach(metric -> {
                if (metric == null) {
                    return;
                }
                QueryColumn column = new QueryColumn();
                column.setName(StringUtils.defaultIfBlank(metric.getName(), metric.getBizName()));
                column.setBizName(
                        StringUtils.defaultIfBlank(metric.getBizName(), metric.getName()));
                column.setShowType("NUMBER");
                column.setType("DOUBLE");
                columns.add(column);
            });
        }
        if (parseInfo.getDimensions() != null) {
            parseInfo.getDimensions().forEach(dimension -> {
                if (dimension == null) {
                    return;
                }
                QueryColumn column = new QueryColumn();
                column.setName(
                        StringUtils.defaultIfBlank(dimension.getName(), dimension.getBizName()));
                column.setBizName(
                        StringUtils.defaultIfBlank(dimension.getBizName(), dimension.getName()));
                boolean isTime = SchemaElementType.DATE.equals(dimension.getType())
                        || dimension.isPartitionTime();
                column.setShowType(isTime ? "DATE" : "CATEGORY");
                column.setType(isTime ? "DATE" : "STRING");
                columns.add(column);
            });
        }
        return columns;
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

    private String buildDashboardTitle(ExecuteContext executeContext, String fallback) {
        if (executeContext != null && executeContext.getRequest() != null) {
            String queryText = StringUtils.trimToEmpty(executeContext.getRequest().getQueryText());
            if (StringUtils.isNotBlank(queryText)) {
                return queryText;
            }
        }
        return fallback;
    }

    private List<String> buildDashboardTags(ExecuteContext executeContext) {
        List<String> tags = new ArrayList<>();
        tags.add("supersonic");
        tags.add("supersonic-single-chart");
        tags.add("supersonic-chat-dashboard");
        SemanticParseInfo parseInfo = executeContext == null ? null : executeContext.getParseInfo();
        if (parseInfo != null && parseInfo.getDataSet() != null
                && parseInfo.getDataSet().getDataSetId() != null) {
            tags.add("supersonic-dataset-" + parseInfo.getDataSet().getDataSetId());
        }
        return tags;
    }

    private List<SupersetChartBuildRequest> buildChartRequests(
            List<SupersetVizTypeSelector.VizTypeItem> candidates, String chartName,
            SupersetPluginConfig config, ExecuteContext executeContext, QueryResult queryResult,
            SupersetDatasetInfo datasetInfo) {
        List<SupersetChartBuildRequest> requests = new ArrayList<>();
        List<SupersetVizTypeSelector.VizTypeItem> prioritized =
                prioritizeChartCandidates(candidates);
        for (int i = 0; i < prioritized.size(); i++) {
            SupersetVizTypeSelector.VizTypeItem candidate = prioritized.get(i);
            if (candidate == null || StringUtils.isBlank(candidate.getVizType())) {
                continue;
            }
            String vizType = candidate.getVizType();
            String candidateChartName =
                    buildCandidateChartName(chartName, vizType, i, candidate.getLlmName());
            try {
                Map<String, Object> formData = buildFormData(config, executeContext.getParseInfo(),
                        queryResult, datasetInfo, vizType, executeContext.getAgent(),
                        executeContext.getRequest() == null ? null
                                : executeContext.getRequest().getQueryText());
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
                SupersetChartBuildRequest request = new SupersetChartBuildRequest();
                request.setVizType(vizType);
                request.setVizName(StringUtils.defaultIfBlank(candidate.getLlmName(),
                        StringUtils.defaultIfBlank(candidate.getName(), vizType)));
                request.setChartName(candidateChartName);
                request.setDashboardHeight(resolveDashboardHeight(config, vizType));
                request.setFormData(formData);
                requests.add(request);
            } catch (Exception ex) {
                log.warn("superset chart candidate skipped, vizType={}, reason={}", vizType,
                        ex.getMessage());
                log.debug("superset chart candidate error", ex);
            }
        }
        return requests;
    }

    private List<SupersetVizTypeSelector.VizTypeItem> prioritizeChartCandidates(
            List<SupersetVizTypeSelector.VizTypeItem> candidates) {
        List<SupersetVizTypeSelector.VizTypeItem> prioritized = new ArrayList<>();
        SupersetVizTypeSelector.VizTypeItem tableCandidate = null;
        if (!CollectionUtils.isEmpty(candidates)) {
            for (SupersetVizTypeSelector.VizTypeItem candidate : candidates) {
                if (candidate == null || StringUtils.isBlank(candidate.getVizType())) {
                    continue;
                }
                if (StringUtils.equalsIgnoreCase(candidate.getVizType(), "table")) {
                    if (tableCandidate == null) {
                        tableCandidate = candidate;
                    }
                    continue;
                }
                if (prioritized.size() < 3) {
                    prioritized.add(candidate);
                }
            }
        }
        if (tableCandidate == null) {
            tableCandidate = buildFallbackTableCandidate();
        }
        if (tableCandidate != null) {
            prioritized.add(tableCandidate);
        }
        return prioritized;
    }

    private SupersetVizTypeSelector.VizTypeItem buildFallbackTableCandidate() {
        SupersetVizTypeSelector.VizTypeItem item = new SupersetVizTypeSelector.VizTypeItem();
        item.setVizType("table");
        item.setName("Table");
        item.setLlmName("Table");
        return item;
    }

    private List<SupersetChartCandidate> buildEmbeddedChartCandidates(SupersetApiClient client,
            String dashboardTitle, List<SupersetChartBuildRequest> chartRequests, String sql,
            Long datasetId, Long databaseId, String schema, List<String> dashboardTags,
            SupersetPluginConfig config) {
        if (client == null || CollectionUtils.isEmpty(chartRequests) || datasetId == null) {
            return Collections.emptyList();
        }
        List<SupersetChartCandidate> candidates = new ArrayList<>();
        String supersetDomain = buildSupersetDomain(config);
        for (int i = 0; i < chartRequests.size(); i++) {
            SupersetChartBuildRequest request = chartRequests.get(i);
            if (request == null || StringUtils.isBlank(request.getVizType())) {
                continue;
            }
            String candidateDashboardTitle =
                    buildCandidateDashboardTitle(dashboardTitle, request.getVizName(), i);
            SupersetChartInfo chartInfo = client.createEmbeddedChart(sql, request.getChartName(),
                    request.getVizType(), request.getFormData(), datasetId, databaseId, schema,
                    dashboardTags, request.getDashboardHeight(), candidateDashboardTitle);
            SupersetChartCandidate candidate = new SupersetChartCandidate();
            candidate.setVizType(request.getVizType());
            candidate.setVizName(request.getVizName());
            candidate.setDashboardId(chartInfo.getDashboardId());
            candidate.setDashboardTitle(chartInfo.getDashboardTitle());
            candidate.setChartId(chartInfo.getChartId());
            candidate.setChartUuid(chartInfo.getChartUuid());
            candidate.setDashboardHeight(request.getDashboardHeight());
            candidate.setGuestToken(chartInfo.getGuestToken());
            candidate.setEmbeddedId(chartInfo.getEmbeddedId());
            candidate.setSupersetDomain(supersetDomain);
            candidates.add(candidate);
        }
        return candidates;
    }

    void populateFinalDashboardResponse(SupersetChartResp response, String dashboardTitle,
            List<SupersetChartCandidate> chartCandidates, SupersetPluginConfig config,
            List<Integer> chartHeights) {
        if (response == null || CollectionUtils.isEmpty(chartCandidates)) {
            return;
        }
        SupersetChartCandidate primary = chartCandidates.get(0);
        response.setDashboardId(primary.getDashboardId());
        response.setDashboardTitle(
                StringUtils.defaultIfBlank(dashboardTitle, primary.getDashboardTitle()));
        response.setGuestToken(primary.getGuestToken());
        response.setEmbeddedId(primary.getEmbeddedId());
        response.setSupersetDomain(StringUtils.defaultIfBlank(primary.getSupersetDomain(),
                buildSupersetDomain(config)));
        response.setVizType(primary.getVizType());
        response.setVizTypeCandidates(chartCandidates);
        response.setWebPage(buildWebPage(resolveDashboardHeight(chartHeights)));
    }

    private List<Integer> resolveChartHeights(List<SupersetChartBuildRequest> chartRequests) {
        if (CollectionUtils.isEmpty(chartRequests)) {
            return Collections.emptyList();
        }
        return chartRequests.stream().map(SupersetChartBuildRequest::getDashboardHeight)
                .collect(Collectors.toList());
    }

    private String buildCandidateChartName(String baseName, String vizType, int index,
            String displayName) {
        String suffix = baseName + "_" + (index + 1);
        String safeDisplay = sanitizeChartName(displayName);
        if (StringUtils.isNotBlank(safeDisplay)) {
            return safeDisplay + "_" + suffix;
        }
        String safeVizType =
                StringUtils.defaultIfBlank(vizType, "candidate").replaceAll("[^a-zA-Z0-9_-]", "_");
        return baseName + "_" + safeVizType + "_" + (index + 1);
    }

    private String buildCandidateDashboardTitle(String baseTitle, String displayName, int index) {
        String safeBase = StringUtils.defaultIfBlank(StringUtils.trimToEmpty(baseTitle),
                "supersonic_dashboard");
        String safeDisplay = sanitizeChartName(displayName);
        if (StringUtils.isBlank(safeDisplay)) {
            return safeBase + " - View " + (index + 1);
        }
        return safeBase + " - " + safeDisplay;
    }

    private String sanitizeChartName(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        String cleaned = name.replaceAll("[\\r\\n]+", " ").trim();
        cleaned = cleaned.replaceAll("\\s{2,}", " ");
        if (cleaned.length() > 60) {
            cleaned = cleaned.substring(0, 60).trim();
        }
        return StringUtils.defaultIfBlank(cleaned, null);
    }

    /**
     * 构建 Superset 图表的 formData，优先合并插件自定义配置。
     *
     * Args: config: Superset 插件配置。 parseInfo: 语义解析信息。 queryResult: 查询结果。 datasetInfo: Superset
     * dataset 信息。 vizType: 图表类型。
     *
     * Returns: 合并后的 formData。
     */
    Map<String, Object> buildFormData(SupersetPluginConfig config, SemanticParseInfo parseInfo,
            QueryResult queryResult, SupersetDatasetInfo datasetInfo, String vizType, Agent agent,
            String queryText) {
        FormDataProfile profile = resolveFormDataProfile(vizType);
        FormDataContext context = buildFormDataContext(parseInfo, queryResult, datasetInfo);
        log.debug(
                "superset buildFormData context, vizType={}, parseLimit={}, parseDateInfo={}, resolvedTimeColumn={}, resolvedTimeRange={}, resolvedTimeGrain={}, resolvedOrders={}, selectedColumns={}",
                vizType, parseInfo == null ? null : parseInfo.getLimit(),
                parseInfo == null ? null : parseInfo.getDateInfo(), context == null ? null
                        : context.timeColumn,
                context == null ? null : context.timeRange,
                context == null ? null : context.timeGrain,
                context == null || context.orders == null ? 0 : context.orders.size(),
                context == null ? null : context.selectedColumns);
        Map<String, Object> baseFormData;
        if (config != null && config.isVizTypeLlmEnabled()) {
            baseFormData = buildLlmFormData(config, queryResult, datasetInfo, vizType, profile,
                    agent, queryText);
        } else {
            baseFormData = buildAutoFormData(context, parseInfo, queryResult, profile);
        }
        Map<String, Object> customFormData = config == null ? null : config.getFormData();
        Map<String, Object> merged = new HashMap<>();
        if (baseFormData != null && !baseFormData.isEmpty()) {
            merged.putAll(baseFormData);
        }
        if (customFormData == null || customFormData.isEmpty()) {
            return applySemanticFormData(merged, context);
        }
        if (merged.isEmpty()) {
            merged.putAll(customFormData);
            return applySemanticFormData(merged, context);
        }
        merged.putAll(customFormData);
        return applySemanticFormData(merged, context);
    }

    /**
     * 基于 dataset 与解析信息生成 Superset formData，确保由 Superset 计算结果。
     *
     * Args: parseInfo: 语义解析信息。 queryResult: 查询结果。 datasetInfo: Superset dataset 信息。 vizType: 图表类型。
     *
     * Returns: 自动生成的 formData。
     */
    private Map<String, Object> buildAutoFormData(FormDataContext context,
            SemanticParseInfo parseInfo, QueryResult queryResult, FormDataProfile profile) {
        switch (profile) {
            case TABLE:
                return buildTableFormData(context,
                        shouldPreferRawTable(parseInfo, queryResult, context));
            case PIVOT_TABLE:
                return buildPivotTableFormData(context);
            case TIME_TABLE:
                return buildTimeTableFormData(context);
            case TIME_PIVOT:
                return buildTimePivotFormData(context);
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

    private Map<String, Object> buildLlmFormData(SupersetPluginConfig config,
            QueryResult queryResult, SupersetDatasetInfo datasetInfo, String vizType,
            FormDataProfile profile, Agent agent, String queryText) {
        JSONObject llmKeys = resolveLlmFormDataKeys(config, queryResult, datasetInfo, vizType,
                profile, agent, queryText);
        validateLlmFormDataKeys(vizType, profile, llmKeys);
        Map<String, Object> formData = new HashMap<>();
        String queryMode = llmKeys.getString("query_mode");
        if (StringUtils.isBlank(queryMode) || requiresAggregate(profile)) {
            queryMode = "aggregate";
        }
        formData.put("query_mode", queryMode);
        applyLlmKey(formData, llmKeys, "metrics");
        applyLlmKey(formData, llmKeys, "metrics_b");
        applyLlmKey(formData, llmKeys, "metric");
        applyLlmKey(formData, llmKeys, "secondary_metric");
        applyLlmKey(formData, llmKeys, "tooltip_metrics");
        applyLlmKey(formData, llmKeys, "groupby");
        applyLlmKey(formData, llmKeys, "groupbyRows");
        applyLlmKey(formData, llmKeys, "groupbyColumns");
        applyLlmKey(formData, llmKeys, "columns");
        applyLlmKey(formData, llmKeys, "column");
        applyLlmKey(formData, llmKeys, "column_collection");
        applyLlmKey(formData, llmKeys, "granularity_sqla");
        applyLlmKey(formData, llmKeys, "time_grain_sqla");
        applyLlmKey(formData, llmKeys, "x_axis");
        applyLlmKey(formData, llmKeys, "y_axis");
        applyLlmKey(formData, llmKeys, "source");
        applyLlmKey(formData, llmKeys, "target");
        applyLlmKey(formData, llmKeys, "entity");
        applyLlmKey(formData, llmKeys, "latitude");
        applyLlmKey(formData, llmKeys, "longitude");
        applyLlmKey(formData, llmKeys, "start");
        applyLlmKey(formData, llmKeys, "end");
        applyLlmKey(formData, llmKeys, "x");
        applyLlmKey(formData, llmKeys, "y");
        applyLlmKey(formData, llmKeys, "size");
        applyLlmKey(formData, llmKeys, "select_country");
        applyLlmKey(formData, llmKeys, "time_series_option");
        applyLlmKey(formData, llmKeys, "all_columns");
        applyLlmKey(formData, llmKeys, "all_columns_x");
        applyLlmKey(formData, llmKeys, "all_columns_y");
        normalizeLlmMetrics(formData, datasetInfo);
        if (FormDataProfile.TABLE == profile) {
            applyTableDefaults(formData);
        }
        return formData;
    }

    private void normalizeLlmMetrics(Map<String, Object> formData,
            SupersetDatasetInfo datasetInfo) {
        if (formData == null || datasetInfo == null) {
            return;
        }
        Map<String, SupersetDatasetColumn> columnMap =
                toColumnMap(datasetInfo.getColumns() == null ? Collections.emptyList()
                        : datasetInfo.getColumns());
        List<String> metricNames = resolveDatasetMetrics(datasetInfo);
        normalizeMetricField(formData, columnMap, metricNames, "metric");
        normalizeMetricField(formData, columnMap, metricNames, "secondary_metric");
        normalizeMetricListField(formData, columnMap, metricNames, "metrics");
        normalizeMetricListField(formData, columnMap, metricNames, "metrics_b");
        normalizeMetricListField(formData, columnMap, metricNames, "tooltip_metrics");
    }

    private void normalizeMetricField(Map<String, Object> formData,
            Map<String, SupersetDatasetColumn> columnMap, List<String> metricNames, String key) {
        Object value = formData.get(key);
        if (!(value instanceof String)) {
            return;
        }
        String metricName = (String) value;
        Object resolved = resolveMetricValue(metricName, columnMap, metricNames);
        if (resolved != null) {
            formData.put(key, resolved);
        }
    }

    private void normalizeMetricListField(Map<String, Object> formData,
            Map<String, SupersetDatasetColumn> columnMap, List<String> metricNames, String key) {
        Object value = formData.get(key);
        if (!(value instanceof List)) {
            return;
        }
        List<?> rawList = (List<?>) value;
        if (rawList.isEmpty()) {
            return;
        }
        List<Object> normalized = new ArrayList<>();
        for (Object item : rawList) {
            if (item instanceof Map) {
                normalized.add(item);
                continue;
            }
            if (!(item instanceof String)) {
                continue;
            }
            String name = (String) item;
            Object resolved = resolveMetricValue(name, columnMap, metricNames);
            if (resolved != null) {
                normalized.add(resolved);
            }
        }
        if (!normalized.isEmpty()) {
            formData.put(key, normalized);
        }
    }

    private Object resolveMetricValue(String metricName,
            Map<String, SupersetDatasetColumn> columnMap, List<String> metricNames) {
        if (StringUtils.isBlank(metricName)) {
            return null;
        }
        if (metricNames.contains(metricName)) {
            return metricName;
        }
        String normalized = normalizeMetricName(metricName);
        for (String metric : metricNames) {
            if (normalizeMetricName(metric).equals(normalized)) {
                return metric;
            }
        }
        SupersetDatasetColumn column = columnMap.get(normalizeName(metricName));
        if (column != null) {
            if (looksAggregated(column.getColumnName())) {
                return buildSqlAdhocMetric(column.getColumnName());
            }
            return buildAdhocMetric(column, "SUM");
        }
        for (Map.Entry<String, SupersetDatasetColumn> entry : columnMap.entrySet()) {
            if (normalized.contains(entry.getKey())) {
                SupersetDatasetColumn resolved = entry.getValue();
                if (resolved == null) {
                    continue;
                }
                if (looksAggregated(resolved.getColumnName())) {
                    return buildSqlAdhocMetric(resolved.getColumnName());
                }
                return buildAdhocMetric(resolved, "SUM");
            }
        }
        return null;
    }

    private String normalizeMetricName(String name) {
        if (StringUtils.isBlank(name)) {
            return "";
        }
        String trimmed = name.trim();
        String normalized = normalizeName(trimmed);
        normalized = normalized.replaceAll("^sum\\(|^avg\\(|^count\\(|^min\\(|^max\\(", "");
        normalized = normalized.replaceAll("\\)$", "");
        normalized = normalized.replaceAll("[\"`\\s]", "");
        return normalized;
    }

    private boolean looksAggregated(String name) {
        if (StringUtils.isBlank(name)) {
            return false;
        }
        String normalized = normalizeName(name);
        return normalized.contains("sum(") || normalized.contains("avg(")
                || normalized.contains("count(") || normalized.contains("min(")
                || normalized.contains("max(");
    }

    private Map<String, Object> buildSqlAdhocMetric(String expression) {
        Map<String, Object> metric = new HashMap<>();
        metric.put("expressionType", "SQL");
        metric.put("sqlExpression", expression);
        metric.put("label", expression);
        metric.put("optionName", "metric_" + normalizeName(expression));
        return metric;
    }

    private JSONObject resolveLlmFormDataKeys(SupersetPluginConfig config, QueryResult queryResult,
            SupersetDatasetInfo datasetInfo, String vizType, FormDataProfile profile, Agent agent,
            String queryText) {
        ChatLanguageModel chatLanguageModel = resolveChatModel(config, agent);
        if (chatLanguageModel == null) {
            throw new IllegalStateException("superset formdata llm chat model missing");
        }
        Map<String, Object> variables = new HashMap<>();
        variables.put("instruction", StringUtils.defaultString(queryText));
        variables.put("viz_type", StringUtils.defaultString(vizType));
        variables.put("required_keys", JsonUtil.toString(resolveRequiredKeys(vizType, profile)));
        variables.put("columns", JsonUtil.toString(resolveDatasetColumns(datasetInfo)));
        variables.put("metrics", JsonUtil.toString(resolveFormDataMetricCandidates(datasetInfo)));
        Prompt prompt = PromptTemplate.from(FORMDATA_LLM_PROMPT).apply(variables);
        Response<AiMessage> response = chatLanguageModel.generate(prompt.toUserMessage());
        String answer =
                response == null || response.content() == null ? null : response.content().text();
        log.info("superset formdata llm req:\n{} \nresp:\n{}", prompt.text(), answer);
        JSONObject payload = resolveJsonPayload(answer);
        if (payload == null) {
            throw new IllegalStateException("superset formdata llm response invalid");
        }
        validateLlmFieldsAgainstDataset(payload, datasetInfo);
        return payload;
    }

    private void validateLlmFormDataKeys(String vizType, FormDataProfile profile,
            JSONObject payload) {
        if (payload == null) {
            throw new IllegalStateException("superset formdata llm response empty");
        }
        RequiredKeyRules rules = resolveRequiredKeyRules(vizType, profile);
        if (rules == null) {
            return;
        }
        List<String> missing = new ArrayList<>();
        for (String key : rules.getRequired()) {
            if (!hasList(payload, key)) {
                missing.add(key);
            }
        }
        boolean anyOfSatisfied = true;
        if (!rules.getRequiredAnyOf().isEmpty()) {
            anyOfSatisfied = false;
            for (List<String> group : rules.getRequiredAnyOf()) {
                if (group == null || group.isEmpty()) {
                    continue;
                }
                boolean groupSatisfied = true;
                for (String key : group) {
                    if (!hasList(payload, key)) {
                        groupSatisfied = false;
                        break;
                    }
                }
                if (groupSatisfied) {
                    anyOfSatisfied = true;
                    break;
                }
            }
        }
        if (!missing.isEmpty() || !anyOfSatisfied) {
            List<String> reasons = new ArrayList<>();
            if (!missing.isEmpty()) {
                reasons.add(String.join(" + ", missing));
            }
            if (!anyOfSatisfied) {
                reasons.add("anyOf " + formatAnyOf(rules.getRequiredAnyOf()));
            }
            throw new IllegalStateException("vizType requires " + String.join(", ", reasons));
        }
    }

    private boolean requiresAggregate(FormDataProfile profile) {
        switch (profile) {
            case TABLE:
            case HANDLEBARS:
                return false;
            default:
                return true;
        }
    }

    private boolean hasMetrics(JSONObject payload) {
        if (payload == null) {
            return false;
        }
        return hasList(payload, "metrics") || StringUtils.isNotBlank(payload.getString("metric"));
    }

    private boolean hasList(JSONObject payload, String key) {
        if (payload == null || StringUtils.isBlank(key)) {
            return false;
        }
        Object value = payload.get(key);
        if (value instanceof List) {
            return !((List<?>) value).isEmpty();
        }
        if (value instanceof String) {
            return StringUtils.isNotBlank((String) value);
        }
        return false;
    }

    private void applyLlmKey(Map<String, Object> formData, JSONObject payload, String key) {
        if (formData == null || payload == null || StringUtils.isBlank(key)) {
            return;
        }
        if (!payload.containsKey(key)) {
            return;
        }
        Object value = payload.get(key);
        if (value instanceof String && StringUtils.isBlank((String) value)) {
            return;
        }
        formData.put(key, value);
    }

    private void validateLlmFieldsAgainstDataset(JSONObject payload,
            SupersetDatasetInfo datasetInfo) {
        if (payload == null || datasetInfo == null) {
            return;
        }
        Set<String> metricNames = resolveFormDataMetricCandidates(datasetInfo).stream()
                .map(String::valueOf).collect(Collectors.toSet());
        Set<String> columnNames = resolveDatasetColumns(datasetInfo).stream().map(String::valueOf)
                .collect(Collectors.toSet());
        validateFieldInSet(payload, "metric", metricNames);
        validateFieldInSet(payload, "secondary_metric", metricNames);
        validateFieldListInSet(payload, "metrics", metricNames);
        validateFieldListInSet(payload, "metrics_b", metricNames);
        validateFieldListInSet(payload, "tooltip_metrics", metricNames);
        validateFieldListInSet(payload, "groupby", columnNames);
        validateFieldListInSet(payload, "groupbyRows", columnNames);
        validateFieldListInSet(payload, "groupbyColumns", columnNames);
        validateFieldListInSet(payload, "columns", columnNames);
        validateFieldListInSet(payload, "column_collection", columnNames);
        validateFieldInSet(payload, "column", columnNames);
        validateFieldListInSet(payload, "all_columns", columnNames);
        validateFieldInSet(payload, "granularity_sqla", columnNames);
        validateFieldInSet(payload, "x_axis", columnNames);
        validateFieldInSet(payload, "y_axis", columnNames);
        validateFieldInSet(payload, "source", columnNames);
        validateFieldInSet(payload, "target", columnNames);
        validateFieldInSet(payload, "entity", columnNames);
        validateFieldInSet(payload, "latitude", columnNames);
        validateFieldInSet(payload, "longitude", columnNames);
        validateFieldInSet(payload, "start", columnNames);
        validateFieldInSet(payload, "end", columnNames);
        validateFieldInSet(payload, "x", columnNames);
        validateFieldInSet(payload, "y", columnNames);
        validateFieldInSet(payload, "size", columnNames);
        validateFieldInSet(payload, "all_columns_x", columnNames);
        validateFieldInSet(payload, "all_columns_y", columnNames);
    }

    private void validateFieldInSet(JSONObject payload, String key, Set<String> allowList) {
        if (payload == null || StringUtils.isBlank(key) || allowList == null
                || allowList.isEmpty()) {
            return;
        }
        String value = payload.getString(key);
        if (StringUtils.isBlank(value)) {
            return;
        }
        if (!allowList.contains(value)) {
            throw new IllegalStateException("llm formData field invalid: " + key);
        }
    }

    private void validateFieldListInSet(JSONObject payload, String key, Set<String> allowList) {
        if (payload == null || StringUtils.isBlank(key) || allowList == null
                || allowList.isEmpty()) {
            return;
        }
        Object value = payload.get(key);
        if (!(value instanceof List)) {
            if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                if (!allowList.contains(value)) {
                    throw new IllegalStateException("llm formData field invalid: " + key);
                }
            }
            return;
        }
        for (Object item : (List<?>) value) {
            if (item == null) {
                continue;
            }
            String name = String.valueOf(item);
            if (!allowList.contains(name)) {
                throw new IllegalStateException("llm formData field invalid: " + key);
            }
        }
    }

    private Map<String, Object> resolveRequiredKeys(String vizType, FormDataProfile profile) {
        RequiredKeyRules rules = resolveRequiredKeyRules(vizType, profile);
        Map<String, Object> payload = new HashMap<>();
        payload.put("required", rules == null ? Collections.emptyList() : rules.getRequired());
        payload.put("requiredAnyOf",
                rules == null ? Collections.emptyList() : rules.getRequiredAnyOf());
        return payload;
    }

    private RequiredKeyRules resolveRequiredKeyRules(String vizType, FormDataProfile profile) {
        SupersetVizTypeSelector.VizTypeItem item =
                SupersetVizTypeSelector.resolveItemByVizType(vizType, null);
        if (item != null && item.getFormDataRules() != null) {
            List<String> required =
                    item.getFormDataRules().getRequired() == null ? Collections.emptyList()
                            : item.getFormDataRules().getRequired().stream()
                                    .filter(StringUtils::isNotBlank).distinct()
                                    .collect(Collectors.toList());
            List<List<String>> anyOf =
                    resolveRequiredAnyOfKeys(item.getFormDataRules().getRequiredAnyOf());
            if (!required.isEmpty() || !anyOf.isEmpty()) {
                return new RequiredKeyRules(required, anyOf);
            }
        }
        return buildFallbackRequiredRules(profile);
    }

    private List<List<String>> resolveRequiredAnyOfKeys(
            List<List<SupersetVizTypeSelector.FormDataField>> requiredAnyOf) {
        if (requiredAnyOf == null || requiredAnyOf.isEmpty()) {
            return Collections.emptyList();
        }
        List<List<String>> groups = new ArrayList<>();
        for (Object groupObj : requiredAnyOf) {
            if (groupObj == null) {
                continue;
            }
            List<?> group;
            if (groupObj instanceof List) {
                group = (List<?>) groupObj;
            } else {
                group = Collections.singletonList(groupObj);
            }
            List<String> keys = new ArrayList<>();
            for (Object fieldObj : group) {
                if (fieldObj instanceof SupersetVizTypeSelector.FormDataField) {
                    String key = ((SupersetVizTypeSelector.FormDataField) fieldObj).getKey();
                    if (StringUtils.isNotBlank(key)) {
                        keys.add(key);
                    }
                } else if (fieldObj instanceof Map) {
                    Object key = ((Map<?, ?>) fieldObj).get("key");
                    if (key != null && StringUtils.isNotBlank(String.valueOf(key))) {
                        keys.add(String.valueOf(key));
                    }
                } else if (fieldObj instanceof String) {
                    if (StringUtils.isNotBlank((String) fieldObj)) {
                        keys.add((String) fieldObj);
                    }
                }
            }
            if (!keys.isEmpty()) {
                groups.add(keys);
            }
        }
        return groups;
    }

    private RequiredKeyRules buildFallbackRequiredRules(FormDataProfile profile) {
        List<String> required = new ArrayList<>();
        List<List<String>> requiredAnyOf = new ArrayList<>();
        switch (profile) {
            case TABLE:
                requiredAnyOf.add(Collections.singletonList("columns"));
                requiredAnyOf.add(Arrays.asList("metrics", "groupby"));
                break;
            case TIME_SERIES:
            case TIME_SERIES_MULTI:
                required.add("metrics");
                required.add("granularity_sqla");
                break;
            case KPI:
                required.add("metric");
                break;
            case KPI_TIME:
                required.add("metric");
                required.add("granularity_sqla");
                break;
            case PROPORTION:
            case RANKING:
                required.add("metric");
                required.add("groupby");
                break;
            case DISTRIBUTION:
                required.add("metrics");
                break;
            case HISTOGRAM:
                required.add("column");
                break;
            case HEATMAP:
                required.add("metric");
                required.add("x_axis");
                required.add("y_axis");
                break;
            case CALENDAR:
                required.add("metric");
                required.add("granularity_sqla");
                break;
            case BUBBLE:
                required.add("x");
                required.add("y");
                break;
            case FLOW:
                required.add("source");
                required.add("target");
                required.add("metric");
                break;
            case MAP_LATLON:
                required.add("latitude");
                required.add("longitude");
                break;
            case MAP_REGION:
                required.add("entity");
                required.add("metric");
                break;
            case GANTT:
                required.add("start");
                required.add("end");
                required.add("groupby");
                break;
            case HANDLEBARS:
                requiredAnyOf.add(Collections.singletonList("columns"));
                requiredAnyOf.add(Collections.singletonList("all_columns"));
                break;
            case GENERIC:
            default:
                required.add("metric");
                required.add("groupby");
                break;
        }
        return new RequiredKeyRules(required, requiredAnyOf);
    }

    private String formatAnyOf(List<List<String>> groups) {
        if (groups == null || groups.isEmpty()) {
            return "[]";
        }
        return groups.stream()
                .map(group -> group == null ? "[]" : "[" + String.join(" + ", group) + "]")
                .collect(Collectors.joining(" or "));
    }

    private FormDataContext buildFormDataContext(SemanticParseInfo parseInfo,
            QueryResult queryResult, SupersetDatasetInfo datasetInfo) {
        List<SupersetDatasetColumn> datasetColumns =
                datasetInfo == null ? Collections.emptyList() : datasetInfo.getColumns();
        if (CollectionUtils.isEmpty(datasetColumns)) {
            datasetColumns = resolveQueryColumns(queryResult);
        }
        List<String> columnNames = resolveDatasetColumns(datasetColumns);
        Map<String, SupersetDatasetColumn> columnMap = toColumnMap(datasetColumns);
        List<String> dimensionColumns = resolveDimensionColumns(parseInfo, columnMap);
        List<Object> metrics = resolveMetrics(parseInfo, datasetInfo, columnMap);
        String timeColumn = resolveTimeColumn(parseInfo, datasetInfo, columnMap);
        List<String> timeColumns = resolveTimeColumns(datasetColumns, datasetInfo);
        List<String> numericColumns = resolveNumericColumns(datasetColumns);
        List<String> selectedColumns =
                resolveSelectedColumns(parseInfo, queryResult, columnMap, dimensionColumns,
                        timeColumn);
        DateConf dateInfo = parseInfo == null ? null : parseInfo.getDateInfo();
        List<FormDataOrder> orders = resolveOrders(parseInfo, datasetInfo, columnMap);
        long rowLimit = resolveRowLimit(parseInfo);
        String timeRange = resolveTimeRange(dateInfo);
        String timeGrain = resolveTimeGrain(dateInfo);
        return new FormDataContext(datasetColumns, columnNames, selectedColumns, dimensionColumns,
                metrics, timeColumn, timeColumns, numericColumns, dateInfo, orders, rowLimit,
                timeRange, timeGrain);
    }

    private Map<String, Object> applySemanticFormData(Map<String, Object> formData,
            FormDataContext context) {
        Map<String, Object> resolved = formData == null ? new HashMap<>() : formData;
        if (context == null) {
            return resolved;
        }
        boolean applyTimeContext = shouldApplyTimeContext(resolved, context);
        if (context.rowLimit > 0) {
            resolved.put("row_limit", context.rowLimit);
            resolved.putIfAbsent("series_limit", context.rowLimit);
        }
        if (applyTimeContext && StringUtils.isNotBlank(context.timeColumn)) {
            resolved.putIfAbsent("granularity_sqla", context.timeColumn);
        }
        if (StringUtils.isNotBlank(context.timeRange)) {
            resolved.put("time_range", context.timeRange);
            if (context.dateInfo != null) {
                resolved.put("since", context.dateInfo.getStartDate());
                resolved.put("until", context.dateInfo.getEndDate());
            }
            appendTemporalAdhocFilter(resolved, context.timeColumn, context.timeRange);
        }
        if (applyTimeContext && StringUtils.isNotBlank(context.timeGrain)) {
            resolved.putIfAbsent("time_grain_sqla", context.timeGrain);
        }
        applyResolvedOrders(resolved, context.orders);
        if (isRawMode(resolved) && !CollectionUtils.isEmpty(context.selectedColumns)) {
            resolved.put("columns", context.selectedColumns);
            resolved.put("all_columns", context.selectedColumns);
        }
        return resolved;
    }

    private boolean shouldApplyTimeContext(Map<String, Object> formData, FormDataContext context) {
        if (context == null || StringUtils.isBlank(context.timeColumn)) {
            return false;
        }
        if (formData != null && (formData.containsKey("granularity_sqla")
                || formData.containsKey("time_grain_sqla") || formData.containsKey("x_axis")
                || formData.containsKey("start") || formData.containsKey("end"))) {
            return true;
        }
        if (context.dateInfo != null) {
            return true;
        }
        return !CollectionUtils.isEmpty(context.dimensions)
                && context.dimensions.contains(context.timeColumn);
    }

    private List<SupersetDatasetColumn> resolveQueryColumns(QueryResult queryResult) {
        if (queryResult == null || CollectionUtils.isEmpty(queryResult.getQueryColumns())) {
            return Collections.emptyList();
        }
        List<SupersetDatasetColumn> columns = new ArrayList<>();
        for (QueryColumn column : queryResult.getQueryColumns()) {
            if (column == null || StringUtils.isBlank(column.getName())) {
                continue;
            }
            SupersetDatasetColumn datasetColumn = new SupersetDatasetColumn();
            datasetColumn.setColumnName(column.getName());
            datasetColumn.setType(column.getType());
            datasetColumn.setIsDttm(isTimeType(column.getType()));
            datasetColumn.setGroupby(true);
            datasetColumn.setFilterable(true);
            datasetColumn.setIsActive(true);
            columns.add(datasetColumn);
        }
        return columns;
    }

    private FormDataProfile resolveFormDataProfile(String vizType) {
        SupersetVizTypeSelector.VizTypeItem item =
                SupersetVizTypeSelector.resolveItemByVizType(vizType, null);
        if (item != null && item.getFormDataRules() != null
                && StringUtils.isNotBlank(item.getFormDataRules().getProfile())) {
            try {
                return FormDataProfile
                        .valueOf(StringUtils.upperCase(item.getFormDataRules().getProfile()));
            } catch (Exception ex) {
                log.debug("superset formData profile parse failed, vizType={}, profile={}", vizType,
                        item.getFormDataRules().getProfile(), ex);
            }
        }
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

    private boolean shouldPreferRawTable(SemanticParseInfo parseInfo, QueryResult queryResult,
            FormDataContext context) {
        boolean hasSemanticSelections =
                parseInfo != null && (!CollectionUtils.isEmpty(parseInfo.getMetrics())
                        || !CollectionUtils.isEmpty(parseInfo.getDimensions()));
        if (queryResult != null && !CollectionUtils.isEmpty(queryResult.getQueryColumns())) {
            if (queryResult.getAggregateInfo() != null) {
                return false;
            }
            if (parseInfo == null || parseInfo.getAggType() == null) {
                return true;
            }
            return StringUtils.equalsIgnoreCase("NONE", parseInfo.getAggType().name());
        }
        if (parseInfo != null && parseInfo.getAggType() != null
                && !StringUtils.equalsIgnoreCase("NONE", parseInfo.getAggType().name())) {
            return false;
        }
        if (context == null || CollectionUtils.isEmpty(context.columns)) {
            return false;
        }
        return !hasSemanticSelections;
    }

    private Map<String, Object> buildTableFormData(FormDataContext context, boolean preferRaw) {
        Map<String, Object> formData = new HashMap<>();
        List<String> selectedColumns = resolveSelectedRawColumns(context);
        if (preferRaw && !context.columns.isEmpty()) {
            formData.put("query_mode", "raw");
            formData.put("all_columns", selectedColumns);
            formData.put("columns", selectedColumns);
            applyTableDefaults(formData);
            return formData;
        }
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
            applyTableDefaults(formData);
            return formData;
        }
        if (!context.columns.isEmpty()) {
            formData.put("query_mode", "raw");
            formData.put("all_columns", selectedColumns);
            formData.put("columns", selectedColumns);
            applyTableDefaults(formData);
            return formData;
        }
        throw new IllegalStateException("vizType requires columns");
    }

    private Map<String, Object> buildPivotTableFormData(FormDataContext context) {
        requireMetrics(context, "pivot_table_v2");
        List<String> rows = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        if (!context.dimensions.isEmpty()) {
            rows.add(context.dimensions.get(0));
        }
        if (context.dimensions.size() > 1) {
            columns.add(context.dimensions.get(1));
        }
        if (rows.isEmpty() && columns.isEmpty()) {
            throw new IllegalStateException("vizType requires dimensions: pivot_table_v2");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("metrics", context.metrics);
        if (!rows.isEmpty()) {
            formData.put("groupbyRows", rows);
        }
        if (!columns.isEmpty()) {
            formData.put("groupbyColumns", columns);
        }
        return formData;
    }

    private Map<String, Object> buildTimeTableFormData(FormDataContext context) {
        requireMetrics(context, "time_table");
        if (StringUtils.isBlank(context.timeColumn)) {
            throw new IllegalStateException("vizType requires time column");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("granularity_sqla", context.timeColumn);
        formData.put("metrics", context.metrics);
        if (!context.dimensions.isEmpty()) {
            formData.put("groupby", context.dimensions);
        }
        return formData;
    }

    private Map<String, Object> buildTimePivotFormData(FormDataContext context) {
        Object metric = requireSingleMetric(context, "time_pivot");
        if (StringUtils.isBlank(context.timeColumn)) {
            throw new IllegalStateException("vizType requires time column");
        }
        Map<String, Object> formData = new HashMap<>();
        formData.put("query_mode", "aggregate");
        formData.put("granularity_sqla", context.timeColumn);
        formData.put("metric", metric);
        return formData;
    }

    private void applyTableDefaults(Map<String, Object> formData) {
        if (formData == null) {
            return;
        }
        formData.putIfAbsent("all_columns", Collections.emptyList());
        formData.putIfAbsent("percent_metrics", Collections.emptyList());
        formData.putIfAbsent("adhoc_filters", Collections.emptyList());
        formData.putIfAbsent("order_by_cols", Collections.emptyList());
        formData.putIfAbsent("extra_filters", Collections.emptyList());
        formData.putIfAbsent("label_colors", Collections.emptyMap());
        formData.putIfAbsent("shared_label_colors", Collections.emptyList());
        formData.putIfAbsent("map_label_colors", Collections.emptyMap());
        formData.putIfAbsent("temporal_columns_lookup", Collections.emptyMap());
        formData.putIfAbsent("order_desc", true);
        formData.putIfAbsent("row_limit", 10000);
        formData.putIfAbsent("server_page_length", 10);
        formData.putIfAbsent("percent_metric_calculation", "row_limit");
        formData.putIfAbsent("table_timestamp_format", "smart_date");
        formData.putIfAbsent("allow_render_html", true);
        formData.putIfAbsent("show_cell_bars", true);
        formData.putIfAbsent("color_pn", true);
        formData.putIfAbsent("comparison_color_scheme", "Green");
        formData.putIfAbsent("comparison_type", "values");
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
        List<String> selectedColumns = resolveSelectedRawColumns(context);
        formData.put("all_columns", selectedColumns);
        formData.put("columns", selectedColumns);
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

    private List<String> resolveSelectedRawColumns(FormDataContext context) {
        if (context == null) {
            return Collections.emptyList();
        }
        if (!CollectionUtils.isEmpty(context.selectedColumns)) {
            return context.selectedColumns;
        }
        return context.columns;
    }

    private boolean isRawMode(Map<String, Object> formData) {
        if (formData == null) {
            return false;
        }
        return StringUtils.equalsIgnoreCase(String.valueOf(formData.get("query_mode")), "raw");
    }

    private void appendTemporalAdhocFilter(Map<String, Object> formData, String timeColumn,
            String timeRange) {
        if (formData == null || StringUtils.isBlank(timeColumn) || StringUtils.isBlank(timeRange)) {
            return;
        }
        List<Object> filters = new ArrayList<>();
        Object existing = formData.get("adhoc_filters");
        if (existing instanceof List) {
            for (Object item : (List<?>) existing) {
                if (item instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) item;
                    if (StringUtils.equalsIgnoreCase(String.valueOf(map.get("operator")),
                            "TEMPORAL_RANGE")
                            && StringUtils.equalsIgnoreCase(String.valueOf(map.get("subject")),
                                    timeColumn)) {
                        continue;
                    }
                }
                filters.add(item);
            }
        }
        Map<String, Object> temporal = new HashMap<>();
        temporal.put("clause", "WHERE");
        temporal.put("expressionType", "SIMPLE");
        temporal.put("subject", timeColumn);
        temporal.put("operator", "TEMPORAL_RANGE");
        temporal.put("comparator", timeRange);
        filters.add(temporal);
        formData.put("adhoc_filters", filters);
    }

    private void applyResolvedOrders(Map<String, Object> formData, List<FormDataOrder> orders) {
        if (formData == null || CollectionUtils.isEmpty(orders)) {
            return;
        }
        List<Object> orderBy = new ArrayList<>();
        for (FormDataOrder order : orders) {
            if (order == null || order.target == null) {
                continue;
            }
            List<Object> entry = new ArrayList<>();
            entry.add(order.target);
            entry.add(!order.descending);
            orderBy.add(entry);
        }
        if (orderBy.isEmpty()) {
            return;
        }
        formData.put("orderby", orderBy);
        formData.put("order_by_cols", orderBy);
        formData.put("order_desc", orders.get(0).descending);
        if (orders.get(0).metric) {
            formData.put("timeseries_limit_metric", orders.get(0).target);
            formData.put("series_limit_metric", orders.get(0).target);
        }
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

    private ChatLanguageModel resolveChatModel(SupersetPluginConfig config, Agent agent) {
        Integer chatModelId = resolveChatModelId(config, agent);
        if (chatModelId == null) {
            return null;
        }
        ChatModelConfig chatModelConfig = resolveChatModelConfig(chatModelId);
        if (chatModelConfig == null) {
            return null;
        }
        return ModelProvider.getChatModel(chatModelConfig);
    }

    private Integer resolveChatModelId(SupersetPluginConfig config, Agent agent) {
        if (config != null && config.getVizTypeLlmChatModelId() != null) {
            return config.getVizTypeLlmChatModelId();
        }
        return resolveAgentChatModelId(agent);
    }

    private Integer resolveAgentChatModelId(Agent agent) {
        if (agent == null) {
            return null;
        }
        Map<String, ChatApp> chatAppConfig = agent.getChatAppConfig();
        if (chatAppConfig == null || chatAppConfig.isEmpty()) {
            return null;
        }
        Integer enabledChatModelId = resolveChatModelIdFromApps(chatAppConfig, true);
        if (enabledChatModelId != null) {
            return enabledChatModelId;
        }
        return resolveChatModelIdFromApps(chatAppConfig, false);
    }

    private Integer resolveChatModelIdFromApps(Map<String, ChatApp> chatAppConfig,
            boolean enabledOnly) {
        if (chatAppConfig == null || chatAppConfig.isEmpty()) {
            return null;
        }
        ChatApp preferred = chatAppConfig.get("S2SQL_PARSER");
        if (isChatAppUsable(preferred, enabledOnly)) {
            return preferred.getChatModelId();
        }
        List<String> keys = new ArrayList<>(chatAppConfig.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            ChatApp app = chatAppConfig.get(key);
            if (isChatAppUsable(app, enabledOnly)) {
                return app.getChatModelId();
            }
        }
        return null;
    }

    private boolean isChatAppUsable(ChatApp app, boolean enabledOnly) {
        if (app == null || app.getChatModelId() == null) {
            return false;
        }
        return !enabledOnly || app.isEnable();
    }

    private ChatModelConfig resolveChatModelConfig(Integer chatModelId) {
        if (chatModelId == null) {
            return null;
        }
        ChatModelService chatModelService = ContextUtils.getBean(ChatModelService.class);
        if (chatModelService == null) {
            return null;
        }
        ChatModel chatModel = chatModelService.getChatModel(chatModelId);
        return chatModel == null ? null : chatModel.getConfig();
    }

    private JSONObject resolveJsonPayload(String response) {
        if (StringUtils.isBlank(response)) {
            return null;
        }
        int start = response.indexOf('{');
        int end = response.lastIndexOf('}');
        if (start < 0 || end <= start) {
            return null;
        }
        String payload = response.substring(start, end + 1);
        try {
            return JSONObject.parseObject(payload);
        } catch (Exception ex) {
            log.warn("superset llm response parse failed", ex);
            return null;
        }
    }

    private static class FormDataContext {
        private final List<SupersetDatasetColumn> datasetColumns;
        private final List<String> columns;
        private final List<String> selectedColumns;
        private final List<String> dimensions;
        private final List<Object> metrics;
        private final String timeColumn;
        private final List<String> timeColumns;
        private final List<String> numericColumns;
        private final DateConf dateInfo;
        private final List<FormDataOrder> orders;
        private final long rowLimit;
        private final String timeRange;
        private final String timeGrain;

        private FormDataContext(List<SupersetDatasetColumn> datasetColumns, List<String> columns,
                List<String> selectedColumns, List<String> dimensions, List<Object> metrics,
                String timeColumn, List<String> timeColumns, List<String> numericColumns,
                DateConf dateInfo, List<FormDataOrder> orders, long rowLimit, String timeRange,
                String timeGrain) {
            this.datasetColumns = datasetColumns;
            this.columns = columns;
            this.selectedColumns = selectedColumns;
            this.dimensions = dimensions;
            this.metrics = metrics;
            this.timeColumn = timeColumn;
            this.timeColumns = timeColumns;
            this.numericColumns = numericColumns;
            this.dateInfo = dateInfo;
            this.orders = orders;
            this.rowLimit = rowLimit;
            this.timeRange = timeRange;
            this.timeGrain = timeGrain;
        }
    }

    private static class FormDataOrder {
        private final Object target;
        private final boolean metric;
        private final boolean descending;

        private FormDataOrder(Object target, boolean metric, boolean descending) {
            this.target = target;
            this.metric = metric;
            this.descending = descending;
        }
    }

    private static class RequiredKeyRules {
        private final List<String> required;
        private final List<List<String>> requiredAnyOf;

        private RequiredKeyRules(List<String> required, List<List<String>> requiredAnyOf) {
            this.required = required == null ? Collections.emptyList() : required;
            this.requiredAnyOf = requiredAnyOf == null ? Collections.emptyList() : requiredAnyOf;
        }

        private List<String> getRequired() {
            return required;
        }

        private List<List<String>> getRequiredAnyOf() {
            return requiredAnyOf;
        }
    }

    private enum FormDataProfile {
        TABLE,
        PIVOT_TABLE,
        TIME_TABLE,
        TIME_PIVOT,
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

    private List<String> resolveDatasetColumns(SupersetDatasetInfo datasetInfo) {
        if (datasetInfo == null) {
            return Collections.emptyList();
        }
        return resolveDatasetColumns(datasetInfo.getColumns());
    }

    private List<String> resolveDatasetMetrics(SupersetDatasetInfo datasetInfo) {
        if (datasetInfo == null || CollectionUtils.isEmpty(datasetInfo.getMetrics())) {
            return Collections.emptyList();
        }
        Map<String, SupersetDatasetColumn> columnMap =
                toColumnMap(datasetInfo.getColumns() == null ? Collections.emptyList()
                        : datasetInfo.getColumns());
        Map<String, SupersetDatasetColumn> relaxedColumnMap = buildRelaxedColumnMap(columnMap);
        return datasetInfo.getMetrics().stream()
                .filter(metric -> isDatasetMetricExecutable(metric, columnMap, relaxedColumnMap))
                .map(SupersetDatasetMetric::getMetricName).filter(StringUtils::isNotBlank).distinct()
                .collect(Collectors.toList());
    }

    private List<String> resolveFormDataMetricCandidates(SupersetDatasetInfo datasetInfo) {
        List<String> candidates = new ArrayList<>();
        candidates.addAll(resolveDatasetMetrics(datasetInfo));
        if (datasetInfo == null || CollectionUtils.isEmpty(datasetInfo.getColumns())) {
            return candidates.stream().distinct().collect(Collectors.toList());
        }
        for (SupersetDatasetColumn column : datasetInfo.getColumns()) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            String columnName = column.getColumnName();
            if (looksAggregated(columnName)) {
                candidates.add(columnName);
                continue;
            }
            if (isNumericType(column.getType())) {
                candidates.add(columnName);
            }
        }
        return candidates.stream().filter(StringUtils::isNotBlank).distinct()
                .collect(Collectors.toList());
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

    private Map<String, SupersetDatasetMetric> toMetricMap(List<SupersetDatasetMetric> metrics,
            Map<String, SupersetDatasetColumn> columnMap) {
        if (CollectionUtils.isEmpty(metrics)) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetColumn> safeColumnMap =
                columnMap == null ? Collections.emptyMap() : columnMap;
        Map<String, SupersetDatasetColumn> relaxedColumnMap = buildRelaxedColumnMap(safeColumnMap);
        Map<String, SupersetDatasetMetric> map = new HashMap<>();
        for (SupersetDatasetMetric metric : metrics) {
            if (!isDatasetMetricExecutable(metric, safeColumnMap, relaxedColumnMap)) {
                continue;
            }
            map.put(normalizeName(metric.getMetricName()), metric);
        }
        return map;
    }

    private boolean isDatasetMetricExecutable(SupersetDatasetMetric metric,
            Map<String, SupersetDatasetColumn> columnMap,
            Map<String, SupersetDatasetColumn> relaxedColumnMap) {
        if (metric == null || StringUtils.isBlank(metric.getMetricName())) {
            return false;
        }
        String expression = StringUtils.trimToNull(metric.getExpression());
        if (expression == null) {
            return false;
        }
        Set<String> sourceFields;
        try {
            sourceFields = SqlSelectHelper.getFieldsFromExpr(expression);
        } catch (RuntimeException ex) {
            log.debug("superset dataset metric expression parse failed, metricName={}, expression={}",
                    metric.getMetricName(), expression, ex);
            return false;
        }
        if (CollectionUtils.isEmpty(sourceFields)) {
            return true;
        }
        if (CollectionUtils.isEmpty(columnMap)) {
            return false;
        }
        for (String sourceField : sourceFields) {
            if (StringUtils.isBlank(sourceField)) {
                continue;
            }
            if (resolveColumnName(sourceField, columnMap, relaxedColumnMap) == null) {
                return false;
            }
        }
        return true;
    }

    private List<String> resolveSelectedColumns(SemanticParseInfo parseInfo, QueryResult queryResult,
            Map<String, SupersetDatasetColumn> columnMap, List<String> dimensionColumns,
            String timeColumn) {
        List<String> selected = new ArrayList<>();
        Map<String, SupersetDatasetColumn> relaxedMap = buildRelaxedColumnMap(columnMap);
        if (queryResult != null && !CollectionUtils.isEmpty(queryResult.getQueryColumns())) {
            for (QueryColumn queryColumn : queryResult.getQueryColumns()) {
                String resolved = resolveQueryColumnName(queryColumn, columnMap, relaxedMap);
                if (StringUtils.isNotBlank(resolved)) {
                    selected.add(resolved);
                }
            }
        }
        if (!selected.isEmpty()) {
            return selected.stream().distinct().collect(Collectors.toList());
        }
        if (parseInfo == null) {
            return Collections.emptyList();
        }
        if (!CollectionUtils.isEmpty(dimensionColumns)) {
            selected.addAll(dimensionColumns);
        }
        if (StringUtils.isNotBlank(timeColumn) && !selected.contains(timeColumn)) {
            selected.add(timeColumn);
        }
        return selected.stream().distinct().collect(Collectors.toList());
    }

    private String resolveQueryColumnName(QueryColumn queryColumn,
            Map<String, SupersetDatasetColumn> columnMap,
            Map<String, SupersetDatasetColumn> relaxedColumnMap) {
        if (queryColumn == null) {
            return null;
        }
        List<String> candidates = new ArrayList<>();
        candidates.add(queryColumn.getName());
        candidates.add(queryColumn.getBizName());
        candidates.add(queryColumn.getNameEn());
        for (String candidate : candidates) {
            String resolved = resolveColumnName(candidate, columnMap, relaxedColumnMap);
            if (StringUtils.isNotBlank(resolved)) {
                return resolved;
            }
        }
        for (String candidate : candidates) {
            if (StringUtils.isNotBlank(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private String resolveColumnName(String name, Map<String, SupersetDatasetColumn> columnMap,
            Map<String, SupersetDatasetColumn> relaxedColumnMap) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        SupersetDatasetColumn column = columnMap.get(normalizeName(name));
        if (column == null && relaxedColumnMap != null && !relaxedColumnMap.isEmpty()) {
            column = relaxedColumnMap.get(normalizeRelaxedName(name));
        }
        if (column != null && StringUtils.isNotBlank(column.getColumnName())) {
            return column.getColumnName();
        }
        return null;
    }

    private List<FormDataOrder> resolveOrders(SemanticParseInfo parseInfo,
            SupersetDatasetInfo datasetInfo, Map<String, SupersetDatasetColumn> columnMap) {
        if (parseInfo == null || CollectionUtils.isEmpty(parseInfo.getOrders())) {
            return Collections.emptyList();
        }
        Map<String, SupersetDatasetColumn> relaxedColumnMap = buildRelaxedColumnMap(columnMap);
        Map<String, SupersetDatasetMetric> metricMap = toMetricMap(
                datasetInfo == null ? Collections.emptyList() : datasetInfo.getMetrics(),
                columnMap);
        List<FormDataOrder> resolved = new ArrayList<>();
        List<Order> sourceOrders = parseInfo.getOrders().stream()
                .sorted((left, right) -> {
                    String leftKey = left == null ? "" : StringUtils.defaultString(left.getColumn());
                    String rightKey =
                            right == null ? "" : StringUtils.defaultString(right.getColumn());
                    int compare = leftKey.compareToIgnoreCase(rightKey);
                    if (compare != 0) {
                        return compare;
                    }
                    String leftDirection =
                            left == null ? "" : StringUtils.defaultString(left.getDirection());
                    String rightDirection =
                            right == null ? "" : StringUtils.defaultString(right.getDirection());
                    return leftDirection.compareToIgnoreCase(rightDirection);
                }).collect(Collectors.toList());
        for (Order order : sourceOrders) {
            FormDataOrder item =
                    resolveOrder(parseInfo, order, metricMap, columnMap, relaxedColumnMap);
            if (item != null) {
                resolved.add(item);
            }
        }
        return resolved;
    }

    private FormDataOrder resolveOrder(SemanticParseInfo parseInfo, Order order,
            Map<String, SupersetDatasetMetric> metricMap,
            Map<String, SupersetDatasetColumn> columnMap,
            Map<String, SupersetDatasetColumn> relaxedColumnMap) {
        if (order == null || StringUtils.isBlank(order.getColumn())) {
            return null;
        }
        boolean descending = !StringUtils.equalsIgnoreCase(order.getDirection(), "asc");
        String orderColumn = order.getColumn();
        String normalizedOrder = normalizeName(orderColumn);
        boolean metricOrder = isMetricOrder(parseInfo, normalizedOrder);
        if (metricOrder) {
            SupersetDatasetMetric datasetMetric = metricMap.get(normalizedOrder);
            if (datasetMetric != null && StringUtils.isNotBlank(datasetMetric.getMetricName())) {
                return new FormDataOrder(datasetMetric.getMetricName(), true, descending);
            }
            SupersetDatasetColumn metricColumn =
                    resolveOrderColumn(orderColumn, columnMap, relaxedColumnMap);
            if (metricColumn != null) {
                return new FormDataOrder(buildAdhocMetric(metricColumn,
                        resolveMetricAggregate(parseInfo, normalizedOrder)), true, descending);
            }
        }
        SupersetDatasetColumn column = resolveOrderColumn(orderColumn, columnMap, relaxedColumnMap);
        if (column != null) {
            return new FormDataOrder(column.getColumnName(), false, descending);
        }
        if (!metricMap.isEmpty()) {
            for (Map.Entry<String, SupersetDatasetMetric> entry : metricMap.entrySet()) {
                if (normalizedOrder.contains(entry.getKey())
                        || entry.getKey().contains(normalizedOrder)) {
                    return new FormDataOrder(entry.getValue().getMetricName(), true, descending);
                }
            }
        }
        return new FormDataOrder(orderColumn, false, descending);
    }

    private boolean isMetricOrder(SemanticParseInfo parseInfo, String normalizedOrder) {
        if (parseInfo == null || StringUtils.isBlank(normalizedOrder)
                || CollectionUtils.isEmpty(parseInfo.getMetrics())) {
            return false;
        }
        for (SchemaElement metric : parseInfo.getMetrics()) {
            String metricName = resolveSchemaElementName(metric);
            if (normalizeName(metricName).equals(normalizedOrder)) {
                return true;
            }
        }
        return false;
    }

    private SupersetDatasetColumn resolveOrderColumn(String name,
            Map<String, SupersetDatasetColumn> columnMap,
            Map<String, SupersetDatasetColumn> relaxedColumnMap) {
        String resolvedName = resolveColumnName(name, columnMap, relaxedColumnMap);
        if (StringUtils.isBlank(resolvedName)) {
            return null;
        }
        return columnMap.get(normalizeName(resolvedName));
    }

    private String resolveMetricAggregate(SemanticParseInfo parseInfo, String normalizedMetricName) {
        if (parseInfo == null || CollectionUtils.isEmpty(parseInfo.getMetrics())) {
            return "SUM";
        }
        for (SchemaElement metric : parseInfo.getMetrics()) {
            String metricName = resolveSchemaElementName(metric);
            if (normalizeName(metricName).equals(normalizedMetricName)) {
                String aggregate = resolveAggregate(metric);
                return StringUtils.defaultIfBlank(aggregate, "SUM");
            }
        }
        return "SUM";
    }

    private long resolveRowLimit(SemanticParseInfo parseInfo) {
        if (parseInfo == null || parseInfo.getLimit() <= 0) {
            return 0L;
        }
        return parseInfo.getLimit();
    }

    private String resolveTimeRange(DateConf dateInfo) {
        if (dateInfo == null || DateConf.DateMode.ALL.equals(dateInfo.getDateMode())) {
            return null;
        }
        String start = StringUtils.trimToNull(dateInfo.getStartDate());
        String end = StringUtils.trimToNull(dateInfo.getEndDate());
        if ((start == null || end == null) && !CollectionUtils.isEmpty(dateInfo.getDateList())) {
            start = start == null ? dateInfo.getDateList().get(0) : start;
            end = end == null ? dateInfo.getDateList().get(dateInfo.getDateList().size() - 1) : end;
        }
        if (start != null && end != null) {
            return start + " : " + end;
        }
        return firstNonBlank(start, end);
    }

    private String resolveTimeGrain(DateConf dateInfo) {
        if (dateInfo == null || dateInfo.getPeriod() == null) {
            return null;
        }
        DatePeriodEnum period = dateInfo.getPeriod();
        if (DatePeriodEnum.DAY.equals(period)) {
            return "P1D";
        }
        if (DatePeriodEnum.WEEK.equals(period)) {
            return "P1W";
        }
        if (DatePeriodEnum.MONTH.equals(period)) {
            return "P1M";
        }
        if (DatePeriodEnum.QUARTER.equals(period)) {
            return "P3M";
        }
        if (DatePeriodEnum.YEAR.equals(period)) {
            return "P1Y";
        }
        return null;
    }

    private String firstNonBlank(String left, String right) {
        if (StringUtils.isNotBlank(left)) {
            return left;
        }
        return StringUtils.trimToNull(right);
    }

    private List<String> resolveDimensionColumns(SemanticParseInfo parseInfo,
            Map<String, SupersetDatasetColumn> columnMap) {
        if (parseInfo != null && !CollectionUtils.isEmpty(parseInfo.getDimensions())) {
            Map<String, SupersetDatasetColumn> relaxedMap = buildRelaxedColumnMap(columnMap);
            List<String> dimensions = new ArrayList<>();
            for (SchemaElement element : parseInfo.getDimensions()) {
                String name = resolveSchemaElementName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                if (!columnMap.isEmpty()) {
                    SupersetDatasetColumn column = columnMap.get(normalizeName(name));
                    if (column == null && !relaxedMap.isEmpty()) {
                        column = relaxedMap.get(normalizeRelaxedName(name));
                    }
                    if (column != null && StringUtils.isNotBlank(column.getColumnName())) {
                        dimensions.add(column.getColumnName());
                        continue;
                    }
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

    private Map<String, SupersetDatasetColumn> buildRelaxedColumnMap(
            Map<String, SupersetDatasetColumn> columnMap) {
        if (columnMap == null || columnMap.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetColumn> relaxed = new HashMap<>();
        for (SupersetDatasetColumn column : columnMap.values()) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            relaxed.put(normalizeRelaxedName(column.getColumnName()), column);
        }
        return relaxed;
    }

    private String normalizeRelaxedName(String name) {
        String normalized = StringUtils.lowerCase(StringUtils.trimToEmpty(name));
        return normalized.replaceAll("\\s+", "");
    }

    private List<Object> resolveMetrics(SemanticParseInfo parseInfo,
            SupersetDatasetInfo datasetInfo, Map<String, SupersetDatasetColumn> columnMap) {
        List<Object> metrics = new ArrayList<>();
        List<SupersetDatasetMetric> datasetMetrics =
                datasetInfo == null ? Collections.emptyList() : datasetInfo.getMetrics();
        Map<String, SupersetDatasetMetric> metricMap = toMetricMap(datasetMetrics, columnMap);
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
                        continue;
                    }
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
        Map<String, SupersetDatasetColumn> relaxedMap = buildRelaxedColumnMap(columnMap);
        if (parseInfo != null && parseInfo.getDateInfo() != null
                && StringUtils.isNotBlank(parseInfo.getDateInfo().getDateField())) {
            String resolved = resolveColumnName(parseInfo.getDateInfo().getDateField(), columnMap,
                    relaxedMap);
            if (StringUtils.isNotBlank(resolved)) {
                return resolved;
            }
        }
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
        if (!columnMap.isEmpty()) {
            for (SupersetDatasetColumn column : columnMap.values()) {
                if (column == null || StringUtils.isBlank(column.getColumnName())) {
                    continue;
                }
                if (isTimeType(column.getType())) {
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
                if (column == null && !relaxedMap.isEmpty()) {
                    column = relaxedMap.get(normalizeRelaxedName(name));
                }
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

    private boolean isTimeType(String type) {
        String normalized = StringUtils.defaultString(type).toUpperCase();
        return normalized.contains("DATE") || normalized.contains("TIME")
                || normalized.contains("TIMESTAMP");
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

    /**
     * 根据 Superset 图表类型和插件配置解析 dashboard 高度。
     *
     * Args: config: Superset 插件配置。 vizType: Superset 图表类型。
     *
     * Returns: dashboard 高度。
     */
    private Integer resolveDashboardHeight(SupersetPluginConfig config, String vizType) {
        Integer configuredHeight = config == null ? null : config.getHeight();
        if (configuredHeight != null && configuredHeight > 0) {
            return configuredHeight;
        }
        if (isLineVizType(vizType)) {
            return LINE_CHART_HEIGHT;
        }
        return DEFAULT_SINGLE_CHART_HEIGHT;
    }

    private Integer resolveDashboardHeight(List<Integer> chartHeights) {
        if (CollectionUtils.isEmpty(chartHeights)) {
            return DEFAULT_SINGLE_CHART_HEIGHT;
        }
        int maxHeight = DEFAULT_SINGLE_CHART_HEIGHT;
        for (Integer chartHeight : chartHeights) {
            int resolved = chartHeight == null || chartHeight <= 0 ? DEFAULT_SINGLE_CHART_HEIGHT
                    : chartHeight;
            maxHeight = Math.max(maxHeight, resolved);
        }
        return maxHeight;
    }

    /**
     * 判断是否为折线类图表。
     *
     * Args: vizType: 图表类型。
     *
     * Returns: 是否为 line 类。
     */
    private boolean isLineVizType(String vizType) {
        if (StringUtils.isBlank(vizType)) {
            return false;
        }
        return vizType.toLowerCase().contains("line");
    }

    private WebBase buildWebPage(Integer height) {
        WebBase webBase = new WebBase();
        webBase.setUrl("");
        List<ParamOption> paramOptions = new ArrayList<>();
        Integer resolvedHeight = height;
        if (resolvedHeight == null || resolvedHeight <= 0) {
            resolvedHeight = DEFAULT_SINGLE_CHART_HEIGHT;
        }
        ParamOption heightOption = new ParamOption();
        heightOption.setParamType(ParamOption.ParamType.FORWARD);
        heightOption.setKey("height");
        heightOption.setValue(resolvedHeight);
        paramOptions.add(heightOption);
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
