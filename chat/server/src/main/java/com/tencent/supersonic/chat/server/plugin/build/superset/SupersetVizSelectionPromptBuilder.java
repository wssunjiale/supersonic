package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
import com.tencent.supersonic.common.pojo.Order;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.request.QueryFilter;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
class SupersetVizSelectionPromptBuilder {

    private static final int MAX_PREVIEW_ROWS = 8;
    private static final int MAX_PROFILE_ROWS = 50;
    private static final int MAX_SAMPLE_VALUES = 5;

    static String buildJsonPayload(String queryText, String executedSql, QueryResult queryResult,
            SemanticParseInfo parseInfo, SupersetDatasetInfo datasetInfo, SelectionSignals signals,
            List<SupersetVizTypeSelector.VizTypeItem> candidates) {
        return JsonUtil.toString(buildPayload(queryText, executedSql, queryResult, parseInfo,
                datasetInfo, signals, candidates));
    }

    static VizSelectionPromptPayload buildPayload(String queryText, String executedSql,
            QueryResult queryResult, SemanticParseInfo parseInfo, SupersetDatasetInfo datasetInfo,
            SelectionSignals signals, List<SupersetVizTypeSelector.VizTypeItem> candidates) {
        VizSelectionPromptPayload payload = new VizSelectionPromptPayload();
        payload.setUserQuestion(StringUtils.defaultString(queryText));
        payload.setExecutedSql(StringUtils.defaultString(executedSql));
        payload.setSqlSummary(buildSqlSummary(executedSql));
        payload.setQueryResult(buildResultContext(queryResult));
        payload.setDataProfile(buildDataProfile(queryResult));
        payload.setDatasetContext(buildDatasetContext(datasetInfo));
        payload.setSemanticContext(buildSemanticContext(parseInfo));
        payload.setSignals(signals == null ? new SelectionSignals() : signals);
        payload.setIntentProfile(buildIntentProfile(queryText, payload.getSqlSummary(), parseInfo,
                payload.getSignals()));
        payload.setCandidateViztypes(buildCandidateContexts(candidates));
        return payload;
    }

    static IntentProfile buildIntentProfile(String queryText, SqlSummary sqlSummary,
            SemanticParseInfo parseInfo, SelectionSignals signals) {
        IntentProfile profile = new IntentProfile();
        String normalizedQuestion = normalize(queryText);
        boolean detailRequest = containsAny(normalizedQuestion, "明细", "列出", "列表", "清单", "详情", "原始",
                "记录", "detail", "details", "list", "raw", "rows", "records");
        boolean pivotRequested = containsAny(normalizedQuestion, "透视", "交叉", "pivot", "crosstab");
        boolean timeSeriesRequested = containsAny(normalizedQuestion, "趋势", "时序", "时间序列", "同比",
                "环比", "trend", "time series", "sparkline", "period over period");
        boolean aggregationDetected = hasAggregation(sqlSummary, parseInfo);
        boolean rawRowResultLikely = detailRequest && !aggregationDetected && !pivotRequested
                && (signals == null || signals.getColumnCount() >= 3);

        profile.setDetailRequest(detailRequest);
        profile.setPivotRequested(pivotRequested);
        profile.setTimeSeriesRequested(timeSeriesRequested);
        profile.setAggregationDetected(aggregationDetected);
        profile.setRawRowResultLikely(rawRowResultLikely);

        List<String> reasons = new ArrayList<>();
        if (detailRequest) {
            reasons.add("question_requests_detail_rows");
        }
        if (pivotRequested) {
            reasons.add("question_mentions_pivot_or_crosstab");
        }
        if (timeSeriesRequested) {
            reasons.add("question_mentions_time_series_or_period_comparison");
        }
        if (aggregationDetected) {
            reasons.add("sql_or_semantic_context_detects_aggregation");
        }
        if (rawRowResultLikely) {
            reasons.add("detail_request_without_group_by_or_explicit_pivot");
        }
        profile.setReasons(reasons);

        if (rawRowResultLikely && !timeSeriesRequested) {
            profile.setPreferredTableVizType("table");
        } else if (pivotRequested) {
            profile.setPreferredTableVizType("pivot_table_v2");
        } else if (detailRequest && timeSeriesRequested) {
            profile.setPreferredTableVizType("time_table");
        }
        return profile;
    }

    static IntentProfile buildIntentProfile(String queryText, String executedSql,
            SemanticParseInfo parseInfo, SelectionSignals signals) {
        return buildIntentProfile(queryText, buildSqlSummary(executedSql), parseInfo, signals);
    }

    private static boolean hasAggregation(SqlSummary sqlSummary, SemanticParseInfo parseInfo) {
        if (sqlSummary != null && !CollectionUtils.isEmpty(sqlSummary.getGroupByFields())) {
            return true;
        }
        return parseInfo != null && parseInfo.getAggType() != null
                && !StringUtils.equalsIgnoreCase("NONE", parseInfo.getAggType().name());
    }

    private static boolean containsAny(String value, String... tokens) {
        if (StringUtils.isBlank(value) || tokens == null || tokens.length == 0) {
            return false;
        }
        return Arrays.stream(tokens).filter(StringUtils::isNotBlank)
                .anyMatch(token -> value.contains(normalize(token)));
    }

    private static SqlSummary buildSqlSummary(String executedSql) {
        SqlSummary summary = new SqlSummary();
        if (StringUtils.isBlank(executedSql)) {
            return summary;
        }
        summary.setTableName(safeString(() -> SqlSelectHelper.getTableName(executedSql)));
        summary.setSelectFields(safeList(() -> SqlSelectHelper.getSelectFields(executedSql)));
        summary.setWhereFields(safeList(() -> SqlSelectHelper.getWhereFields(executedSql)));
        summary.setGroupByFields(safeList(() -> SqlSelectHelper.getGroupByFields(executedSql)));
        summary.setOrderByFields(safeList(() -> SqlSelectHelper.getOrderByFields(executedSql)));
        summary.setAllReferencedFields(
                safeList(() -> SqlSelectHelper.getAllSelectFields(executedSql)));
        summary.setHasLimit(safeBoolean(() -> SqlSelectHelper.hasLimit(executedSql)));
        summary.setHasSubQuery(safeBoolean(() -> SqlSelectHelper.hasSubSelect(executedSql)));
        summary.setJoinCount(countJoins(executedSql));
        summary.setHasJoin(summary.getJoinCount() > 0);
        return summary;
    }

    private static int countJoins(String executedSql) {
        List<PlainSelect> selects = safeList(() -> SqlSelectHelper.getPlainSelect(executedSql));
        int count = 0;
        for (PlainSelect select : selects) {
            if (select == null || CollectionUtils.isEmpty(select.getJoins())) {
                continue;
            }
            count += select.getJoins().size();
        }
        return count;
    }

    private static ResultContext buildResultContext(QueryResult queryResult) {
        ResultContext context = new ResultContext();
        if (queryResult == null) {
            return context;
        }
        List<Map<String, Object>> rows = defaultRows(queryResult.getQueryResults());
        context.setRowCount(rows.size());
        context.setColumnCount(
                queryResult.getQueryColumns() == null ? 0 : queryResult.getQueryColumns().size());
        context.setPreviewRows(sampleRows(rows, MAX_PREVIEW_ROWS));
        if (queryResult.getAggregateInfo() != null) {
            context.setAggregateInfo(queryResult.getAggregateInfo().getMetricInfos());
        }
        return context;
    }

    private static Map<String, Object> buildDataProfile(QueryResult queryResult) {
        Map<String, Object> profile = new LinkedHashMap<>();
        List<QueryColumn> columns =
                queryResult == null ? Collections.emptyList() : queryResult.getQueryColumns();
        List<Map<String, Object>> rows =
                queryResult == null ? Collections.emptyList() : queryResult.getQueryResults();
        List<Map<String, Object>> sampleRows = sampleRows(rows, MAX_PROFILE_ROWS);
        profile.put("sampleRowCount", sampleRows.size());
        if (!CollectionUtils.isEmpty(columns)) {
            List<Map<String, Object>> normalizedRows = normalizeRows(sampleRows);
            List<Map<String, Object>> columnProfiles = new ArrayList<>();
            for (QueryColumn column : columns) {
                columnProfiles.add(buildColumnProfile(column, normalizedRows));
            }
            profile.put("columns", columnProfiles);
        }
        return profile;
    }

    private static DatasetContext buildDatasetContext(SupersetDatasetInfo datasetInfo) {
        DatasetContext context = new DatasetContext();
        if (datasetInfo == null) {
            return context;
        }
        context.setDatasetId(datasetInfo.getId());
        context.setDatabaseId(datasetInfo.getDatabaseId());
        context.setSchema(datasetInfo.getSchema());
        context.setTableName(datasetInfo.getTableName());
        context.setMainDttmCol(datasetInfo.getMainDttmCol());
        context.setDescription(datasetInfo.getDescription());
        context.setColumns(toColumnPayloads(datasetInfo.getColumns()));
        context.setMetrics(toMetricPayloads(datasetInfo.getMetrics()));
        context.setRoleInventory(buildDatasetRoleInventory(datasetInfo));
        return context;
    }

    private static Map<String, Object> buildDatasetRoleInventory(SupersetDatasetInfo datasetInfo) {
        Map<String, Object> inventory = new LinkedHashMap<>();
        List<SupersetDatasetColumn> columns =
                datasetInfo == null || datasetInfo.getColumns() == null ? Collections.emptyList()
                        : datasetInfo.getColumns();
        inventory.put("timeColumns",
                columns.stream()
                        .filter(column -> column != null && Boolean.TRUE.equals(column.getIsDttm()))
                        .map(SupersetDatasetColumn::getColumnName).filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList()));
        inventory.put("groupbyColumns", columns.stream()
                .filter(column -> column != null && Boolean.TRUE.equals(column.getGroupby()))
                .map(SupersetDatasetColumn::getColumnName).filter(StringUtils::isNotBlank)
                .collect(Collectors.toList()));
        inventory.put("filterableColumns", columns.stream()
                .filter(column -> column != null && Boolean.TRUE.equals(column.getFilterable()))
                .map(SupersetDatasetColumn::getColumnName).filter(StringUtils::isNotBlank)
                .collect(Collectors.toList()));
        inventory.put("metricNames",
                datasetInfo == null || datasetInfo.getMetrics() == null ? Collections.emptyList()
                        : datasetInfo.getMetrics().stream()
                                .map(SupersetDatasetMetric::getMetricName)
                                .filter(StringUtils::isNotBlank).collect(Collectors.toList()));
        return inventory;
    }

    private static SemanticContext buildSemanticContext(SemanticParseInfo parseInfo) {
        SemanticContext context = new SemanticContext();
        if (parseInfo == null) {
            return context;
        }
        context.setDataSetId(parseInfo.getDataSetId());
        context.setQueryMode(parseInfo.getQueryMode());
        context.setAggType(parseInfo.getAggType() == null ? null : parseInfo.getAggType().name());
        context.setLimit(parseInfo.getLimit());
        context.setMetrics(toSchemaElementPayloads(parseInfo.getMetrics()));
        context.setDimensions(toSchemaElementPayloads(parseInfo.getDimensions()));
        context.setDimensionFilters(toFilterPayloads(parseInfo.getDimensionFilters()));
        context.setMetricFilters(toFilterPayloads(parseInfo.getMetricFilters()));
        context.setOrders(toOrderPayloads(parseInfo.getOrders()));
        return context;
    }

    private static List<CandidateContext> buildCandidateContexts(
            List<SupersetVizTypeSelector.VizTypeItem> candidates) {
        if (CollectionUtils.isEmpty(candidates)) {
            return Collections.emptyList();
        }
        List<CandidateContext> contexts = new ArrayList<>();
        for (SupersetVizTypeSelector.VizTypeItem item : candidates) {
            if (item == null || StringUtils.isBlank(item.getVizType())) {
                continue;
            }
            CandidateContext context = new CandidateContext();
            context.setVizType(item.getVizType());
            context.setDisplayName(item.getName());
            context.setCategory(item.getCategory());
            context.setDescription(item.getDescription());
            context.setSourcePath(item.getSourcePath());
            context.setSelectionSummary(buildSelectionSummary(item));
            contexts.add(context);
        }
        return contexts;
    }

    private static Map<String, Object> buildSelectionSummary(
            SupersetVizTypeSelector.VizTypeItem item) {
        if (item == null) {
            return Collections.emptyMap();
        }
        if (item.getSelectionPromptCard() != null && !item.getSelectionPromptCard().isEmpty()) {
            return new LinkedHashMap<>(item.getSelectionPromptCard());
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("viz_type", item.getVizType());
        summary.put("display_name", item.getName());
        summary.put("category", item.getCategory());
        summary.put("description_zh", item.getDescription());
        if (item.getSelectionSummary() != null) {
            SupersetVizTypeSelector.SelectionSummary selectionSummary = item.getSelectionSummary();
            summary.put("visual_shape_zh", selectionSummary.getVisualShapeZh());
            summary.put("best_for", selectionSummary.getUseCasesZh());
            summary.put("required_dataset_roles", selectionSummary.getRequiredDatasetRoles());
            summary.put("optional_dataset_roles", selectionSummary.getOptionalDatasetRoles());
            summary.put("required_slots", selectionSummary.getRequiredSlots());
            summary.put("optional_slot_names",
                    selectionSummary.getOptionalSlots().stream()
                            .map(SupersetVizTypeSelector.SelectionSlot::getFieldName)
                            .filter(StringUtils::isNotBlank).collect(Collectors.toList()));
            summary.put("structural_flags", selectionSummary.getStructuralFlags());
            summary.put("selection_hints", selectionSummary.getSelectionHints());
        }
        return summary;
    }

    private static List<Map<String, Object>> toColumnPayloads(List<SupersetDatasetColumn> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return Collections.emptyList();
        }
        return columns.stream().filter(Objects::nonNull).map(SupersetDatasetColumn::toPayload)
                .collect(Collectors.toList());
    }

    private static List<Map<String, Object>> toMetricPayloads(List<SupersetDatasetMetric> metrics) {
        if (CollectionUtils.isEmpty(metrics)) {
            return Collections.emptyList();
        }
        return metrics.stream().filter(Objects::nonNull).map(SupersetDatasetMetric::toPayload)
                .collect(Collectors.toList());
    }

    private static List<Map<String, Object>> toSchemaElementPayloads(
            Iterable<SchemaElement> items) {
        if (items == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> payloads = new ArrayList<>();
        for (SchemaElement item : items) {
            if (item == null) {
                continue;
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("name", item.getName());
            payload.put("bizName", item.getBizName());
            payload.put("type", item.getType());
            payload.put("description", item.getDescription());
            payload.put("partitionTime", item.isPartitionTime());
            payloads.add(payload);
        }
        return payloads;
    }

    private static List<Map<String, Object>> toFilterPayloads(Iterable<QueryFilter> filters) {
        if (filters == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> payloads = new ArrayList<>();
        for (QueryFilter filter : filters) {
            if (filter == null) {
                continue;
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("name", filter.getName());
            payload.put("bizName", filter.getBizName());
            payload.put("operator",
                    filter.getOperator() == null ? null : filter.getOperator().name());
            payload.put("value", filter.getValue());
            payloads.add(payload);
        }
        return payloads;
    }

    private static List<Map<String, Object>> toOrderPayloads(Iterable<Order> orders) {
        if (orders == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> payloads = new ArrayList<>();
        for (Order order : orders) {
            if (order == null) {
                continue;
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("column", order.getColumn());
            payload.put("direction", order.getDirection());
            payloads.add(payload);
        }
        return payloads;
    }

    private static List<Map<String, Object>> sampleRows(List<Map<String, Object>> rows, int limit) {
        if (CollectionUtils.isEmpty(rows)) {
            return Collections.emptyList();
        }
        if (rows.size() <= limit) {
            return rows;
        }
        return rows.subList(0, limit);
    }

    private static List<Map<String, Object>> normalizeRows(List<Map<String, Object>> rows) {
        List<Map<String, Object>> normalized = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> normalizedRow = new HashMap<>();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (entry.getKey() != null) {
                    normalizedRow.put(normalize(entry.getKey()), entry.getValue());
                }
            }
            normalized.add(normalizedRow);
        }
        return normalized;
    }

    private static Map<String, Object> buildColumnProfile(QueryColumn column,
            List<Map<String, Object>> rows) {
        Map<String, Object> profile = new LinkedHashMap<>();
        if (column == null) {
            return profile;
        }
        profile.put("name", column.getName());
        profile.put("bizName", column.getBizName());
        profile.put("type", column.getType());
        profile.put("showType", column.getShowType());
        profile.put("role", resolveRole(column));
        String[] keys = resolveKeys(column);
        List<Object> values = new ArrayList<>();
        int nullCount = 0;
        Map<String, Long> frequency = new HashMap<>();
        DoubleSummaryStatistics numericStats = new DoubleSummaryStatistics();
        for (Map<String, Object> row : rows) {
            Object value = resolveValue(row, keys);
            if (value == null) {
                nullCount++;
                continue;
            }
            values.add(value);
            String stringValue = String.valueOf(value);
            frequency.put(stringValue, frequency.getOrDefault(stringValue, 0L) + 1L);
            Double numeric = tryParseNumber(value);
            if (numeric != null) {
                numericStats.accept(numeric);
            }
        }
        int sampleSize = rows == null ? 0 : rows.size();
        profile.put("nullRate", sampleSize == 0 ? 0 : (double) nullCount / sampleSize);
        profile.put("distinctCount", frequency.size());
        profile.put("sampleValues", values.stream().filter(Objects::nonNull)
                .limit(MAX_SAMPLE_VALUES).collect(Collectors.toList()));
        if (numericStats.getCount() > 0) {
            Map<String, Object> numericSummary = new LinkedHashMap<>();
            numericSummary.put("min", numericStats.getMin());
            numericSummary.put("max", numericStats.getMax());
            numericSummary.put("avg", numericStats.getAverage());
            profile.put("numericStats", numericSummary);
        }
        String grain = detectTimeGrain(values);
        if (StringUtils.isNotBlank(grain)) {
            profile.put("timeGrain", grain);
        }
        if (!frequency.isEmpty()) {
            List<Map<String, Object>> topValues = frequency.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(MAX_SAMPLE_VALUES).map(entry -> {
                        Map<String, Object> item = new LinkedHashMap<>();
                        item.put("value", entry.getKey());
                        item.put("count", entry.getValue());
                        return item;
                    }).collect(Collectors.toList());
            profile.put("topValues", topValues);
        }
        return profile;
    }

    private static String resolveRole(QueryColumn column) {
        if (column == null) {
            return "UNKNOWN";
        }
        if ("DATE".equalsIgnoreCase(column.getShowType()) || isDateType(column.getType())) {
            return "TIME";
        }
        if ("NUMBER".equalsIgnoreCase(column.getShowType()) || isNumericType(column.getType())) {
            return "METRIC";
        }
        return "DIMENSION";
    }

    private static String[] resolveKeys(QueryColumn column) {
        return new String[] {normalize(column.getName()), normalize(column.getBizName()),
                        normalize(column.getNameEn())};
    }

    private static Object resolveValue(Map<String, Object> row, String[] keys) {
        if (row == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            if (StringUtils.isBlank(key)) {
                continue;
            }
            if (row.containsKey(key)) {
                return row.get(key);
            }
        }
        return null;
    }

    private static Double tryParseNumber(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception ex) {
            return null;
        }
    }

    private static String detectTimeGrain(List<Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return null;
        }
        TreeMap<Integer, String> grains = new TreeMap<>(Comparator.naturalOrder());
        for (Object value : values) {
            String grain = detectTimeGrain(value);
            if (StringUtils.isNotBlank(grain)) {
                grains.put(grainRank(grain), grain);
            }
        }
        return grains.isEmpty() ? null : grains.get(grains.lastKey());
    }

    private static String detectTimeGrain(Object value) {
        if (value == null) {
            return null;
        }
        String raw = String.valueOf(value).trim();
        if (raw.matches("^\\d{4}$")) {
            return "YEAR";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}$")) {
            return "MONTH";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}$")) {
            return "DAY";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}[ T]\\d{1,2}$")) {
            return "HOUR";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}[ T]\\d{1,2}:\\d{1,2}$")) {
            return "MINUTE";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}[ T]\\d{1,2}:\\d{1,2}:\\d{1,2}$")) {
            return "SECOND";
        }
        if (value instanceof Number) {
            String digits = String.valueOf(((Number) value).longValue());
            if (digits.length() == 10) {
                return "SECOND";
            }
            if (digits.length() == 13) {
                return "MILLISECOND";
            }
        }
        return null;
    }

    private static int grainRank(String grain) {
        if ("YEAR".equalsIgnoreCase(grain)) {
            return 1;
        }
        if ("MONTH".equalsIgnoreCase(grain)) {
            return 2;
        }
        if ("DAY".equalsIgnoreCase(grain)) {
            return 3;
        }
        if ("HOUR".equalsIgnoreCase(grain)) {
            return 4;
        }
        if ("MINUTE".equalsIgnoreCase(grain)) {
            return 5;
        }
        if ("SECOND".equalsIgnoreCase(grain)) {
            return 6;
        }
        if ("MILLISECOND".equalsIgnoreCase(grain)) {
            return 7;
        }
        return 0;
    }

    private static String normalize(String value) {
        return StringUtils.lowerCase(StringUtils.trimToEmpty(value), Locale.ROOT);
    }

    private static boolean isDateType(String type) {
        return StringUtils.equalsAnyIgnoreCase(type, "DATE", "DATETIME", "TIMESTAMP");
    }

    private static boolean isNumericType(String type) {
        return StringUtils.equalsAnyIgnoreCase(type, "INT", "INTEGER", "BIGINT", "LONG", "FLOAT",
                "DOUBLE", "DECIMAL", "NUMBER");
    }

    private static List<Map<String, Object>> defaultRows(List<Map<String, Object>> rows) {
        return rows == null ? Collections.emptyList() : rows;
    }

    private static <T> List<T> safeList(SupplierWithException<List<T>> supplier) {
        try {
            List<T> value = supplier.get();
            return value == null ? Collections.emptyList() : value;
        } catch (Exception ex) {
            log.debug("superset selection payload list build skipped", ex);
            return Collections.emptyList();
        }
    }

    private static String safeString(SupplierWithException<String> supplier) {
        try {
            return supplier.get();
        } catch (Exception ex) {
            log.debug("superset selection payload string build skipped", ex);
            return null;
        }
    }

    private static boolean safeBoolean(BooleanSupplierWithException supplier) {
        try {
            return supplier.getAsBoolean();
        } catch (Exception ex) {
            log.debug("superset selection payload boolean build skipped", ex);
            return false;
        }
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    private interface BooleanSupplierWithException {
        boolean getAsBoolean() throws Exception;
    }

    @Data
    public static class SelectionSignals {
        @JsonProperty("column_count")
        private int columnCount;
        @JsonProperty("row_count")
        private int rowCount;
        @JsonProperty("metric_count")
        private int metricCount;
        @JsonProperty("time_count")
        private int timeCount;
        @JsonProperty("dimension_count")
        private int dimensionCount;
        @JsonProperty("inferred_from_values")
        private boolean inferredFromValues;
        @JsonProperty("time_grain_detected")
        private boolean timeGrainDetected;
    }

    @Data
    static class VizSelectionPromptPayload {
        @JsonProperty("user_question")
        private String userQuestion;
        @JsonProperty("executed_sql")
        private String executedSql;
        @JsonProperty("sql_summary")
        private SqlSummary sqlSummary = new SqlSummary();
        @JsonProperty("query_result")
        private ResultContext queryResult = new ResultContext();
        @JsonProperty("data_profile")
        private Map<String, Object> dataProfile = Collections.emptyMap();
        @JsonProperty("dataset_context")
        private DatasetContext datasetContext = new DatasetContext();
        @JsonProperty("semantic_context")
        private SemanticContext semanticContext = new SemanticContext();
        @JsonProperty("signals")
        private SelectionSignals signals = new SelectionSignals();
        @JsonProperty("intent_profile")
        private IntentProfile intentProfile = new IntentProfile();
        @JsonProperty("candidate_viztypes")
        private List<CandidateContext> candidateViztypes = Collections.emptyList();
    }

    @Data
    static class IntentProfile {
        @JsonProperty("detail_request")
        private boolean detailRequest;
        @JsonProperty("pivot_requested")
        private boolean pivotRequested;
        @JsonProperty("time_series_requested")
        private boolean timeSeriesRequested;
        @JsonProperty("aggregation_detected")
        private boolean aggregationDetected;
        @JsonProperty("raw_row_result_likely")
        private boolean rawRowResultLikely;
        @JsonProperty("preferred_table_viz_type")
        private String preferredTableVizType;
        private List<String> reasons = Collections.emptyList();
    }

    @Data
    static class SqlSummary {
        @JsonProperty("table_name")
        private String tableName;
        @JsonProperty("select_fields")
        private List<String> selectFields = Collections.emptyList();
        @JsonProperty("where_fields")
        private List<String> whereFields = Collections.emptyList();
        @JsonProperty("group_by_fields")
        private List<String> groupByFields = Collections.emptyList();
        @JsonProperty("order_by_fields")
        private List<String> orderByFields = Collections.emptyList();
        @JsonProperty("all_referenced_fields")
        private List<String> allReferencedFields = Collections.emptyList();
        @JsonProperty("has_limit")
        private boolean hasLimit;
        @JsonProperty("has_sub_query")
        private boolean hasSubQuery;
        @JsonProperty("has_join")
        private boolean hasJoin;
        @JsonProperty("join_count")
        private int joinCount;
    }

    @Data
    static class ResultContext {
        @JsonProperty("row_count")
        private int rowCount;
        @JsonProperty("column_count")
        private int columnCount;
        @JsonProperty("preview_rows")
        private List<Map<String, Object>> previewRows = Collections.emptyList();
        @JsonProperty("aggregate_info")
        private Object aggregateInfo;
    }

    @Data
    static class DatasetContext {
        @JsonProperty("dataset_id")
        private Long datasetId;
        @JsonProperty("database_id")
        private Long databaseId;
        private String schema;
        @JsonProperty("table_name")
        private String tableName;
        @JsonProperty("main_dttm_col")
        private String mainDttmCol;
        private String description;
        private List<Map<String, Object>> columns = Collections.emptyList();
        private List<Map<String, Object>> metrics = Collections.emptyList();
        @JsonProperty("role_inventory")
        private Map<String, Object> roleInventory = Collections.emptyMap();
    }

    @Data
    static class SemanticContext {
        @JsonProperty("dataset_id")
        private Long dataSetId;
        @JsonProperty("query_mode")
        private String queryMode;
        @JsonProperty("agg_type")
        private String aggType;
        private long limit;
        private List<Map<String, Object>> metrics = Collections.emptyList();
        private List<Map<String, Object>> dimensions = Collections.emptyList();
        @JsonProperty("dimension_filters")
        private List<Map<String, Object>> dimensionFilters = Collections.emptyList();
        @JsonProperty("metric_filters")
        private List<Map<String, Object>> metricFilters = Collections.emptyList();
        private List<Map<String, Object>> orders = Collections.emptyList();
    }

    @Data
    static class CandidateContext {
        @JsonProperty("viz_type")
        private String vizType;
        @JsonProperty("display_name")
        private String displayName;
        private String category;
        private String description;
        @JsonProperty("source_path")
        private String sourcePath;
        @JsonProperty("selection_summary")
        private Map<String, Object> selectionSummary = Collections.emptyMap();
    }
}
