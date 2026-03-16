package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.tencent.supersonic.common.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class SupersetQueryContextTemplates {

    private static final Set<String> TIMESERIES_TYPES = new HashSet<>();
    private static final Set<String> SIMPLE_SORT_BY_METRIC_TYPES = new HashSet<>();

    static {
        Collections.addAll(TIMESERIES_TYPES, "echarts_timeseries", "echarts_area",
                "echarts_timeseries_bar", "echarts_timeseries_line", "echarts_timeseries_scatter",
                "echarts_timeseries_smooth", "echarts_timeseries_step");
        Collections.addAll(SIMPLE_SORT_BY_METRIC_TYPES, "pie", "treemap_v2", "sunburst_v2",
                "funnel", "gauge_chart");
    }

    private SupersetQueryContextTemplates() {}

    /**
     * 根据 Superset 前端 buildQuery 规则为 query_context 追加 vizType 模板字段。
     *
     * @param vizType Superset vizType。
     * @param formData chart form_data。
     * @param queries 现有 queries 列表。
     * @return 处理后的 queries 列表。
     */
    static List<Map<String, Object>> apply(String vizType, Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        if (formData == null) {
            return queries == null ? Collections.emptyList() : queries;
        }
        String normalized = normalize(vizType);
        if (StringUtils.isBlank(normalized)) {
            return queries == null ? Collections.emptyList() : queries;
        }
        if ("mixed_timeseries".equals(normalized)) {
            return applyMixedTimeseries(formData, queries);
        }
        if (TIMESERIES_TYPES.contains(normalized)) {
            return applyTimeseries(formData, queries);
        }
        if ("big_number".equals(normalized)) {
            return applyBigNumberTrendline(formData, queries);
        }
        if ("big_number_total".equals(normalized)) {
            return applyBigNumberTotal(formData, queries);
        }
        if ("pop_kpi".equals(normalized)) {
            return applyPopKpi(formData, queries);
        }
        if ("box_plot".equals(normalized)) {
            return applyBoxPlot(formData, queries);
        }
        if ("histogram_v2".equals(normalized)) {
            return applyHistogram(formData, queries);
        }
        if ("heatmap_v2".equals(normalized)) {
            return applyHeatmap(formData, queries);
        }
        if ("bubble_v2".equals(normalized) || "bubble".equals(normalized)) {
            return applyBubble(formData, queries);
        }
        if ("graph_chart".equals(normalized)) {
            return applyGraph(formData, queries);
        }
        if ("tree_chart".equals(normalized)) {
            return applyTree(formData, queries);
        }
        if ("sankey_v2".equals(normalized)) {
            return applySankey(formData, queries);
        }
        if ("waterfall".equals(normalized)) {
            return applyWaterfall(formData, queries);
        }
        if ("gantt_chart".equals(normalized)) {
            return applyGantt(formData, queries);
        }
        if ("pivot_table_v2".equals(normalized)) {
            return applyPivotTable(formData, queries);
        }
        if ("table".equals(normalized) || "ag-grid-table".equals(normalized)) {
            return applyTable(formData, queries);
        }
        if ("word_cloud".equals(normalized)) {
            return applyWordCloud(formData, queries);
        }
        if (SIMPLE_SORT_BY_METRIC_TYPES.contains(normalized)) {
            return applySortByMetric(formData, queries);
        }
        if ("radar".equals(normalized)) {
            return applyRadar(formData, queries);
        }
        if ("cal_heatmap".equals(normalized)) {
            return applyCalendarHeatmap(formData, queries);
        }
        if ("horizon".equals(normalized)) {
            return applyHorizon(formData, queries);
        }
        if ("paired_ttest".equals(normalized)) {
            return applyPairedTTest(formData, queries);
        }
        if ("compare".equals(normalized)) {
            return applyCompare(formData, queries);
        }
        if ("time_pivot".equals(normalized)) {
            return applyTimePivot(formData, queries);
        }
        if ("chord".equals(normalized)) {
            return applyChord(formData, queries);
        }
        if ("country_map".equals(normalized)) {
            return applyCountryMap(formData, queries);
        }
        if ("world_map".equals(normalized)) {
            return applyWorldMap(formData, queries);
        }
        if ("mapbox".equals(normalized)) {
            return applyMapbox(formData, queries);
        }
        if ("partition".equals(normalized)) {
            return applyPartition(formData, queries);
        }
        if ("rose".equals(normalized)) {
            return applyRose(formData, queries);
        }
        if ("para".equals(normalized)) {
            return applyParallelCoordinates(formData, queries);
        }
        if ("time_table".equals(normalized)) {
            return applyTimeTable(formData, queries);
        }
        if ("bullet".equals(normalized)) {
            return applyBullet(formData, queries);
        }
        if ("handlebars".equals(normalized)) {
            return ensureQueries(queries);
        }
        return queries == null ? Collections.emptyList() : queries;
    }

    private static List<Map<String, Object>> applyTimeseries(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        applyTimeseriesToQueries(formData, normalized);
        return normalized;
    }

    private static List<Map<String, Object>> applyMixedTimeseries(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        Map<String, Object> base = deepCopyMap(normalized.get(0));
        applyTimeseriesToQueries(formData, Collections.singletonList(base));
        List<Map<String, Object>> result = new ArrayList<>();
        result.add(base);
        List<Object> metricsB = toList(formData.get("metrics_b"));
        if (!metricsB.isEmpty()) {
            Map<String, Object> second = deepCopyMap(base);
            second.put("metrics", metricsB);
            result.add(second);
        }
        return result;
    }

    private static void applyTimeseriesToQueries(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        for (Map<String, Object> query : queries) {
            List<Object> groupby = toList(formData.get("groupby"));
            List<Object> columns = new ArrayList<>();
            Object xAxis = firstNonNull(formData.get("x_axis"), formData.get("granularity_sqla"));
            if (xAxis != null) {
                columns.add(xAxis);
            }
            columns.addAll(groupby);
            if (!columns.isEmpty()) {
                List<Object> deduped = dedupe(columns);
                query.put("columns", applyBaseAxisColumn(deduped, xAxis));
            }
            if (!groupby.isEmpty()) {
                query.put("series_columns", groupby);
            }
            if (xAxis != null || formData.get("granularity_sqla") != null) {
                query.putIfAbsent("is_timeseries", true);
            }
            applyTimeOffsets(query, formData);
        }
    }

    private static List<Map<String, Object>> applyBigNumberTrendline(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        Object xAxis = firstNonNull(formData.get("x_axis"), formData.get("granularity_sqla"));
        for (Map<String, Object> query : normalized) {
            if (xAxis != null) {
                query.put("columns", Collections.singletonList(xAxis));
            } else {
                query.putIfAbsent("is_timeseries", true);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyBigNumberTotal(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        return ensureQueries(queries);
    }

    private static List<Map<String, Object>> applyPopKpi(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> groupby = toList(firstNonNull(formData.get("cols"), formData.get("groupby")));
        for (Map<String, Object> query : normalized) {
            if (!groupby.isEmpty()) {
                query.put("groupby", groupby);
            }
            applyTimeOffsets(query, formData);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyBoxPlot(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = toList(formData.get("columns"));
        if (columns.isEmpty() && formData.get("granularity_sqla") != null) {
            columns = Collections.singletonList(formData.get("granularity_sqla"));
        }
        List<Object> groupby = toList(formData.get("groupby"));
        List<Object> merged = merge(columns, groupby);
        for (Map<String, Object> query : normalized) {
            if (!merged.isEmpty()) {
                query.put("columns", merged);
            }
            if (!groupby.isEmpty()) {
                query.put("series_columns", groupby);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyHistogram(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> groupby = toList(formData.get("groupby"));
        List<Object> columns = merge(groupby, toList(formData.get("column")));
        for (Map<String, Object> query : normalized) {
            if (!columns.isEmpty()) {
                query.put("columns", columns);
            }
            query.remove("metrics");
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyHeatmap(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> groupby = toList(formData.get("groupby"));
        if (groupby.isEmpty() && formData.get("y_axis") != null) {
            groupby = Collections.singletonList(formData.get("y_axis"));
        }
        List<Object> columns = new ArrayList<>(toList(formData.get("x_axis")));
        columns.addAll(groupby);
        for (Map<String, Object> query : normalized) {
            if (!groupby.isEmpty()) {
                query.put("groupby", groupby);
            }
            if (!columns.isEmpty()) {
                query.put("columns", dedupe(columns));
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyBubble(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns =
                merge(toList(formData.get("entity")), toList(formData.get("series")));
        if (columns.isEmpty()) {
            columns = merge(toList(formData.get("x")), toList(formData.get("y")));
        }
        for (Map<String, Object> query : normalized) {
            if (!columns.isEmpty()) {
                query.put("columns", columns);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyGraph(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns =
                merge(merge(toList(formData.get("source")), toList(formData.get("target"))),
                        merge(toList(formData.get("source_category")),
                                toList(formData.get("target_category"))));
        for (Map<String, Object> query : normalized) {
            if (!columns.isEmpty()) {
                query.put("columns", dedupe(columns));
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyTree(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns =
                merge(merge(toList(formData.get("id")), toList(formData.get("parent"))),
                        toList(formData.get("name")));
        for (Map<String, Object> query : normalized) {
            if (!columns.isEmpty()) {
                query.put("columns", dedupe(columns));
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applySankey(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> groupby =
                merge(toList(formData.get("source")), toList(formData.get("target")));
        for (Map<String, Object> query : normalized) {
            if (!groupby.isEmpty()) {
                query.put("groupby", groupby);
                query.put("columns", groupby);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyWaterfall(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = merge(
                toList(firstNonNull(formData.get("x_axis"), formData.get("granularity_sqla"))),
                toList(formData.get("groupby")));
        List<Object> orderby = new ArrayList<>();
        for (Object column : columns) {
            List<Object> order = new ArrayList<>();
            order.add(column);
            order.add(true);
            orderby.add(order);
        }
        for (Map<String, Object> query : normalized) {
            if (!columns.isEmpty()) {
                query.put("columns", columns);
            }
            if (!orderby.isEmpty()) {
                query.put("orderby", orderby);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyGantt(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        Object start = firstNonNull(formData.get("start_time"), formData.get("start"));
        Object end = firstNonNull(formData.get("end_time"), formData.get("end"));
        Object yAxis = firstNonNull(formData.get("y_axis"), firstOf(formData.get("groupby")));
        List<Object> series = toList(formData.get("series"));
        List<Object> tooltipColumns = toList(formData.get("tooltip_columns"));
        List<Object> tooltipMetrics = toList(formData.get("tooltip_metrics"));
        List<Object> orderByCols = toList(formData.get("order_by_cols"));
        List<Object> columns = new ArrayList<>();
        addIfNotNull(columns, start);
        addIfNotNull(columns, end);
        addIfNotNull(columns, yAxis);
        columns.addAll(series);
        columns.addAll(tooltipColumns);
        for (Object order : orderByCols) {
            Object column = parseOrderByColumn(order);
            addIfNotNull(columns, column);
        }
        for (Map<String, Object> query : normalized) {
            if (!columns.isEmpty()) {
                query.put("columns", dedupe(columns));
            }
            if (!series.isEmpty()) {
                query.put("series_columns", series);
            }
            if (!tooltipMetrics.isEmpty()) {
                query.put("metrics", tooltipMetrics);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyPivotTable(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns =
                merge(toList(formData.get("groupbyColumns")), toList(formData.get("groupbyRows")));
        for (Map<String, Object> query : normalized) {
            if (!columns.isEmpty()) {
                query.put("columns", dedupe(columns));
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyTable(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        applyTimeOffsetsToQueries(normalized, formData);
        return normalized;
    }

    private static List<Map<String, Object>> applyWordCloud(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        return applySortByMetric(formData, queries);
    }

    private static List<Map<String, Object>> applySortByMetric(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        Object sortByMetric = formData.get("sort_by_metric");
        Object metric = formData.get("metric");
        if (Boolean.TRUE.equals(sortByMetric) && metric != null) {
            List<Object> order = new ArrayList<>();
            order.add(metric);
            order.add(false);
            List<Object> orderby = Collections.singletonList(order);
            for (Map<String, Object> query : normalized) {
                query.put("orderby", orderby);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyRadar(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        Object seriesLimitMetric = firstOf(formData.get("series_limit_metric"));
        Object metric = firstOf(formData.get("metrics"));
        Object sortMetric = seriesLimitMetric != null ? seriesLimitMetric : metric;
        if (sortMetric != null) {
            List<Object> order = new ArrayList<>();
            order.add(sortMetric);
            order.add(false);
            List<Object> orderby = Collections.singletonList(order);
            for (Map<String, Object> query : normalized) {
                query.put("orderby", orderby);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyCalendarHeatmap(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        return ensureQueries(queries);
    }

    private static List<Map<String, Object>> applyHorizon(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "groupby");
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        Object orderMetric =
                resolveOrderMetric(formData, "timeseries_limit_metric", "metrics", "metric");
        boolean orderDesc = resolveOrderDesc(formData);
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
            setOrderByIfAbsent(query, orderMetric, orderDesc);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyPairedTTest(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "groupby");
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        Object orderMetric =
                resolveOrderMetric(formData, "timeseries_limit_metric", "metrics", "metric");
        boolean orderDesc = resolveOrderDesc(formData);
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
            setOrderByIfAbsent(query, orderMetric, orderDesc);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyCompare(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "groupby");
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyTimePivot(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "metrics", metrics);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyChord(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "groupby", "columns");
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
        }
        Object sortByMetric = formData.get("sort_by_metric");
        if (Boolean.TRUE.equals(sortByMetric)) {
            Object metric = firstOf(formData.get("metric"));
            for (Map<String, Object> query : normalized) {
                setOrderByIfAbsent(query, metric, true);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyCountryMap(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "entity");
        List<Object> metrics = collectValues(formData, "metric", "metrics");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyWorldMap(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "entity");
        List<Object> metrics = collectValues(formData, "metric", "metrics", "secondary_metric");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
        }
        if (Boolean.TRUE.equals(formData.get("sort_by_metric"))) {
            Object metric = firstOf(formData.get("metric"));
            boolean orderDesc = resolveOrderDesc(formData);
            for (Map<String, Object> query : normalized) {
                setOrderByIfAbsent(query, metric, orderDesc);
            }
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyMapbox(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "all_columns_x", "all_columns_y", "groupby");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyPartition(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "groupby");
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        Object orderMetric =
                resolveOrderMetric(formData, "timeseries_limit_metric", "metrics", "metric");
        boolean orderDesc = resolveOrderDesc(formData);
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
            setOrderByIfAbsent(query, orderMetric, orderDesc);
            applyTimeOffsets(query, formData);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyRose(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "groupby");
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        Object orderMetric =
                resolveOrderMetric(formData, "timeseries_limit_metric", "metrics", "metric");
        boolean orderDesc = resolveOrderDesc(formData);
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
            setOrderByIfAbsent(query, orderMetric, orderDesc);
            applyTimeOffsets(query, formData);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyParallelCoordinates(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "series");
        List<Object> metrics = collectValues(formData, "metrics", "metric", "secondary_metric");
        Object orderMetric = resolveOrderMetric(formData, "timeseries_limit_metric", "metrics",
                "metric", "secondary_metric");
        boolean orderDesc = resolveOrderDesc(formData);
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
            setOrderByIfAbsent(query, orderMetric, orderDesc);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyTimeTable(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> columns = collectValues(formData, "groupby");
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "columns", columns);
            mergeQueryList(query, "metrics", metrics);
        }
        return normalized;
    }

    private static List<Map<String, Object>> applyBullet(Map<String, Object> formData,
            List<Map<String, Object>> queries) {
        List<Map<String, Object>> normalized = ensureQueries(queries);
        List<Object> metrics = collectValues(formData, "metrics", "metric");
        for (Map<String, Object> query : normalized) {
            mergeQueryList(query, "metrics", metrics);
        }
        return normalized;
    }

    private static void applyTimeOffsetsToQueries(List<Map<String, Object>> queries,
            Map<String, Object> formData) {
        for (Map<String, Object> query : queries) {
            applyTimeOffsets(query, formData);
        }
    }

    private static void applyTimeOffsets(Map<String, Object> query, Map<String, Object> formData) {
        List<Object> timeCompare = toList(formData.get("time_compare"));
        if (timeCompare.isEmpty()) {
            return;
        }
        List<Object> resolved = new ArrayList<>();
        for (Object value : timeCompare) {
            if (!(value instanceof String)) {
                resolved.add(value);
                continue;
            }
            String token = (String) value;
            if ("custom".equalsIgnoreCase(token)) {
                Object startOffset = formData.get("start_date_offset");
                if (startOffset != null) {
                    resolved.add(startOffset);
                }
                continue;
            }
            if ("inherit".equalsIgnoreCase(token)) {
                resolved.add("inherit");
                continue;
            }
            resolved.add(token);
        }
        if (!resolved.isEmpty()) {
            query.put("time_offsets", resolved);
        }
    }

    private static List<Map<String, Object>> ensureQueries(List<Map<String, Object>> queries) {
        if (queries == null || queries.isEmpty()) {
            List<Map<String, Object>> created = new ArrayList<>();
            created.add(new HashMap<>());
            return created;
        }
        List<Map<String, Object>> copied = new ArrayList<>();
        for (Map<String, Object> query : queries) {
            copied.add(deepCopyMap(query));
        }
        return copied;
    }

    private static Map<String, Object> deepCopyMap(Map<String, Object> source) {
        if (source == null) {
            return new HashMap<>();
        }
        String json = JsonUtil.toString(source);
        Map<String, Object> copy = JsonUtil.toObject(json, Map.class);
        return copy == null ? new HashMap<>() : new HashMap<>(copy);
    }

    private static List<Object> toList(Object value) {
        if (value instanceof List) {
            return new ArrayList<>((List<Object>) value);
        }
        if (value == null) {
            return new ArrayList<>();
        }
        List<Object> list = new ArrayList<>();
        list.add(value);
        return list;
    }

    private static List<Object> merge(List<Object> left, List<Object> right) {
        List<Object> merged = new ArrayList<>();
        if (left != null) {
            merged.addAll(left);
        }
        if (right != null) {
            merged.addAll(right);
        }
        return dedupe(merged);
    }

    private static List<Object> dedupe(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return new ArrayList<>();
        }
        List<Object> result = new ArrayList<>();
        for (Object value : values) {
            if (!result.contains(value)) {
                result.add(value);
            }
        }
        return result;
    }

    private static List<Object> collectValues(Map<String, Object> formData, String... keys) {
        if (formData == null || keys == null || keys.length == 0) {
            return new ArrayList<>();
        }
        List<Object> values = new ArrayList<>();
        for (String key : keys) {
            values.addAll(toList(formData.get(key)));
        }
        return dedupe(values);
    }

    private static void mergeQueryList(Map<String, Object> query, String key, List<Object> values) {
        if (query == null || values == null || values.isEmpty()) {
            return;
        }
        List<Object> merged = merge(toList(query.get(key)), values);
        if (!merged.isEmpty()) {
            query.put(key, merged);
        }
    }

    private static void setOrderByIfAbsent(Map<String, Object> query, Object metric,
            boolean orderDesc) {
        if (query == null || metric == null) {
            return;
        }
        Object existing = query.get("orderby");
        if (existing instanceof List && !((List<?>) existing).isEmpty()) {
            return;
        }
        List<Object> order = new ArrayList<>();
        order.add(metric);
        order.add(!orderDesc);
        query.put("orderby", Collections.singletonList(order));
    }

    private static Object resolveOrderMetric(Map<String, Object> formData, String... keys) {
        if (formData == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            Object candidate = firstOf(formData.get(key));
            if (candidate != null) {
                return candidate;
            }
        }
        return null;
    }

    private static boolean resolveOrderDesc(Map<String, Object> formData) {
        if (formData == null) {
            return true;
        }
        Object value = formData.get("order_desc");
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return true;
    }

    private static Object firstNonNull(Object first, Object second) {
        return first != null ? first : second;
    }

    private static Object firstOf(Object value) {
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return list.isEmpty() ? null : list.get(0);
        }
        return value;
    }

    private static void addIfNotNull(List<Object> list, Object value) {
        if (value != null) {
            list.add(value);
        }
    }

    private static Object parseOrderByColumn(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return list.isEmpty() ? null : list.get(0);
        }
        if (value instanceof String) {
            String raw = (String) value;
            if (raw.startsWith("[") && raw.endsWith("]")) {
                try {
                    List<?> parsed = JsonUtil.toObject(raw, List.class);
                    if (parsed != null && !parsed.isEmpty()) {
                        return parsed.get(0);
                    }
                } catch (Exception ignored) {
                    return raw;
                }
            }
        }
        return value;
    }

    private static String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static List<Object> applyBaseAxisColumn(List<Object> columns, Object xAxis) {
        if (xAxis == null || columns == null || columns.isEmpty()) {
            return columns;
        }
        if (!(xAxis instanceof String)) {
            return columns;
        }
        String axis = (String) xAxis;
        List<Object> updated = new ArrayList<>();
        for (Object column : columns) {
            if (axis.equals(column)) {
                updated.add(buildBaseAxisColumn(axis));
            } else {
                updated.add(column);
            }
        }
        return updated;
    }

    private static Map<String, Object> buildBaseAxisColumn(String axis) {
        Map<String, Object> column = new HashMap<>();
        column.put("columnType", "BASE_AXIS");
        column.put("sqlExpression", axis);
        column.put("label", axis);
        column.put("expressionType", "SQL");
        column.put("isColumnReference", true);
        return column;
    }
}
