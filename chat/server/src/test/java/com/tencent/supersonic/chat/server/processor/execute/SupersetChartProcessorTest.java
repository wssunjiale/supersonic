package com.tencent.supersonic.chat.server.processor.execute;

import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetPluginConfig;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.common.pojo.QueryColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Method;

public class SupersetChartProcessorTest {

    @Test
    public void testBuildFormDataTableUsesDatasetColumns() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("id", "INT", true, false),
                buildColumn("name", "STRING", true, false)));

        Map<String, Object> formData =
                processor.buildFormData(config, null, null, datasetInfo, "table", null, null);

        Assertions.assertEquals("raw", formData.get("query_mode"));
        Assertions.assertEquals(Arrays.asList("id", "name"), formData.get("all_columns"));
        Assertions.assertEquals(Arrays.asList("id", "name"), formData.get("columns"));
    }

    @Test
    public void testBuildFormDataTableFallsBackToQueryColumns() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryColumns(Arrays.asList(new QueryColumn("id", "INT", "id"),
                new QueryColumn("name", "STRING", "name")));

        Map<String, Object> formData =
                processor.buildFormData(config, null, queryResult, datasetInfo, "table", null,
                        null);

        Assertions.assertEquals("raw", formData.get("query_mode"));
        Assertions.assertEquals(Arrays.asList("id", "name"), formData.get("all_columns"));
        Assertions.assertEquals(Arrays.asList("id", "name"), formData.get("columns"));
    }

    @Test
    public void testBuildFormDataUsesDatasetMetricsAndDimensions() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = new SupersetPluginConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));

        Map<String, Object> formData =
                processor.buildFormData(config, parseInfo, null, datasetInfo, "pie", null, null);

        Assertions.assertEquals("amount", formData.get("metric"));
        Assertions.assertEquals(Collections.singletonList("category"), formData.get("groupby"));
        Assertions.assertEquals("aggregate", formData.get("query_mode"));
    }

    @Test
    public void testBuildFormDataNormalizesDimensionNameToDatasetColumn() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = new SupersetPluginConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("count").name("访问人数").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("访 问人数").name("访问人数").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Collections.singletonList(
                buildColumn("访问人数", "INT", true, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("count")));

        Map<String, Object> formData =
                processor.buildFormData(config, parseInfo, null, datasetInfo, "table", null,
                        null);

        Assertions.assertEquals(Collections.singletonList("访问人数"), formData.get("groupby"));
    }

    @Test
    public void testBuildFormDataBuildsAdhocMetricWhenDatasetMetricMissing() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = new SupersetPluginConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false),
                buildColumn("ds", "DATE", false, true)));

        Map<String, Object> formData =
                processor.buildFormData(config, parseInfo, null, datasetInfo,
                        "echarts_timeseries_line", null, null);

        Object metrics = formData.get("metrics");
        Assertions.assertTrue(metrics instanceof List);
        List<?> metricList = (List<?>) metrics;
        Assertions.assertEquals(1, metricList.size());
        Assertions.assertTrue(metricList.get(0) instanceof Map);
        Map<?, ?> metric = (Map<?, ?>) metricList.get(0);
        Assertions.assertEquals("SIMPLE", metric.get("expressionType"));
        Assertions.assertEquals("SUM", metric.get("aggregate"));
        Assertions.assertEquals("aggregate", formData.get("query_mode"));
        Assertions.assertEquals("ds", formData.get("granularity_sqla"));
    }

    @Test
    public void testBuildFormDataMissingHeatmapDimensionsThrows() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = new SupersetPluginConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));

        Assertions.assertThrows(IllegalStateException.class,
                () -> processor.buildFormData(config, parseInfo, null, datasetInfo, "heatmap_v2",
                        null, null));
    }

    @Test
    public void testBuildColumnsFromParseUsesMetricAndDate() throws Exception {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("count").name("访问人数")
                .type(SchemaElementType.METRIC).build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("ds").name("时间")
                .type(SchemaElementType.DATE).build());

        Method method = SupersetChartProcessor.class.getDeclaredMethod("buildColumnsFromParse",
                SemanticParseInfo.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<QueryColumn> columns = (List<QueryColumn>) method.invoke(processor, parseInfo);

        Assertions.assertEquals(2, columns.size());
        Assertions.assertEquals("NUMBER", columns.get(0).getShowType());
        Assertions.assertEquals("DATE", columns.get(1).getShowType());
    }

    @Test
    public void testBuildColumnsFromDatasetUsesMetricsAndGroupby() throws Exception {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("count")));
        datasetInfo.setColumns(Arrays.asList(buildColumn("ds", "DATE", true, true),
                buildColumn("category", "STRING", true, false)));

        Method method = SupersetChartProcessor.class.getDeclaredMethod("buildColumnsFromDataset",
                SupersetDatasetInfo.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<QueryColumn> columns = (List<QueryColumn>) method.invoke(processor, datasetInfo);

        Assertions.assertEquals(3, columns.size());
        Assertions.assertEquals("NUMBER", columns.get(0).getShowType());
        Assertions.assertEquals("DATE", columns.get(1).getShowType());
        Assertions.assertEquals("CATEGORY", columns.get(2).getShowType());
    }

    @Test
    public void testResolveDashboardHeightPrefersConfigAndLineFallback() throws Exception {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        Method method = SupersetChartProcessor.class.getDeclaredMethod("resolveDashboardHeight",
                SupersetPluginConfig.class, String.class);
        method.setAccessible(true);

        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setHeight(420);
        Integer configuredHeight = (Integer) method.invoke(processor, config,
                "echarts_timeseries_line");
        Assertions.assertEquals(420, configuredHeight);

        SupersetPluginConfig defaultConfig = new SupersetPluginConfig();
        Integer lineHeight = (Integer) method.invoke(processor, defaultConfig,
                "echarts_timeseries_line");
        Assertions.assertEquals(300, lineHeight);

        Integer defaultHeight = (Integer) method.invoke(processor, defaultConfig, "bar");
        Assertions.assertEquals(260, defaultHeight);
    }

    private SupersetDatasetColumn buildColumn(String name, String type, boolean groupby,
            boolean isDttm) {
        SupersetDatasetColumn column = new SupersetDatasetColumn();
        column.setColumnName(name);
        column.setType(type);
        column.setGroupby(groupby);
        column.setIsDttm(isDttm);
        return column;
    }

    private SupersetDatasetMetric buildMetric(String name) {
        SupersetDatasetMetric metric = new SupersetDatasetMetric();
        metric.setMetricName(name);
        metric.setExpression("SUM(" + name + ")");
        return metric;
    }
}
