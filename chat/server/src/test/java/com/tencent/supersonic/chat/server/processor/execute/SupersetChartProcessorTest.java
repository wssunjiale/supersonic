package com.tencent.supersonic.chat.server.processor.execute;

import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetPluginConfig;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SupersetChartProcessorTest {

    @Test
    public void testBuildFormDataTableUsesDatasetColumns() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("id", "INT", true, false),
                buildColumn("name", "STRING", true, false)));

        Map<String, Object> formData = processor.buildFormData(config, null, datasetInfo, "table");

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
                processor.buildFormData(config, parseInfo, datasetInfo, "pie");

        Assertions.assertEquals("amount", formData.get("metric"));
        Assertions.assertEquals(Collections.singletonList("category"), formData.get("groupby"));
        Assertions.assertEquals("aggregate", formData.get("query_mode"));
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
                processor.buildFormData(config, parseInfo, datasetInfo, "echarts_timeseries_line");

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
                () -> processor.buildFormData(config, parseInfo, datasetInfo, "heatmap_v2"));
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
