package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

public class SupersetVizSelectionPromptBuilderTest {

    @Test
    public void testBuildJsonPayloadIncludesSelectionContext() {
        QueryResult queryResult = new QueryResult();
        queryResult.setQuerySql(
                "select ds, sum(sales) as sales from orders group by ds order by ds limit 10");
        QueryColumn timeColumn = new QueryColumn();
        timeColumn.setName("ds");
        timeColumn.setBizName("ds");
        timeColumn.setShowType("DATE");
        timeColumn.setType("DATE");
        QueryColumn metricColumn = new QueryColumn();
        metricColumn.setName("sales");
        metricColumn.setBizName("sales");
        metricColumn.setShowType("NUMBER");
        metricColumn.setType("DOUBLE");
        queryResult.setQueryColumns(Arrays.asList(timeColumn, metricColumn));
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("ds", "2024-01-01");
        row.put("sales", 10);
        queryResult.setQueryResults(Collections.singletonList(row));

        SemanticParseInfo parseInfo = new SemanticParseInfo();
        SchemaElement metric = SchemaElement.builder().name("sales").bizName("sales")
                .type(SchemaElementType.METRIC).order(1).build();
        SchemaElement dimension = SchemaElement.builder().name("ds").bizName("ds")
                .type(SchemaElementType.DATE).order(2).build();
        parseInfo.getMetrics().add(metric);
        parseInfo.getDimensions().add(dimension);

        SupersetDatasetColumn datasetTimeColumn = new SupersetDatasetColumn();
        datasetTimeColumn.setColumnName("ds");
        datasetTimeColumn.setType("DATE");
        datasetTimeColumn.setIsDttm(true);
        datasetTimeColumn.setGroupby(true);
        datasetTimeColumn.setFilterable(true);
        SupersetDatasetMetric datasetMetric = new SupersetDatasetMetric();
        datasetMetric.setMetricName("sales");
        datasetMetric.setMetricType("DOUBLE");
        datasetMetric.setExpression("SUM(sales)");
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setId(1L);
        datasetInfo.setDatabaseId(2L);
        datasetInfo.setSchema("analytics");
        datasetInfo.setTableName("orders");
        datasetInfo.setMainDttmCol("ds");
        datasetInfo.setColumns(Collections.singletonList(datasetTimeColumn));
        datasetInfo.setMetrics(Collections.singletonList(datasetMetric));

        SupersetVizTypeSelector.VizTypeItem area =
                SupersetVizTypeSelector.resolveItemByVizType("echarts_area", null);
        SupersetVizSelectionPromptBuilder.SelectionSignals signals =
                new SupersetVizSelectionPromptBuilder.SelectionSignals();
        signals.setColumnCount(2);
        signals.setRowCount(1);
        signals.setMetricCount(1);
        signals.setTimeCount(1);
        signals.setDimensionCount(0);
        signals.setTimeGrainDetected(true);

        String payload = SupersetVizSelectionPromptBuilder.buildJsonPayload("按日期看销售趋势",
                queryResult.getQuerySql(), queryResult, parseInfo, datasetInfo, signals,
                Collections.singletonList(area));
        JSONObject json = JSONObject.parseObject(payload);

        Assertions.assertEquals("按日期看销售趋势", json.getString("user_question"));
        Assertions.assertNotNull(json.getJSONObject("sql_summary"));
        Assertions.assertNotNull(json.getJSONObject("dataset_context"));
        Assertions.assertNotNull(json.getJSONObject("data_profile"));
        JSONArray candidates = json.getJSONArray("candidate_viztypes");
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertEquals("echarts_area", candidates.getJSONObject(0).getString("viz_type"));
        Assertions.assertNotNull(candidates.getJSONObject(0).getJSONObject("selection_summary"));
    }

    @Test
    public void testBuildJsonPayloadIncludesDetailIntentProfile() {
        QueryResult queryResult = new QueryResult();
        queryResult.setQuerySql(
                "select order_id, ds, category, user_name, sales from orders order by ds desc limit 20");
        queryResult.setQueryColumns(
                Arrays.asList(buildColumn("order_id", "order_id", "CATEGORY", "BIGINT"),
                        buildColumn("ds", "ds", "DATE", "DATE"),
                        buildColumn("category", "category", "CATEGORY", "STRING"),
                        buildColumn("user_name", "user_name", "CATEGORY", "STRING"),
                        buildColumn("sales", "sales", "NUMBER", "DOUBLE")));
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", 1L);
        row.put("ds", "2024-01-01");
        row.put("category", "手机");
        row.put("user_name", "张三");
        row.put("sales", 10);
        queryResult.setQueryResults(Collections.singletonList(row));

        SupersetVizSelectionPromptBuilder.SelectionSignals signals =
                new SupersetVizSelectionPromptBuilder.SelectionSignals();
        signals.setColumnCount(5);
        signals.setRowCount(1);
        signals.setMetricCount(1);
        signals.setTimeCount(1);
        signals.setDimensionCount(3);

        String payload = SupersetVizSelectionPromptBuilder.buildJsonPayload("把订单明细列出来",
                queryResult.getQuerySql(), queryResult, null, null, signals,
                Collections.emptyList());
        JSONObject json = JSONObject.parseObject(payload);
        JSONObject intentProfile = json.getJSONObject("intent_profile");

        Assertions.assertNotNull(intentProfile);
        Assertions.assertTrue(intentProfile.getBooleanValue("detail_request"));
        Assertions.assertFalse(intentProfile.getBooleanValue("aggregation_detected"));
        Assertions.assertEquals("table", intentProfile.getString("preferred_table_viz_type"));
    }

    private QueryColumn buildColumn(String name, String bizName, String showType, String type) {
        QueryColumn column = new QueryColumn();
        column.setName(name);
        column.setBizName(bizName);
        column.setShowType(showType);
        column.setType(type);
        return column;
    }
}
