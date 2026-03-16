package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.chat.server.agent.Agent;
import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.QueryColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SupersetVizTypeSelectorTest {

    @Test
    public void testSelectLineForDateAndMetric() {
        QueryResult result = new QueryResult();
        QueryColumn dateColumn = new QueryColumn();
        dateColumn.setShowType("DATE");
        dateColumn.setType("DATE");
        QueryColumn metricColumn = new QueryColumn();
        metricColumn.setShowType("NUMBER");
        metricColumn.setType("BIGINT");
        result.setQueryColumns(Arrays.asList(dateColumn, metricColumn));
        result.setQueryResults(Arrays.asList(new HashMap<>(), new HashMap<>()));
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        Assertions.assertEquals("echarts_timeseries_line",
                SupersetVizTypeSelector.select(config, result, null));
    }

    @Test
    public void testSelectTableWhenEmpty() {
        QueryResult result = new QueryResult();
        result.setQueryColumns(Collections.emptyList());
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        Assertions.assertEquals("table", SupersetVizTypeSelector.select(config, result, null));
    }

    @Test
    public void testSelectTableWhenNull() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        Assertions.assertEquals("table", SupersetVizTypeSelector.select(config, null, null));
    }

    @Test
    public void testSelectPieForSmallCategory() {
        QueryResult result = new QueryResult();
        QueryColumn categoryColumn = new QueryColumn();
        categoryColumn.setShowType("CATEGORY");
        categoryColumn.setType("STRING");
        QueryColumn metricColumn = new QueryColumn();
        metricColumn.setShowType("NUMBER");
        metricColumn.setType("DOUBLE");
        result.setQueryColumns(Arrays.asList(categoryColumn, metricColumn));
        result.setQueryResults(Arrays.asList(new HashMap<>(), new HashMap<>(), new HashMap<>()));
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        Assertions.assertEquals("pie", SupersetVizTypeSelector.select(config, result, null));
    }

    @Test
    public void testSelectLineWhenTimeGrainDetectedFromValues() {
        QueryResult result = new QueryResult();
        QueryColumn timeColumn = new QueryColumn();
        timeColumn.setName("day");
        timeColumn.setType("STRING");
        QueryColumn metricColumn = new QueryColumn();
        metricColumn.setName("count");
        metricColumn.setType("STRING");
        HashMap<String, Object> row1 = new HashMap<>();
        row1.put("day", "2024-01-01");
        row1.put("count", 12);
        HashMap<String, Object> row2 = new HashMap<>();
        row2.put("day", "2024-01-02");
        row2.put("count", 18);
        result.setQueryColumns(Arrays.asList(timeColumn, metricColumn));
        result.setQueryResults(Arrays.asList(row1, row2));
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        Assertions.assertEquals("echarts_timeseries_line",
                SupersetVizTypeSelector.select(config, result, null));
    }

    @Test
    public void testAllowListOverridesRule() {
        QueryResult result = new QueryResult();
        QueryColumn categoryColumn = new QueryColumn();
        categoryColumn.setShowType("CATEGORY");
        categoryColumn.setType("STRING");
        QueryColumn metricColumn = new QueryColumn();
        metricColumn.setShowType("NUMBER");
        metricColumn.setType("DOUBLE");
        result.setQueryColumns(Arrays.asList(categoryColumn, metricColumn));
        result.setQueryResults(Arrays.asList(new HashMap<>(), new HashMap<>(), new HashMap<>()));
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        config.setVizTypeAllowList(Collections.singletonList("table"));
        Assertions.assertEquals("table", SupersetVizTypeSelector.select(config, result, null));
    }

    @Test
    public void testResolveFromModelResponse() {
        SupersetVizTypeSelector.VizTypeItem pie = new SupersetVizTypeSelector.VizTypeItem();
        pie.setVizType("pie");
        pie.setName("Pie Chart");
        SupersetVizTypeSelector.VizTypeItem bar = new SupersetVizTypeSelector.VizTypeItem();
        bar.setVizType("bar");
        bar.setName("Bar Chart");
        String response =
                "{\"viz_type\":\"Pie Chart\",\"alternatives\":[\"bar\"],\"reason\":\"match\"}";
        String resolved =
                SupersetVizTypeSelector.resolveFromModelResponse(response, Arrays.asList(pie, bar));
        Assertions.assertEquals("pie", resolved);
    }

    @Test
    public void testResolveCandidatesFromModelResponse() {
        SupersetVizTypeSelector.VizTypeItem pie = new SupersetVizTypeSelector.VizTypeItem();
        pie.setVizType("pie");
        pie.setName("Pie Chart");
        SupersetVizTypeSelector.VizTypeItem bar = new SupersetVizTypeSelector.VizTypeItem();
        bar.setVizType("bar");
        bar.setName("Bar Chart");
        SupersetVizTypeSelector.VizTypeItem table = new SupersetVizTypeSelector.VizTypeItem();
        table.setVizType("table");
        table.setName("Table");
        String response =
                "{\"viz_type\":\"Pie Chart\",\"alternatives\":[\"Bar Chart\",\"Pie Chart\",\"table\"],\"reason\":\"match\"}";
        Assertions.assertEquals(Arrays.asList("pie", "bar", "table"), SupersetVizTypeSelector
                .resolveCandidatesFromModelResponse(response, Arrays.asList(pie, bar, table)));
    }

    @Test
    public void testResolveCandidatesFromRankedCandidatesResponse() {
        SupersetVizTypeSelector.VizTypeItem pie = new SupersetVizTypeSelector.VizTypeItem();
        pie.setVizType("pie");
        pie.setName("Pie Chart");
        SupersetVizTypeSelector.VizTypeItem bar = new SupersetVizTypeSelector.VizTypeItem();
        bar.setVizType("bar");
        bar.setName("Bar Chart");
        SupersetVizTypeSelector.VizTypeItem table = new SupersetVizTypeSelector.VizTypeItem();
        table.setVizType("table");
        table.setName("Table");
        String response = "{\"ranked_candidates\":["
                + "{\"viz_type\":\"bar\",\"score\":0.96,\"reason\":\"best\"},"
                + "{\"viz_type\":\"pie\",\"chart_name\":\"占比图\",\"score\":0.8},"
                + "{\"viz_type\":\"table\",\"score\":0.4}]}";
        Assertions.assertEquals(Arrays.asList("bar", "pie", "table"), SupersetVizTypeSelector
                .resolveCandidatesFromModelResponse(response, Arrays.asList(pie, bar, table)));
    }

    @Test
    public void testResolveCandidatesFromModelResponseCapsRankedCandidatesToTopThree() {
        SupersetVizTypeSelector.VizTypeItem pie = new SupersetVizTypeSelector.VizTypeItem();
        pie.setVizType("pie");
        pie.setName("Pie Chart");
        SupersetVizTypeSelector.VizTypeItem bar = new SupersetVizTypeSelector.VizTypeItem();
        bar.setVizType("bar");
        bar.setName("Bar Chart");
        SupersetVizTypeSelector.VizTypeItem table = new SupersetVizTypeSelector.VizTypeItem();
        table.setVizType("table");
        table.setName("Table");
        SupersetVizTypeSelector.VizTypeItem line = new SupersetVizTypeSelector.VizTypeItem();
        line.setVizType("echarts_timeseries_line");
        line.setName("Line Chart");
        String response = "{\"ranked_candidates\":[" + "{\"viz_type\":\"bar\",\"score\":0.96},"
                + "{\"viz_type\":\"pie\",\"score\":0.8},"
                + "{\"viz_type\":\"table\",\"score\":0.4},"
                + "{\"viz_type\":\"echarts_timeseries_line\",\"score\":0.2}]}";

        Assertions.assertEquals(Arrays.asList("bar", "pie", "table"),
                SupersetVizTypeSelector.resolveCandidatesFromModelResponse(response,
                        Arrays.asList(pie, bar, table, line)));
    }

    @Test
    public void testResolveItemByVizTypeReadsParallelSupersetSpecCatalog() {
        SupersetVizTypeSelector.VizTypeItem item =
                SupersetVizTypeSelector.resolveItemByVizType("echarts_area", null);

        Assertions.assertNotNull(item);
        Assertions.assertEquals("Area Chart", item.getName());
        Assertions.assertEquals("timeseries", item.getCategory());
        Assertions.assertNotNull(item.getSelectionSummary());
        Assertions.assertTrue(
                item.getSelectionSummary().getRequiredDatasetRoles().contains("metric"));
    }

    @Test
    public void testResolveItemByVizTypeTableReadsParallelSupersetSpecCatalog() {
        SupersetVizTypeSelector.VizTypeItem item =
                SupersetVizTypeSelector.resolveItemByVizType("table", null);

        Assertions.assertNotNull(item);
        Assertions.assertEquals("Table", item.getName());
        Assertions.assertEquals("table", item.getCategory());
        Assertions.assertNotNull(item.getSelectionSummary());
        Assertions.assertTrue(item.getSelectionSummary().getSelectionHints().stream()
                .anyMatch(hint -> hint.contains("明细") || hint.contains("原始记录")));
    }

    @Test
    public void testResolveItemByVizTypeParsesFormDataRulesForTableLikeVizTypes() {
        SupersetVizTypeSelector.VizTypeItem pivot =
                SupersetVizTypeSelector.resolveItemByVizType("pivot_table_v2", null);
        SupersetVizTypeSelector.VizTypeItem timeTable =
                SupersetVizTypeSelector.resolveItemByVizType("time_table", null);
        SupersetVizTypeSelector.VizTypeItem timePivot =
                SupersetVizTypeSelector.resolveItemByVizType("time_pivot", null);

        Assertions.assertNotNull(pivot);
        Assertions.assertNotNull(pivot.getFormDataRules());
        Assertions.assertEquals("PIVOT_TABLE", pivot.getFormDataRules().getProfile());
        Assertions.assertEquals(Collections.singletonList("metrics"),
                pivot.getFormDataRules().getRequired());
        Assertions.assertEquals(
                Arrays.asList(Collections.singletonList("groupbyRows"),
                        Collections.singletonList("groupbyColumns")),
                pivot.getFormDataRules().getRequiredAnyOf().stream()
                        .map(group -> group.stream()
                                .map(SupersetVizTypeSelector.FormDataField::getKey)
                                .collect(java.util.stream.Collectors.toList()))
                        .collect(java.util.stream.Collectors.toList()));

        Assertions.assertNotNull(timeTable);
        Assertions.assertNotNull(timeTable.getFormDataRules());
        Assertions.assertEquals("TIME_TABLE", timeTable.getFormDataRules().getProfile());
        Assertions.assertEquals(Arrays.asList("granularity_sqla", "metrics"),
                timeTable.getFormDataRules().getRequired());

        Assertions.assertNotNull(timePivot);
        Assertions.assertNotNull(timePivot.getFormDataRules());
        Assertions.assertEquals("TIME_PIVOT", timePivot.getFormDataRules().getProfile());
        Assertions.assertEquals(Arrays.asList("granularity_sqla", "metric"),
                timePivot.getFormDataRules().getRequired());
    }

    @Test
    public void testResolveChatModelIdUsesConfig() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmChatModelId(12);
        Agent agent = new Agent();
        Map<String, ChatApp> chatApps = new HashMap<>();
        chatApps.put("S2SQL_PARSER", ChatApp.builder().chatModelId(23).enable(true).build());
        agent.setChatAppConfig(chatApps);
        Assertions.assertEquals(12, SupersetVizTypeSelector.resolveChatModelId(config, agent));
    }

    @Test
    public void testResolveChatModelIdFallsBackToAgent() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        Agent agent = new Agent();
        Map<String, ChatApp> chatApps = new LinkedHashMap<>();
        chatApps.put("S2SQL_PARSER", ChatApp.builder().chatModelId(33).enable(true).build());
        agent.setChatAppConfig(chatApps);
        Assertions.assertEquals(33, SupersetVizTypeSelector.resolveChatModelId(config, agent));
    }

    @Test
    public void testSelectByLlmMissingModelIdThrows() {
        QueryResult result = new QueryResult();
        QueryColumn categoryColumn = new QueryColumn();
        categoryColumn.setShowType("CATEGORY");
        categoryColumn.setType("STRING");
        QueryColumn metricColumn = new QueryColumn();
        metricColumn.setShowType("NUMBER");
        metricColumn.setType("DOUBLE");
        result.setQueryColumns(Arrays.asList(categoryColumn, metricColumn));
        result.setQueryResults(Arrays.asList(new HashMap<>(), new HashMap<>()));
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(true);
        Assertions.assertThrows(IllegalStateException.class,
                () -> SupersetVizTypeSelector.select(config, result, null, null));
    }

    @Test
    public void testGuardLlmCandidatesKeepsTableForDetailLikeContext() throws Exception {
        List<String> guarded = invokeGuardLlmCandidates(Arrays.asList("table", "pivot_table_v2"),
                buildDecisionContext(5, 2, 1, 1, 3),
                Arrays.asList(vizType("table", "Table"), vizType("pivot_table_v2", "Pivot Table"),
                        vizType("echarts_timeseries_line", "Line Chart")));

        Assertions.assertEquals(Arrays.asList("table", "pivot_table_v2"), guarded);
    }

    @Test
    public void testGuardLlmCandidatesStillAvoidsTableForTrendContext() throws Exception {
        List<String> guarded = invokeGuardLlmCandidates(Arrays.asList("table", "echarts_area"),
                buildDecisionContext(2, 30, 1, 1, 0),
                Arrays.asList(vizType("table", "Table"), vizType("echarts_area", "Area Chart"),
                        vizType("echarts_timeseries_line", "Line Chart")));

        Assertions.assertEquals(Collections.singletonList("echarts_area"), guarded);
    }

    @SuppressWarnings("unchecked")
    private List<String> invokeGuardLlmCandidates(List<String> llmChoices, Object context,
            List<SupersetVizTypeSelector.VizTypeItem> candidates) throws Exception {
        Method method = SupersetVizTypeSelector.class.getDeclaredMethod("guardLlmCandidates",
                List.class, context.getClass(), List.class);
        method.setAccessible(true);
        return (List<String>) method.invoke(null, llmChoices, context, candidates);
    }

    private Object buildDecisionContext(int columnCount, int rowCount, int metricCount,
            int timeCount, int dimensionCount) throws Exception {
        Class<?> contextClass = Class.forName(
                "com.tencent.supersonic.chat.server.plugin.build.superset.SupersetVizTypeSelector$DecisionContext");
        Constructor<?> constructor = contextClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object context = constructor.newInstance();
        invokeSetter(contextClass, context, "setColumnCount", int.class, columnCount);
        invokeSetter(contextClass, context, "setRowCount", int.class, rowCount);
        invokeSetter(contextClass, context, "setMetricCount", int.class, metricCount);
        invokeSetter(contextClass, context, "setTimeCount", int.class, timeCount);
        invokeSetter(contextClass, context, "setDimensionCount", int.class, dimensionCount);
        return context;
    }

    private void invokeSetter(Class<?> type, Object target, String name, Class<?> argType,
            Object value) throws Exception {
        Method method = type.getDeclaredMethod(name, argType);
        method.setAccessible(true);
        method.invoke(target, value);
    }

    private SupersetVizTypeSelector.VizTypeItem vizType(String vizType, String name) {
        SupersetVizTypeSelector.VizTypeItem item = new SupersetVizTypeSelector.VizTypeItem();
        item.setVizType(vizType);
        item.setName(name);
        return item;
    }
}
