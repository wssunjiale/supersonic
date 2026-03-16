package com.tencent.supersonic.chat.server.processor.execute;

import com.tencent.supersonic.chat.api.pojo.request.ChatExecuteReq;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.chat.server.plugin.build.ParamOption;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartCandidate;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartInfo;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetChartResp;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetEmbeddedDashboardInfo;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetPluginConfig;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetVizTypeSelector;
import com.tencent.supersonic.chat.server.pojo.ExecuteContext;
import com.tencent.supersonic.common.config.ChatModel;
import com.tencent.supersonic.common.pojo.ChatModelConfig;
import com.tencent.supersonic.common.pojo.DateConf;
import com.tencent.supersonic.common.pojo.Order;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.pojo.enums.DatePeriodEnum;
import com.tencent.supersonic.common.service.ChatModelService;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.service.SupersetDatasetRegistryService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncService;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.provider.ModelProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SupersetChartProcessorTest {

    @Test
    public void testBuildFormDataTableUsesDatasetColumns() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
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
        SupersetPluginConfig config = buildConfig();
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryColumns(Arrays.asList(new QueryColumn("id", "INT", "id"),
                new QueryColumn("name", "STRING", "name")));

        Map<String, Object> formData = processor.buildFormData(config, null, queryResult,
                datasetInfo, "table", null, null);

        Assertions.assertEquals("raw", formData.get("query_mode"));
        Assertions.assertEquals(Arrays.asList("id", "name"), formData.get("all_columns"));
        Assertions.assertEquals(Arrays.asList("id", "name"), formData.get("columns"));
    }

    @Test
    public void testBuildFormDataUsesDatasetMetricsAndDimensions() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
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
    public void testBuildFormDataFallsBackToAdhocMetricWhenDatasetMetricExpressionInvalid() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额")
                .defaultAgg("SUM").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount", "SUM(revenue)")));

        Map<String, Object> formData =
                processor.buildFormData(config, parseInfo, null, datasetInfo, "pie", null, null);

        Assertions.assertTrue(formData.get("metric") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> metric = (Map<String, Object>) formData.get("metric");
        Assertions.assertEquals("SIMPLE", metric.get("expressionType"));
        Assertions.assertEquals("SUM", metric.get("aggregate"));
        Assertions.assertTrue(metric.get("column") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> column = (Map<String, Object>) metric.get("column");
        Assertions.assertEquals("amount", column.get("column_name"));
    }

    @Test
    public void testBuildFormDataNormalizesDimensionNameToDatasetColumn() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("count").name("访问人数").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("访 问人数").name("访问人数").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Collections.singletonList(buildColumn("访问人数", "INT", true, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("count")));

        Map<String, Object> formData =
                processor.buildFormData(config, parseInfo, null, datasetInfo, "table", null, null);

        Assertions.assertEquals(Collections.singletonList("访问人数"), formData.get("groupby"));
    }

    @Test
    public void testBuildFormDataBuildsAdhocMetricWhenDatasetMetricMissing() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false),
                buildColumn("ds", "DATE", false, true)));

        Map<String, Object> formData = processor.buildFormData(config, parseInfo, null, datasetInfo,
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
    public void testBuildFormDataPivotTableUsesPivotKeys() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("region").name("区域").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("region", "STRING", true, false),
                buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));

        Map<String, Object> formData = processor.buildFormData(config, parseInfo, null, datasetInfo,
                "pivot_table_v2", null, null);

        Assertions.assertEquals("aggregate", formData.get("query_mode"));
        Assertions.assertEquals(Collections.singletonList("amount"), formData.get("metrics"));
        Assertions.assertEquals(Collections.singletonList("region"), formData.get("groupbyRows"));
        Assertions.assertEquals(Collections.singletonList("category"),
                formData.get("groupbyColumns"));
    }

    @Test
    public void testBuildFormDataTimeTableUsesTimeTableKeys() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("region").name("区域").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("ds", "DATE", false, true),
                buildColumn("region", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));

        Map<String, Object> formData = processor.buildFormData(config, parseInfo, null, datasetInfo,
                "time_table", null, null);

        Assertions.assertEquals("aggregate", formData.get("query_mode"));
        Assertions.assertEquals("ds", formData.get("granularity_sqla"));
        Assertions.assertEquals(Collections.singletonList("amount"), formData.get("metrics"));
        Assertions.assertEquals(Collections.singletonList("region"), formData.get("groupby"));
    }

    @Test
    public void testBuildFormDataTimePivotUsesMetricAndTimeColumn() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("ds", "DATE", false, true),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));

        Map<String, Object> formData = processor.buildFormData(config, parseInfo, null, datasetInfo,
                "time_pivot", null, null);

        Assertions.assertEquals("aggregate", formData.get("query_mode"));
        Assertions.assertEquals("ds", formData.get("granularity_sqla"));
        Assertions.assertEquals("amount", formData.get("metric"));
    }

    @Test
    public void testBuildFormDataTablePrefersRawWhenQueryResultLooksLikeDetailRows() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("region").name("区域").build());
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryColumns(Arrays.asList(new QueryColumn("ds", "DATE", "ds"),
                new QueryColumn("region", "STRING", "region"),
                new QueryColumn("amount", "DECIMAL", "amount")));
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("ds", "DATE", true, true),
                buildColumn("region", "STRING", true, false),
                buildColumn("amount", "DECIMAL", true, false)));

        Map<String, Object> formData = processor.buildFormData(config, parseInfo, queryResult,
                datasetInfo, "table", null, null);

        Assertions.assertEquals("raw", formData.get("query_mode"));
        Assertions.assertEquals(Arrays.asList("ds", "region", "amount"), formData.get("columns"));
        Assertions.assertEquals(Arrays.asList("ds", "region", "amount"),
                formData.get("all_columns"));
    }

    @Test
    public void testBuildFormDataTableRawUsesSelectedColumnsInsteadOfWholeDataset() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryColumns(Arrays.asList(new QueryColumn("ds", "DATE", "ds"),
                new QueryColumn("region", "STRING", "region")));
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("ds", "DATE", true, true),
                buildColumn("region", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false),
                buildColumn("department", "STRING", true, false)));

        Map<String, Object> formData = processor.buildFormData(config, null, queryResult,
                datasetInfo, "table", null, null);

        Assertions.assertEquals("raw", formData.get("query_mode"));
        Assertions.assertEquals(Arrays.asList("ds", "region"), formData.get("columns"));
        Assertions.assertEquals(Arrays.asList("ds", "region"), formData.get("all_columns"));
    }

    @Test
    public void testBuildFormDataCarriesDateOrderAndLimitIntoSupersetPayload() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.setLimit(3L);
        parseInfo.setDateInfo(buildDateConf("imp_date", "2026-02-14", "2026-03-15"));
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("pv").name("访问次数")
                .defaultAgg("SUM").build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("department").name("部门")
                .build());
        parseInfo.getOrders().add(new Order("pv", "DESC"));
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setMainDttmCol("imp_date");
        datasetInfo.setColumns(Arrays.asList(buildColumn("imp_date", "DATE", true, true),
                buildColumn("department", "STRING", true, false),
                buildColumn("pv", "BIGINT", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("pv")));

        Map<String, Object> formData = processor.buildFormData(config, parseInfo, null, datasetInfo,
                "pie", null, null);

        Assertions.assertEquals("aggregate", formData.get("query_mode"));
        Assertions.assertEquals(3L, formData.get("row_limit"));
        Assertions.assertEquals("2026-02-14 : 2026-03-15", formData.get("time_range"));
        Assertions.assertEquals("imp_date", formData.get("granularity_sqla"));
        Assertions.assertEquals("P1D", formData.get("time_grain_sqla"));
        Assertions.assertEquals(Boolean.TRUE, formData.get("order_desc"));
        Assertions.assertEquals("pv", formData.get("series_limit_metric"));
        Assertions.assertEquals("pv", formData.get("timeseries_limit_metric"));
        Object adhocFilters = formData.get("adhoc_filters");
        Assertions.assertTrue(adhocFilters instanceof List);
        Assertions.assertEquals(1, ((List<?>) adhocFilters).size());
    }

    @Test
    public void testBuildFormDataPieWithoutDateInfoDoesNotInjectDefaultGranularity() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setMainDttmCol("brand_established_time");
        datasetInfo.setColumns(Arrays.asList(
                buildColumn("brand_established_time", "DATE", false, true),
                buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));

        Map<String, Object> formData =
                processor.buildFormData(config, parseInfo, null, datasetInfo, "pie", null, null);

        Assertions.assertFalse(formData.containsKey("granularity_sqla"));
        Assertions.assertFalse(formData.containsKey("time_grain_sqla"));
    }

    @Test
    public void testBuildFormDataLlmPathCarriesDateOrderAndLimitIntoSupersetPayload() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        config.setVizTypeLlmEnabled(true);
        config.setVizTypeLlmChatModelId(2);
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.setLimit(500L);
        parseInfo.setDateInfo(buildDateConf("imp_date", "2026-02-15", "2026-03-16"));
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("pv").name("访问次数")
                .defaultAgg("SUM").build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("department").name("部门")
                .build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setMainDttmCol("imp_date");
        datasetInfo.setColumns(Arrays.asList(buildColumn("imp_date", "DATE", true, true),
                buildColumn("department", "STRING", true, false),
                buildColumn("pv", "BIGINT", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("pv")));

        ChatModelConfig chatModelConfig = new ChatModelConfig();
        ChatModel chatModel = new ChatModel();
        chatModel.setConfig(chatModelConfig);
        ChatModelService chatModelService = Mockito.mock(ChatModelService.class);
        Mockito.when(chatModelService.getChatModel(2)).thenReturn(chatModel);

        ChatLanguageModel chatLanguageModel = Mockito.mock(ChatLanguageModel.class);
        Mockito.when(chatLanguageModel.generate(Mockito.any(ChatMessage.class)))
                .thenReturn(Response.from(
                        AiMessage.from("{\"metrics\":[\"pv\"],\"groupbyRows\":[\"department\"]}")));

        try (MockedStatic<ContextUtils> mockedContext = Mockito.mockStatic(ContextUtils.class);
                MockedStatic<ModelProvider> mockedModelProvider =
                        Mockito.mockStatic(ModelProvider.class)) {
            mockedContext.when(() -> ContextUtils.getBean(ChatModelService.class))
                    .thenReturn(chatModelService);
            mockedModelProvider.when(() -> ModelProvider.getChatModel(chatModelConfig))
                    .thenReturn(chatLanguageModel);

            Map<String, Object> formData = processor.buildFormData(config, parseInfo, null,
                    datasetInfo, "pivot_table_v2", null, "过去30天访问次数最高的部门top3");

            Assertions.assertEquals("aggregate", formData.get("query_mode"));
            Assertions.assertEquals(500L, formData.get("row_limit"));
            Assertions.assertEquals("2026-02-15 : 2026-03-16", formData.get("time_range"));
            Assertions.assertEquals("imp_date", formData.get("granularity_sqla"));
            Assertions.assertEquals("P1D", formData.get("time_grain_sqla"));
            Assertions.assertEquals(Collections.singletonList("pv"), formData.get("metrics"));
            Assertions.assertEquals(Collections.singletonList("department"),
                    formData.get("groupbyRows"));
            Object adhocFilters = formData.get("adhoc_filters");
            Assertions.assertTrue(adhocFilters instanceof List);
            Assertions.assertEquals(1, ((List<?>) adhocFilters).size());
        }
    }

    @Test
    public void testBuildFormDataMissingHeatmapDimensionsThrows() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions()
                .add(SchemaElement.builder().bizName("category").name("品类").build());
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setColumns(Arrays.asList(buildColumn("category", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));

        Assertions.assertThrows(IllegalStateException.class, () -> processor.buildFormData(config,
                parseInfo, null, datasetInfo, "heatmap_v2", null, null));
    }

    @Test
    public void testResolveSupersetDatasetShouldPreferPersistentDataset() throws Exception {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetSyncService syncService = Mockito.mock(SupersetSyncService.class);
        SupersetDatasetRegistryService registryService =
                Mockito.mock(SupersetDatasetRegistryService.class);
        SupersetDatasetDO candidate = buildRegistryCandidate(11L, "SEMANTIC_DATASET", "VIRTUAL");
        SupersetDatasetInfo persistentDataset = new SupersetDatasetInfo();
        persistentDataset.setId(201L);
        persistentDataset.setColumns(Arrays.asList(buildColumn("department", "STRING", true, false),
                buildColumn("pv", "BIGINT", false, false)));
        persistentDataset.setMetrics(Collections.singletonList(buildMetric("pv")));

        Mockito.when(registryService.listAvailablePersistentDatasets(9L))
                .thenReturn(Collections.singletonList(candidate));
        Mockito.when(syncService.resolveDatasetInfo(candidate)).thenReturn(persistentDataset);

        try (MockedStatic<ContextUtils> mocked = Mockito.mockStatic(ContextUtils.class)) {
            mocked.when(() -> ContextUtils.getBean(SupersetSyncService.class))
                    .thenReturn(syncService);
            mocked.when(() -> ContextUtils.getBean(SupersetDatasetRegistryService.class))
                    .thenReturn(registryService);

            SupersetDatasetInfo resolved = invokeResolveSupersetDataset(processor,
                    buildPersistentExecuteContext(9L), buildQuerySql(),
                    buildAliasedQueryResult("_总访问次数"));

            Assertions.assertSame(persistentDataset, resolved);
            Mockito.verify(syncService).resolveDatasetInfo(candidate);
            Mockito.verify(syncService, Mockito.never()).registerAndSyncDataset(Mockito.any(),
                    Mockito.anyString(), Mockito.anyList(), Mockito.any());
        }
    }

    @Test
    public void testResolveSupersetDatasetShouldFallbackWhenPersistentFieldsMissing()
            throws Exception {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetSyncService syncService = Mockito.mock(SupersetSyncService.class);
        SupersetDatasetRegistryService registryService =
                Mockito.mock(SupersetDatasetRegistryService.class);
        SupersetDatasetDO candidate = buildRegistryCandidate(12L, "SEMANTIC_DATASET", "VIRTUAL");
        SupersetDatasetInfo persistentDataset = new SupersetDatasetInfo();
        persistentDataset.setId(202L);
        persistentDataset.setColumns(
                Collections.singletonList(buildColumn("department", "STRING", true, false)));
        SupersetDatasetInfo fallbackDataset = new SupersetDatasetInfo();
        fallbackDataset.setId(303L);

        ExecuteContext executeContext = buildPersistentExecuteContext(9L);
        QueryResult queryResult = buildAliasedQueryResult("_总访问次数");

        Mockito.when(registryService.listAvailablePersistentDatasets(9L))
                .thenReturn(Collections.singletonList(candidate));
        Mockito.when(syncService.resolveDatasetInfo(candidate)).thenReturn(persistentDataset);
        Mockito.when(syncService.registerAndSyncDataset(executeContext.getParseInfo(),
                buildQuerySql(), queryResult.getQueryColumns(), null)).thenReturn(fallbackDataset);

        try (MockedStatic<ContextUtils> mocked = Mockito.mockStatic(ContextUtils.class)) {
            mocked.when(() -> ContextUtils.getBean(SupersetSyncService.class))
                    .thenReturn(syncService);
            mocked.when(() -> ContextUtils.getBean(SupersetDatasetRegistryService.class))
                    .thenReturn(registryService);

            SupersetDatasetInfo resolved = invokeResolveSupersetDataset(processor, executeContext,
                    buildQuerySql(), queryResult);

            Assertions.assertSame(fallbackDataset, resolved);
            Mockito.verify(syncService).resolveDatasetInfo(candidate);
            Mockito.verify(syncService).registerAndSyncDataset(executeContext.getParseInfo(),
                    buildQuerySql(), queryResult.getQueryColumns(), null);
        }
    }

    @Test
    public void testResolveSupersetDatasetShouldFallbackWhenPersistentMetricExpressionInvalid()
            throws Exception {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetSyncService syncService = Mockito.mock(SupersetSyncService.class);
        SupersetDatasetRegistryService registryService =
                Mockito.mock(SupersetDatasetRegistryService.class);
        SupersetDatasetDO candidate = buildRegistryCandidate(13L, "SEMANTIC_DATASET", "VIRTUAL");
        SupersetDatasetInfo persistentDataset = new SupersetDatasetInfo();
        persistentDataset.setId(204L);
        persistentDataset.setColumns(
                Collections.singletonList(buildColumn("department", "STRING", true, false)));
        persistentDataset.setMetrics(Collections.singletonList(buildMetric("pv", "SUM(revenue)")));
        SupersetDatasetInfo fallbackDataset = new SupersetDatasetInfo();
        fallbackDataset.setId(304L);

        ExecuteContext executeContext = buildPersistentExecuteContext(9L);
        QueryResult queryResult = buildAliasedQueryResult("_总访问次数");

        Mockito.when(registryService.listAvailablePersistentDatasets(9L))
                .thenReturn(Collections.singletonList(candidate));
        Mockito.when(syncService.resolveDatasetInfo(candidate)).thenReturn(persistentDataset);
        Mockito.when(syncService.registerAndSyncDataset(executeContext.getParseInfo(),
                buildQuerySql(), queryResult.getQueryColumns(), null)).thenReturn(fallbackDataset);

        try (MockedStatic<ContextUtils> mocked = Mockito.mockStatic(ContextUtils.class)) {
            mocked.when(() -> ContextUtils.getBean(SupersetSyncService.class))
                    .thenReturn(syncService);
            mocked.when(() -> ContextUtils.getBean(SupersetDatasetRegistryService.class))
                    .thenReturn(registryService);

            SupersetDatasetInfo resolved = invokeResolveSupersetDataset(processor, executeContext,
                    buildQuerySql(), queryResult);

            Assertions.assertSame(fallbackDataset, resolved);
            Mockito.verify(syncService).resolveDatasetInfo(candidate);
            Mockito.verify(syncService).registerAndSyncDataset(executeContext.getParseInfo(),
                    buildQuerySql(), queryResult.getQueryColumns(), null);
        }
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
        Integer configuredHeight =
                (Integer) method.invoke(processor, config, "echarts_timeseries_line");
        Assertions.assertEquals(420, configuredHeight);

        SupersetPluginConfig defaultConfig = new SupersetPluginConfig();
        Integer lineHeight =
                (Integer) method.invoke(processor, defaultConfig, "echarts_timeseries_line");
        Assertions.assertEquals(300, lineHeight);

        Integer defaultHeight = (Integer) method.invoke(processor, defaultConfig, "bar");
        Assertions.assertEquals(260, defaultHeight);
    }

    @Test
    public void testBuildChartRequestsKeepsTopThreeChartsAndAddsTableFallback() throws Exception {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetPluginConfig config = buildConfig();
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("amount").name("金额").build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("brand").name("品牌").build());
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryColumns(Arrays.asList(new QueryColumn("ds", "DATE", "ds"),
                new QueryColumn("brand", "STRING", "brand"),
                new QueryColumn("amount", "DECIMAL", "amount")));
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setMainDttmCol("ds");
        datasetInfo.setColumns(Arrays.asList(buildColumn("ds", "DATE", true, true),
                buildColumn("brand", "STRING", true, false),
                buildColumn("amount", "DECIMAL", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("amount")));
        ExecuteContext executeContext = new ExecuteContext(
                ChatExecuteReq.builder().queryId(9L).queryText("各品牌历年收入趋势").build());
        executeContext.setParseInfo(parseInfo);
        executeContext.setResponse(queryResult);
        List<SupersetVizTypeSelector.VizTypeItem> candidates =
                Arrays.asList(buildVizTypeItem("echarts_timeseries_line", "Line Chart"),
                        buildVizTypeItem("echarts_timeseries_bar", "Bar Chart"),
                        buildVizTypeItem("pie", "Pie Chart"), buildVizTypeItem("table", "Table"),
                        buildVizTypeItem("bubble", "Bubble Chart"));

        Method method = SupersetChartProcessor.class.getDeclaredMethod("buildChartRequests",
                List.class, String.class, SupersetPluginConfig.class, ExecuteContext.class,
                QueryResult.class, SupersetDatasetInfo.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Object> requests = (List<Object>) method.invoke(processor, candidates,
                "supersonic_chart_9", config, executeContext, queryResult, datasetInfo);

        Assertions.assertEquals(4, requests.size());
        Assertions.assertEquals("echarts_timeseries_line",
                invokeGetter(requests.get(0), "getVizType"));
        Assertions.assertEquals("echarts_timeseries_bar",
                invokeGetter(requests.get(1), "getVizType"));
        Assertions.assertEquals("pie", invokeGetter(requests.get(2), "getVizType"));
        Assertions.assertEquals("table", invokeGetter(requests.get(3), "getVizType"));
    }

    @Test
    public void testPopulateFinalDashboardResponseUsesPrimaryViewEmbedAndPerCandidateEmbeds() {
        SupersetChartProcessor processor = new SupersetChartProcessor();
        SupersetChartResp response = new SupersetChartResp();
        SupersetPluginConfig config = buildConfig();
        config.setBaseUrl("https://superset.example.com");

        List<SupersetChartCandidate> chartCandidates = new ArrayList<>();
        chartCandidates.add(buildChartCandidate("echarts_timeseries_line", "趋势折线图", 90L,
                "embed-line", 101L, "chart-uuid-101"));
        chartCandidates.add(buildChartCandidate("echarts_timeseries_bar", "趋势柱状图", 91L, "embed-bar",
                202L, "chart-uuid-202"));
        chartCandidates.add(
                buildChartCandidate("table", "Table", 92L, "embed-table", 303L, "chart-uuid-303"));

        processor.populateFinalDashboardResponse(response, "访问趋势分析", chartCandidates, config,
                Arrays.asList(300, 260, 260));

        Assertions.assertEquals(90L, response.getDashboardId());
        Assertions.assertEquals("访问趋势分析", response.getDashboardTitle());
        Assertions.assertEquals("embed-line", response.getEmbeddedId());
        Assertions.assertEquals("https://superset.example.com", response.getSupersetDomain());
        Assertions.assertEquals("echarts_timeseries_line", response.getVizType());
        Assertions.assertEquals(3, response.getVizTypeCandidates().size());
        Assertions.assertEquals(101L, response.getVizTypeCandidates().get(0).getChartId());
        Assertions.assertEquals("chart-uuid-101",
                response.getVizTypeCandidates().get(0).getChartUuid());
        Assertions.assertEquals("embed-line",
                response.getVizTypeCandidates().get(0).getEmbeddedId());
        Assertions.assertEquals("趋势折线图", response.getVizTypeCandidates().get(0).getVizName());
        Assertions.assertEquals(202L, response.getVizTypeCandidates().get(1).getChartId());
        Assertions.assertEquals("趋势柱状图", response.getVizTypeCandidates().get(1).getVizName());
        Assertions.assertEquals("embed-table",
                response.getVizTypeCandidates().get(2).getEmbeddedId());

        Assertions.assertNotNull(response.getWebPage());
        Assertions.assertNotNull(response.getWebPage().getParamOptions());
        ParamOption heightOption = response.getWebPage().getParamOptions().stream()
                .filter(option -> option.getParamType() == ParamOption.ParamType.FORWARD)
                .filter(option -> "height".equals(option.getKey())).findFirst().orElse(null);
        Assertions.assertNotNull(heightOption);
        Assertions.assertEquals(300, heightOption.getValue());
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
        return buildMetric(name, "SUM(" + name + ")");
    }

    private SupersetDatasetMetric buildMetric(String name, String expression) {
        SupersetDatasetMetric metric = new SupersetDatasetMetric();
        metric.setMetricName(name);
        metric.setExpression(expression);
        return metric;
    }

    private SupersetPluginConfig buildConfig() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        return config;
    }

    private DateConf buildDateConf(String dateField, String startDate, String endDate) {
        DateConf dateConf = new DateConf();
        dateConf.setDateField(dateField);
        dateConf.setDateMode(DateConf.DateMode.BETWEEN);
        dateConf.setStartDate(startDate);
        dateConf.setEndDate(endDate);
        dateConf.setPeriod(DatePeriodEnum.DAY);
        return dateConf;
    }

    private ExecuteContext buildPersistentExecuteContext(Long dataSetId) {
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.setDataSet(SchemaElement.builder().dataSetId(dataSetId).name("demo_dataset")
                .type(SchemaElementType.DATASET).build());
        parseInfo.getDimensions().add(SchemaElement.builder().bizName("department").name("部门")
                .type(SchemaElementType.DIMENSION).build());
        parseInfo.getMetrics().add(SchemaElement.builder().bizName("pv").name("访问次数")
                .type(SchemaElementType.METRIC).defaultAgg("SUM").build());

        ExecuteContext executeContext =
                new ExecuteContext(ChatExecuteReq.builder().queryId(88L).build());
        executeContext.setParseInfo(parseInfo);
        return executeContext;
    }

    private QueryResult buildAliasedQueryResult(String metricAlias) {
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryColumns(Arrays.asList(new QueryColumn("department", "STRING", "department"),
                new QueryColumn(metricAlias, "BIGINT", metricAlias),
                new QueryColumn("_排名", "BIGINT", "_排名")));
        return queryResult;
    }

    private String buildQuerySql() {
        return "SELECT department, pv AS _总访问次数, "
                + "ROW_NUMBER() OVER (ORDER BY pv DESC) AS _排名 FROM demo";
    }

    private SupersetDatasetDO buildRegistryCandidate(Long registryId, String sourceType,
            String datasetType) {
        SupersetDatasetDO record = new SupersetDatasetDO();
        record.setId(registryId);
        record.setDataSetId(9L);
        record.setSupersetDatasetId(1000L + registryId);
        record.setSourceType(sourceType);
        record.setDatasetType(datasetType);
        return record;
    }

    private SupersetDatasetInfo invokeResolveSupersetDataset(SupersetChartProcessor processor,
            ExecuteContext executeContext, String sql, QueryResult queryResult) throws Exception {
        Method method = SupersetChartProcessor.class.getDeclaredMethod("resolveSupersetDataset",
                ExecuteContext.class, String.class, QueryResult.class);
        method.setAccessible(true);
        return (SupersetDatasetInfo) method.invoke(processor, executeContext, sql, queryResult);
    }

    private SupersetChartInfo buildChartInfo(Long chartId, String chartUuid) {
        SupersetChartInfo chartInfo = new SupersetChartInfo();
        chartInfo.setChartId(chartId);
        chartInfo.setChartUuid(chartUuid);
        return chartInfo;
    }

    private SupersetChartCandidate buildChartCandidate(String vizType, String vizName,
            Long dashboardId, String embeddedId, Long chartId, String chartUuid) {
        SupersetChartCandidate chartCandidate = new SupersetChartCandidate();
        chartCandidate.setVizType(vizType);
        chartCandidate.setVizName(vizName);
        chartCandidate.setDashboardId(dashboardId);
        chartCandidate.setEmbeddedId(embeddedId);
        chartCandidate.setChartId(chartId);
        chartCandidate.setChartUuid(chartUuid);
        chartCandidate.setSupersetDomain("https://superset.example.com");
        return chartCandidate;
    }

    private SupersetVizTypeSelector.VizTypeItem buildVizTypeItem(String vizType, String name) {
        SupersetVizTypeSelector.VizTypeItem item = new SupersetVizTypeSelector.VizTypeItem();
        item.setVizType(vizType);
        item.setLlmName(name);
        item.setName(name);
        return item;
    }

    private Object invokeGetter(Object target, String methodName) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }
}
