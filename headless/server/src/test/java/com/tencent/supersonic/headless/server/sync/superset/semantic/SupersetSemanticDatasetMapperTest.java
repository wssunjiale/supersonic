package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.enums.DataTypeEnums;
import com.tencent.supersonic.headless.api.pojo.DataSetDetail;
import com.tencent.supersonic.headless.api.pojo.DataSetModelConfig;
import com.tencent.supersonic.headless.api.pojo.Measure;
import com.tencent.supersonic.headless.api.pojo.MetricDefineByMeasureParams;
import com.tencent.supersonic.headless.api.pojo.MetricDefineByMetricParams;
import com.tencent.supersonic.headless.api.pojo.MetricParam;
import com.tencent.supersonic.headless.api.pojo.enums.DimensionType;
import com.tencent.supersonic.headless.api.pojo.enums.MetricDefineType;
import com.tencent.supersonic.headless.api.pojo.request.QuerySqlReq;
import com.tencent.supersonic.headless.api.pojo.response.DataSetResp;
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;
import com.tencent.supersonic.headless.api.pojo.response.DimensionResp;
import com.tencent.supersonic.headless.api.pojo.response.MetricResp;
import com.tencent.supersonic.headless.api.pojo.response.ModelResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticTranslateResp;
import com.tencent.supersonic.headless.server.facade.service.SemanticLayerService;
import com.tencent.supersonic.headless.server.service.DataSetService;
import com.tencent.supersonic.headless.server.service.DatabaseService;
import com.tencent.supersonic.headless.server.service.DimensionService;
import com.tencent.supersonic.headless.server.service.MetricService;
import com.tencent.supersonic.headless.server.service.ModelService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SupersetSemanticDatasetMapperTest {

    @Test
    public void buildMappingShouldExposeAtomicMetricColumnsAndAggregateExpressions() {
        SupersetSemanticDatasetMapper mapper = buildMapper(
                List.of(buildDimension(101L, "brand_name", "品牌名称")),
                List.of(buildMeasureMetric(201L, "revenue", "营收", "SUM"),
                        buildMeasureMetric(202L, "registered_capital", "注册资本", "MAX")));

        SemanticDatasetMapping mapping = mapper.buildMapping(buildDataSetResp(), User.getDefaultUser());

        Assertions.assertNotNull(mapping);
        Assertions.assertEquals("SELECT brand_name, revenue, registered_capital FROM t_2",
                mapping.getSupersetSql());

        Map<String, SupersetDatasetColumn> columns = mapping.getColumns().stream()
                .collect(Collectors.toMap(SupersetDatasetColumn::getColumnName, item -> item));
        Assertions.assertTrue(columns.containsKey("brand_name"));
        Assertions.assertTrue(columns.containsKey("revenue"));
        Assertions.assertTrue(columns.containsKey("registered_capital"));

        Map<String, SupersetDatasetMetric> metrics = mapping.getMetrics().stream()
                .collect(Collectors.toMap(SupersetDatasetMetric::getMetricName, item -> item));
        Assertions.assertEquals("SUM(revenue)", metrics.get("revenue").getExpression());
        Assertions.assertEquals("MAX(registered_capital)",
                metrics.get("registered_capital").getExpression());
    }

    @Test
    public void buildMappingShouldExpandDerivedMetricsWithAggregatedDependencies() {
        MetricResp revenue = buildMeasureMetric(201L, "revenue", "营收", "SUM");
        MetricResp profit = buildMeasureMetric(202L, "profit", "利润", "SUM");
        MetricResp margin = buildDerivedMetric(203L, "profit_margin", "利润率", "profit / revenue",
                List.of(new MetricParam(202L, "profit"), new MetricParam(201L, "revenue")));

        SupersetSemanticDatasetMapper mapper = buildMapper(
                List.of(buildDimension(101L, "brand_name", "品牌名称")),
                List.of(revenue, profit, margin));

        SemanticDatasetMapping mapping = mapper.buildMapping(buildDataSetResp(), User.getDefaultUser());

        Assertions.assertNotNull(mapping);
        Assertions.assertEquals("SELECT brand_name, revenue, profit FROM t_2",
                mapping.getSupersetSql());

        Map<String, SupersetDatasetMetric> metrics = mapping.getMetrics().stream()
                .collect(Collectors.toMap(SupersetDatasetMetric::getMetricName, item -> item));
        Assertions.assertEquals("SUM(revenue)", metrics.get("revenue").getExpression());
        Assertions.assertEquals("SUM(profit)", metrics.get("profit").getExpression());
        Assertions.assertEquals("(SUM(profit)) / (SUM(revenue))",
                metrics.get("profit_margin").getExpression());
    }

    private SupersetSemanticDatasetMapper buildMapper(List<DimensionResp> dimensions,
            List<MetricResp> metrics) {
        DataSetService dataSetService = Mockito.mock(DataSetService.class);
        ModelService modelService = Mockito.mock(ModelService.class);
        DatabaseService databaseService = Mockito.mock(DatabaseService.class);
        DimensionService dimensionService = Mockito.mock(DimensionService.class);
        MetricService metricService = Mockito.mock(MetricService.class);
        SemanticLayerService semanticLayerService = Mockito.mock(SemanticLayerService.class);

        ModelResp model = new ModelResp();
        model.setId(1L);
        model.setDatabaseId(11L);

        DatabaseResp database = DatabaseResp.builder().id(11L).type("POSTGRESQL").schema("public")
                .build();

        Mockito.when(modelService.getModel(1L)).thenReturn(model);
        Mockito.when(databaseService.getDatabase(11L)).thenReturn(database);
        Mockito.when(dimensionService.getDimensions(Mockito.any())).thenReturn(dimensions);
        Mockito.when(metricService.getMetrics(Mockito.any())).thenReturn(metrics);
        try {
            Mockito.when(
                    semanticLayerService.translate(Mockito.any(QuerySqlReq.class), Mockito.any(User.class)))
                    .thenAnswer(invocation -> {
                        QuerySqlReq req = invocation.getArgument(0);
                        return SemanticTranslateResp.builder().querySQL(req.getSql()).isOk(true)
                                .build();
                    });
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return new SupersetSemanticDatasetMapper(dataSetService, modelService, databaseService,
                dimensionService, metricService, semanticLayerService);
    }

    private DataSetResp buildDataSetResp() {
        DataSetResp dataSet = new DataSetResp();
        dataSet.setId(2L);
        dataSet.setName("企业数据集");
        dataSet.setBizName("CorporateData");
        DataSetDetail detail = new DataSetDetail();
        detail.setDataSetModelConfigs(List.of(
                new DataSetModelConfig(1L, false, List.of(201L, 202L, 203L), List.of(101L))));
        dataSet.setDataSetDetail(detail);
        return dataSet;
    }

    private DimensionResp buildDimension(Long id, String bizName, String name) {
        DimensionResp dimension = new DimensionResp();
        dimension.setId(id);
        dimension.setModelId(1L);
        dimension.setBizName(bizName);
        dimension.setName(name);
        dimension.setType(DimensionType.categorical);
        dimension.setDataType(DataTypeEnums.VARCHAR);
        return dimension;
    }

    private MetricResp buildMeasureMetric(Long id, String bizName, String name, String agg) {
        MetricResp metric = new MetricResp();
        metric.setId(id);
        metric.setModelId(1L);
        metric.setBizName(bizName);
        metric.setName(name);
        metric.setDefaultAgg(agg);
        metric.setMetricDefineType(MetricDefineType.MEASURE);
        MetricDefineByMeasureParams params = new MetricDefineByMeasureParams();
        params.setExpr(bizName);
        params.setMeasures(List.of(new Measure(name, bizName, bizName, agg, null, 1)));
        metric.setMetricDefineByMeasureParams(params);
        return metric;
    }

    private MetricResp buildDerivedMetric(Long id, String bizName, String name, String expr,
            List<MetricParam> metricParams) {
        MetricResp metric = new MetricResp();
        metric.setId(id);
        metric.setModelId(1L);
        metric.setBizName(bizName);
        metric.setName(name);
        metric.setMetricDefineType(MetricDefineType.METRIC);
        MetricDefineByMetricParams params = new MetricDefineByMetricParams();
        params.setExpr(expr);
        params.setMetrics(metricParams);
        metric.setMetricDefineByMetricParams(params);
        return metric;
    }
}
