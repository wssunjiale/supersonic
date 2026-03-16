package com.tencent.supersonic.headless.server.service;

import com.google.common.collect.Lists;
import com.tencent.supersonic.common.pojo.enums.StatusEnum;
import com.tencent.supersonic.headless.api.pojo.DataSetDetail;
import com.tencent.supersonic.headless.api.pojo.DataSetModelConfig;
import com.tencent.supersonic.headless.api.pojo.DrillDownDimension;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.request.DataSetFilterReq;
import com.tencent.supersonic.headless.api.pojo.response.*;
import com.tencent.supersonic.headless.server.service.impl.SchemaServiceImpl;
import com.tencent.supersonic.headless.server.utils.DataSetSchemaBuilder;
import com.tencent.supersonic.headless.server.utils.StatUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class SchemaServiceImplTest {

    @Test
    void buildDataSetSchema_shouldMergeModelDrillDownDimensionsIntoMetricRelateDimension() {
        // mocks
        ModelService modelService = Mockito.mock(ModelService.class);
        DimensionService dimensionService = Mockito.mock(DimensionService.class);
        MetricService metricService = Mockito.mock(MetricService.class);
        DomainService domainService = Mockito.mock(DomainService.class);
        DataSetService dataSetService = Mockito.mock(DataSetService.class);
        ModelRelaService modelRelaService = Mockito.mock(ModelRelaService.class);
        TermService termService = Mockito.mock(TermService.class);
        DatabaseService databaseService = Mockito.mock(DatabaseService.class);
        StatUtils statUtils = Mockito.mock(StatUtils.class);

        SchemaServiceImpl schemaService =
                new SchemaServiceImpl(modelService, dimensionService, metricService, domainService,
                        dataSetService, modelRelaService, statUtils, termService, databaseService);

        Long dataSetId = 1L;
        Long modelId = 200L;
        Long metricId = 100L;
        Long drillDownDimensionId = 10L;

        // dataset with one model config including the metric
        DataSetResp dataSetResp = new DataSetResp();
        dataSetResp.setId(dataSetId);
        dataSetResp.setName("ds");
        dataSetResp.setBizName("ds_biz");
        dataSetResp.setStatus(StatusEnum.ONLINE.getCode());
        dataSetResp.setDomainId(1L);
        DataSetDetail detail = new DataSetDetail();
        detail.setDataSetModelConfigs(Lists.newArrayList(new DataSetModelConfig(modelId,
                Lists.newArrayList(), Lists.newArrayList(metricId))));
        dataSetResp.setDataSetDetail(detail);

        MetricResp metricResp = new MetricResp();
        metricResp.setId(metricId);
        metricResp.setModelId(modelId);
        metricResp.setName("metric");
        metricResp.setBizName("metric_biz");
        metricResp.setStatus(StatusEnum.ONLINE.getCode());
        metricResp.setRelateDimension(null);

        ModelResp modelResp = new ModelResp();
        modelResp.setId(modelId);
        modelResp.setName("model");
        modelResp.setBizName("model_biz");
        modelResp.setDomainId(1L);
        modelResp.setDatabaseId(1L);
        modelResp.setDrillDownDimensions(
                Lists.newArrayList(new DrillDownDimension(drillDownDimensionId)));

        DatabaseResp databaseResp = new DatabaseResp();
        databaseResp.setId(1L);
        databaseResp.setType("mysql");
        databaseResp.setVersion("8");

        // stubs
        when(dataSetService.getDataSetList(any())).thenReturn(Lists.newArrayList(dataSetResp));
        when(metricService.getMetrics(any())).thenReturn(Lists.newArrayList(metricResp));
        when(dimensionService.getDimensions(any())).thenReturn(Collections.emptyList());
        when(modelService.getModelList(any())).thenReturn(Lists.newArrayList(modelResp));
        when(termService.getTermSets(any())).thenReturn(new HashMap<>());
        when(databaseService.getDatabase(any())).thenReturn(databaseResp);
        when(statUtils.getStatInfo(any())).thenReturn(Collections.emptyList());
        doNothing().when(metricService).batchFillMetricDefaultAgg(any(), any());

        DataSetFilterReq filterReq = new DataSetFilterReq(dataSetId);
        List<DataSetSchemaResp> schemaResps = schemaService.buildDataSetSchema(filterReq);

        Assert.assertEquals(schemaResps.size(), 1);
        DataSetSchemaResp schemaResp = schemaResps.get(0);
        Assert.assertEquals(schemaResp.getMetrics().size(), 1);
        MetricSchemaResp metricSchemaResp = schemaResp.getMetrics().get(0);

        Assert.assertNotNull(metricSchemaResp.getRelateDimension());
        Assert.assertFalse(
                metricSchemaResp.getRelateDimension().getDrillDownDimensions().isEmpty());
        DrillDownDimension drillDownDimension =
                metricSchemaResp.getRelateDimension().getDrillDownDimensions().get(0);
        Assert.assertEquals(drillDownDimension.getDimensionId(), drillDownDimensionId);
        Assert.assertTrue(drillDownDimension.isInheritedFromModel());
    }

    @Test
    void buildDataSetSchema_shouldExposeRelatedSchemaElementsForDrillDownRecommendation() {
        // mocks
        ModelService modelService = Mockito.mock(ModelService.class);
        DimensionService dimensionService = Mockito.mock(DimensionService.class);
        MetricService metricService = Mockito.mock(MetricService.class);
        DomainService domainService = Mockito.mock(DomainService.class);
        DataSetService dataSetService = Mockito.mock(DataSetService.class);
        ModelRelaService modelRelaService = Mockito.mock(ModelRelaService.class);
        TermService termService = Mockito.mock(TermService.class);
        DatabaseService databaseService = Mockito.mock(DatabaseService.class);
        StatUtils statUtils = Mockito.mock(StatUtils.class);

        SchemaServiceImpl schemaService =
                new SchemaServiceImpl(modelService, dimensionService, metricService, domainService,
                        dataSetService, modelRelaService, statUtils, termService, databaseService);

        Long dataSetId = 1L;
        Long modelId = 200L;
        Long metricId = 100L;
        Long drillDownDimensionId = 10L;

        DataSetResp dataSetResp = new DataSetResp();
        dataSetResp.setId(dataSetId);
        dataSetResp.setName("ds");
        dataSetResp.setBizName("ds_biz");
        dataSetResp.setStatus(StatusEnum.ONLINE.getCode());
        dataSetResp.setDomainId(1L);
        DataSetDetail detail = new DataSetDetail();
        detail.setDataSetModelConfigs(Lists.newArrayList(new DataSetModelConfig(modelId,
                Lists.newArrayList(), Lists.newArrayList(metricId))));
        dataSetResp.setDataSetDetail(detail);

        MetricResp metricResp = new MetricResp();
        metricResp.setId(metricId);
        metricResp.setModelId(modelId);
        metricResp.setName("metric");
        metricResp.setBizName("metric_biz");
        metricResp.setStatus(StatusEnum.ONLINE.getCode());
        metricResp.setRelateDimension(null);

        ModelResp modelResp = new ModelResp();
        modelResp.setId(modelId);
        modelResp.setName("model");
        modelResp.setBizName("model_biz");
        modelResp.setDomainId(1L);
        modelResp.setDatabaseId(1L);
        modelResp.setDrillDownDimensions(
                Lists.newArrayList(new DrillDownDimension(drillDownDimensionId)));

        DatabaseResp databaseResp = new DatabaseResp();
        databaseResp.setId(1L);
        databaseResp.setType("mysql");
        databaseResp.setVersion("8");

        // stubs
        when(dataSetService.getDataSetList(any())).thenReturn(Lists.newArrayList(dataSetResp));
        when(metricService.getMetrics(any())).thenReturn(Lists.newArrayList(metricResp));
        when(dimensionService.getDimensions(any())).thenReturn(Collections.emptyList());
        when(modelService.getModelList(any())).thenReturn(Lists.newArrayList(modelResp));
        when(termService.getTermSets(any())).thenReturn(new HashMap<>());
        when(databaseService.getDatabase(any())).thenReturn(databaseResp);
        when(statUtils.getStatInfo(any())).thenReturn(Collections.emptyList());
        doNothing().when(metricService).batchFillMetricDefaultAgg(any(), any());

        DataSetFilterReq filterReq = new DataSetFilterReq(dataSetId);
        DataSetSchemaResp schemaResp = schemaService.buildDataSetSchema(filterReq).get(0);

        com.tencent.supersonic.headless.api.pojo.DataSetSchema dataSetSchema =
                DataSetSchemaBuilder.build(schemaResp);
        SchemaElement metric = dataSetSchema.getElement(SchemaElementType.METRIC, metricId);
        Assert.assertNotNull(metric);
        Assert.assertNotNull(metric.getRelatedSchemaElements());
        Assert.assertFalse(metric.getRelatedSchemaElements().isEmpty());
        Assert.assertEquals(metric.getRelatedSchemaElements().get(0).getDimensionId(),
                drillDownDimensionId);
    }
}
