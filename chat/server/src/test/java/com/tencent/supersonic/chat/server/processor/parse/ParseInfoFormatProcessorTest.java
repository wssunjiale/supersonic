package com.tencent.supersonic.chat.server.processor.parse;

import com.tencent.supersonic.chat.api.pojo.request.ChatParseReq;
import com.tencent.supersonic.chat.api.pojo.response.ChatParseResp;
import com.tencent.supersonic.chat.server.pojo.ParseContext;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.enums.QueryType;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.headless.api.pojo.DataSetSchema;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.request.DimensionValueReq;
import com.tencent.supersonic.headless.api.pojo.request.SemanticQueryReq;
import com.tencent.supersonic.headless.api.pojo.response.DimensionResp;
import com.tencent.supersonic.headless.api.pojo.response.ItemResp;
import com.tencent.supersonic.headless.api.pojo.response.MetricResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticQueryResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticTranslateResp;
import com.tencent.supersonic.headless.server.facade.service.SemanticLayerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.StaticApplicationContext;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

class ParseInfoFormatProcessorTest {

    private ApplicationContext originalContext;

    @AfterEach
    void tearDown() throws Exception {
        setContext(originalContext);
    }

    @Test
    void processShouldResolveFieldsFiltersAndDateByBizName() throws Exception {
        originalContext = currentContext();
        DataSetSchema dataSetSchema = buildDataSetSchema();
        StaticApplicationContext context = new StaticApplicationContext();
        context.getBeanFactory().registerSingleton("semanticLayerService",
                new StubSemanticLayerService(dataSetSchema));
        setContext(context);

        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.setDataSet(schemaElement(2L, 2L, "企业数据集", "CorporateData",
                SchemaElementType.DATASET));
        String sql = "(SELECT brand_name, ds FROM `企业数据集` WHERE ds >= '2024-01-01'"
                + " AND ds <= '2024-01-31' AND brand_name = 'OPPO' LIMIT 500 OFFSET 0)";
        parseInfo.getSqlInfo().setParsedS2SQL(sql);
        parseInfo.getSqlInfo().setCorrectedS2SQL(sql);

        ChatParseResp response = new ChatParseResp(1L);
        response.getSelectedParses().add(parseInfo);
        ParseContext parseContext = new ParseContext(ChatParseReq.builder().queryId(1L)
                .queryText("查 OPPO 品牌明细").chatId(1).agentId(1).user(User.getDefaultUser())
                .build(), response);

        ParseInfoFormatProcessor processor = new ParseInfoFormatProcessor();
        processor.process(parseContext);

        Assertions.assertEquals(QueryType.DETAIL, parseInfo.getQueryType());
        Assertions.assertEquals(Collections.singletonList("品牌名称"),
                parseInfo.getDimensions().stream().map(SchemaElement::getName).toList());
        Assertions.assertEquals("2024-01-01", parseInfo.getDateInfo().getStartDate());
        Assertions.assertEquals("2024-01-31", parseInfo.getDateInfo().getEndDate());
        Assertions.assertEquals(1, parseInfo.getDimensionFilters().size());
        Assertions.assertEquals("品牌名称",
                parseInfo.getDimensionFilters().iterator().next().getName());
    }

    private static DataSetSchema buildDataSetSchema() {
        DataSetSchema dataSetSchema = new DataSetSchema();
        dataSetSchema.setDataSet(schemaElement(2L, 2L, "企业数据集", "CorporateData",
                SchemaElementType.DATASET));
        dataSetSchema.getDimensions().add(
                schemaElement(2L, 10L, "品牌名称", "brand_name", SchemaElementType.DIMENSION));
        SchemaElement partitionDimension =
                schemaElement(2L, 11L, "日期", "ds", SchemaElementType.DIMENSION);
        partitionDimension.getExtInfo().put("dimension_type", "partition_time");
        dataSetSchema.getDimensions().add(partitionDimension);
        return dataSetSchema;
    }

    private static SchemaElement schemaElement(Long dataSetId, Long id, String name,
            String bizName, SchemaElementType type) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setDataSetId(dataSetId);
        schemaElement.setId(id);
        schemaElement.setName(name);
        schemaElement.setBizName(bizName);
        schemaElement.setType(type);
        return schemaElement;
    }

    private static ApplicationContext currentContext() throws Exception {
        Field field = ContextUtils.class.getDeclaredField("context");
        field.setAccessible(true);
        return (ApplicationContext) field.get(null);
    }

    private static void setContext(ApplicationContext context) throws Exception {
        Field field = ContextUtils.class.getDeclaredField("context");
        field.setAccessible(true);
        field.set(null, context);
    }

    private static class StubSemanticLayerService implements SemanticLayerService {

        private final DataSetSchema dataSetSchema;

        private StubSemanticLayerService(DataSetSchema dataSetSchema) {
            this.dataSetSchema = dataSetSchema;
        }

        @Override
        public SemanticTranslateResp translate(SemanticQueryReq queryReq, User user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SemanticQueryResp queryByReq(SemanticQueryReq queryReq, User user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SemanticQueryResp queryDimensionValue(DimensionValueReq dimensionValueReq,
                User user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataSetSchema getDataSetSchema(Long id) {
            return dataSetSchema;
        }

        @Override
        public List<ItemResp> getDomainDataSetTree(User user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DimensionResp> getDimensions(
                com.tencent.supersonic.headless.api.pojo.MetaFilter metaFilter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<MetricResp> getMetrics(
                com.tencent.supersonic.headless.api.pojo.MetaFilter metaFilter) {
            throw new UnsupportedOperationException();
        }
    }
}
