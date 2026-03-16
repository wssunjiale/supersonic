package com.tencent.supersonic.headless.chat.parser.llm;

import com.google.common.collect.Sets;
import com.tencent.supersonic.common.pojo.Parameter;
import com.tencent.supersonic.headless.api.pojo.DataSetSchema;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementMatch;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticSchema;
import com.tencent.supersonic.headless.api.pojo.request.QueryNLReq;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.parser.ParserConfig;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

class LLMRequestServiceTest {

    @Test
    void getLlmReqShouldFallbackToFullDataSetMetricsWhenMappedMetricsMissing() throws Exception {
        LLMRequestService requestService = new LLMRequestService();
        setField(requestService, "parserConfig", new StubParserConfig());

        QueryNLReq request = new QueryNLReq();
        request.setQueryText("各品牌收入比例");
        request.setDataSetIds(Sets.newHashSet(2L));

        ChatQueryContext queryCtx = new ChatQueryContext(request);
        DataSetSchema dataSetSchema = buildDataSetSchema();
        queryCtx.setSemanticSchema(new SemanticSchema(List.of(dataSetSchema)));
        queryCtx.getMapInfo().setMatchedElements(2L, List.of(
                match(dataSetSchema.getDataSet()),
                match(dimension(2L, 14L, "品牌名称", "brand_name"))));

        LLMReq llmReq = requestService.getLlmReq(queryCtx, 2L);

        Assertions.assertEquals(List.of("利润", "营收"),
                llmReq.getSchema().getMetrics().stream().map(SchemaElement::getName).sorted()
                        .toList());
        Assertions.assertEquals(List.of("品牌名称"),
                llmReq.getSchema().getDimensions().stream().map(SchemaElement::getName).toList());
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = LLMRequestService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static SchemaElementMatch match(SchemaElement element) {
        return SchemaElementMatch.builder().element(element).detectWord("各品牌收入比例")
                .word(element.getName()).similarity(1.0).build();
    }

    private static DataSetSchema buildDataSetSchema() {
        DataSetSchema dataSetSchema = new DataSetSchema();
        dataSetSchema.setDataSet(dataset(2L, "企业数据集", "CorporateData"));
        dataSetSchema.getMetrics().add(metric(2L, 8L, "营收", "revenue"));
        dataSetSchema.getMetrics().add(metric(2L, 9L, "利润", "profit"));
        dataSetSchema.getDimensions().add(dimension(2L, 14L, "品牌名称", "brand_name"));
        dataSetSchema.getDimensions().add(dimension(2L, 19L, "财年", "year_time"));
        return dataSetSchema;
    }

    private static SchemaElement dataset(Long id, String name, String bizName) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setDataSetId(id);
        schemaElement.setId(id);
        schemaElement.setName(name);
        schemaElement.setBizName(bizName);
        schemaElement.setType(SchemaElementType.DATASET);
        return schemaElement;
    }

    private static SchemaElement metric(Long dataSetId, Long id, String name, String bizName) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setDataSetId(dataSetId);
        schemaElement.setId(id);
        schemaElement.setName(name);
        schemaElement.setBizName(bizName);
        schemaElement.setType(SchemaElementType.METRIC);
        return schemaElement;
    }

    private static SchemaElement dimension(Long dataSetId, Long id, String name, String bizName) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setDataSetId(dataSetId);
        schemaElement.setId(id);
        schemaElement.setName(name);
        schemaElement.setBizName(bizName);
        schemaElement.setType(SchemaElementType.DIMENSION);
        return schemaElement;
    }

    private static class StubParserConfig extends ParserConfig {

        @Override
        public String getParameterValue(Parameter parameter) {
            if (ParserConfig.PARSER_FIELDS_COUNT_THRESHOLD.getName().equals(parameter.getName())) {
                return "0";
            }
            if (ParserConfig.PARSER_LINKING_VALUE_ENABLE.getName().equals(parameter.getName())) {
                return "false";
            }
            return parameter.getDefaultValue();
        }
    }
}
