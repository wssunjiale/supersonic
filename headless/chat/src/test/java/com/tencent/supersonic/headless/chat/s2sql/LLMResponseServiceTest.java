package com.tencent.supersonic.headless.chat.s2sql;

import com.tencent.supersonic.common.pojo.Constants;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.request.QueryNLReq;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.parser.llm.LLMRequestService;
import com.tencent.supersonic.headless.chat.parser.llm.LLMResponseService;
import com.tencent.supersonic.headless.chat.parser.llm.ParseResult;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMReq;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMResp;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMSqlResp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class LLMResponseServiceTest {

    @Test
    void deduplicationSqlWeight() {
        String sql1 = "SELECT a,b,c,d FROM table1 WHERE column1 = 1 AND column2 = 2 order by a";
        String sql2 = "SELECT d,c,b,a FROM table1 WHERE column2 = 2 AND column1 = 1 order by a";

        LLMResp llmResp = new LLMResp();
        Map<String, LLMSqlResp> sqlWeight = new HashMap<>();
        sqlWeight.put(sql1, LLMSqlResp.builder().sqlWeight(0.20).build());
        sqlWeight.put(sql2, LLMSqlResp.builder().sqlWeight(0.80).build());

        llmResp.setSqlRespMap(sqlWeight);
        LLMResponseService llmResponseService = new LLMResponseService();
        Map<String, LLMSqlResp> deduplicationSqlResp =
                llmResponseService.getDeduplicationSqlResp(0, llmResp);

        Assert.assertEquals(deduplicationSqlResp.size(), 1);

        sql1 = "SELECT a,b,c,d FROM table1 WHERE column1 = 1 AND column2 = 2 order by a";
        sql2 = "SELECT d,c,b,a FROM table1 WHERE column2 = 2 AND column1 = 1 order by a";

        LLMResp llmResp2 = new LLMResp();
        Map<String, LLMSqlResp> sqlWeight2 = new HashMap<>();
        sqlWeight2.put(sql1, LLMSqlResp.builder().sqlWeight(0.20).build());
        sqlWeight2.put(sql2, LLMSqlResp.builder().sqlWeight(0.80).build());

        llmResp2.setSqlRespMap(sqlWeight2);
        deduplicationSqlResp = llmResponseService.getDeduplicationSqlResp(0, llmResp2);

        Assert.assertEquals(deduplicationSqlResp.size(), 1);

        sql1 = "SELECT a,b,c,d,e FROM table1 WHERE column1 = 1 AND column2 = 2 order by a";
        sql2 = "SELECT d,c,b,a FROM table1 WHERE column2 = 2 AND column1 = 1 order by a";

        LLMResp llmResp3 = new LLMResp();
        Map<String, LLMSqlResp> sqlWeight3 = new HashMap<>();
        sqlWeight3.put(sql1, LLMSqlResp.builder().sqlWeight(0.20).build());
        sqlWeight3.put(sql2, LLMSqlResp.builder().sqlWeight(0.80).build());
        llmResp3.setSqlRespMap(sqlWeight3);
        deduplicationSqlResp = llmResponseService.getDeduplicationSqlResp(0, llmResp3);

        Assert.assertEquals(deduplicationSqlResp.size(), 2);
    }

    @Test
    void addParseContextShouldAttachCompatibleParseResultForRuleParse() throws Exception {
        LLMResponseService llmResponseService = new LLMResponseService();
        setField(llmResponseService, "requestService", new StubLLMRequestService());

        QueryNLReq request = new QueryNLReq();
        request.setQueryText("品牌营收");
        ChatQueryContext queryCtx = new ChatQueryContext(request);

        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.setDataSet(dataset(7L, "企业数据集"));
        parseInfo.getProperties().put("existing", "value");

        llmResponseService.addParseContext(queryCtx, parseInfo);

        Object context = parseInfo.getProperties().get(Constants.CONTEXT);
        Assert.assertTrue(context instanceof ParseResult);
        ParseResult parseResult = (ParseResult) context;
        Assert.assertEquals(Long.valueOf(7L), parseResult.getDataSetId());
        Assert.assertNull(parseResult.getLlmResp());
        Assert.assertEquals("品牌营收", parseResult.getLlmReq().getQueryText());
        Assert.assertEquals("营收", parseResult.getLlmReq().getSchema().getMetrics().get(0).getName());
        Assert.assertEquals("品牌名称",
                parseResult.getLlmReq().getSchema().getDimensions().get(0).getName());
        Assert.assertEquals("品牌名称",
                parseResult.getLlmReq().getSchema().getValues().get(0).getFieldName());
        Assert.assertEquals("腾讯",
                parseResult.getLlmReq().getSchema().getValues().get(0).getFieldValue());
        Assert.assertEquals("internal", parseInfo.getProperties().get("type"));
        Assert.assertEquals("value", parseInfo.getProperties().get("existing"));
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = LLMResponseService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static SchemaElement dataset(Long id, String name) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setId(id);
        schemaElement.setDataSetId(id);
        schemaElement.setName(name);
        schemaElement.setType(SchemaElementType.DATASET);
        return schemaElement;
    }

    private static SchemaElement metric(String name) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setName(name);
        schemaElement.setType(SchemaElementType.METRIC);
        return schemaElement;
    }

    private static SchemaElement dimension(String name) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setName(name);
        schemaElement.setType(SchemaElementType.DIMENSION);
        return schemaElement;
    }

    private static class StubLLMRequestService extends LLMRequestService {

        @Override
        public LLMReq getLlmReq(ChatQueryContext queryCtx, Long dataSetId) {
            LLMReq llmReq = new LLMReq();
            llmReq.setQueryText(queryCtx.getRequest().getQueryText());

            LLMReq.LLMSchema schema = new LLMReq.LLMSchema();
            schema.setDataSetId(dataSetId);
            schema.setDataSetName("企业数据集");
            schema.setMetrics(List.of(metric("营收")));
            schema.setDimensions(List.of(dimension("品牌名称")));

            LLMReq.ElementValue elementValue = new LLMReq.ElementValue();
            elementValue.setFieldName("品牌名称");
            elementValue.setFieldValue("腾讯");
            schema.setValues(List.of(elementValue));
            llmReq.setSchema(schema);

            LLMReq.Term term = new LLMReq.Term();
            term.setName("GMV");
            term.setDescription("成交额");
            llmReq.setTerms(List.of(term));
            return llmReq;
        }
    }
}
