package com.tencent.supersonic.headless.chat.parser.llm;

import com.tencent.supersonic.common.pojo.Parameter;
import com.tencent.supersonic.common.pojo.Text2SQLExemplar;
import com.tencent.supersonic.common.service.ExemplarService;
import com.tencent.supersonic.headless.chat.parser.ParserConfig;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

class PromptHelperTest {

    @Test
    void getFewShotExemplarsShouldReturnEmptyListsWhenFewShotIsZero() throws Exception {
        PromptHelper promptHelper = new PromptHelper();
        setField(promptHelper, "parserConfig", new StubParserConfig());
        setField(promptHelper, "exemplarService", new StubExemplarService());

        LLMReq llmReq = new LLMReq();
        llmReq.setQueryText("各品牌收入比例");
        llmReq.setDynamicExemplars(Collections.emptyList());

        List<List<Text2SQLExemplar>> results = promptHelper.getFewShotExemplars(llmReq);

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.stream().allMatch(List::isEmpty));
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = PromptHelper.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class StubParserConfig extends ParserConfig {

        @Override
        public String getParameterValue(Parameter parameter) {
            if (ParserConfig.PARSER_EXEMPLAR_RECALL_NUMBER.getName().equals(parameter.getName())) {
                return "0";
            }
            if (ParserConfig.PARSER_FEW_SHOT_NUMBER.getName().equals(parameter.getName())) {
                return "0";
            }
            if (ParserConfig.PARSER_SELF_CONSISTENCY_NUMBER.getName()
                    .equals(parameter.getName())) {
                return "2";
            }
            return parameter.getDefaultValue();
        }
    }

    private static class StubExemplarService implements ExemplarService {

        @Override
        public void storeExemplar(String collection, Text2SQLExemplar exemplar) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeExemplar(String collection, Text2SQLExemplar exemplar) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Text2SQLExemplar> recallExemplars(String collection, String query, int num) {
            return Collections.emptyList();
        }

        @Override
        public List<Text2SQLExemplar> recallExemplars(String query, int num) {
            return Collections.emptyList();
        }

        @Override
        public void loadSysExemplars() {
            throw new UnsupportedOperationException();
        }
    }
}
