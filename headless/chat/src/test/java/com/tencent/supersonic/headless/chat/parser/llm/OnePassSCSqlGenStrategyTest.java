package com.tencent.supersonic.headless.chat.parser.llm;

import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.ChatModelConfig;
import com.tencent.supersonic.common.util.ChatAppManager;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMReq;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMResp;
import dev.langchain4j.model.input.Prompt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

class OnePassSCSqlGenStrategyTest {

    @Test
    void resolveChatAppShouldFallbackToDefaultPromptWhenConfiguredPromptBlank()
            throws Exception {
        OnePassSCSqlGenStrategy strategy = new OnePassSCSqlGenStrategy();
        setPromptHelper(strategy, new StubPromptHelper());

        ChatModelConfig chatModelConfig = new ChatModelConfig();
        ChatApp configured = ChatApp.builder().enable(true).chatModelId(9).chatModelConfig(
                chatModelConfig).prompt("").name("自定义解析器").build();
        LLMReq llmReq = new LLMReq();
        llmReq.setQueryText("过去30天访问次数最高的部门top3");
        llmReq.setDynamicExemplars(Collections.emptyList());
        llmReq.setChatAppConfig(Map.of(OnePassSCSqlGenStrategy.APP_KEY, configured));

        ChatApp resolved = strategy.resolveChatApp(llmReq);

        Assertions.assertEquals("自定义解析器", resolved.getName());
        Assertions.assertEquals(9, resolved.getChatModelId());
        Assertions.assertSame(chatModelConfig, resolved.getChatModelConfig());
        Assertions.assertEquals(OnePassSCSqlGenStrategy.INSTRUCTION, resolved.getPrompt());

        Prompt prompt = invokeGeneratePrompt(strategy, llmReq, resolved);
        Assertions.assertTrue(prompt.text().contains("Question:过去30天访问次数最高的部门top3"));
        Assertions.assertTrue(prompt.text().contains("Schema:mock_schema"));
    }

    @Test
    void resolvePromptTemplateShouldFallbackToRegisteredDefault() {
        OnePassSCSqlGenStrategy strategy = new OnePassSCSqlGenStrategy();
        ChatAppManager.register(OnePassSCSqlGenStrategy.APP_KEY,
                ChatApp.builder().prompt("DEFAULT_PROMPT").enable(true).build());

        String promptTemplate = strategy.resolvePromptTemplate(ChatApp.builder().prompt("").build());

        Assertions.assertEquals("DEFAULT_PROMPT", promptTemplate);
    }

    private static Prompt invokeGeneratePrompt(OnePassSCSqlGenStrategy strategy, LLMReq llmReq,
            ChatApp chatApp) throws Exception {
        Method method = OnePassSCSqlGenStrategy.class.getDeclaredMethod("generatePrompt",
                LLMReq.class, LLMResp.class, ChatApp.class);
        method.setAccessible(true);
        return (Prompt) method.invoke(strategy, llmReq, new LLMResp(), chatApp);
    }

    private static void setPromptHelper(OnePassSCSqlGenStrategy strategy, PromptHelper helper)
            throws Exception {
        Field field = SqlGenStrategy.class.getDeclaredField("promptHelper");
        field.setAccessible(true);
        field.set(strategy, helper);
    }

    private static class StubPromptHelper extends PromptHelper {

        @Override
        public String buildSchemaStr(LLMReq llmReq) {
            return "mock_schema";
        }

        @Override
        public String buildSideInformation(LLMReq llmReq) {
            return "mock_side_info";
        }
    }
}
