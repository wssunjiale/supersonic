package com.tencent.supersonic.chat.server.service.impl;

import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.enums.AppModule;
import com.tencent.supersonic.common.util.ChatAppManager;
import com.tencent.supersonic.common.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class AgentServiceImplTest {

    @Test
    void mergeChatAppConfigsShouldFallbackToDefaultPrompt() {
        AgentServiceImpl service = new AgentServiceImpl();
        ChatAppManager.register("S2SQL_PARSER",
                ChatApp.builder().name("语义SQL解析").description("通过大模型生成语义SQL")
                        .prompt("DEFAULT_PROMPT").enable(true).chatModelId(1)
                        .appModule(AppModule.CHAT).build());

        Map<String, ChatApp> merged = service.mergeChatAppConfigs(Map.of("S2SQL_PARSER",
                ChatApp.builder().name("语义SQL解析").description("通过大模型生成语义SQL")
                        .prompt("").enable(true).chatModelId(2).build()));

        Assertions.assertEquals(1, merged.size());
        Assertions.assertEquals("DEFAULT_PROMPT", merged.get("S2SQL_PARSER").getPrompt());
        Assertions.assertEquals(2, merged.get("S2SQL_PARSER").getChatModelId());
    }

    @Test
    void normalizeChatAppConfigsShouldDropBlankPromptBeforePersist() {
        AgentServiceImpl service = new AgentServiceImpl();

        Map<String, ChatApp> normalized = service.normalizeChatAppConfigs(Map.of("S2SQL_PARSER",
                ChatApp.builder().name("语义SQL解析").description("通过大模型生成语义SQL")
                        .prompt("").enable(true).chatModelId(2).build()));

        Assertions.assertNull(normalized.get("S2SQL_PARSER").getPrompt());
        Assertions.assertFalse(JsonUtil.toString(normalized).contains("\"prompt\""));
    }
}
