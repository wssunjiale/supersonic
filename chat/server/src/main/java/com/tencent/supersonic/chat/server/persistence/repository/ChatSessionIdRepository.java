package com.tencent.supersonic.chat.server.persistence.repository;

import com.tencent.supersonic.chat.server.persistence.dataobject.ChatSessionIdDO;

public interface ChatSessionIdRepository {

    ChatSessionIdDO getByChatId(Long chatId);

    void createIfAbsent(Long chatId, Long agentId);

    void createWithConversationId(Long chatId, Long agentId, String conversationId);

    void updateConversationId(Long chatId, String conversationId);
}
