package com.tencent.supersonic.chat.server.persistence.repository.impl;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.tencent.supersonic.chat.server.persistence.dataobject.ChatSessionIdDO;
import com.tencent.supersonic.chat.server.persistence.mapper.ChatSessionIdMapper;
import com.tencent.supersonic.chat.server.persistence.repository.ChatSessionIdRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

@Repository
@Primary
@Slf4j
public class ChatSessionIdRepositoryImpl implements ChatSessionIdRepository {

    private final ChatSessionIdMapper chatSessionIdMapper;

    public ChatSessionIdRepositoryImpl(ChatSessionIdMapper chatSessionIdMapper) {
        this.chatSessionIdMapper = chatSessionIdMapper;
    }

    @Override
    public ChatSessionIdDO getByChatId(Long chatId) {
        if (chatId == null) {
            return null;
        }
        return chatSessionIdMapper.getByChatId(chatId);
    }

    @Override
    public void createIfAbsent(Long chatId, Long agentId) {
        if (chatId == null) {
            return;
        }
        if (getByChatId(chatId) != null) {
            return;
        }
        ChatSessionIdDO chatSessionIdDO = new ChatSessionIdDO();
        chatSessionIdDO.setChatId(chatId);
        chatSessionIdDO.setAgentId(agentId);
        chatSessionIdDO.setConversationId(null);
        chatSessionIdMapper.insert(chatSessionIdDO);
    }


    @Override
    public void createWithConversationId(Long chatId, Long agentId, String conversationId) {
        if (chatId == null || StringUtils.isBlank(conversationId)) {
            return;
        }
        if (getByChatId(chatId) != null) {
            return;
        }
        ChatSessionIdDO chatSessionIdDO = new ChatSessionIdDO();
        chatSessionIdDO.setChatId(chatId);
        chatSessionIdDO.setAgentId(agentId);
        chatSessionIdDO.setConversationId(conversationId);
        chatSessionIdMapper.insert(chatSessionIdDO);
    }

    @Override
    public void updateConversationId(Long chatId, String conversationId) {
        if (chatId == null || StringUtils.isBlank(conversationId)) {
            return;
        }
        UpdateWrapper<ChatSessionIdDO> updateWrapper = new UpdateWrapper<>();
        updateWrapper.lambda().eq(ChatSessionIdDO::getChatId, chatId)
                .set(ChatSessionIdDO::getConversationId, conversationId);
        chatSessionIdMapper.update(null, updateWrapper);
    }
}
