package com.tencent.supersonic.chat.server.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tencent.supersonic.chat.server.persistence.dataobject.ChatSessionIdDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ChatSessionIdMapper extends BaseMapper<ChatSessionIdDO> {

    ChatSessionIdDO getByChatId(Long chatId);
}
