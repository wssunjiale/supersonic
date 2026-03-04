package com.tencent.supersonic.chat.server.persistence.dataobject;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("s2_chat_sessionid")
public class ChatSessionIdDO {

    @TableId(type = IdType.INPUT)
    private Long chatId;
    private Long agentId;
    private String conversationId;
}
