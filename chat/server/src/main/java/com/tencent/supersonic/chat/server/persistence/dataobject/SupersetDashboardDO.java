package com.tencent.supersonic.chat.server.persistence.dataobject;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

@Data
@TableName("s2_superset_dashboard")
public class SupersetDashboardDO {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long pluginId;

    private Long dashboardId;

    private String title;

    private String embeddedId;

    private Long ownerId;

    private String ownerName;

    private Integer deleted;

    private Date createdAt;

    private String createdBy;

    private Date updatedAt;

    private String updatedBy;
}
