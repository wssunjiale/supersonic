package com.tencent.supersonic.chat.server.plugin.build.superset;

import lombok.Data;

import java.util.List;

@Data
public class SupersetDashboardInfo {

    private Long id;

    private String title;

    private String embeddedId;

    private List<String> tags;

    private Long ownerId;

    private String ownerName;

    private String supersetDomain;

    private String editUrl;
}
