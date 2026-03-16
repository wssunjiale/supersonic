package com.tencent.supersonic.chat.server.plugin.build.superset;

import lombok.Data;

@Data
public class SupersetChartCandidate {

    private String vizType;

    private String vizName;

    private Long dashboardId;

    private String dashboardTitle;

    private Long chartId;

    private String chartUuid;

    private Integer dashboardHeight;

    private String guestToken;

    private String embeddedId;

    private String supersetDomain;
}
