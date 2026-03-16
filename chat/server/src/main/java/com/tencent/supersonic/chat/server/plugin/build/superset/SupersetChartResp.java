package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.tencent.supersonic.chat.server.plugin.build.WebBase;
import lombok.Data;

import java.util.List;

@Data
public class SupersetChartResp {

    private Long pluginId;

    private String pluginType;

    private String name;

    private WebBase webPage;

    private boolean fallback;

    private String fallbackReason;

    private String vizType;

    private List<SupersetChartCandidate> vizTypeCandidates;

    private Long dashboardId;

    private String dashboardTitle;

    private String guestToken;

    private String embeddedId;

    private String supersetDomain;
}
