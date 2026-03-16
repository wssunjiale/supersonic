package com.tencent.supersonic.chat.server.plugin.build.superset;

import lombok.Data;

import java.util.List;

@Data
public class SupersetEmbeddedDashboardInfo {

    private Long dashboardId;

    private String title;

    private String embeddedId;

    private String guestToken;

    private Long datasetId;

    private List<SupersetChartInfo> charts;
}
