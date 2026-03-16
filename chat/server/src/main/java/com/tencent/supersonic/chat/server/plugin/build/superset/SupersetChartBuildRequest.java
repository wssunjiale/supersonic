package com.tencent.supersonic.chat.server.plugin.build.superset;

import lombok.Data;

import java.util.Map;

@Data
public class SupersetChartBuildRequest {

    private String vizType;

    private String vizName;

    private String chartName;

    private Integer dashboardHeight;

    private Map<String, Object> formData;
}
