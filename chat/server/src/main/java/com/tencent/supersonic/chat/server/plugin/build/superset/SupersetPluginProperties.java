package com.tencent.supersonic.chat.server.plugin.build.superset;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "s2.superset")
public class SupersetPluginProperties {

    private boolean enabled = true;

    private String baseUrl;

    private boolean authEnabled = false;

    private String jwtUsername;

    private String jwtPassword;

    private String jwtProvider = "db";

    private Integer timeoutSeconds = 30;

    private String datasourceType = "table";

    private String vizType;

    private boolean vizTypeLlmEnabled = true;

    private Integer vizTypeLlmTopN = 3;

    private List<String> vizTypeAllowList;

    private List<String> vizTypeDenyList;

    private Map<String, Object> formData;

    private Integer height;

    private Long templateChartId;

    private Map<String, Long> templateChartIds;

    private String guestTokenUserUsername = "supersonic-guest";

    private String guestTokenUserFirstName = "Supersonic";

    private String guestTokenUserLastName = "Guest";
}
