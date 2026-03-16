package com.tencent.supersonic.chat.server.plugin.build.superset;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

@Data
public class SupersetPluginConfig {

    private boolean enabled = true;

    private boolean authEnabled = true;

    private String baseUrl;

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

    private Integer vizTypeLlmChatModelId;

    private String vizTypeLlmPrompt;

    private Map<String, Object> formData;

    private Integer height;

    private Long templateChartId;

    private Map<String, Long> templateChartIds;

    private String guestTokenUserUsername = "supersonic-guest";

    private String guestTokenUserFirstName = "Supersonic";

    private String guestTokenUserLastName = "Guest";

    /**
     * 校验 Superset 认证配置是否可用。
     *
     * Returns: 认证关闭时返回 true，认证开启时需提供 JWT 用户名和密码。
     */
    public boolean hasValidAuthConfig() {
        if (!authEnabled) {
            return true;
        }
        boolean hasJwt = StringUtils.isNotBlank(jwtUsername) && StringUtils.isNotBlank(jwtPassword);
        return hasJwt;
    }
}
