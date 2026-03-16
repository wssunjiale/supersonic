package com.tencent.supersonic.chat.server.rest;

import com.tencent.supersonic.auth.api.authentication.utils.UserHolder;
import com.tencent.supersonic.chat.api.pojo.request.SupersetDashboardCreateReq;
import com.tencent.supersonic.chat.api.pojo.request.SupersetDashboardDeleteReq;
import com.tencent.supersonic.chat.api.pojo.request.SupersetDashboardListReq;
import com.tencent.supersonic.chat.api.pojo.request.SupersetDashboardPushReq;
import com.tencent.supersonic.chat.api.pojo.request.SupersetGuestTokenReq;
import com.tencent.supersonic.chat.api.pojo.response.SupersetGuestTokenResp;
import com.tencent.supersonic.chat.server.plugin.ChatPlugin;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetApiClient;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetDashboardInfo;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetDashboardTagHelper;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetPluginConfig;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetPluginProperties;
import com.tencent.supersonic.chat.server.service.PluginService;
import com.tencent.supersonic.chat.server.service.SupersetDashboardRegistryService;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.exception.InvalidArgumentException;
import com.tencent.supersonic.common.util.JsonUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping({"/api/chat/superset", "/openapi/chat/superset"})
public class SupersetController {

    @Autowired
    private PluginService pluginService;

    @Autowired
    private SupersetPluginProperties supersetProperties;

    @Autowired
    private SupersetDashboardRegistryService supersetDashboardRegistryService;

    @PostMapping("dashboards")
    public List<SupersetDashboardInfo> dashboards(@RequestBody SupersetDashboardListReq req) {
        ChatPlugin plugin = resolveSupersetPlugin(req == null ? null : req.getPluginId());
        String accessToken = req == null ? null : req.getAccessToken();
        SupersetApiClient client =
                new SupersetApiClient(buildConfig(plugin, StringUtils.isNotBlank(accessToken)));
        if (StringUtils.isNotBlank(accessToken)) {
            return client.listDashboards(accessToken);
        }
        return client.listDashboards();
    }

    @PostMapping("dashboards/manage")
    public Map<String, Object> manualDashboards(
            @RequestBody(required = false) SupersetDashboardListReq req,
            HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {
        User user = UserHolder.findUser(httpServletRequest, httpServletResponse);
        ChatPlugin plugin = resolveSupersetPlugin(req == null ? null : req.getPluginId());
        SupersetPluginConfig config = buildConfig(plugin);
        List<SupersetDashboardInfo> filtered =
                supersetDashboardRegistryService.listManualDashboards(plugin.getId(), user);
        String supersetDomain = normalizeBaseUrl(config.getBaseUrl());
        for (SupersetDashboardInfo dashboard : filtered) {
            dashboard.setSupersetDomain(supersetDomain);
            dashboard.setEditUrl(buildDashboardEditUrl(supersetDomain, dashboard.getId()));
        }
        Map<String, Object> response = new HashMap<>();
        response.put("pluginId", plugin.getId());
        response.put("supersetDomain", supersetDomain);
        response.put("dashboards", filtered);
        return response;
    }

    @PostMapping("dashboard/create")
    public SupersetDashboardInfo createDashboard(@RequestBody SupersetDashboardCreateReq req,
            HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {
        if (req == null || StringUtils.isBlank(req.getTitle())) {
            throw new InvalidArgumentException("dashboard title required");
        }
        User user = UserHolder.findUser(httpServletRequest, httpServletResponse);
        ChatPlugin plugin = resolveSupersetPlugin(req.getPluginId());
        SupersetPluginConfig config = buildConfig(plugin);
        SupersetApiClient client = new SupersetApiClient(config);
        List<String> tags = SupersetDashboardTagHelper.buildManualTags(user);
        SupersetDashboardInfo info = client.createEmptyDashboard(req.getTitle(), tags);
        info = supersetDashboardRegistryService.registerManualDashboard(plugin.getId(), info, user);
        String supersetDomain = normalizeBaseUrl(config.getBaseUrl());
        info.setSupersetDomain(supersetDomain);
        info.setEditUrl(buildDashboardEditUrl(supersetDomain, info.getId()));
        return info;
    }

    @PostMapping("dashboard/delete")
    public boolean deleteDashboard(@RequestBody SupersetDashboardDeleteReq req,
            HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {
        if (req == null || req.getDashboardId() == null) {
            throw new InvalidArgumentException("dashboardId required");
        }
        User user = UserHolder.findUser(httpServletRequest, httpServletResponse);
        ChatPlugin plugin = resolveSupersetPlugin(req.getPluginId());
        SupersetDashboardInfo dashboard = supersetDashboardRegistryService
                .getManualDashboard(plugin.getId(), req.getDashboardId());
        if (dashboard == null) {
            throw new InvalidArgumentException("dashboard not found");
        }
        if (user == null || (!user.isSuperAdmin() && !isDashboardOwner(user, dashboard))) {
            throw new InvalidArgumentException("dashboard permission denied");
        }
        SupersetApiClient client = new SupersetApiClient(buildConfig(plugin));
        client.deleteDashboard(req.getDashboardId());
        supersetDashboardRegistryService.markDeleted(plugin.getId(), req.getDashboardId(), user);
        return true;
    }

    @PostMapping("dashboard/push")
    public boolean pushToDashboard(@RequestBody SupersetDashboardPushReq req) {
        if (req == null || req.getDashboardId() == null || req.getChartId() == null) {
            throw new InvalidArgumentException("dashboardId/chartId required");
        }
        ChatPlugin plugin = resolveSupersetPlugin(req.getPluginId());
        SupersetDashboardInfo dashboard = supersetDashboardRegistryService
                .getManualDashboard(plugin.getId(), req.getDashboardId());
        if (dashboard == null) {
            throw new InvalidArgumentException("dashboard not found");
        }
        SupersetApiClient client = new SupersetApiClient(buildConfig(plugin));
        client.appendChartToDashboard(req.getDashboardId(), req.getChartId());
        return true;
    }

    @PostMapping("guest-token")
    public SupersetGuestTokenResp createGuestToken(@RequestBody SupersetGuestTokenReq req) {
        if (req == null || StringUtils.isBlank(req.getEmbeddedId())) {
            throw new InvalidArgumentException("embeddedId required");
        }
        ChatPlugin plugin = resolveSupersetPlugin(req.getPluginId());
        SupersetApiClient client = new SupersetApiClient(buildConfig(plugin));
        SupersetGuestTokenResp response = new SupersetGuestTokenResp();
        response.setToken(client.createEmbeddedGuestToken(req.getEmbeddedId()));
        return response;
    }

    @RequestMapping(value = {"jwt-login", "jwt-login/"},
            method = {RequestMethod.POST, RequestMethod.GET})
    public Map<String, Object> loginWithConfig() {
        SupersetPluginConfig config = buildConfigFromProperties();
        if (config == null || !config.isEnabled() || StringUtils.isBlank(config.getBaseUrl())
                || StringUtils.isBlank(config.getJwtUsername())
                || StringUtils.isBlank(config.getJwtPassword())) {
            throw new InvalidArgumentException("superset config invalid");
        }
        SupersetApiClient client = new SupersetApiClient(config);
        String token = client.fetchAccessToken();
        Map<String, Object> response = new HashMap<>();
        response.put("baseUrl", config.getBaseUrl());
        response.put("accessToken", token);
        return response;
    }

    @PostMapping("databases")
    public Map<String, Object> listDatabases(
            @RequestBody(required = false) Map<String, Object> req) {
        int page = parseInt(req == null ? null : req.get("page"), 0);
        int pageSize = parseInt(req == null ? null : req.get("pageSize"), 500);
        String accessToken = resolveAccessToken(req);
        SupersetApiClient client = new SupersetApiClient(buildConfigFromPropertiesRequired());
        return client.listDatabases(accessToken, page, pageSize);
    }

    private ChatPlugin resolveSupersetPlugin(Long pluginId) {
        List<ChatPlugin> plugins = pluginService.getPluginList();
        if (plugins == null || plugins.isEmpty()) {
            throw new InvalidArgumentException("superset plugin missing");
        }
        Optional<ChatPlugin> candidate = plugins.stream()
                .filter(plugin -> "SUPERSET".equalsIgnoreCase(plugin.getType()))
                .filter(plugin -> pluginId == null || pluginId.equals(plugin.getId())).findFirst();
        if (!candidate.isPresent()) {
            throw new InvalidArgumentException("superset plugin not found");
        }
        return candidate.get();
    }

    private SupersetPluginConfig buildConfig(ChatPlugin plugin) {
        return buildConfig(plugin, false);
    }

    private SupersetPluginConfig buildConfig(ChatPlugin plugin, boolean allowAccessToken) {
        SupersetPluginConfig config =
                JsonUtil.toObject(plugin.getConfig(), SupersetPluginConfig.class);
        if (config == null || !config.isEnabled() || StringUtils.isBlank(config.getBaseUrl())) {
            throw new InvalidArgumentException("superset config invalid");
        }
        if (!allowAccessToken && !config.hasValidAuthConfig()) {
            throw new InvalidArgumentException("superset config invalid");
        }
        return config;
    }

    private SupersetPluginConfig buildConfigFromProperties() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setEnabled(supersetProperties.isEnabled());
        config.setBaseUrl(supersetProperties.getBaseUrl());
        config.setAuthEnabled(supersetProperties.isAuthEnabled());
        config.setJwtUsername(supersetProperties.getJwtUsername());
        config.setJwtPassword(supersetProperties.getJwtPassword());
        config.setJwtProvider(supersetProperties.getJwtProvider());
        config.setTimeoutSeconds(supersetProperties.getTimeoutSeconds());
        config.setDatasourceType(supersetProperties.getDatasourceType());
        config.setVizType(supersetProperties.getVizType());
        config.setVizTypeLlmEnabled(supersetProperties.isVizTypeLlmEnabled());
        config.setVizTypeLlmTopN(supersetProperties.getVizTypeLlmTopN());
        config.setVizTypeAllowList(supersetProperties.getVizTypeAllowList());
        config.setVizTypeDenyList(supersetProperties.getVizTypeDenyList());
        config.setFormData(supersetProperties.getFormData());
        config.setHeight(supersetProperties.getHeight());
        config.setTemplateChartId(supersetProperties.getTemplateChartId());
        config.setTemplateChartIds(supersetProperties.getTemplateChartIds());
        config.setGuestTokenUserUsername(supersetProperties.getGuestTokenUserUsername());
        config.setGuestTokenUserFirstName(supersetProperties.getGuestTokenUserFirstName());
        config.setGuestTokenUserLastName(supersetProperties.getGuestTokenUserLastName());
        return config;
    }

    private SupersetPluginConfig buildConfigFromPropertiesRequired() {
        SupersetPluginConfig config = buildConfigFromProperties();
        if (config == null || !config.isEnabled() || StringUtils.isBlank(config.getBaseUrl())) {
            throw new InvalidArgumentException("superset config invalid");
        }
        return config;
    }

    private int parseInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private String resolveAccessToken(Map<String, Object> req) {
        if (req == null) {
            throw new InvalidArgumentException("superset access token required");
        }
        Object value = req.get("accessToken");
        if (value == null) {
            value = req.get("token");
        }
        String accessToken = value == null ? null : String.valueOf(value);
        if (StringUtils.isBlank(accessToken)) {
            throw new InvalidArgumentException("superset access token required");
        }
        return accessToken;
    }

    private boolean isDashboardOwner(User user, SupersetDashboardInfo dashboard) {
        if (user == null || dashboard == null) {
            return false;
        }
        if (user.getId() != null && dashboard.getOwnerId() != null) {
            return user.getId().equals(dashboard.getOwnerId());
        }
        return StringUtils.isNotBlank(user.getName())
                && StringUtils.equalsIgnoreCase(user.getName(), dashboard.getOwnerName());
    }

    private String normalizeBaseUrl(String baseUrl) {
        if (StringUtils.isBlank(baseUrl)) {
            return null;
        }
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    private String buildDashboardEditUrl(String supersetDomain, Long dashboardId) {
        if (StringUtils.isBlank(supersetDomain) || dashboardId == null) {
            return null;
        }
        return String.format("%s/superset/dashboard/%s/?edit=true", supersetDomain, dashboardId);
    }
}
