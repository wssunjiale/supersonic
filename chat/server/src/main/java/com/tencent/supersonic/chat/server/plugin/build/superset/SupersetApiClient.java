package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.tencent.supersonic.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class SupersetApiClient {

    private static final String CHART_API = "/api/v1/chart/";
    private static final String GUEST_TOKEN_API = "/api/v1/security/guest_token/";
    private static final String DASHBOARD_API = "/api/v1/dashboard/";
    private static final String EMBEDDED_UI_CONFIG = "3";
    private static final String TAG_API = "/api/v1/tag/";
    private static final String LOGIN_API = "/api/v1/security/login";
    private static final String REFRESH_API = "/api/v1/security/refresh";
    private static final String CSRF_API = "/api/v1/security/csrf_token/";
    private static final int TAG_OBJECT_DASHBOARD = 3;

    private static final String AUTH_STRATEGY_JWT_FIRST = "JWT_FIRST";
    private static final String AUTH_STRATEGY_API_KEY_FIRST = "API_KEY_FIRST";

    private final SupersetPluginConfig config;
    private final RestTemplate restTemplate;
    private final String baseUrl;
    private JwtSession jwtSession;

    private static class JwtSession {
        private String accessToken;
        private String refreshToken;
        private String csrfToken;
        private String cookie;
    }

    public SupersetApiClient(SupersetPluginConfig config) {
        this.config = config;
        int timeoutMs =
                Math.max(1, config.getTimeoutSeconds() == null ? 30 : config.getTimeoutSeconds())
                        * 1000;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        this.restTemplate = new RestTemplate(factory);
        this.baseUrl = normalizeBaseUrl(config.getBaseUrl());
    }

    public SupersetChartInfo createEmbeddedChart(String sql, String chartName, String vizType,
            Map<String, Object> formData, Long datasetId, Long databaseId, String schema,
            List<String> dashboardTags) {
        log.debug(
                "superset createEmbeddedChart start, datasetId={}, databaseId={}, schema={}, vizType={}, chartName={}",
                datasetId, databaseId, schema, vizType, chartName);
        if (datasetId == null) {
            throw new IllegalStateException("superset datasetId is required");
        }
        Long chartId = createChart(datasetId, vizType, formData, chartName);
        String chartUuid = fetchChartUuid(chartId);
        Long dashboardId = createDashboard(chartName);
        addChartToDashboard(dashboardId, chartId);
        ensureDashboardChartLinked(dashboardId, chartId);
        updateChartParams(chartId, dashboardId, formData, vizType, datasetId);
        addTagsToDashboard(dashboardId, dashboardTags);
        String embeddedUuid = ensureEmbeddedDashboardUuid(dashboardId);
        String guestToken = createGuestToken("dashboard", embeddedUuid);
        log.debug(
                "superset embedded dashboard created, chartId={}, chartUuid={}, dashboardId={}, embeddedId={}, guestToken={}",
                chartId, chartUuid, dashboardId, embeddedUuid, StringUtils.isNotBlank(guestToken));

        SupersetChartInfo chartInfo = new SupersetChartInfo();
        chartInfo.setDatasetId(datasetId);
        chartInfo.setChartId(chartId);
        chartInfo.setChartUuid(chartUuid);
        chartInfo.setGuestToken(guestToken);
        chartInfo.setEmbeddedId(embeddedUuid);
        return chartInfo;
    }

    public List<SupersetDashboardInfo> listDashboards() {
        Map<String, Object> response = get(DASHBOARD_API + "?q=(page:0,page_size:200)");
        List<SupersetDashboardInfo> dashboards = extractDashboards(response);
        for (SupersetDashboardInfo dashboard : dashboards) {
            if (dashboard == null || dashboard.getId() == null) {
                continue;
            }
            try {
                String embeddedId = fetchEmbeddedDashboardUuid(dashboard.getId());
                if (StringUtils.isNotBlank(embeddedId)) {
                    dashboard.setEmbeddedId(embeddedId);
                }
            } catch (Exception ex) {
                log.warn("superset embedded uuid fetch failed, dashboardId={}", dashboard.getId(),
                        ex);
            }
        }
        return dashboards;
    }

    public Map<String, Object> listDatabases(String accessToken, int page, int pageSize) {
        int safePage = Math.max(0, page);
        int safeSize = Math.max(1, pageSize);
        String query = String.format("(page:%d,page_size:%d)", safePage, safeSize);
        return getWithAccessToken("/api/v1/database/?q=" + query, accessToken);
    }

    public String fetchAccessToken() {
        return ensureAccessToken();
    }

    public String createEmbeddedGuestToken(String embeddedUuid) {
        String dashboardId = resolveEmbeddedDashboardId(embeddedUuid);
        if (StringUtils.isNotBlank(dashboardId)) {
            return createGuestToken("dashboard", dashboardId);
        }
        return createGuestToken("dashboard", embeddedUuid);
    }

    public void addChartToDashboard(Long dashboardId, Long chartId) {
        if (dashboardId == null || chartId == null) {
            throw new IllegalStateException("superset dashboardId or chartId missing");
        }
        Map<String, Object> payload = new HashMap<>();
        payload.put("dashboards", Collections.singletonList(dashboardId));
        put(CHART_API + chartId, payload);
    }

    private void updateChartParams(Long chartId, Long dashboardId, Map<String, Object> formData,
            String vizType, Long datasetId) {
        if (chartId == null) {
            return;
        }
        Map<String, Object> merged = resolveChartParams(chartId);
        if (merged == null) {
            merged = new HashMap<>();
        }
        if (formData != null && !formData.isEmpty()) {
            merged.putAll(formData);
        }
        if (dashboardId != null) {
            merged.put("dashboardId", dashboardId);
        }
        if (StringUtils.isNotBlank(vizType)) {
            merged.put("viz_type", vizType);
        }
        if (datasetId != null) {
            merged.put("datasource", datasetId + "__" + getDatasourceType());
        }
        merged.put("slice_id", chartId);
        merged.put("chart_id", chartId);
        merged.put("force", false);
        merged.put("result_format", "json");
        merged.put("result_type", "full");
        Map<String, Object> urlParams =
                formData == null ? null : resolveMap(formData, "url_params");
        Map<String, Object> resolvedUrlParams =
                urlParams == null ? new HashMap<>() : new HashMap<>(urlParams);
        resolvedUrlParams.put("uiConfig", EMBEDDED_UI_CONFIG);
        resolvedUrlParams.put("show_filters", "false");
        resolvedUrlParams.put("expand_filters", "false");
        merged.put("url_params", resolvedUrlParams);
        Map<String, Object> payload = new HashMap<>();
        payload.put("params", JsonUtil.toString(merged));
        Map<String, Object> queryContext = buildQueryContext(merged, datasetId, vizType);
        if (queryContext != null) {
            payload.put("query_context", JsonUtil.toString(queryContext));
            payload.put("query_context_generation", true);
        }
        try {
            log.debug("superset chart params update payload, chartId={}, payload={}", chartId,
                    JsonUtil.toString(payload));
            put(CHART_API + chartId, payload);
        } catch (HttpStatusCodeException ex) {
            log.warn("superset chart params update failed, chartId={}", chartId, ex);
        }
    }

    private Map<String, Object> buildQueryContext(Map<String, Object> formData, Long datasetId,
            String vizType) {
        if (formData == null || datasetId == null) {
            return null;
        }
        if (StringUtils.isBlank(vizType) || !vizType.toLowerCase().contains("table")) {
            return null;
        }
        Map<String, Object> query = new HashMap<>();
        Object queryMode = formData.get("query_mode");
        String mode = queryMode == null ? "aggregate" : String.valueOf(queryMode);
        List<Object> metrics = toList(formData.get("metrics"));
        List<Object> groupby = toList(formData.get("groupby"));
        List<Object> columns = toList(formData.get("columns"));
        if ("raw".equalsIgnoreCase(mode)) {
            query.put("columns", columns);
        } else {
            query.put("metrics", metrics);
            if (!groupby.isEmpty()) {
                query.put("columns", groupby);
            }
            Object granularity = formData.get("granularity_sqla");
            if (granularity != null) {
                query.put("granularity", granularity);
            }
            if (!metrics.isEmpty()) {
                List<Object> orderby = new ArrayList<>();
                List<Object> order = new ArrayList<>();
                order.add(metrics.get(0));
                order.add(false);
                orderby.add(order);
                query.put("orderby", orderby);
            }
        }
        query.put("filters", Collections.emptyList());
        Map<String, Object> extras = new HashMap<>();
        extras.put("having", "");
        extras.put("where", "");
        query.put("extras", extras);
        query.put("applied_time_extras", Collections.emptyMap());
        query.put("annotation_layers", Collections.emptyList());
        query.put("row_limit", formData.getOrDefault("row_limit", 10000));
        query.put("series_limit", 0);
        query.put("group_others_when_limit_reached", false);
        query.put("order_desc", formData.getOrDefault("order_desc", true));
        query.put("url_params", formData.getOrDefault("url_params", Collections.emptyMap()));
        query.put("custom_params", Collections.emptyMap());
        query.put("custom_form_data", Collections.emptyMap());
        query.put("post_processing", Collections.emptyList());
        query.put("time_offsets", Collections.emptyList());

        Map<String, Object> datasource = new HashMap<>();
        datasource.put("id", datasetId);
        datasource.put("type", getDatasourceType());
        Map<String, Object> context = new HashMap<>();
        context.put("datasource", datasource);
        context.put("force", false);
        context.put("queries", Collections.singletonList(query));
        context.put("result_format", "json");
        context.put("result_type", "full");
        context.put("form_data", formData);
        return context;
    }

    private List<Object> toList(Object value) {
        if (value instanceof List) {
            return new ArrayList<>((List<Object>) value);
        }
        return Collections.emptyList();
    }

    private Map<String, Object> resolveChartParams(Long chartId) {
        try {
            Map<String, Object> response = get(CHART_API + chartId);
            Object params = resolveValue(response, "params");
            if (params == null) {
                Map<String, Object> result = resolveMap(response, "result");
                params = resolveValue(result, "params");
            }
            if (params instanceof Map) {
                return new HashMap<>((Map<String, Object>) params);
            }
            if (params instanceof String && StringUtils.isNotBlank((String) params)) {
                Map<String, Object> parsed =
                        JsonUtil.toObject((String) params, Map.class);
                return parsed == null ? null : new HashMap<>(parsed);
            }
        } catch (Exception ex) {
            log.debug("superset chart params fetch failed, chartId={}", chartId, ex);
        }
        return null;
    }

    private void ensureDashboardChartLinked(Long dashboardId, Long chartId) {
        if (dashboardId == null || chartId == null) {
            return;
        }
        int attempts = 3;
        for (int i = 0; i < attempts; i++) {
            if (isChartLinkedToDashboard(dashboardId, chartId)) {
                return;
            }
            try {
                Thread.sleep(200L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("superset dashboard chart link not ready, dashboardId={}, chartId={}",
                dashboardId, chartId);
    }

    private boolean isChartLinkedToDashboard(Long dashboardId, Long chartId) {
        try {
            Map<String, Object> response = get(DASHBOARD_API + dashboardId + "/charts");
            List<Map<String, Object>> result = resolveResultList(response);
            if (result == null) {
                return false;
            }
            for (Map<String, Object> item : result) {
                Long id = parseLong(resolveValue(item, "id"));
                if (Objects.equals(id, chartId)) {
                    return true;
                }
            }
        } catch (HttpStatusCodeException ex) {
            if (HttpStatus.NOT_FOUND.equals(ex.getStatusCode())) {
                log.debug("superset dashboard charts endpoint not found, skip check, dashboardId={}",
                        dashboardId);
                return true;
            }
            log.debug("superset dashboard chart fetch failed, dashboardId={}, chartId={}",
                    dashboardId, chartId, ex);
        } catch (Exception ex) {
            log.debug("superset dashboard chart fetch failed, dashboardId={}, chartId={}",
                    dashboardId, chartId, ex);
        }
        return false;
    }

    public void addTagsToDashboard(Long dashboardId, List<String> tags) {
        if (dashboardId == null || tags == null || tags.isEmpty()) {
            return;
        }
        Map<String, Object> payload = buildTagPayload(tags);
        log.debug("superset dashboard tag payload, dashboardId={}, payload={}", dashboardId,
                JsonUtil.toString(payload));
        try {
            post(TAG_API + TAG_OBJECT_DASHBOARD + "/" + dashboardId + "/", payload);
        } catch (HttpStatusCodeException ex) {
            log.warn("superset dashboard tag failed, dashboardId={}, tags={}", dashboardId, tags,
                    ex);
        }
    }

    /**
     * 构建 Superset tag 请求体。
     *
     * Args: tags: 标签名称列表。
     *
     * Returns: 包含 properties.tags 的请求体。
     */
    Map<String, Object> buildTagPayload(List<String> tags) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("tags", tags);
        Map<String, Object> payload = new HashMap<>();
        payload.put("properties", properties);
        return payload;
    }

    private Long createChart(Long datasetId, String vizType, Map<String, Object> formData,
            String chartName) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("slice_name", chartName);
        payload.put("viz_type", vizType);
        payload.put("datasource_id", datasetId);
        payload.put("datasource_type", getDatasourceType());
        Map<String, Object> mergedFormData =
                formData == null ? new HashMap<>() : new HashMap<>(formData);
        mergedFormData.putIfAbsent("viz_type", vizType);
        mergedFormData.putIfAbsent("datasource", datasetId + "__" + getDatasourceType());
        payload.put("params", JsonUtil.toString(mergedFormData));
        log.debug("superset chart create payload, chartName={}, datasetId={}, vizType={}, payload={}",
                chartName, datasetId, vizType, JsonUtil.toString(payload));
        Map<String, Object> response = post(CHART_API, payload);
        Long chartId = parseLong(resolveValue(response, "id"));
        if (chartId == null) {
            Map<String, Object> result = resolveMap(response, "result");
            chartId = parseLong(resolveValue(result, "id"));
        }
        if (chartId == null) {
            throw new IllegalStateException("superset chart create failed");
        }
        return chartId;
    }

    private String fetchChartUuid(Long chartId) {
        if (chartId == null) {
            throw new IllegalStateException("superset chartId missing");
        }
        Map<String, Object> response = get(CHART_API + chartId);
        String uuid = resolveString(response, "uuid");
        if (StringUtils.isBlank(uuid)) {
            Map<String, Object> result = resolveMap(response, "result");
            uuid = resolveString(result, "uuid");
        }
        if (StringUtils.isBlank(uuid)) {
            throw new IllegalStateException("superset chart uuid missing");
        }
        return uuid;
    }

    private String createGuestToken(String resourceType, String resourceId) {
        if (StringUtils.isBlank(resourceType) || StringUtils.isBlank(resourceId)) {
            throw new IllegalStateException("superset guest token resource missing");
        }
        Map<String, Object> resource = new HashMap<>();
        resource.put("type", resourceType);
        resource.put("id", resourceId);
        Map<String, Object> payload = buildGuestTokenPayload(resource);
        Map<String, Object> response = post(GUEST_TOKEN_API, payload);
        String token = resolveString(response, "token");
        if (StringUtils.isBlank(token)) {
            Map<String, Object> result = resolveMap(response, "result");
            token = resolveString(result, "token");
        }
        if (StringUtils.isBlank(token)) {
            throw new IllegalStateException("superset guest token missing");
        }
        return token;
    }

    Map<String, Object> buildGuestTokenPayload(Map<String, Object> resource) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("resources", new Object[] {resource});
        payload.put("rls", new Object[] {});
        payload.put("user", buildGuestUser());
        return payload;
    }

    private Map<String, Object> buildGuestUser() {
        Map<String, Object> user = new HashMap<>();
        String username =
                StringUtils.defaultIfBlank(config.getGuestTokenUserUsername(), "supersonic-guest");
        String firstName =
                StringUtils.defaultIfBlank(config.getGuestTokenUserFirstName(), "Supersonic");
        String lastName = StringUtils.defaultIfBlank(config.getGuestTokenUserLastName(), "Guest");
        user.put("username", username);
        user.put("first_name", firstName);
        user.put("last_name", lastName);
        return user;
    }


    private Long createDashboard(String dashboardTitle) {
        String title = StringUtils.defaultIfBlank(dashboardTitle, "supersonic_dashboard");
        Map<String, Object> payload = new HashMap<>();
        payload.put("dashboard_title", title);
        log.debug("superset dashboard create payload, title={}, payload={}", title,
                JsonUtil.toString(payload));
        Map<String, Object> response = post(DASHBOARD_API, payload);
        Long dashboardId = parseLong(resolveValue(response, "id"));
        if (dashboardId == null) {
            Map<String, Object> result = resolveMap(response, "result");
            dashboardId = parseLong(resolveValue(result, "id"));
        }
        if (dashboardId == null) {
            throw new IllegalStateException("superset dashboard create failed");
        }
        return dashboardId;
    }

    private String fetchDashboardUuid(Long dashboardId) {
        if (dashboardId == null) {
            throw new IllegalStateException("superset dashboardId missing");
        }
        Map<String, Object> response = get(DASHBOARD_API + dashboardId);
        String uuid = resolveString(response, "uuid");
        if (StringUtils.isBlank(uuid)) {
            Map<String, Object> result = resolveMap(response, "result");
            uuid = resolveString(result, "uuid");
        }
        if (StringUtils.isBlank(uuid)) {
            throw new IllegalStateException("superset dashboard uuid missing");
        }
        return uuid;
    }


    private String ensureEmbeddedDashboardUuid(Long dashboardId) {
        String embeddedUuid = fetchEmbeddedDashboardUuid(dashboardId);
        if (StringUtils.isNotBlank(embeddedUuid)) {
            return embeddedUuid;
        }
        return createEmbeddedDashboardConfig(dashboardId);
    }

    private String resolveEmbeddedDashboardId(String embeddedUuid) {
        if (StringUtils.isBlank(embeddedUuid)) {
            return null;
        }
        try {
            Map<String, Object> response = get("/api/v1/embedded_dashboard/" + embeddedUuid);
            String dashboardId = resolveString(response, "dashboard_id");
            if (StringUtils.isBlank(dashboardId)) {
                Map<String, Object> result = resolveMap(response, "result");
                dashboardId = resolveString(result, "dashboard_id");
            }
            return dashboardId;
        } catch (HttpStatusCodeException ex) {
            log.warn("superset embedded dashboard fetch failed, embeddedId={}", embeddedUuid, ex);
            return null;
        }
    }

    private String fetchEmbeddedDashboardUuid(Long dashboardId) {
        if (dashboardId == null) {
            throw new IllegalStateException("superset dashboardId missing");
        }
        try {
            Map<String, Object> response = get(DASHBOARD_API + dashboardId + "/embedded");
            return resolveEmbeddedDashboardUuid(response);
        } catch (HttpStatusCodeException ex) {
            if (HttpStatus.NOT_FOUND.equals(ex.getStatusCode())) {
                return null;
            }
            throw ex;
        }
    }

    private String createEmbeddedDashboardConfig(Long dashboardId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("allowed_domains", Collections.emptyList());
        log.debug("superset embedded dashboard payload, dashboardId={}, payload={}", dashboardId,
                JsonUtil.toString(payload));
        Map<String, Object> response = post(DASHBOARD_API + dashboardId + "/embedded", payload);
        String embeddedUuid = resolveEmbeddedDashboardUuid(response);
        if (StringUtils.isBlank(embeddedUuid)) {
            throw new IllegalStateException("superset embedded dashboard uuid missing");
        }
        return embeddedUuid;
    }

    private String resolveEmbeddedDashboardUuid(Map<String, Object> response) {
        String uuid = resolveString(response, "uuid");
        if (StringUtils.isBlank(uuid)) {
            Map<String, Object> result = resolveMap(response, "result");
            uuid = resolveString(result, "uuid");
        }
        return uuid;
    }

    private Map<String, Object> post(String path, Object body) {
        return request(HttpMethod.POST, path, body);
    }

    private Map<String, Object> put(String path, Object body) {
        return request(HttpMethod.PUT, path, body);
    }

    private Map<String, Object> get(String path) {
        return request(HttpMethod.GET, path, null);
    }

    private Map<String, Object> getWithAccessToken(String path, String accessToken) {
        if (StringUtils.isBlank(accessToken)) {
            throw new IllegalArgumentException("superset access token required");
        }
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + accessToken);
        return execute(HttpMethod.GET, path, null, headers);
    }

    HttpHeaders buildHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (config.isAuthEnabled() && StringUtils.isNotBlank(config.getApiKey())) {
            headers.set("Authorization", "Bearer " + config.getApiKey());
        }
        return headers;
    }

    /**
     * 执行带认证策略的 Superset 请求。
     *
     * Args: method: HTTP 方法。 path: API 路径。 body: 请求体（可为空）。
     *
     * Returns: 解析后的响应 Map。
     */
    private Map<String, Object> request(HttpMethod method, String path, Object body) {
        AuthHeaders authHeaders = resolveAuthHeaders(method);
        log.debug("superset request, method={}, path={}, authMode={}", method, path,
                authHeaders.mode);
        try {
            return execute(method, path, body, authHeaders.headers);
        } catch (HttpStatusCodeException ex) {
            if (authHeaders.mode == AuthMode.JWT && canFallback(ex)) {
                if (refreshJwt()) {
                    AuthHeaders refreshed = tryBuildJwtHeaders(method);
                    if (refreshed != null) {
                        return execute(method, path, body, refreshed.headers);
                    }
                }
                if (StringUtils.isNotBlank(config.getApiKey())) {
                    log.warn("superset jwt request failed, fallback to api key: {}",
                            ex.getStatusCode());
                    return execute(method, path, body, buildHeaders());
                }
            }
            throw ex;
        }
    }

    /**
     * 发送 HTTP 请求并解析结果。
     *
     * Args: method: HTTP 方法。 path: API 路径。 body: 请求体（可为空）。 headers: 请求头。
     *
     * Returns: 解析后的响应 Map。
     */
    private Map<String, Object> execute(HttpMethod method, String path, Object body,
            HttpHeaders headers) {
        String url = baseUrl + path;
        HttpEntity<String> entity = body == null ? new HttpEntity<>(headers)
                : new HttpEntity<>(JsonUtil.toString(body), headers);
        ResponseEntity<Object> response = restTemplate.exchange(url, method, entity, Object.class);
        log.debug("superset response, method={}, path={}, status={}", method, path,
                response.getStatusCode());
        return JsonUtil.objectToMap(response.getBody());
    }

    /**
     * 根据认证策略生成请求头。
     *
     * Args: method: HTTP 方法。
     *
     * Returns: 包含请求头与认证模式的结构。
     */
    private AuthHeaders resolveAuthHeaders(HttpMethod method) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (!config.isAuthEnabled()) {
            return new AuthHeaders(headers, AuthMode.NONE);
        }
        boolean jwtFirst = !AUTH_STRATEGY_API_KEY_FIRST.equalsIgnoreCase(config.getAuthStrategy());
        if (jwtFirst) {
            AuthHeaders jwtHeaders = tryBuildJwtHeaders(method);
            if (jwtHeaders != null) {
                return jwtHeaders;
            }
            AuthHeaders apiHeaders = tryBuildApiKeyHeaders(headers);
            if (apiHeaders != null) {
                return apiHeaders;
            }
        } else {
            AuthHeaders apiHeaders = tryBuildApiKeyHeaders(headers);
            if (apiHeaders != null) {
                return apiHeaders;
            }
            AuthHeaders jwtHeaders = tryBuildJwtHeaders(method);
            if (jwtHeaders != null) {
                return jwtHeaders;
            }
        }
        return new AuthHeaders(headers, AuthMode.NONE);
    }

    /**
     * 构建 API key 请求头。
     *
     * Args: headers: 需要补充的请求头。
     *
     * Returns: 构建成功返回 AuthHeaders，否则返回 null。
     */
    private AuthHeaders tryBuildApiKeyHeaders(HttpHeaders headers) {
        if (StringUtils.isBlank(config.getApiKey())) {
            return null;
        }
        headers.set("Authorization", "Bearer " + config.getApiKey());
        return new AuthHeaders(headers, AuthMode.API_KEY);
    }

    /**
     * 构建 JWT 请求头并确保 CSRF 准备完毕。
     *
     * Args: method: HTTP 方法。
     *
     * Returns: 构建成功返回 AuthHeaders，否则返回 null。
     */
    private AuthHeaders tryBuildJwtHeaders(HttpMethod method) {
        if (StringUtils.isBlank(config.getJwtUsername())
                || StringUtils.isBlank(config.getJwtPassword())) {
            return null;
        }
        try {
            String accessToken = ensureAccessToken();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Authorization", "Bearer " + accessToken);
            if (requiresCsrf(method)) {
                ensureCsrfToken(accessToken);
                headers.set("X-CSRFToken", jwtSession.csrfToken);
                if (StringUtils.isNotBlank(jwtSession.cookie)) {
                    headers.set("Cookie", jwtSession.cookie);
                }
            }
            return new AuthHeaders(headers, AuthMode.JWT);
        } catch (Exception ex) {
            log.warn("superset jwt auth failed, fallback to api key", ex);
            return null;
        }
    }

    /**
     * 确保 JWT access token 可用。
     *
     * Returns: access token。
     */
    private String ensureAccessToken() {
        if (jwtSession == null) {
            jwtSession = new JwtSession();
        }
        if (StringUtils.isNotBlank(jwtSession.accessToken)) {
            return jwtSession.accessToken;
        }
        loginJwt();
        return jwtSession.accessToken;
    }

    /**
     * 确保 CSRF token 与 session cookie 可用。
     *
     * Args: accessToken: JWT access token。
     */
    private void ensureCsrfToken(String accessToken) {
        if (jwtSession == null) {
            jwtSession = new JwtSession();
        }
        if (StringUtils.isNotBlank(jwtSession.csrfToken)
                && StringUtils.isNotBlank(jwtSession.cookie)) {
            return;
        }
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + accessToken);
        ResponseEntity<Object> response = restTemplate.exchange(baseUrl + CSRF_API, HttpMethod.GET,
                new HttpEntity<>(headers), Object.class);
        Map<String, Object> body = JsonUtil.objectToMap(response.getBody());
        jwtSession.csrfToken = resolveString(body, "result");
        jwtSession.cookie = extractCookie(response.getHeaders());
        if (StringUtils.isBlank(jwtSession.csrfToken) || StringUtils.isBlank(jwtSession.cookie)) {
            throw new IllegalStateException("superset csrf token missing");
        }
    }

    /**
     * 执行 JWT 登录并刷新会话状态。
     */
    private void loginJwt() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("username", config.getJwtUsername());
        payload.put("password", config.getJwtPassword());
        payload.put("provider", StringUtils.defaultIfBlank(config.getJwtProvider(), "db"));
        payload.put("refresh", true);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<Object> response =
                restTemplate.exchange(baseUrl + LOGIN_API, HttpMethod.POST,
                        new HttpEntity<>(JsonUtil.toString(payload), headers), Object.class);
        Map<String, Object> body = JsonUtil.objectToMap(response.getBody());
        String accessToken = resolveString(body, "access_token");
        String refreshToken = resolveString(body, "refresh_token");
        if (StringUtils.isBlank(accessToken) || StringUtils.isBlank(refreshToken)) {
            throw new IllegalStateException("superset jwt login failed");
        }
        jwtSession.accessToken = accessToken;
        jwtSession.refreshToken = refreshToken;
        jwtSession.csrfToken = null;
        jwtSession.cookie = null;
    }

    /**
     * 通过 refresh token 刷新 access token。
     *
     * Returns: 刷新成功返回 true。
     */
    private boolean refreshJwt() {
        if (jwtSession == null || StringUtils.isBlank(jwtSession.refreshToken)) {
            return false;
        }
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Authorization", "Bearer " + jwtSession.refreshToken);
            ResponseEntity<Object> response = restTemplate.exchange(baseUrl + REFRESH_API,
                    HttpMethod.POST, new HttpEntity<>(headers), Object.class);
            Map<String, Object> body = JsonUtil.objectToMap(response.getBody());
            String accessToken = resolveString(body, "access_token");
            if (StringUtils.isBlank(accessToken)) {
                return false;
            }
            jwtSession.accessToken = accessToken;
            jwtSession.csrfToken = null;
            jwtSession.cookie = null;
            return true;
        } catch (HttpStatusCodeException ex) {
            log.warn("superset jwt refresh failed: {}", ex.getStatusCode());
            return false;
        }
    }

    /**
     * 判断请求是否需要 CSRF token。
     *
     * Args: method: HTTP 方法。
     *
     * Returns: 非 GET 请求返回 true。
     */
    private boolean requiresCsrf(HttpMethod method) {
        return method != HttpMethod.GET;
    }

    /**
     * 判断是否允许回退到 API key。
     *
     * Args: ex: HTTP 状态异常。
     *
     * Returns: 401/403 返回 true。
     */
    private boolean canFallback(HttpStatusCodeException ex) {
        return ex.getStatusCode().value() == 401 || ex.getStatusCode().value() == 403;
    }

    /**
     * 从响应头中解析 Cookie。
     *
     * Args: headers: 响应头。
     *
     * Returns: 拼接后的 Cookie 字符串。
     */
    private String extractCookie(HttpHeaders headers) {
        List<String> setCookies = headers.get(HttpHeaders.SET_COOKIE);
        if (setCookies == null || setCookies.isEmpty()) {
            return null;
        }
        List<String> cookies = new ArrayList<>();
        for (String setCookie : setCookies) {
            if (StringUtils.isBlank(setCookie)) {
                continue;
            }
            String value = setCookie.split(";", 2)[0];
            if (StringUtils.isNotBlank(value)) {
                cookies.add(value);
            }
        }
        return cookies.isEmpty() ? null : String.join("; ", cookies);
    }

    private enum AuthMode {
        NONE, JWT, API_KEY
    }

    private static class AuthHeaders {
        private final HttpHeaders headers;
        private final AuthMode mode;

        private AuthHeaders(HttpHeaders headers, AuthMode mode) {
            this.headers = headers;
            this.mode = mode;
        }
    }

    private String getDatasourceType() {
        return StringUtils.isBlank(config.getDatasourceType()) ? "table"
                : config.getDatasourceType();
    }

    private String normalizeBaseUrl(String baseUrl) {
        if (StringUtils.isBlank(baseUrl)) {
            return "";
        }
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    private Object resolveValue(Map<String, Object> map, String key) {
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    private Map<String, Object> resolveMap(Map<String, Object> map, String key) {
        Object value = resolveValue(map, key);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    private List<Map<String, Object>> resolveList(Map<String, Object> map, String key) {
        Object value = resolveValue(map, key);
        if (value instanceof List) {
            return (List<Map<String, Object>>) value;
        }
        return null;
    }

    private List<Map<String, Object>> resolveResultList(Map<String, Object> response) {
        if (response == null) {
            return null;
        }
        List<Map<String, Object>> result = resolveList(response, "result");
        if (result == null) {
            Map<String, Object> wrapper = resolveMap(response, "result");
            if (wrapper != null) {
                result = resolveList(wrapper, "result");
            }
        }
        return result;
    }

    private String resolveString(Map<String, Object> map, String key) {
        Object value = resolveValue(map, key);
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    private Long parseLong(Object value) {
        if (Objects.isNull(value)) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException ex) {
            log.warn("superset id parse failed:{}", value);
            return null;
        }
    }

    List<SupersetDashboardInfo> extractDashboards(Map<String, Object> response) {
        if (response == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> result = resolveList(response, "result");
        if (result == null) {
            Map<String, Object> wrapper = resolveMap(response, "result");
            if (wrapper != null) {
                result = resolveList(wrapper, "result");
            }
        }
        if (result == null) {
            return Collections.emptyList();
        }
        List<SupersetDashboardInfo> dashboards = new ArrayList<>();
        for (Map<String, Object> item : result) {
            if (item == null) {
                continue;
            }
            SupersetDashboardInfo info = new SupersetDashboardInfo();
            info.setId(parseLong(resolveValue(item, "id")));
            String title = resolveString(item, "dashboard_title");
            if (StringUtils.isBlank(title)) {
                title = resolveString(item, "title");
            }
            info.setTitle(title);
            if (info.getId() != null || StringUtils.isNotBlank(info.getTitle())) {
                dashboards.add(info);
            }
        }
        return dashboards;
    }
}
