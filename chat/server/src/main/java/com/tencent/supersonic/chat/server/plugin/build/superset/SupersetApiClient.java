package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SupersetApiClient {

    private static final String CHART_API = "/api/v1/chart/";
    private static final String GUEST_TOKEN_API = "/api/v1/security/guest_token/";
    private static final String DASHBOARD_API = "/api/v1/dashboard/";
    private static final int DEFAULT_SINGLE_CHART_HEIGHT = 260;
    private static final int DASHBOARD_GRID_UNIT = 8;
    private static final String DASHBOARD_ROOT_ID = "ROOT_ID";
    private static final String DASHBOARD_GRID_ID = "GRID_ID";
    private static final String DASHBOARD_ROW_ID = "ROW-1";
    private static final String DASHBOARD_ROW_PREFIX = "ROW-";
    private static final String DASHBOARD_CHART_PREFIX = "CHART-";
    private static final String SINGLE_CHART_DASHBOARD_CSS =
            "html, body, #app { height: 100%; overflow: hidden; } "
                    + ".dashboard-content, .dashboard-content > div, .grid-container { height: 100%; } "
                    + ".dashboard-content { padding: 0 !important; } "
                    + ".dashboard-component-chart-holder, .dashboard-component-chart-holder > div, "
                    + ".chart-container, .chart-container .chart, .slice_container { height: 100% !important; } "
                    + ".dashboard-component-chart-holder { margin: 0 !important; }";
    private static final String STACKED_DASHBOARD_CSS = "html, body, #app { min-height: 100%; } "
            + ".dashboard-content { padding: 0 !important; } "
            + ".dashboard-component-chart-holder { margin: 0 !important; }";
    private static final String EMBEDDED_UI_CONFIG = "3";
    private static final String TAG_API = "/api/v1/tag/";
    private static final String LOGIN_PAGE = "/login/";
    private static final String LOGIN_PAGE_NEXT =
            "/login/?next=%2Fsuperset%2Fwelcome%2F";
    private static final String WELCOME_PAGE = "/superset/welcome/";
    private static final String LOGIN_API = "/api/v1/security/login";
    private static final String REFRESH_API = "/api/v1/security/refresh";
    private static final String CSRF_API = "/api/v1/security/csrf_token/";
    private static final int TAG_OBJECT_DASHBOARD = 3;
    private static final Pattern HTML_CSRF_PATTERN = Pattern.compile(
            "name=[\"']csrf_token[\"'][^>]*value=[\"']([^\"']+)[\"']",
            Pattern.CASE_INSENSITIVE);

    private static volatile SupersetVizTypeSelector.VizTypeCatalog VIZTYPE_CATALOG;

    private final SupersetPluginConfig config;
    private final RestTemplate restTemplate;
    private final String baseUrl;
    private JwtSession jwtSession;
    private BrowserSession browserSession;

    private static class JwtSession {
        private String accessToken;
        private String refreshToken;
        private String csrfToken;
        private String cookie;
    }

    private static class BrowserSession {
        private String csrfToken;
        private String cookie;
    }

    private static class ChartTemplateSnapshot {
        private final Long chartId;
        private final String vizType;
        private final Map<String, Object> formData;
        private final Map<String, Object> queryContext;

        private ChartTemplateSnapshot(Long chartId, String vizType, Map<String, Object> formData,
                Map<String, Object> queryContext) {
            this.chartId = chartId;
            this.vizType = vizType;
            this.formData = formData;
            this.queryContext = queryContext;
        }
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
            List<String> dashboardTags, Integer dashboardHeight, String dashboardTitle) {
        log.debug(
                "superset createEmbeddedChart start, datasetId={}, databaseId={}, schema={}, vizType={}, chartName={}",
                datasetId, databaseId, schema, vizType, chartName);
        if (datasetId == null) {
            throw new IllegalStateException("superset datasetId is required");
        }
        String resolvedDashboardTitle = StringUtils.defaultIfBlank(dashboardTitle, chartName);
        Long dashboardId = createDashboard(resolvedDashboardTitle);
        SupersetChartInfo chartInfo =
                createChartForDashboard(dashboardId, datasetId, chartName, vizType, formData);
        updateDashboardLayout(dashboardId, chartInfo.getChartId(), dashboardHeight);
        addTagsToDashboard(dashboardId, dashboardTags);
        String embeddedUuid = ensureEmbeddedDashboardUuid(dashboardId);
        String guestToken = createGuestToken("dashboard", embeddedUuid);
        log.debug(
                "superset embedded dashboard created, chartId={}, chartUuid={}, dashboardId={}, embeddedId={}, guestToken={}",
                chartInfo.getChartId(), chartInfo.getChartUuid(), dashboardId, embeddedUuid,
                StringUtils.isNotBlank(guestToken));
        chartInfo.setDashboardId(dashboardId);
        chartInfo.setDashboardTitle(resolvedDashboardTitle);
        chartInfo.setGuestToken(guestToken);
        chartInfo.setEmbeddedId(embeddedUuid);
        return chartInfo;
    }

    public SupersetEmbeddedDashboardInfo createEmbeddedDashboard(String dashboardTitle,
            List<SupersetChartBuildRequest> chartRequests, Long datasetId, Long databaseId,
            String schema, List<String> dashboardTags) {
        log.debug(
                "superset createEmbeddedDashboard start, datasetId={}, databaseId={}, schema={}, chartCount={}",
                datasetId, databaseId, schema, chartRequests == null ? 0 : chartRequests.size());
        if (datasetId == null) {
            throw new IllegalStateException("superset datasetId is required");
        }
        if (chartRequests == null || chartRequests.isEmpty()) {
            throw new IllegalStateException("superset chart requests missing");
        }
        String resolvedTitle =
                StringUtils.defaultIfBlank(dashboardTitle, chartRequests.get(0).getChartName());
        Long dashboardId = createDashboard(resolvedTitle);
        List<SupersetChartInfo> charts = new ArrayList<>();
        List<Long> chartIds = new ArrayList<>();
        List<Integer> chartHeights = new ArrayList<>();
        for (SupersetChartBuildRequest request : chartRequests) {
            if (request == null || StringUtils.isBlank(request.getVizType())) {
                continue;
            }
            SupersetChartInfo chartInfo = createChartForDashboard(dashboardId, datasetId,
                    request.getChartName(), request.getVizType(), request.getFormData());
            charts.add(chartInfo);
            chartIds.add(chartInfo.getChartId());
            chartHeights.add(request.getDashboardHeight());
        }
        if (charts.isEmpty()) {
            throw new IllegalStateException("superset chart build failed");
        }
        updateDashboardLayout(dashboardId, chartIds, chartHeights);
        addTagsToDashboard(dashboardId, dashboardTags);
        String embeddedUuid = ensureEmbeddedDashboardUuid(dashboardId);
        String guestToken = createGuestToken("dashboard", embeddedUuid);
        SupersetEmbeddedDashboardInfo dashboardInfo = new SupersetEmbeddedDashboardInfo();
        dashboardInfo.setDashboardId(dashboardId);
        dashboardInfo.setTitle(resolvedTitle);
        dashboardInfo.setEmbeddedId(embeddedUuid);
        dashboardInfo.setGuestToken(guestToken);
        dashboardInfo.setDatasetId(datasetId);
        dashboardInfo.setCharts(charts);
        return dashboardInfo;
    }

    public List<SupersetDashboardInfo> listDashboards() {
        return listDashboardsInternal(null);
    }

    public List<SupersetDashboardInfo> listDashboards(String accessToken) {
        if (StringUtils.isBlank(accessToken)) {
            throw new IllegalArgumentException("superset access token required");
        }
        return listDashboardsInternal(accessToken);
    }

    public SupersetDashboardInfo createEmptyDashboard(String dashboardTitle, List<String> tags) {
        Long dashboardId = createDashboard(dashboardTitle, false);
        addTagsToDashboard(dashboardId, tags);
        String embeddedId = ensureEmbeddedDashboardUuid(dashboardId);
        SupersetDashboardInfo info = new SupersetDashboardInfo();
        info.setId(dashboardId);
        info.setTitle(StringUtils.defaultIfBlank(dashboardTitle, "supersonic_dashboard").trim());
        info.setEmbeddedId(embeddedId);
        info.setTags(tags);
        return info;
    }

    public void deleteDashboard(Long dashboardId) {
        if (dashboardId == null) {
            throw new IllegalStateException("superset dashboardId missing");
        }
        request(HttpMethod.DELETE, DASHBOARD_API + dashboardId, null);
    }

    private List<SupersetDashboardInfo> listDashboardsInternal(String accessToken) {
        Map<String, Object> response = StringUtils.isBlank(accessToken)
                ? get(DASHBOARD_API + "?q=(page:0,page_size:200)")
                : getWithAccessToken(DASHBOARD_API + "?q=(page:0,page_size:200)", accessToken);
        List<SupersetDashboardInfo> dashboards = extractDashboards(response);
        for (SupersetDashboardInfo dashboard : dashboards) {
            if (dashboard == null || dashboard.getId() == null) {
                continue;
            }
            try {
                String embeddedId = StringUtils.isBlank(accessToken)
                        ? fetchEmbeddedDashboardUuid(dashboard.getId())
                        : fetchEmbeddedDashboardUuid(dashboard.getId(), accessToken);
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
            Long resolvedDashboardId = parseLong(dashboardId);
            if (resolvedDashboardId != null) {
                ensureDashboardColorConfig(resolvedDashboardId);
            }
            return createGuestToken("dashboard", dashboardId);
        }
        return createGuestToken("dashboard", embeddedUuid);
    }

    public void addChartToDashboard(Long dashboardId, Long chartId) {
        if (dashboardId == null || chartId == null) {
            throw new IllegalStateException("superset dashboardId or chartId missing");
        }
        List<Long> dashboardIds = resolveChartDashboardIds(chartId);
        if (!dashboardIds.contains(dashboardId)) {
            dashboardIds.add(dashboardId);
        }
        Map<String, Object> payload = new HashMap<>();
        payload.put("dashboards",
                dashboardIds.isEmpty() ? Collections.singletonList(dashboardId) : dashboardIds);
        put(CHART_API + chartId, payload);
    }

    public void appendChartToDashboard(Long dashboardId, Long chartId) {
        if (dashboardId == null || chartId == null) {
            throw new IllegalStateException("superset dashboardId or chartId missing");
        }
        addChartToDashboard(dashboardId, chartId);
        appendChartToDashboardLayout(dashboardId, chartId, null);
    }

    void appendChartToDashboardLayout(Long dashboardId, Long chartId, Integer dashboardHeight) {
        if (dashboardId == null || chartId == null) {
            return;
        }
        Map<String, Object> position = fetchDashboardPosition(dashboardId);
        Map<String, Object> updated =
                appendChartToDashboardPosition(position, chartId, dashboardHeight);
        Map<String, Object> payload = new HashMap<>();
        payload.put("position_json", JsonUtil.toString(updated));
        payload.put("css", countDashboardCharts(updated) > 1 ? STACKED_DASHBOARD_CSS
                : SINGLE_CHART_DASHBOARD_CSS);
        put(DASHBOARD_API + dashboardId, payload);
    }

    void updateDashboardLayout(Long dashboardId, Long chartId, Integer dashboardHeight) {
        if (dashboardId == null || chartId == null) {
            return;
        }
        Map<String, Object> position = buildDashboardPosition(chartId, dashboardHeight);
        Map<String, Object> payload = new HashMap<>();
        payload.put("position_json", JsonUtil.toString(position));
        payload.put("css", SINGLE_CHART_DASHBOARD_CSS);
        try {
            put(DASHBOARD_API + dashboardId, payload);
        } catch (HttpStatusCodeException ex) {
            log.warn("superset dashboard layout update failed, dashboardId={}, chartId={}",
                    dashboardId, chartId, ex);
        }
    }

    void updateDashboardLayout(Long dashboardId, List<Long> chartIds, List<Integer> chartHeights) {
        if (dashboardId == null || chartIds == null || chartIds.isEmpty()) {
            return;
        }
        Map<String, Object> position = buildDashboardPosition(chartIds, chartHeights);
        Map<String, Object> payload = new HashMap<>();
        payload.put("position_json", JsonUtil.toString(position));
        payload.put("css",
                chartIds.size() > 1 ? STACKED_DASHBOARD_CSS : SINGLE_CHART_DASHBOARD_CSS);
        try {
            put(DASHBOARD_API + dashboardId, payload);
        } catch (HttpStatusCodeException ex) {
            log.warn(
                    "superset dashboard stacked layout update failed, dashboardId={}, chartCount={}",
                    dashboardId, chartIds.size(), ex);
        }
    }

    Map<String, Object> buildDashboardPosition(Long chartId, Integer dashboardHeight) {
        int gridHeight = resolveDashboardGridHeight(dashboardHeight);
        String chartKey = DASHBOARD_CHART_PREFIX + chartId;
        Map<String, Object> root = new HashMap<>();
        root.put("type", "ROOT");
        root.put("id", DASHBOARD_ROOT_ID);
        root.put("meta", buildTransparentMeta());
        root.put("children", Collections.singletonList(DASHBOARD_GRID_ID));
        Map<String, Object> grid = new HashMap<>();
        grid.put("type", "GRID");
        grid.put("id", DASHBOARD_GRID_ID);
        grid.put("meta", buildTransparentMeta());
        grid.put("children", Collections.singletonList(DASHBOARD_ROW_ID));
        Map<String, Object> row = new HashMap<>();
        row.put("type", "ROW");
        row.put("id", DASHBOARD_ROW_ID);
        Map<String, Object> rowMeta = buildTransparentMeta();
        rowMeta.put("width", 12);
        rowMeta.put("height", gridHeight);
        row.put("meta", rowMeta);
        row.put("children", Collections.singletonList(chartKey));
        Map<String, Object> chart = new HashMap<>();
        chart.put("type", "CHART");
        chart.put("id", chartKey);
        chart.put("children", Collections.emptyList());
        Map<String, Object> meta = new HashMap<>();
        meta.put("chartId", chartId);
        meta.put("width", 12);
        meta.put("height", gridHeight);
        meta.put("show_title", false);
        chart.put("meta", meta);
        Map<String, Object> position = new HashMap<>();
        position.put(DASHBOARD_ROOT_ID, root);
        position.put(DASHBOARD_GRID_ID, grid);
        position.put(DASHBOARD_ROW_ID, row);
        position.put(chartKey, chart);
        return position;
    }

    Map<String, Object> buildDashboardPosition(List<Long> chartIds, List<Integer> chartHeights) {
        if (chartIds == null || chartIds.isEmpty()) {
            return Collections.emptyMap();
        }
        if (chartIds.size() == 1) {
            Integer dashboardHeight =
                    chartHeights == null || chartHeights.isEmpty() ? null : chartHeights.get(0);
            return buildDashboardPosition(chartIds.get(0), dashboardHeight);
        }
        Map<String, Object> root = new HashMap<>();
        root.put("type", "ROOT");
        root.put("id", DASHBOARD_ROOT_ID);
        root.put("meta", buildTransparentMeta());
        root.put("children", Collections.singletonList(DASHBOARD_GRID_ID));

        Map<String, Object> grid = new HashMap<>();
        grid.put("type", "GRID");
        grid.put("id", DASHBOARD_GRID_ID);
        grid.put("meta", buildTransparentMeta());

        List<String> rowIds = new ArrayList<>();
        Map<String, Object> position = new HashMap<>();
        position.put(DASHBOARD_ROOT_ID, root);
        position.put(DASHBOARD_GRID_ID, grid);

        for (int i = 0; i < chartIds.size(); i++) {
            Long chartId = chartIds.get(i);
            if (chartId == null) {
                continue;
            }
            String rowId = DASHBOARD_ROW_PREFIX + (i + 1);
            String chartKey = DASHBOARD_CHART_PREFIX + chartId;
            int gridHeight = resolveDashboardGridHeight(resolveChartHeight(chartHeights, i));
            Map<String, Object> row = new HashMap<>();
            row.put("type", "ROW");
            row.put("id", rowId);
            Map<String, Object> rowMeta = buildTransparentMeta();
            rowMeta.put("width", 12);
            rowMeta.put("height", gridHeight);
            row.put("meta", rowMeta);
            row.put("children", Collections.singletonList(chartKey));

            Map<String, Object> chart = new HashMap<>();
            chart.put("type", "CHART");
            chart.put("id", chartKey);
            chart.put("children", Collections.emptyList());
            Map<String, Object> meta = new HashMap<>();
            meta.put("chartId", chartId);
            meta.put("width", 12);
            meta.put("height", gridHeight);
            meta.put("show_title", false);
            chart.put("meta", meta);

            rowIds.add(rowId);
            position.put(rowId, row);
            position.put(chartKey, chart);
        }
        grid.put("children", rowIds);
        return position;
    }

    Map<String, Object> appendChartToDashboardPosition(Map<String, Object> position, Long chartId,
            Integer dashboardHeight) {
        if (chartId == null) {
            return position == null ? Collections.emptyMap() : deepCopyMap(position);
        }
        Map<String, Object> resolved =
                position == null ? buildEmptyDashboardPosition() : deepCopyMap(position);
        if (resolved.isEmpty() || !resolved.containsKey(DASHBOARD_ROOT_ID)
                || !resolved.containsKey(DASHBOARD_GRID_ID)) {
            resolved = buildEmptyDashboardPosition();
        }
        String chartKey = DASHBOARD_CHART_PREFIX + chartId;
        if (resolved.containsKey(chartKey)) {
            return resolved;
        }
        Map<String, Object> root = resolveNode(resolved, DASHBOARD_ROOT_ID, "ROOT");
        Map<String, Object> grid = resolveNode(resolved, DASHBOARD_GRID_ID, "GRID");
        List<String> rootChildren = toMutableStringList(root.get("children"));
        if (!rootChildren.contains(DASHBOARD_GRID_ID)) {
            rootChildren.add(DASHBOARD_GRID_ID);
        }
        root.put("children", rootChildren);
        List<String> rowIds = toMutableStringList(grid.get("children"));
        String rowId = nextDashboardRowId(resolved);
        int gridHeight = resolveDashboardGridHeight(dashboardHeight);

        Map<String, Object> row = new HashMap<>();
        row.put("type", "ROW");
        row.put("id", rowId);
        Map<String, Object> rowMeta = buildTransparentMeta();
        rowMeta.put("width", 12);
        rowMeta.put("height", gridHeight);
        row.put("meta", rowMeta);
        row.put("children", Collections.singletonList(chartKey));

        Map<String, Object> chart = new HashMap<>();
        chart.put("type", "CHART");
        chart.put("id", chartKey);
        chart.put("children", Collections.emptyList());
        Map<String, Object> chartMeta = new HashMap<>();
        chartMeta.put("chartId", chartId);
        chartMeta.put("width", 12);
        chartMeta.put("height", gridHeight);
        chartMeta.put("show_title", false);
        chart.put("meta", chartMeta);

        rowIds.add(rowId);
        grid.put("children", rowIds);
        resolved.put(DASHBOARD_ROOT_ID, root);
        resolved.put(DASHBOARD_GRID_ID, grid);
        resolved.put(rowId, row);
        resolved.put(chartKey, chart);
        return resolved;
    }

    private Map<String, Object> buildTransparentMeta() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("background", "BACKGROUND_TRANSPARENT");
        return meta;
    }

    int resolveDashboardGridHeight(Integer dashboardHeight) {
        int height = dashboardHeight == null || dashboardHeight <= 0 ? DEFAULT_SINGLE_CHART_HEIGHT
                : dashboardHeight;
        int gridHeight = Math.round(height / (float) DASHBOARD_GRID_UNIT);
        return Math.max(1, gridHeight);
    }

    private Integer resolveChartHeight(List<Integer> chartHeights, int index) {
        if (chartHeights == null || index < 0 || index >= chartHeights.size()) {
            return null;
        }
        return chartHeights.get(index);
    }

    private Map<String, Object> fetchDashboardPosition(Long dashboardId) {
        if (dashboardId == null) {
            return Collections.emptyMap();
        }
        try {
            Map<String, Object> response = get(DASHBOARD_API + dashboardId);
            Map<String, Object> position = parseJsonMap(resolveValue(response, "position_json"));
            if (position != null && !position.isEmpty()) {
                return position;
            }
            Map<String, Object> result = resolveMap(response, "result");
            position = parseJsonMap(resolveValue(result, "position_json"));
            return position == null ? Collections.emptyMap() : position;
        } catch (HttpStatusCodeException ex) {
            if (HttpStatus.NOT_FOUND.equals(ex.getStatusCode())) {
                log.warn(
                        "superset dashboard position unavailable, fallback to empty layout, dashboardId={}",
                        dashboardId);
                return Collections.emptyMap();
            }
            throw ex;
        }
    }

    private Map<String, Object> buildEmptyDashboardPosition() {
        Map<String, Object> position = new HashMap<>();
        Map<String, Object> root = new HashMap<>();
        root.put("type", "ROOT");
        root.put("id", DASHBOARD_ROOT_ID);
        root.put("meta", buildTransparentMeta());
        root.put("children", Collections.singletonList(DASHBOARD_GRID_ID));
        Map<String, Object> grid = new HashMap<>();
        grid.put("type", "GRID");
        grid.put("id", DASHBOARD_GRID_ID);
        grid.put("meta", buildTransparentMeta());
        grid.put("children", new ArrayList<>());
        position.put(DASHBOARD_ROOT_ID, root);
        position.put(DASHBOARD_GRID_ID, grid);
        return position;
    }

    private Map<String, Object> resolveNode(Map<String, Object> position, String nodeId,
            String type) {
        Object existing = position.get(nodeId);
        if (existing instanceof Map) {
            return new HashMap<>((Map<String, Object>) existing);
        }
        Map<String, Object> node = new HashMap<>();
        node.put("type", type);
        node.put("id", nodeId);
        node.put("meta", buildTransparentMeta());
        node.put("children", new ArrayList<>());
        return node;
    }

    private List<String> toMutableStringList(Object value) {
        List<String> result = new ArrayList<>();
        if (!(value instanceof List)) {
            return result;
        }
        for (Object item : (List<?>) value) {
            if (item != null) {
                result.add(String.valueOf(item));
            }
        }
        return result;
    }

    private String nextDashboardRowId(Map<String, Object> position) {
        int maxIndex = 0;
        for (String key : position.keySet()) {
            if (StringUtils.startsWith(key, DASHBOARD_ROW_PREFIX)) {
                try {
                    int index = Integer.parseInt(key.substring(DASHBOARD_ROW_PREFIX.length()));
                    maxIndex = Math.max(maxIndex, index);
                } catch (NumberFormatException ex) {
                    log.debug("superset row id parse ignored, rowId={}", key);
                }
            }
        }
        return DASHBOARD_ROW_PREFIX + (maxIndex + 1);
    }

    private int countDashboardCharts(Map<String, Object> position) {
        if (position == null || position.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (Object value : position.values()) {
            if (!(value instanceof Map)) {
                continue;
            }
            Object type = ((Map<?, ?>) value).get("type");
            if ("CHART".equals(type)) {
                count++;
            }
        }
        return count;
    }

    private SupersetChartInfo createChartForDashboard(Long dashboardId, Long datasetId,
            String chartName, String vizType, Map<String, Object> formData) {
        ChartTemplateSnapshot template = resolveTemplateChartSnapshot(vizType);
        Map<String, Object> mergedFormData = mergeTemplateFormData(formData, template);
        Long chartId = createChart(datasetId, vizType, mergedFormData, chartName);
        String chartUuid = fetchChartUuid(chartId);
        addChartToDashboard(dashboardId, chartId);
        ensureDashboardChartLinked(dashboardId, chartId);
        updateChartParams(chartId, dashboardId, mergedFormData, vizType, datasetId, template);
        SupersetChartInfo chartInfo = new SupersetChartInfo();
        chartInfo.setDatasetId(datasetId);
        chartInfo.setChartId(chartId);
        chartInfo.setChartUuid(chartUuid);
        return chartInfo;
    }

    private void updateChartParams(Long chartId, Long dashboardId, Map<String, Object> formData,
            String vizType, Long datasetId, ChartTemplateSnapshot template) {
        if (chartId == null) {
            return;
        }
        Map<String, Object> merged = new HashMap<>();
        if (template != null && template.formData != null) {
            merged.putAll(template.formData);
        } else {
            Map<String, Object> existing = resolveChartParams(chartId);
            if (existing != null) {
                merged.putAll(existing);
            }
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
        merged.put("show_title", false);
        merged.put("show_chart_title", false);
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
        normalizeAccessFields(merged);
        sanitizeMetricOptionNames(merged);
        Map<String, Object> payload = new HashMap<>();
        payload.put("params", JsonUtil.toString(merged));
        Map<String, Object> templateContext = template == null ? null : template.queryContext;
        Map<String, Object> queryContext =
                buildQueryContext(merged, datasetId, vizType, templateContext);
        if (queryContext != null) {
            sanitizeQueryContext(queryContext);
            payload.put("query_context", JsonUtil.toString(queryContext));
            payload.put("query_context_generation", true);
        }
        try {
            log.debug("superset chart params update payload, chartId={}, templateId={}, payload={}",
                    chartId, template == null ? null : template.chartId,
                    JsonUtil.toString(payload));
            put(CHART_API + chartId, payload);
        } catch (HttpStatusCodeException ex) {
            log.warn("superset chart params update failed, chartId={}", chartId, ex);
        }
    }

    Map<String, Object> buildQueryContext(Map<String, Object> formData, Long datasetId,
            String vizType) {
        return buildQueryContext(formData, datasetId, vizType, null);
    }

    private Map<String, Object> buildQueryContext(Map<String, Object> formData, Long datasetId,
            String vizType, Map<String, Object> templateContext) {
        if (formData == null || datasetId == null) {
            return null;
        }
        Map<String, Object> context =
                templateContext == null ? new HashMap<>() : deepCopyMap(templateContext);
        Map<String, Object> datasource = new HashMap<>();
        datasource.put("id", datasetId);
        datasource.put("type", getDatasourceType());
        context.put("datasource", datasource);
        context.put("force", false);
        context.put("result_format", "json");
        context.put("result_type", "full");
        context.put("form_data", formData);
        List<Map<String, Object>> queries = resolveQueries(context.get("queries"));
        if (queries.isEmpty()) {
            queries.add(new HashMap<>());
        }
        for (Map<String, Object> query : queries) {
            syncQueryObject(query, formData);
        }
        List<Map<String, Object>> templated =
                SupersetQueryContextTemplates.apply(vizType, formData, queries);
        if (templated == null || templated.isEmpty()) {
            templated = queries;
        }
        context.put("queries", templated);
        sanitizeQueryContext(context);
        return context;
    }

    private void syncQueryObject(Map<String, Object> query, Map<String, Object> formData) {
        if (query == null || formData == null) {
            return;
        }
        Object queryMode = formData.get("query_mode");
        String mode = queryMode == null ? "aggregate" : String.valueOf(queryMode);
        List<Object> metrics = resolveMetricsForAccess(formData);
        List<Object> groupby = toList(formData.get("groupby"));
        List<Object> columns = toList(formData.get("columns"));
        if (!groupby.isEmpty()) {
            columns = mergeList(columns, groupby);
        }
        if ("raw".equalsIgnoreCase(mode)) {
            List<Object> rawColumns = columns;
            if (rawColumns.isEmpty()) {
                rawColumns = toList(formData.get("all_columns"));
            }
            if (!rawColumns.isEmpty()) {
                query.put("columns", rawColumns);
            }
        } else {
            query.put("metrics", metrics);
            if (!groupby.isEmpty()) {
                query.put("groupby", groupby);
            }
            if (!columns.isEmpty()) {
                query.put("columns", columns);
            }
            Object granularity = formData.get("granularity_sqla");
            if (granularity != null) {
                query.put("granularity", granularity);
            }
        }
        Object orderby = formData.get("orderby");
        if (orderby == null) {
            orderby = resolveDefaultOrderby(metrics);
        }
        if (orderby != null) {
            query.put("orderby", orderby);
        }
        List<Map<String, Object>> filters = resolveFilters(formData);
        if (!filters.isEmpty()) {
            query.put("filters", filters);
        } else {
            query.putIfAbsent("filters", Collections.emptyList());
        }
        Map<String, Object> extras = resolveMap(query, "extras");
        if (extras == null || extras.isEmpty()) {
            extras = new HashMap<>();
            extras.put("having", "");
            extras.put("where", "");
        }
        applyTimeRange(query, extras, formData);
        query.put("extras", extras);
        query.putIfAbsent("applied_time_extras", Collections.emptyMap());
        query.putIfAbsent("annotation_layers", Collections.emptyList());
        query.putIfAbsent("row_limit", formData.getOrDefault("row_limit", 10000));
        query.putIfAbsent("series_limit", formData.getOrDefault("series_limit", 0));
        query.putIfAbsent("group_others_when_limit_reached", false);
        query.putIfAbsent("order_desc", formData.getOrDefault("order_desc", true));
        query.putIfAbsent("url_params",
                formData.getOrDefault("url_params", Collections.emptyMap()));
        query.putIfAbsent("custom_params", Collections.emptyMap());
        query.putIfAbsent("custom_form_data", Collections.emptyMap());
        query.putIfAbsent("post_processing", Collections.emptyList());
        query.putIfAbsent("time_offsets", Collections.emptyList());
    }

    private void applyTimeRange(Map<String, Object> query, Map<String, Object> extras,
            Map<String, Object> formData) {
        if (query == null || formData == null) {
            return;
        }
        String timeRange = resolveTimeRange(formData);
        if (StringUtils.isBlank(timeRange)) {
            return;
        }
        query.put("time_range", timeRange);
        if (extras != null) {
            extras.put("time_range", timeRange);
        }
    }

    private String resolveTimeRange(Map<String, Object> formData) {
        if (formData == null) {
            return null;
        }
        Object direct = formData.get("time_range");
        if (direct instanceof String && StringUtils.isNotBlank((String) direct)) {
            return (String) direct;
        }
        Object adhocFilters = formData.get("adhoc_filters");
        if (!(adhocFilters instanceof List)) {
            return null;
        }
        for (Object item : (List<?>) adhocFilters) {
            if (!(item instanceof Map)) {
                continue;
            }
            Map<?, ?> filter = (Map<?, ?>) item;
            if (!StringUtils.equalsIgnoreCase(String.valueOf(filter.get("operator")),
                    "TEMPORAL_RANGE")) {
                continue;
            }
            Object comparator = filter.get("comparator");
            if (comparator instanceof String && StringUtils.isNotBlank((String) comparator)) {
                return (String) comparator;
            }
        }
        return null;
    }

    private List<Map<String, Object>> resolveFilters(Map<String, Object> formData) {
        if (formData == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> filters = new ArrayList<>();
        Object adhocFilters = formData.get("adhoc_filters");
        if (adhocFilters instanceof List) {
            for (Object item : (List<?>) adhocFilters) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Map<String, Object> filter = toSimpleFilter((Map<?, ?>) item);
                if (filter != null) {
                    filters.add(filter);
                }
            }
        }
        Object extraFilters = formData.get("extra_filters");
        if (extraFilters instanceof List) {
            for (Object item : (List<?>) extraFilters) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Map<String, Object> filter = toExistingFilter((Map<?, ?>) item);
                if (filter != null) {
                    filters.add(filter);
                }
            }
        }
        return filters;
    }

    private Map<String, Object> toSimpleFilter(Map<?, ?> filter) {
        if (filter == null) {
            return null;
        }
        String operator = String.valueOf(filter.get("operator"));
        if (StringUtils.equalsIgnoreCase(operator, "TEMPORAL_RANGE")) {
            return null;
        }
        String subject = String.valueOf(filter.get("subject"));
        if (StringUtils.isBlank(subject)) {
            return null;
        }
        Object comparator = filter.get("comparator");
        if (comparator == null) {
            return null;
        }
        Map<String, Object> simple = new HashMap<>();
        simple.put("col", subject);
        simple.put("op", operator);
        simple.put("val", comparator);
        return simple;
    }

    private Map<String, Object> toExistingFilter(Map<?, ?> filter) {
        if (filter == null) {
            return null;
        }
        Object col = firstNonNull(filter.get("col"), filter.get("column"));
        Object op = filter.get("op");
        Object val = filter.get("val");
        if (col == null || op == null || val == null) {
            return null;
        }
        Map<String, Object> simple = new HashMap<>();
        simple.put("col", col);
        simple.put("op", op);
        simple.put("val", val);
        return simple;
    }

    private Object firstNonNull(Object left, Object right) {
        return left != null ? left : right;
    }

    private void normalizeAccessFields(Map<String, Object> formData) {
        if (formData == null) {
            return;
        }
        List<Object> metrics = resolveMetricsForAccess(formData);
        formData.put("metrics", metrics);
        List<Object> groupby = toList(formData.get("groupby"));
        List<Object> columns = toList(formData.get("columns"));
        if (!groupby.isEmpty()) {
            formData.put("columns", mergeList(columns, groupby));
        }
        Object orderby = formData.get("orderby");
        if (orderby == null) {
            orderby = resolveDefaultOrderby(metrics);
            if (orderby != null) {
                formData.put("orderby", orderby);
            }
        }
    }

    private void sanitizeMetricOptionNames(Map<String, Object> formData) {
        if (formData == null) {
            return;
        }
        sanitizeMetricContainer(formData.get("metrics"));
        sanitizeMetricContainer(formData.get("metric"));
        sanitizeMetricContainer(formData.get("metrics_b"));
        sanitizeOrderBy(formData.get("orderby"));
    }

    private void sanitizeQueryContext(Map<String, Object> queryContext) {
        if (queryContext == null) {
            return;
        }
        Object formData = queryContext.get("form_data");
        if (formData instanceof Map) {
            sanitizeMetricOptionNames((Map<String, Object>) formData);
        }
        Object queriesObj = queryContext.get("queries");
        if (queriesObj instanceof List) {
            for (Object item : (List<?>) queriesObj) {
                if (item instanceof Map) {
                    Map<String, Object> query = (Map<String, Object>) item;
                    sanitizeMetricContainer(query.get("metrics"));
                    sanitizeOrderBy(query.get("orderby"));
                }
            }
        }
    }

    private void sanitizeMetricContainer(Object value) {
        if (value instanceof List) {
            for (Object item : (List<?>) value) {
                if (item instanceof Map) {
                    sanitizeMetricMap((Map<String, Object>) item);
                }
            }
            return;
        }
        if (value instanceof Map) {
            sanitizeMetricMap((Map<String, Object>) value);
        }
    }

    private void sanitizeMetricMap(Map<String, Object> metric) {
        if (metric == null) {
            return;
        }
        Object optionName = metric.get("optionName");
        if (optionName instanceof String) {
            metric.put("optionName", normalizeMetricOptionName((String) optionName));
        }
    }

    private void sanitizeOrderBy(Object orderby) {
        if (!(orderby instanceof List)) {
            return;
        }
        for (Object entry : (List<?>) orderby) {
            if (entry instanceof List) {
                List<?> order = (List<?>) entry;
                if (!order.isEmpty() && order.get(0) instanceof Map) {
                    sanitizeMetricMap((Map<String, Object>) order.get(0));
                }
            }
        }
    }

    private String normalizeMetricOptionName(String value) {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        return value.replaceAll("\\s+", "");
    }

    private List<Object> resolveMetricsForAccess(Map<String, Object> formData) {
        if (formData == null) {
            return Collections.emptyList();
        }
        List<Object> metrics = new ArrayList<>();
        metrics.addAll(toList(formData.get("metrics")));
        metrics.addAll(toList(formData.get("metric")));
        metrics.addAll(toList(formData.get("metrics_b")));
        return deduplicateList(metrics);
    }

    private Object resolveDefaultOrderby(List<Object> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return null;
        }
        List<Object> orderby = new ArrayList<>();
        List<Object> order = new ArrayList<>();
        order.add(metrics.get(0));
        order.add(false);
        orderby.add(order);
        return orderby;
    }

    private List<Object> mergeList(List<Object> base, List<Object> addition) {
        List<Object> merged = new ArrayList<>();
        if (base != null) {
            merged.addAll(base);
        }
        if (addition != null) {
            merged.addAll(addition);
        }
        return deduplicateList(merged);
    }

    private List<Object> deduplicateList(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<Object> result = new ArrayList<>();
        for (Object value : values) {
            if (!result.contains(value)) {
                result.add(value);
            }
        }
        return result;
    }

    private List<Object> toList(Object value) {
        if (value instanceof List) {
            return new ArrayList<>((List<Object>) value);
        }
        if (value != null) {
            return Collections.singletonList(value);
        }
        return Collections.emptyList();
    }

    private List<Map<String, Object>> resolveQueries(Object value) {
        if (!(value instanceof List)) {
            return new ArrayList<>();
        }
        List<?> rawList = (List<?>) value;
        List<Map<String, Object>> queries = new ArrayList<>();
        for (Object entry : rawList) {
            if (entry instanceof Map) {
                queries.add(new HashMap<>((Map<String, Object>) entry));
            }
        }
        return queries;
    }

    private Map<String, Object> deepCopyMap(Map<String, Object> source) {
        if (source == null) {
            return new HashMap<>();
        }
        String json = JsonUtil.toString(source);
        Map<String, Object> copy = JsonUtil.toObject(json, Map.class);
        return copy == null ? new HashMap<>() : copy;
    }

    private Map<String, Object> resolveChartParams(Long chartId) {
        try {
            Map<String, Object> response = get(CHART_API + chartId);
            return resolveChartParams(response);
        } catch (Exception ex) {
            log.debug("superset chart params fetch failed, chartId={}", chartId, ex);
        }
        return null;
    }

    private List<Long> resolveChartDashboardIds(Long chartId) {
        if (chartId == null) {
            return new ArrayList<>();
        }
        try {
            Map<String, Object> response = get(CHART_API + chartId);
            return resolveChartDashboardIds(response);
        } catch (Exception ex) {
            log.debug("superset chart dashboard fetch failed, chartId={}", chartId, ex);
        }
        return new ArrayList<>();
    }

    private List<Long> resolveChartDashboardIds(Map<String, Object> response) {
        List<Long> dashboardIds = new ArrayList<>();
        Object dashboards = resolveValue(response, "dashboards");
        if (dashboards == null) {
            Map<String, Object> result = resolveMap(response, "result");
            dashboards = resolveValue(result, "dashboards");
        }
        collectDashboardIds(dashboards, dashboardIds);
        return dashboardIds;
    }

    private void collectDashboardIds(Object value, List<Long> dashboardIds) {
        if (value == null || dashboardIds == null) {
            return;
        }
        if (value instanceof List) {
            for (Object item : (List<?>) value) {
                collectDashboardIds(item, dashboardIds);
            }
            return;
        }
        if (value instanceof Map) {
            Long dashboardId = parseLong(((Map<?, ?>) value).get("id"));
            if (dashboardId != null && !dashboardIds.contains(dashboardId)) {
                dashboardIds.add(dashboardId);
            }
            return;
        }
        Long dashboardId = parseLong(value);
        if (dashboardId != null && !dashboardIds.contains(dashboardId)) {
            dashboardIds.add(dashboardId);
        }
    }

    private Map<String, Object> resolveChartParams(Map<String, Object> response) {
        Object params = resolveValue(response, "params");
        if (params == null) {
            Map<String, Object> result = resolveMap(response, "result");
            params = resolveValue(result, "params");
        }
        return parseJsonMap(params);
    }

    private Map<String, Object> resolveChartQueryContext(Map<String, Object> response) {
        Object context = resolveValue(response, "query_context");
        if (context == null) {
            Map<String, Object> result = resolveMap(response, "result");
            context = resolveValue(result, "query_context");
        }
        return parseJsonMap(context);
    }

    private Map<String, Object> parseJsonMap(Object value) {
        if (value instanceof Map) {
            return new HashMap<>((Map<String, Object>) value);
        }
        if (value instanceof String && StringUtils.isNotBlank((String) value)) {
            Map<String, Object> parsed = JsonUtil.toObject((String) value, Map.class);
            return parsed == null ? null : new HashMap<>(parsed);
        }
        return null;
    }

    private ChartTemplateSnapshot resolveTemplateChartSnapshot(String vizType) {
        Long templateId = resolveTemplateChartId(vizType);
        if (templateId == null) {
            return null;
        }
        ChartTemplateSnapshot snapshot = fetchChartSnapshot(templateId);
        if (snapshot != null) {
            log.debug(
                    "superset template chart resolved, vizType={}, templateId={}, hasParams={}, "
                            + "hasQueryContext={}",
                    vizType, templateId, snapshot.formData != null, snapshot.queryContext != null);
        }
        return snapshot;
    }

    private Long resolveTemplateChartId(String vizType) {
        if (config == null) {
            return null;
        }
        Long templateId = null;
        Map<String, Long> templateIds = config.getTemplateChartIds();
        if (templateIds != null && StringUtils.isNotBlank(vizType)) {
            templateId = lookupTemplateId(templateIds, vizType);
            if (templateId == null) {
                SupersetVizTypeSelector.VizTypeCatalog catalog = resolveVizTypeCatalog();
                if (catalog != null && catalog.getItems() != null) {
                    for (SupersetVizTypeSelector.VizTypeItem item : catalog.getItems()) {
                        if (item == null || StringUtils.isBlank(item.getVizType())) {
                            continue;
                        }
                        if (!item.getVizType().equalsIgnoreCase(vizType)) {
                            continue;
                        }
                        templateId = lookupTemplateId(templateIds, item.getVizKey());
                        if (templateId != null) {
                            break;
                        }
                        templateId = lookupTemplateId(templateIds, item.getName());
                        if (templateId != null) {
                            break;
                        }
                    }
                }
            }
        }
        if (templateId == null) {
            templateId = config.getTemplateChartId();
        }
        return templateId;
    }

    private Long lookupTemplateId(Map<String, Long> templateIds, String key) {
        if (templateIds == null || StringUtils.isBlank(key)) {
            return null;
        }
        Long templateId = templateIds.get(key);
        if (templateId == null) {
            templateId = templateIds.get(StringUtils.lowerCase(key));
        }
        if (templateId == null) {
            templateId = templateIds.get(StringUtils.upperCase(key));
        }
        return templateId;
    }

    private SupersetVizTypeSelector.VizTypeCatalog resolveVizTypeCatalog() {
        SupersetVizTypeSelector.VizTypeCatalog cached = VIZTYPE_CATALOG;
        if (cached != null) {
            return cached;
        }
        synchronized (SupersetApiClient.class) {
            if (VIZTYPE_CATALOG == null) {
                VIZTYPE_CATALOG = SupersetVizTypeSelector.VizTypeCatalog.load();
            }
            return VIZTYPE_CATALOG;
        }
    }

    private ChartTemplateSnapshot fetchChartSnapshot(Long chartId) {
        if (chartId == null) {
            return null;
        }
        try {
            Map<String, Object> response = get(CHART_API + chartId);
            String vizType = resolveString(response, "viz_type");
            if (StringUtils.isBlank(vizType)) {
                Map<String, Object> result = resolveMap(response, "result");
                vizType = resolveString(result, "viz_type");
            }
            Map<String, Object> params = resolveChartParams(response);
            Map<String, Object> queryContext = resolveChartQueryContext(response);
            return new ChartTemplateSnapshot(chartId, vizType, params, queryContext);
        } catch (Exception ex) {
            log.warn("superset template chart fetch failed, chartId={}", chartId, ex);
            return null;
        }
    }

    private Map<String, Object> mergeTemplateFormData(Map<String, Object> formData,
            ChartTemplateSnapshot template) {
        Map<String, Object> merged = new HashMap<>();
        if (template != null && template.formData != null) {
            merged.putAll(template.formData);
        }
        if (formData != null && !formData.isEmpty()) {
            merged.putAll(formData);
        }
        return merged;
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
        log.warn("superset dashboard chart link not ready, dashboardId={}, chartId={}", dashboardId,
                chartId);
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
                log.debug(
                        "superset dashboard charts endpoint not found, skip check, dashboardId={}",
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
        log.debug(
                "superset chart create payload, chartName={}, datasetId={}, vizType={}, payload={}",
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
        return createDashboard(dashboardTitle, true);
    }

    private Long createDashboard(String dashboardTitle, boolean sanitizeTitle) {
        String title = sanitizeTitle ? sanitizeDashboardTitle(dashboardTitle)
                : StringUtils.defaultIfBlank(dashboardTitle, "supersonic_dashboard").trim();
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
        ensureDashboardColorConfig(dashboardId);
        return dashboardId;
    }

    private void ensureDashboardColorConfig(Long dashboardId) {
        if (dashboardId == null) {
            return;
        }
        try {
            Map<String, Object> metadata = fetchDashboardJsonMetadata(dashboardId);
            if (!needsDashboardColorInitialization(metadata)) {
                return;
            }
            ObjectNode payload = buildDashboardColorConfigPayload(metadata);
            log.debug("superset dashboard color payload, dashboardId={}, payload={}", dashboardId,
                    JsonUtil.toString(payload));
            put(DASHBOARD_API + dashboardId + "/colors?mark_updated=false", payload);
        } catch (HttpStatusCodeException ex) {
            if (shouldSkipDashboardColorInitialization(ex)) {
                log.debug(
                        "superset dashboard color init skipped, dashboardId={}, status={}, message={}",
                        dashboardId, ex.getStatusCode(), ex.getMessage());
                return;
            }
            log.warn("superset dashboard color init failed, dashboardId={}", dashboardId, ex);
        } catch (Exception ex) {
            log.warn("superset dashboard color init failed, dashboardId={}", dashboardId, ex);
        }
    }

    private boolean shouldSkipDashboardColorInitialization(HttpStatusCodeException ex) {
        int status = ex.getStatusCode().value();
        return status == 401 || status == 403 || status == 404;
    }

    private Map<String, Object> fetchDashboardJsonMetadata(Long dashboardId) {
        if (dashboardId == null) {
            return new HashMap<>();
        }
        Map<String, Object> response = get(DASHBOARD_API + dashboardId);
        Object metadata = resolveValue(response, "json_metadata");
        if (metadata == null) {
            Map<String, Object> result = resolveMap(response, "result");
            metadata = resolveValue(result, "json_metadata");
        }
        Map<String, Object> parsed = parseJsonMap(metadata);
        return parsed == null ? new HashMap<>() : parsed;
    }

    private boolean needsDashboardColorInitialization(Map<String, Object> metadata) {
        if (metadata == null) {
            return true;
        }
        return !metadata.containsKey("color_namespace") || !metadata.containsKey("color_scheme")
                || !(metadata.get("color_scheme_domain") instanceof List)
                || !(metadata.get("shared_label_colors") instanceof List)
                || !(metadata.get("map_label_colors") instanceof Map)
                || !(metadata.get("label_colors") instanceof Map);
    }

    private ObjectNode buildDashboardColorConfigPayload(Map<String, Object> metadata) {
        Map<String, Object> resolved = metadata == null ? new HashMap<>() : metadata;
        ObjectMapper objectMapper = JsonUtil.INSTANCE.getObjectMapper();
        ObjectNode payload = objectMapper.createObjectNode();
        if (resolved.containsKey("color_namespace") && resolved.get("color_namespace") != null) {
            payload.set("color_namespace",
                    objectMapper.valueToTree(resolved.get("color_namespace")));
        } else {
            payload.putNull("color_namespace");
        }
        if (resolved.containsKey("color_scheme") && resolved.get("color_scheme") != null) {
            payload.set("color_scheme", objectMapper.valueToTree(resolved.get("color_scheme")));
        } else {
            payload.putNull("color_scheme");
        }
        payload.set("color_scheme_domain",
                objectMapper.valueToTree(copyObjectList(resolved.get("color_scheme_domain"))));
        payload.set("shared_label_colors",
                objectMapper.valueToTree(copyObjectList(resolved.get("shared_label_colors"))));
        payload.set("map_label_colors",
                objectMapper.valueToTree(copyObjectMap(resolved.get("map_label_colors"))));
        payload.set("label_colors",
                objectMapper.valueToTree(copyObjectMap(resolved.get("label_colors"))));
        return payload;
    }

    private List<Object> copyObjectList(Object value) {
        if (!(value instanceof List)) {
            return new ArrayList<>();
        }
        return new ArrayList<>((List<Object>) value);
    }

    private Map<String, Object> copyObjectMap(Object value) {
        if (!(value instanceof Map)) {
            return new HashMap<>();
        }
        return new HashMap<>((Map<String, Object>) value);
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

    private String fetchEmbeddedDashboardUuid(Long dashboardId, String accessToken) {
        if (dashboardId == null) {
            throw new IllegalStateException("superset dashboardId missing");
        }
        try {
            Map<String, Object> response =
                    getWithAccessToken(DASHBOARD_API + dashboardId + "/embedded", accessToken);
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

    String sanitizeDashboardTitle(String dashboardTitle) {
        String title = StringUtils.defaultIfBlank(dashboardTitle, "supersonic_dashboard");
        String trimmed = title.trim();
        if (trimmed.startsWith("supersonic_")) {
            trimmed = trimmed.substring("supersonic_".length());
        }
        trimmed = trimmed.replaceAll("(?i)_supersonic(_|$)", "_");
        trimmed = trimmed.replaceAll("(?i)_superset(_|$)", "_");
        trimmed = trimmed.replaceAll("_(\\d+)(?:_\\d+)?$", "");
        trimmed = trimmed.replace('_', ' ').trim();
        return StringUtils.defaultIfBlank(trimmed, "supersonic_dashboard");
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

    /**
     * 执行带认证的 Superset 请求。
     *
     * Args: method: HTTP 方法。 path: API 路径。 body: 请求体（可为空）。
     *
     * Returns: 解析后的响应 Map。
     */
    private Map<String, Object> request(HttpMethod method, String path, Object body) {
        AuthHeaders authHeaders = resolveAuthHeaders(method, path);
        log.debug("superset request, method={}, path={}, authMode={}", method, path,
                authHeaders.mode);
        try {
            return execute(method, path, body, authHeaders.headers);
        } catch (HttpStatusCodeException ex) {
            if (authHeaders.mode == AuthMode.WEB_SESSION && canRetryWebSession(ex)) {
                clearBrowserSession();
                AuthHeaders refreshed = tryBuildWebSessionHeaders(method, path);
                if (refreshed != null) {
                    return execute(method, path, body, refreshed.headers);
                }
            }
            if (authHeaders.mode == AuthMode.JWT && canFallback(ex)) {
                if (refreshJwt()) {
                    AuthHeaders refreshed = tryBuildJwtHeaders(method);
                    if (refreshed != null) {
                        return execute(method, path, body, refreshed.headers);
                    }
                }
            }
            if (authHeaders.mode == AuthMode.JWT && canFallbackToWebSession(path, ex)) {
                clearBrowserSession();
                AuthHeaders webSessionHeaders = tryBuildWebSessionHeaders(method, path);
                if (webSessionHeaders != null) {
                    log.warn(
                            "superset jwt request failed, fallback to browser session, path={}, status={}",
                            path, ex.getStatusCode());
                    return execute(method, path, body, webSessionHeaders.headers);
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
     * 根据当前认证方式生成请求头。
     *
     * Args: method: HTTP 方法。
     *
     * Returns: 包含请求头与认证模式的结构。
     */
    private AuthHeaders resolveAuthHeaders(HttpMethod method, String path) {
        AuthHeaders webSessionHeaders = tryBuildWebSessionHeaders(method, path);
        if (webSessionHeaders != null) {
            return webSessionHeaders;
        }
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (!config.isAuthEnabled()) {
            return new AuthHeaders(headers, AuthMode.NONE);
        }
        AuthHeaders jwtHeaders = tryBuildJwtHeaders(method);
        if (jwtHeaders != null) {
            return jwtHeaders;
        }
        return new AuthHeaders(headers, AuthMode.NONE);
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
            log.warn("superset jwt auth failed", ex);
            return null;
        }
    }

    private AuthHeaders tryBuildWebSessionHeaders(HttpMethod method, String path) {
        if (!requiresDashboardSession(path) || !config.isAuthEnabled()
                || StringUtils.isBlank(config.getJwtUsername())
                || StringUtils.isBlank(config.getJwtPassword())) {
            return null;
        }
        try {
            ensureBrowserSession();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set(HttpHeaders.REFERER, baseUrl + WELCOME_PAGE);
            headers.set(HttpHeaders.COOKIE, browserSession.cookie);
            if (requiresCsrf(method)) {
                headers.set("X-CSRFToken", browserSession.csrfToken);
            }
            return new AuthHeaders(headers, AuthMode.WEB_SESSION);
        } catch (Exception ex) {
            log.warn("superset browser session auth failed, fallback to standard auth", ex);
            clearBrowserSession();
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

    private void ensureBrowserSession() {
        if (browserSession == null) {
            browserSession = new BrowserSession();
        }
        if (StringUtils.isNotBlank(browserSession.csrfToken)
                && StringUtils.isNotBlank(browserSession.cookie)) {
            return;
        }
        String loginPageUrl = baseUrl + LOGIN_PAGE_NEXT;
        ResponseEntity<String> loginPageResponse =
                restTemplate.exchange(loginPageUrl, HttpMethod.GET,
                        new HttpEntity<>(new HttpHeaders()), String.class);
        String loginPageCookie = extractCookie(loginPageResponse.getHeaders());
        String loginCsrfToken = extractHtmlCsrfToken(loginPageResponse.getBody());

        MultiValueMap<String, String> loginForm = new LinkedMultiValueMap<>();
        loginForm.add("username", config.getJwtUsername());
        loginForm.add("password", config.getJwtPassword());
        loginForm.add("csrf_token", loginCsrfToken);

        HttpHeaders loginHeaders = new HttpHeaders();
        loginHeaders.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        loginHeaders.set(HttpHeaders.REFERER, loginPageUrl);
        if (StringUtils.isNotBlank(loginPageCookie)) {
            loginHeaders.set(HttpHeaders.COOKIE, loginPageCookie);
        }
        ResponseEntity<String> loginResponse =
                restTemplate.exchange(loginPageUrl, HttpMethod.POST,
                        new HttpEntity<>(loginForm, loginHeaders), String.class);
        String browserCookie =
                mergeCookies(loginPageCookie, extractCookie(loginResponse.getHeaders()));

        HttpHeaders homeHeaders = new HttpHeaders();
        homeHeaders.set(HttpHeaders.REFERER, loginPageUrl);
        if (StringUtils.isNotBlank(browserCookie)) {
            homeHeaders.set(HttpHeaders.COOKIE, browserCookie);
        }
        ResponseEntity<String> homeResponse =
                restTemplate.exchange(baseUrl + WELCOME_PAGE, HttpMethod.GET,
                        new HttpEntity<>(homeHeaders), String.class);
        browserSession.cookie =
                mergeCookies(browserCookie, extractCookie(homeResponse.getHeaders()));
        browserSession.csrfToken = extractHtmlCsrfToken(homeResponse.getBody());
        if (StringUtils.isBlank(browserSession.cookie)
                || StringUtils.isBlank(browserSession.csrfToken)) {
            clearBrowserSession();
            throw new IllegalStateException("superset browser session missing");
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

    private boolean requiresDashboardSession(String path) {
        return StringUtils.isNotBlank(path) && path.startsWith(DASHBOARD_API);
    }

    /**
     * 判断是否允许刷新 JWT 后重试。
     *
     * Args: ex: HTTP 状态异常。
     *
     * Returns: 401/403 返回 true。
     */
    private boolean canFallback(HttpStatusCodeException ex) {
        return ex.getStatusCode().value() == 401 || ex.getStatusCode().value() == 403;
    }

    private boolean canFallbackToWebSession(String path, HttpStatusCodeException ex) {
        if (!requiresDashboardSession(path)) {
            return false;
        }
        int status = ex.getStatusCode().value();
        return status == 401 || status == 403 || status >= 500;
    }

    private boolean canRetryWebSession(HttpStatusCodeException ex) {
        int status = ex.getStatusCode().value();
        return status == 401 || status == 403;
    }

    private void clearBrowserSession() {
        if (browserSession == null) {
            return;
        }
        browserSession.cookie = null;
        browserSession.csrfToken = null;
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

    private String mergeCookies(String... cookieValues) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (cookieValues == null) {
            return null;
        }
        for (String cookieValue : cookieValues) {
            if (StringUtils.isBlank(cookieValue)) {
                continue;
            }
            for (String cookie : cookieValue.split("; ")) {
                if (StringUtils.isBlank(cookie)) {
                    continue;
                }
                String[] parts = cookie.split("=", 2);
                if (parts.length != 2 || StringUtils.isBlank(parts[0])) {
                    continue;
                }
                merged.put(parts[0], parts[0] + "=" + parts[1]);
            }
        }
        return merged.isEmpty() ? null : String.join("; ", merged.values());
    }

    private String extractHtmlCsrfToken(String html) {
        Matcher matcher = HTML_CSRF_PATTERN.matcher(StringUtils.defaultString(html));
        if (!matcher.find()) {
            throw new IllegalStateException("superset html csrf token missing");
        }
        return matcher.group(1);
    }

    private enum AuthMode {
        NONE, JWT, WEB_SESSION
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

    private List<String> resolveTagNames(Object value) {
        if (!(value instanceof List)) {
            return Collections.emptyList();
        }
        List<?> raw = (List<?>) value;
        if (raw.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> tags = new ArrayList<>();
        for (Object item : raw) {
            if (item instanceof Map) {
                String name = resolveString((Map<String, Object>) item, "name");
                if (StringUtils.isNotBlank(name)) {
                    tags.add(name.trim());
                }
            } else if (item != null) {
                String name = String.valueOf(item);
                if (StringUtils.isNotBlank(name)) {
                    tags.add(name.trim());
                }
            }
        }
        return tags;
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
            Object tagsValue = resolveValue(item, "tags");
            if (tagsValue != null) {
                info.setTags(resolveTagNames(tagsValue));
            }
            if (info.getId() != null || StringUtils.isNotBlank(info.getTitle())) {
                dashboards.add(info);
            }
        }
        return dashboards;
    }
}
