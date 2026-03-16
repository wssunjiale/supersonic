package com.tencent.supersonic.chat.server.plugin.build.superset;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.tencent.supersonic.common.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SupersetApiClientTest {

    @Test
    public void testExtractDashboards() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> dashboard = new HashMap<>();
        dashboard.put("id", 12L);
        dashboard.put("dashboard_title", "Sales Overview");
        List<Map<String, Object>> tags = new ArrayList<>();
        Map<String, Object> tag = new HashMap<>();
        tag.put("name", "supersonic-manual");
        tags.add(tag);
        dashboard.put("tags", tags);
        result.add(dashboard);
        response.put("result", result);

        List<SupersetDashboardInfo> dashboards = client.extractDashboards(response);
        Assertions.assertEquals(1, dashboards.size());
        Assertions.assertEquals(12L, dashboards.get(0).getId());
        Assertions.assertEquals("Sales Overview", dashboards.get(0).getTitle());
        Assertions.assertEquals(Collections.singletonList("supersonic-manual"),
                dashboards.get(0).getTags());
    }

    @Test
    public void testBuildTagPayload() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        List<String> tags = Arrays.asList("supersonic", "supersonic-single-chart");

        Map<String, Object> payload = client.buildTagPayload(tags);

        Assertions.assertTrue(payload.containsKey("properties"));
        Object properties = payload.get("properties");
        Assertions.assertTrue(properties instanceof Map);
        Map<?, ?> propertiesMap = (Map<?, ?>) properties;
        Assertions.assertEquals(tags, propertiesMap.get("tags"));
    }

    @Test
    public void testBuildGuestTokenPayloadIncludesUser() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> resource = new HashMap<>();
        resource.put("type", "dashboard");
        resource.put("id", "uuid-123");

        Map<String, Object> payload = client.buildGuestTokenPayload(resource);

        Assertions.assertTrue(payload.containsKey("user"));
        Object user = payload.get("user");
        Assertions.assertTrue(user instanceof Map);
        Map<?, ?> userMap = (Map<?, ?>) user;
        Assertions.assertEquals("supersonic-guest", userMap.get("username"));
        Assertions.assertEquals("Supersonic", userMap.get("first_name"));
        Assertions.assertEquals("Guest", userMap.get("last_name"));
    }

    @Test
    public void testBuildDashboardPositionUsesFullWidth() {
        SupersetApiClient client = new SupersetApiClient(new SupersetPluginConfig());
        Map<String, Object> position = client.buildDashboardPosition(12L, 260);
        Map<?, ?> root = (Map<?, ?>) position.get("ROOT_ID");
        Map<?, ?> rootMeta = (Map<?, ?>) root.get("meta");
        Assertions.assertEquals("BACKGROUND_TRANSPARENT", rootMeta.get("background"));
        Map<?, ?> chart = (Map<?, ?>) position.get("CHART-12");
        Map<?, ?> meta = (Map<?, ?>) chart.get("meta");
        Assertions.assertEquals(12, meta.get("width"));
        Assertions.assertEquals(Math.max(1, Math.round(260 / 8f)), meta.get("height"));
        Assertions.assertEquals(false, meta.get("show_title"));
    }

    @Test
    public void testBuildDashboardPositionStacksMultipleChartsVertically() {
        SupersetApiClient client = new SupersetApiClient(new SupersetPluginConfig());

        Map<String, Object> position = client.buildDashboardPosition(Arrays.asList(12L, 34L, 56L),
                Arrays.asList(260, 300, 180));

        Map<?, ?> grid = (Map<?, ?>) position.get("GRID_ID");
        Assertions.assertEquals(Arrays.asList("ROW-1", "ROW-2", "ROW-3"), grid.get("children"));

        Map<?, ?> firstRow = (Map<?, ?>) position.get("ROW-1");
        Map<?, ?> secondRow = (Map<?, ?>) position.get("ROW-2");
        Map<?, ?> thirdRow = (Map<?, ?>) position.get("ROW-3");
        Assertions.assertEquals(Collections.singletonList("CHART-12"), firstRow.get("children"));
        Assertions.assertEquals(Collections.singletonList("CHART-34"), secondRow.get("children"));
        Assertions.assertEquals(Collections.singletonList("CHART-56"), thirdRow.get("children"));

        Map<?, ?> firstRowMeta = (Map<?, ?>) firstRow.get("meta");
        Map<?, ?> secondRowMeta = (Map<?, ?>) secondRow.get("meta");
        Map<?, ?> thirdRowMeta = (Map<?, ?>) thirdRow.get("meta");
        Assertions.assertEquals(Math.max(1, Math.round(260 / 8f)), firstRowMeta.get("height"));
        Assertions.assertEquals(Math.max(1, Math.round(300 / 8f)), secondRowMeta.get("height"));
        Assertions.assertEquals(Math.max(1, Math.round(180 / 8f)), thirdRowMeta.get("height"));

        Map<?, ?> secondChart = (Map<?, ?>) position.get("CHART-34");
        Map<?, ?> secondChartMeta = (Map<?, ?>) secondChart.get("meta");
        Assertions.assertEquals(34L, secondChartMeta.get("chartId"));
        Assertions.assertEquals(12, secondChartMeta.get("width"));
        Assertions.assertEquals(Math.max(1, Math.round(300 / 8f)), secondChartMeta.get("height"));
    }

    @Test
    public void testAppendChartToDashboardPositionBuildsLayoutWhenEmpty() {
        SupersetApiClient client = new SupersetApiClient(new SupersetPluginConfig());

        Map<String, Object> position =
                client.appendChartToDashboardPosition(new HashMap<>(), 12L, 260);

        Map<?, ?> grid = (Map<?, ?>) position.get("GRID_ID");
        Assertions.assertEquals(Collections.singletonList("ROW-1"), grid.get("children"));
        Map<?, ?> firstRow = (Map<?, ?>) position.get("ROW-1");
        Assertions.assertEquals(Collections.singletonList("CHART-12"), firstRow.get("children"));
        Map<?, ?> chart = (Map<?, ?>) position.get("CHART-12");
        Map<?, ?> chartMeta = (Map<?, ?>) chart.get("meta");
        Assertions.assertEquals(12L, chartMeta.get("chartId"));
    }

    @Test
    public void testAppendChartToDashboardPositionAppendsAfterExistingRows() {
        SupersetApiClient client = new SupersetApiClient(new SupersetPluginConfig());
        Map<String, Object> base = client.buildDashboardPosition(Collections.singletonList(12L),
                Collections.singletonList(260));

        Map<String, Object> position = client.appendChartToDashboardPosition(base, 34L, 300);

        Map<?, ?> grid = (Map<?, ?>) position.get("GRID_ID");
        Assertions.assertEquals(Arrays.asList("ROW-1", "ROW-2"), grid.get("children"));
        Map<?, ?> secondRow = (Map<?, ?>) position.get("ROW-2");
        Assertions.assertEquals(Collections.singletonList("CHART-34"), secondRow.get("children"));
        Map<?, ?> secondChart = (Map<?, ?>) position.get("CHART-34");
        Map<?, ?> secondChartMeta = (Map<?, ?>) secondChart.get("meta");
        Assertions.assertEquals(34L, secondChartMeta.get("chartId"));
        Assertions.assertEquals(Math.max(1, Math.round(300 / 8f)), secondChartMeta.get("height"));
    }

    @Test
    public void testAppendChartToDashboardLayoutFallsBackToEmptyWhenDashboardReadNotFound()
            throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77",
                HttpStatus.NOT_FOUND, "{\"message\":\"Not found\"}");
        factory.add(HttpMethod.PUT, "http://localhost:8088/api/v1/dashboard/77", HttpStatus.OK,
                "{\"result\":{}}");
        replaceRestTemplate(client, new RestTemplate(factory));

        client.appendChartToDashboardLayout(77L, 12L, 260);

        RecordingRequest putRequest =
                factory.getLastRequest(HttpMethod.PUT, "http://localhost:8088/api/v1/dashboard/77");
        Assertions.assertNotNull(putRequest);
        String body = putRequest.getWrittenBody();
        Assertions.assertTrue(body.contains("\"position_json\""));
        Assertions.assertTrue(body.contains("CHART-12"));
        Assertions.assertTrue(body.contains("ROW-1"));
        Assertions.assertTrue(body.contains("\"css\""));
    }

    @Test
    public void testAddChartToDashboardPreservesExistingDashboardRelations() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/chart/12", HttpStatus.OK,
                "{\"result\":{\"dashboards\":[{\"id\":34},{\"id\":56}]}}");
        factory.add(HttpMethod.PUT, "http://localhost:8088/api/v1/chart/12", HttpStatus.OK,
                "{\"result\":{}}");
        replaceRestTemplate(client, new RestTemplate(factory));

        client.addChartToDashboard(78L, 12L);

        RecordingRequest putRequest =
                factory.getLastRequest(HttpMethod.PUT, "http://localhost:8088/api/v1/chart/12");
        Assertions.assertNotNull(putRequest);
        String body = putRequest.getWrittenBody();
        Assertions.assertTrue(body.contains("\"dashboards\":[34,56,78]"));
    }

    @Test
    public void testSanitizeDashboardTitleRemovesSupersetSuffixes() {
        SupersetApiClient client = new SupersetApiClient(new SupersetPluginConfig());
        String title = client.sanitizeDashboardTitle("访问人数趋势图_supersonic_Superset_90_1");
        Assertions.assertEquals("访问人数趋势图", title);
    }

    @Test
    public void testListDatabasesBuildsExpectedUrl() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RecordingFactory factory = new RecordingFactory("{\"result\":[]}");
        replaceRestTemplate(client, new RestTemplate(factory));

        client.listDatabases("jwt-token", 1, 50);

        Assertions.assertEquals("http://localhost:8088/api/v1/database/?q=(page:1,page_size:50)",
                factory.getLastUri().toString());
        Assertions.assertEquals("Bearer jwt-token",
                factory.getLastRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
    }

    @Test
    public void testListDatabasesAccessTokenRequired() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088");
        SupersetApiClient client = new SupersetApiClient(config);

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> client.listDatabases(" ", 0, 10));
        Assertions.assertTrue(ex.getMessage().contains("access token"));
    }

    @Test
    public void testListDashboardsWithAccessToken() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RecordingFactory factory = new RecordingFactory("{\"result\":[]}");
        replaceRestTemplate(client, new RestTemplate(factory));

        client.listDashboards("token-123");

        Assertions.assertEquals("http://localhost:8088/api/v1/dashboard/?q=(page:0,page_size:200)",
                factory.getLastUri().toString());
        Assertions.assertEquals("Bearer token-123",
                factory.getLastRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
    }

    @Test
    public void testCreateEmptyDashboardUsesWebSessionForDashboardRequests() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        config.setJwtUsername("supersonic");
        config.setJwtPassword("secret");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        String encodedLoginUrl = "http://localhost:8088/login/?next=%2Fsuperset%2Fwelcome%2F";
        String decodedLoginUrl = "http://localhost:8088/login/?next=/superset/welcome/";

        HttpHeaders loginPageHeaders = new HttpHeaders();
        loginPageHeaders.setContentType(MediaType.TEXT_HTML);
        loginPageHeaders.add(HttpHeaders.SET_COOKIE, "session=login-session; Path=/");
        factory.add(HttpMethod.GET, encodedLoginUrl, HttpStatus.OK,
                "<input name=\"csrf_token\" value=\"login-csrf\" />", loginPageHeaders);
        factory.add(HttpMethod.GET, decodedLoginUrl, HttpStatus.OK,
                "<input name=\"csrf_token\" value=\"login-csrf\" />", loginPageHeaders);

        HttpHeaders loginHeaders = new HttpHeaders();
        loginHeaders.add(HttpHeaders.SET_COOKIE, "session=browser-session; Path=/");
        factory.add(HttpMethod.POST, encodedLoginUrl, HttpStatus.FOUND, "", loginHeaders);
        factory.add(HttpMethod.POST, decodedLoginUrl, HttpStatus.FOUND, "", loginHeaders);

        HttpHeaders homeHeaders = new HttpHeaders();
        homeHeaders.setContentType(MediaType.TEXT_HTML);
        factory.add(HttpMethod.GET, "http://localhost:8088/superset/welcome/", HttpStatus.OK,
                "<input name=\"csrf_token\" value=\"page-csrf\" />", homeHeaders);

        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/dashboard/", HttpStatus.OK,
                "{\"result\":{\"id\":77}}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77", HttpStatus.OK,
                "{\"result\":{\"json_metadata\":\"{}\"}}");
        factory.add(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/77/colors?mark_updated=false",
                HttpStatus.OK, "{\"result\":[]}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77/embedded",
                HttpStatus.NOT_FOUND, "{\"message\":\"not found\"}");
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/dashboard/77/embedded",
                HttpStatus.OK, "{\"result\":{\"uuid\":\"embed-77\"}}");
        replaceRestTemplate(client, new RestTemplate(factory));

        SupersetDashboardInfo dashboardInfo =
                client.createEmptyDashboard("supersonic_dashboard", Collections.emptyList());

        Assertions.assertEquals(77L, dashboardInfo.getId());

        RecordingRequest loginRequest =
                firstNonNull(factory.getLastRequest(HttpMethod.POST, encodedLoginUrl),
                        factory.getLastRequest(HttpMethod.POST, decodedLoginUrl),
                        factory.findLastRequestContaining(HttpMethod.POST, "/login/?next="));
        Assertions.assertNotNull(loginRequest);
        Assertions.assertEquals("session=login-session",
                loginRequest.getHeaders().getFirst(HttpHeaders.COOKIE));
        Assertions.assertTrue(loginRequest.getHeaders().getFirst(HttpHeaders.REFERER)
                .startsWith("http://localhost:8088/login/?next="));
        String loginBody = loginRequest.getWrittenBody();
        Assertions.assertTrue(loginBody.contains("username=supersonic"));
        Assertions.assertTrue(loginBody.contains("password=secret"));
        Assertions.assertTrue(loginBody.contains("csrf_token=login-csrf"));

        RecordingRequest dashboardCreate = factory.getLastRequest(HttpMethod.POST,
                "http://localhost:8088/api/v1/dashboard/");
        Assertions.assertNotNull(dashboardCreate);
        Assertions.assertNull(dashboardCreate.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        Assertions.assertEquals("session=browser-session",
                dashboardCreate.getHeaders().getFirst(HttpHeaders.COOKIE));
        Assertions.assertEquals("page-csrf",
                dashboardCreate.getHeaders().getFirst("X-CSRFToken"));
        Assertions.assertEquals("http://localhost:8088/superset/welcome/",
                dashboardCreate.getHeaders().getFirst(HttpHeaders.REFERER));

        RecordingRequest colorRequest = factory.getLastRequest(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/77/colors?mark_updated=false");
        Assertions.assertNotNull(colorRequest);
        Assertions.assertNull(colorRequest.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        Assertions.assertEquals("session=browser-session",
                colorRequest.getHeaders().getFirst(HttpHeaders.COOKIE));
        Assertions.assertEquals("page-csrf", colorRequest.getHeaders().getFirst("X-CSRFToken"));
    }

    @Test
    public void testCreateEmptyDashboardFallsBackToWebSessionAfterJwtDashboardFailure()
            throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        config.setJwtUsername("supersonic");
        config.setJwtPassword("secret");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        String encodedLoginUrl = "http://localhost:8088/login/?next=%2Fsuperset%2Fwelcome%2F";
        String decodedLoginUrl = "http://localhost:8088/login/?next=/superset/welcome/";

        factory.addSequence(HttpMethod.GET, encodedLoginUrl,
                new ResponseSpec(HttpStatus.INTERNAL_SERVER_ERROR,
                        "{\"message\":\"Fatal error\"}".getBytes(), jsonHeaders()),
                new ResponseSpec(HttpStatus.OK,
                        "<input name=\"csrf_token\" value=\"login-csrf\" />".getBytes(),
                        htmlHeaders("session=login-session; Path=/")));
        factory.addSequence(HttpMethod.GET, decodedLoginUrl,
                new ResponseSpec(HttpStatus.INTERNAL_SERVER_ERROR,
                        "{\"message\":\"Fatal error\"}".getBytes(), jsonHeaders()),
                new ResponseSpec(HttpStatus.OK,
                        "<input name=\"csrf_token\" value=\"login-csrf\" />".getBytes(),
                        htmlHeaders("session=login-session; Path=/")));
        factory.addSequence(HttpMethod.POST, encodedLoginUrl,
                new ResponseSpec(HttpStatus.FOUND, "".getBytes(),
                        htmlHeaders("session=browser-session; Path=/")));
        factory.addSequence(HttpMethod.POST, decodedLoginUrl,
                new ResponseSpec(HttpStatus.FOUND, "".getBytes(),
                        htmlHeaders("session=browser-session; Path=/")));
        factory.add(HttpMethod.GET, "http://localhost:8088/superset/welcome/", HttpStatus.OK,
                "<input name=\"csrf_token\" value=\"page-csrf\" />", htmlHeaders(null));

        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/security/login",
                HttpStatus.OK, "{\"access_token\":\"access-token\",\"refresh_token\":\"refresh-token\"}");
        HttpHeaders csrfHeaders = new HttpHeaders();
        csrfHeaders.setContentType(MediaType.APPLICATION_JSON);
        csrfHeaders.add(HttpHeaders.SET_COOKIE, "session=jwt-cookie; Path=/");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/security/csrf_token/",
                HttpStatus.OK, "{\"result\":\"jwt-csrf\"}", csrfHeaders);

        factory.addSequence(HttpMethod.POST, "http://localhost:8088/api/v1/dashboard/",
                new ResponseSpec(HttpStatus.INTERNAL_SERVER_ERROR,
                        "{\"message\":\"Fatal error\"}".getBytes(), jsonHeaders()),
                new ResponseSpec(HttpStatus.OK, "{\"result\":{\"id\":77}}".getBytes(),
                        jsonHeaders()));
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77", HttpStatus.OK,
                "{\"result\":{\"json_metadata\":\"{}\"}}");
        factory.add(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/77/colors?mark_updated=false",
                HttpStatus.OK, "{\"result\":[]}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77/embedded",
                HttpStatus.NOT_FOUND, "{\"message\":\"not found\"}");
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/dashboard/77/embedded",
                HttpStatus.OK, "{\"result\":{\"uuid\":\"embed-77\"}}");
        replaceRestTemplate(client, new RestTemplate(factory));

        SupersetDashboardInfo dashboardInfo =
                client.createEmptyDashboard("supersonic_dashboard", Collections.emptyList());

        Assertions.assertEquals(77L, dashboardInfo.getId());
        Assertions.assertNotNull(factory.getLastRequest(HttpMethod.POST,
                "http://localhost:8088/api/v1/security/login"));
        Assertions.assertNotNull(factory.getLastRequest(HttpMethod.GET,
                "http://localhost:8088/api/v1/security/csrf_token/"));
        RecordingRequest dashboardCreate = factory.getLastRequest(HttpMethod.POST,
                "http://localhost:8088/api/v1/dashboard/");
        Assertions.assertNotNull(dashboardCreate);
        Assertions.assertNull(dashboardCreate.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        Assertions.assertEquals("session=browser-session",
                dashboardCreate.getHeaders().getFirst(HttpHeaders.COOKIE));
        Assertions.assertEquals("page-csrf",
                dashboardCreate.getHeaders().getFirst("X-CSRFToken"));
        Assertions.assertEquals("http://localhost:8088/superset/welcome/",
                dashboardCreate.getHeaders().getFirst(HttpHeaders.REFERER));
    }

    @Test
    public void testAddChartToDashboardUsesJwtAuthForChartRequests() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        config.setJwtUsername("supersonic");
        config.setJwtPassword("secret");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/security/login",
                HttpStatus.OK, "{\"access_token\":\"access-token\",\"refresh_token\":\"refresh-token\"}");

        HttpHeaders csrfHeaders = new HttpHeaders();
        csrfHeaders.setContentType(MediaType.APPLICATION_JSON);
        csrfHeaders.add(HttpHeaders.SET_COOKIE, "session=jwt-cookie; Path=/");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/security/csrf_token/",
                HttpStatus.OK, "{\"result\":\"jwt-csrf\"}", csrfHeaders);

        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/chart/12", HttpStatus.OK,
                "{\"result\":{\"dashboards\":[{\"id\":34},{\"id\":56}]}}");
        factory.add(HttpMethod.PUT, "http://localhost:8088/api/v1/chart/12", HttpStatus.OK,
                "{\"result\":{}}");
        replaceRestTemplate(client, new RestTemplate(factory));

        client.addChartToDashboard(78L, 12L);

        RecordingRequest putRequest =
                factory.getLastRequest(HttpMethod.PUT, "http://localhost:8088/api/v1/chart/12");
        Assertions.assertNotNull(putRequest);
        Assertions.assertEquals("Bearer access-token",
                putRequest.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        Assertions.assertEquals("jwt-csrf", putRequest.getHeaders().getFirst("X-CSRFToken"));
        Assertions.assertEquals("session=jwt-cookie",
                putRequest.getHeaders().getFirst(HttpHeaders.COOKIE));
    }

    @Test
    public void testCreateEmptyDashboardInitializesColorConfig() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/dashboard/", HttpStatus.OK,
                "{\"result\":{\"id\":77}}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77", HttpStatus.OK,
                "{\"result\":{\"json_metadata\":\"{}\"}}");
        factory.add(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/77/colors?mark_updated=false",
                HttpStatus.OK, "{\"result\":[]}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77/embedded",
                HttpStatus.NOT_FOUND, "{\"message\":\"not found\"}");
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/dashboard/77/embedded",
                HttpStatus.OK, "{\"result\":{\"uuid\":\"embed-77\"}}");
        replaceRestTemplate(client, new RestTemplate(factory));

        SupersetDashboardInfo dashboardInfo =
                client.createEmptyDashboard("supersonic_dashboard", Collections.emptyList());

        Assertions.assertEquals(77L, dashboardInfo.getId());
        RecordingRequest colorRequest = factory.getLastRequest(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/77/colors?mark_updated=false");
        Assertions.assertNotNull(colorRequest);
        Map<String, Object> colorPayload =
                JsonUtil.toObject(colorRequest.getWrittenBody(), Map.class);
        Assertions.assertTrue(colorPayload.containsKey("color_namespace"));
        Assertions.assertTrue(colorPayload.containsKey("color_scheme"));
        Assertions.assertEquals(Collections.emptyList(), colorPayload.get("color_scheme_domain"));
        Assertions.assertEquals(Collections.emptyList(), colorPayload.get("shared_label_colors"));
        Assertions.assertEquals(Collections.emptyMap(), colorPayload.get("map_label_colors"));
        Assertions.assertEquals(Collections.emptyMap(), colorPayload.get("label_colors"));
    }

    @Test
    public void testCreateEmbeddedGuestTokenInitializesMissingColorConfig() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/embedded_dashboard/embed-77",
                HttpStatus.OK, "{\"result\":{\"dashboard_id\":\"77\"}}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/77", HttpStatus.OK,
                "{\"result\":{\"json_metadata\":{\"label_colors\":{\"Revenue\":\"#fff\"}}}}");
        factory.add(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/77/colors?mark_updated=false",
                HttpStatus.OK, "{\"result\":[]}");
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/security/guest_token/",
                HttpStatus.OK, "{\"token\":\"guest-token-77\"}");
        replaceRestTemplate(client, new RestTemplate(factory));

        String token = client.createEmbeddedGuestToken("embed-77");

        Assertions.assertEquals("guest-token-77", token);
        RecordingRequest colorRequest = factory.getLastRequest(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/77/colors?mark_updated=false");
        Assertions.assertNotNull(colorRequest);
        Map<String, Object> colorPayload =
                JsonUtil.toObject(colorRequest.getWrittenBody(), Map.class);
        Assertions.assertEquals(Collections.emptyList(), colorPayload.get("color_scheme_domain"));
        Assertions.assertEquals(Collections.emptyList(), colorPayload.get("shared_label_colors"));
        Assertions.assertEquals(Collections.emptyMap(), colorPayload.get("map_label_colors"));
        Assertions.assertEquals(Collections.singletonMap("Revenue", "#fff"),
                colorPayload.get("label_colors"));
    }

    @Test
    public void testCreateEmbeddedGuestTokenSkipsColorInitWhenMetadataComplete() throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/embedded_dashboard/embed-88",
                HttpStatus.OK, "{\"result\":{\"dashboard_id\":\"88\"}}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/88", HttpStatus.OK,
                "{\"result\":{\"json_metadata\":{\"color_namespace\":\"dashboard-88\","
                        + "\"color_scheme\":\"supersetColors\","
                        + "\"color_scheme_domain\":[\"A\"]," + "\"shared_label_colors\":[],"
                        + "\"map_label_colors\":{}," + "\"label_colors\":{}}}}");
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/security/guest_token/",
                HttpStatus.OK, "{\"token\":\"guest-token-88\"}");
        replaceRestTemplate(client, new RestTemplate(factory));

        String token = client.createEmbeddedGuestToken("embed-88");

        Assertions.assertEquals("guest-token-88", token);
        RecordingRequest colorRequest = factory.getLastRequest(HttpMethod.PUT,
                "http://localhost:8088/api/v1/dashboard/88/colors?mark_updated=false");
        Assertions.assertNull(colorRequest);
    }

    @Test
    public void testCreateEmbeddedGuestTokenSkipsWarnWhenDashboardMetadataNotFound()
            throws Exception {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setBaseUrl("http://localhost:8088/");
        SupersetApiClient client = new SupersetApiClient(config);
        RoutingFactory factory = new RoutingFactory();
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/embedded_dashboard/embed-99",
                HttpStatus.OK, "{\"result\":{\"dashboard_id\":\"99\"}}");
        factory.add(HttpMethod.GET, "http://localhost:8088/api/v1/dashboard/99",
                HttpStatus.NOT_FOUND, "{\"message\":\"Not found\"}");
        factory.add(HttpMethod.POST, "http://localhost:8088/api/v1/security/guest_token/",
                HttpStatus.OK, "{\"token\":\"guest-token-99\"}");
        replaceRestTemplate(client, new RestTemplate(factory));

        Logger logger = (Logger) LoggerFactory.getLogger(SupersetApiClient.class);
        Level previousLevel = logger.getLevel();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        appender.start();
        try {
            String token = client.createEmbeddedGuestToken("embed-99");

            Assertions.assertEquals("guest-token-99", token);
            RecordingRequest colorRequest = factory.getLastRequest(HttpMethod.PUT,
                    "http://localhost:8088/api/v1/dashboard/99/colors?mark_updated=false");
            Assertions.assertNull(colorRequest);
            Assertions.assertTrue(appender.list.stream().anyMatch(event ->
                    event.getLevel() == Level.DEBUG
                            && event.getFormattedMessage()
                                    .contains("superset dashboard color init skipped")));
            Assertions.assertFalse(appender.list.stream().anyMatch(event ->
                    event.getLevel() == Level.WARN
                            && event.getFormattedMessage()
                                    .contains("superset dashboard color init failed")));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
        }
    }

    @Test
    public void testBuildQueryContextNormalizesMetricsAndColumns() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("metric", "count");
        formData.put("groupby", Arrays.asList("city"));

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "table");
        Assertions.assertNotNull(context);
        Object datasource = context.get("datasource");
        Assertions.assertTrue(datasource instanceof Map);
        Assertions.assertEquals(12L, ((Map<?, ?>) datasource).get("id"));
        Object queriesValue = context.get("queries");
        Assertions.assertTrue(queriesValue instanceof List);
        List<?> queries = (List<?>) queriesValue;
        Assertions.assertFalse(queries.isEmpty());
        Object firstQuery = queries.get(0);
        Assertions.assertTrue(firstQuery instanceof Map);
        Map<?, ?> query = (Map<?, ?>) firstQuery;
        Assertions.assertTrue(((List<?>) query.get("metrics")).contains("count"));
        Assertions.assertTrue(((List<?>) query.get("columns")).contains("city"));
        Assertions.assertTrue(((List<?>) query.get("groupby")).contains("city"));
        Assertions.assertNotNull(query.get("orderby"));
    }

    @Test
    public void testBuildQueryContextTimeseriesColumns() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("metrics", Arrays.asList("count"));
        formData.put("groupby", Arrays.asList("city"));
        formData.put("granularity_sqla", "imp_date");

        Map<String, Object> context =
                client.buildQueryContext(formData, 12L, "echarts_timeseries_line");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> columns = (List<?>) query.get("columns");
        boolean hasAxis = false;
        for (Object column : columns) {
            if (column instanceof Map) {
                Object sqlExpression = ((Map<?, ?>) column).get("sqlExpression");
                Object columnType = ((Map<?, ?>) column).get("columnType");
                if ("imp_date".equals(sqlExpression) && "BASE_AXIS".equals(columnType)) {
                    hasAxis = true;
                    break;
                }
            }
        }
        Assertions.assertTrue(hasAxis);
        Assertions.assertTrue(columns.contains("city"));
        Assertions.assertEquals(Arrays.asList("city"), query.get("series_columns"));
        Assertions.assertEquals(Boolean.TRUE, query.get("is_timeseries"));
    }

    @Test
    public void testBuildQueryContextSankeyGroupby() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("source", "src");
        formData.put("target", "dst");
        formData.put("metric", "count");

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "sankey_v2");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> groupby = (List<?>) query.get("groupby");
        Assertions.assertTrue(groupby.contains("src"));
        Assertions.assertTrue(groupby.contains("dst"));
    }

    @Test
    public void testBuildQueryContextHistogramDropsMetrics() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("column", "age");
        formData.put("groupby", Arrays.asList("city"));
        formData.put("metrics", Arrays.asList("count"));

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "histogram_v2");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> columns = (List<?>) query.get("columns");
        Assertions.assertTrue(columns.contains("age"));
        Assertions.assertTrue(columns.contains("city"));
        Object metrics = query.get("metrics");
        if (metrics instanceof List) {
            Assertions.assertTrue(((List<?>) metrics).isEmpty());
        } else {
            Assertions.assertNull(metrics);
        }
    }

    @Test
    public void testBuildQueryContextMapboxColumns() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("all_columns_x", "lon");
        formData.put("all_columns_y", "lat");
        formData.put("groupby", Arrays.asList("city"));

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "mapbox");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> columns = (List<?>) query.get("columns");
        Assertions.assertTrue(columns.contains("lon"));
        Assertions.assertTrue(columns.contains("lat"));
        Assertions.assertTrue(columns.contains("city"));
    }

    @Test
    public void testBuildQueryContextParallelCoordinatesMetrics() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("series", Arrays.asList("city"));
        formData.put("metrics", Arrays.asList("count"));
        formData.put("secondary_metric", "sum");

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "para");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> metrics = (List<?>) query.get("metrics");
        Assertions.assertTrue(metrics.contains("count"));
        Assertions.assertTrue(metrics.contains("sum"));
        List<?> columns = (List<?>) query.get("columns");
        Assertions.assertTrue(columns.contains("city"));
    }

    @Test
    public void testBuildQueryContextWorldMapSecondaryMetric() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("entity", "country");
        formData.put("metric", "count");
        formData.put("secondary_metric", "population");
        formData.put("sort_by_metric", true);

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "world_map");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> metrics = (List<?>) query.get("metrics");
        Assertions.assertTrue(metrics.contains("count"));
        Assertions.assertTrue(metrics.contains("population"));
        Object orderby = query.get("orderby");
        Assertions.assertTrue(orderby instanceof List);
        Assertions.assertEquals(1, ((List<?>) orderby).size());
    }

    @Test
    public void testBuildQueryContextPartitionTimeOffsets() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("groupby", Arrays.asList("category"));
        formData.put("metrics", Arrays.asList("count"));
        formData.put("time_compare", Arrays.asList("1 year"));

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "partition");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> timeOffsets = (List<?>) query.get("time_offsets");
        Assertions.assertTrue(timeOffsets.contains("1 year"));
    }

    @Test
    public void testBuildQueryContextCarriesTimeRangeFiltersAndLimits() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> formData = new HashMap<>();
        formData.put("metric", "pv");
        formData.put("groupby", Arrays.asList("department"));
        formData.put("row_limit", 3L);
        formData.put("series_limit", 3L);
        formData.put("order_desc", true);
        formData.put("orderby", Collections.singletonList(Arrays.asList("pv", false)));
        formData.put("time_range", "2026-02-14 : 2026-03-15");
        Map<String, Object> temporalFilter = new HashMap<>();
        temporalFilter.put("clause", "WHERE");
        temporalFilter.put("expressionType", "SIMPLE");
        temporalFilter.put("subject", "imp_date");
        temporalFilter.put("operator", "TEMPORAL_RANGE");
        temporalFilter.put("comparator", "2026-02-14 : 2026-03-15");
        Map<String, Object> normalFilter = new HashMap<>();
        normalFilter.put("clause", "WHERE");
        normalFilter.put("expressionType", "SIMPLE");
        normalFilter.put("subject", "department");
        normalFilter.put("operator", "IN");
        normalFilter.put("comparator", Collections.singletonList("研发部"));
        formData.put("adhoc_filters", Arrays.asList(temporalFilter, normalFilter));

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "table");
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        Assertions.assertEquals(3, ((Number) query.get("row_limit")).intValue());
        Assertions.assertEquals(3, ((Number) query.get("series_limit")).intValue());
        Assertions.assertEquals(Boolean.TRUE, query.get("order_desc"));
        Assertions.assertEquals("2026-02-14 : 2026-03-15", query.get("time_range"));
        Map<?, ?> extras = (Map<?, ?>) query.get("extras");
        Assertions.assertEquals("2026-02-14 : 2026-03-15", extras.get("time_range"));
        List<?> filters = (List<?>) query.get("filters");
        Assertions.assertEquals(1, filters.size());
        Map<?, ?> filter = (Map<?, ?>) filters.get(0);
        Assertions.assertEquals("department", filter.get("col"));
        Assertions.assertEquals("IN", filter.get("op"));
    }

    @Test
    public void testBuildQueryContextSanitizesMetricOptionName() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        SupersetApiClient client = new SupersetApiClient(config);
        Map<String, Object> metric = new HashMap<>();
        metric.put("optionName", "metric_ 访问人数");
        metric.put("expressionType", "SIMPLE");
        metric.put("label", "SUM(访问人数)");
        metric.put("column", Collections.singletonMap("column_name", "访问人数"));
        Map<String, Object> formData = new HashMap<>();
        formData.put("metrics", Collections.singletonList(metric));

        Map<String, Object> context = client.buildQueryContext(formData, 12L, "table");
        Map<?, ?> formDataOut = (Map<?, ?>) context.get("form_data");
        List<?> metrics = (List<?>) formDataOut.get("metrics");
        Map<?, ?> metricOut = (Map<?, ?>) metrics.get(0);
        Assertions.assertEquals("metric_访问人数", metricOut.get("optionName"));
        List<?> queries = (List<?>) context.get("queries");
        Map<?, ?> query = (Map<?, ?>) queries.get(0);
        List<?> queryMetrics = (List<?>) query.get("metrics");
        Map<?, ?> queryMetric = (Map<?, ?>) queryMetrics.get(0);
        Assertions.assertEquals("metric_访问人数", queryMetric.get("optionName"));
    }

    private void replaceRestTemplate(SupersetApiClient client, RestTemplate restTemplate)
            throws Exception {
        Field field = SupersetApiClient.class.getDeclaredField("restTemplate");
        field.setAccessible(true);
        field.set(client, restTemplate);
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        if (values == null) {
            return null;
        }
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static class RecordingFactory implements ClientHttpRequestFactory {
        private final byte[] body;
        private URI lastUri;
        private HttpMethod lastMethod;
        private RecordingRequest lastRequest;

        RecordingFactory(String responseJson) {
            this.body = responseJson.getBytes();
        }

        @Override
        public ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod) {
            this.lastUri = uri;
            this.lastMethod = httpMethod;
            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.setContentType(MediaType.APPLICATION_JSON);
            this.lastRequest =
                    new RecordingRequest(uri, httpMethod, HttpStatus.OK, body, responseHeaders);
            return lastRequest;
        }

        URI getLastUri() {
            return lastUri;
        }

        HttpMethod getLastMethod() {
            return lastMethod;
        }

        RecordingRequest getLastRequest() {
            return lastRequest;
        }
    }

    private static class RoutingFactory implements ClientHttpRequestFactory {
        private final Map<String, List<ResponseSpec>> routes = new LinkedHashMap<>();
        private final Map<String, RecordingRequest> requests = new LinkedHashMap<>();

        void add(HttpMethod method, String url, HttpStatus status, String responseJson) {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            add(method, url, status, responseJson, headers);
        }

        void add(HttpMethod method, String url, HttpStatus status, String responseBody,
                HttpHeaders headers) {
            addSequence(method, url, new ResponseSpec(status, responseBody.getBytes(), headers));
        }

        void addSequence(HttpMethod method, String url, ResponseSpec... specs) {
            String key = buildKey(method, url);
            List<ResponseSpec> values = routes.computeIfAbsent(key, ignored -> new ArrayList<>());
            values.addAll(Arrays.asList(specs));
        }

        RecordingRequest getLastRequest(HttpMethod method, String url) {
            String key = buildKey(method, url);
            RecordingRequest request = requests.get(key);
            if (request != null) {
                return request;
            }
            String decodedUrl = decodeUrl(url);
            return requests.get(buildKey(method, decodedUrl));
        }

        RecordingRequest findLastRequestContaining(HttpMethod method, String fragment) {
            for (Map.Entry<String, RecordingRequest> entry : requests.entrySet()) {
                if (!entry.getKey().startsWith(method + " ")) {
                    continue;
                }
                if (entry.getKey().contains(fragment)) {
                    return entry.getValue();
                }
            }
            return null;
        }

        @Override
        public ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod) {
            String requestUrl = uri.toString();
            String key = buildKey(httpMethod, requestUrl);
            List<ResponseSpec> specs = routes.get(key);
            ResponseSpec spec = pollSpec(specs);
            if (spec == null) {
                specs = routes.get(buildKey(httpMethod, decodeUrl(requestUrl)));
                spec = pollSpec(specs);
            }
            if (spec == null) {
                specs = routes.get(buildKey(httpMethod, decodeUrl(decodeUrl(requestUrl))));
                spec = pollSpec(specs);
            }
            if (spec == null) {
                spec = new ResponseSpec(HttpStatus.OK, "{}".getBytes(), new HttpHeaders());
            }
            RecordingRequest request =
                    new RecordingRequest(uri, httpMethod, spec.status, spec.body, spec.headers);
            requests.put(key, request);
            return request;
        }

        private String buildKey(HttpMethod method, String url) {
            return method + " " + url;
        }

        private String decodeUrl(String url) {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        }

        private ResponseSpec pollSpec(List<ResponseSpec> specs) {
            if (specs == null || specs.isEmpty()) {
                return null;
            }
            if (specs.size() == 1) {
                return specs.get(0);
            }
            return specs.remove(0);
        }
    }

    private static class ResponseSpec {
        private final HttpStatus status;
        private final byte[] body;
        private final HttpHeaders headers;

        private ResponseSpec(HttpStatus status, byte[] body, HttpHeaders headers) {
            this.status = status;
            this.body = body;
            this.headers = headers;
        }
    }

    private static HttpHeaders jsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    private static HttpHeaders htmlHeaders(String cookie) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_HTML);
        if (cookie != null) {
            headers.add(HttpHeaders.SET_COOKIE, cookie);
        }
        return headers;
    }

    private static class RecordingRequest implements ClientHttpRequest {
        private final URI uri;
        private final HttpMethod method;
        private final HttpStatus status;
        private final byte[] responseBody;
        private final HttpHeaders responseHeaders;
        private final HttpHeaders headers = new HttpHeaders();
        private final ByteArrayOutputStream requestBody = new ByteArrayOutputStream();

        RecordingRequest(URI uri, HttpMethod method, HttpStatus status, byte[] responseBody,
                HttpHeaders responseHeaders) {
            this.uri = uri;
            this.method = method;
            this.status = status;
            this.responseBody = responseBody;
            this.responseHeaders = responseHeaders;
        }

        @Override
        public ClientHttpResponse execute() {
            return new ClientHttpResponse() {
                @Override
                public HttpStatus getStatusCode() {
                    return status;
                }

                @Override
                public int getRawStatusCode() {
                    return status.value();
                }

                @Override
                public String getStatusText() {
                    return status.getReasonPhrase();
                }

                @Override
                public void close() {}

                @Override
                public InputStream getBody() {
                    return new ByteArrayInputStream(responseBody);
                }

                @Override
                public HttpHeaders getHeaders() {
                    return responseHeaders;
                }
            };
        }

        @Override
        public OutputStream getBody() {
            return requestBody;
        }

        @Override
        public HttpMethod getMethod() {
            return method;
        }

        @Override
        public URI getURI() {
            return uri;
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }

        String getWrittenBody() {
            return requestBody.toString();
        }
    }
}
