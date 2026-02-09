package com.tencent.supersonic.chat.server.plugin.build.superset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
        result.add(dashboard);
        response.put("result", result);

        List<SupersetDashboardInfo> dashboards = client.extractDashboards(response);
        Assertions.assertEquals(1, dashboards.size());
        Assertions.assertEquals(12L, dashboards.get(0).getId());
        Assertions.assertEquals("Sales Overview", dashboards.get(0).getTitle());
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

        Assertions.assertEquals(
                "http://localhost:8088/api/v1/dashboard/?q=(page:0,page_size:200)",
                factory.getLastUri().toString());
        Assertions.assertEquals("Bearer token-123",
                factory.getLastRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
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
            this.lastRequest = new RecordingRequest(uri, httpMethod, body);
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

    private static class RecordingRequest implements ClientHttpRequest {
        private final URI uri;
        private final HttpMethod method;
        private final byte[] responseBody;
        private final HttpHeaders headers = new HttpHeaders();

        RecordingRequest(URI uri, HttpMethod method, byte[] responseBody) {
            this.uri = uri;
            this.method = method;
            this.responseBody = responseBody;
        }

        @Override
        public ClientHttpResponse execute() {
            return new ClientHttpResponse() {
                @Override
                public HttpStatus getStatusCode() {
                    return HttpStatus.OK;
                }

                @Override
                public int getRawStatusCode() {
                    return HttpStatus.OK.value();
                }

                @Override
                public String getStatusText() {
                    return HttpStatus.OK.getReasonPhrase();
                }

                @Override
                public void close() {}

                @Override
                public InputStream getBody() {
                    return new ByteArrayInputStream(responseBody);
                }

                @Override
                public HttpHeaders getHeaders() {
                    HttpHeaders responseHeaders = new HttpHeaders();
                    responseHeaders.setContentType(MediaType.APPLICATION_JSON);
                    return responseHeaders;
                }
            };
        }

        @Override
        public OutputStream getBody() throws IOException {
            return OutputStream.nullOutputStream();
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
    }
}
