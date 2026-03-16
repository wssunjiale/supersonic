package com.tencent.supersonic.headless.server.sync.superset;

import com.tencent.supersonic.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SupersetSyncApiClient implements SupersetSyncClient {

    private static final String DATASET_API = "/api/v1/dataset/";
    private static final String DATABASE_API = "/api/v1/database/";
    private static final String LOGIN_API = "/api/v1/security/login";
    private static final String REFRESH_API = "/api/v1/security/refresh";
    private static final String CSRF_API = "/api/v1/security/csrf_token/";
    private static final String AUTH_STRATEGY_API_KEY_FIRST = "API_KEY_FIRST";

    private final SupersetSyncProperties properties;
    private final RestTemplate restTemplate;
    private final String baseUrl;
    private JwtSession jwtSession;
    private volatile String supersetVersion;

    private static class JwtSession {
        private String accessToken;
        private String refreshToken;
        private String csrfToken;
        private String cookie;
    }

    public SupersetSyncApiClient(SupersetSyncProperties properties) {
        this.properties = properties;
        int timeoutMs = Math.max(1,
                properties.getTimeoutSeconds() == null ? 30 : properties.getTimeoutSeconds())
                * 1000;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        this.restTemplate = new RestTemplate(factory);
        this.baseUrl = normalizeBaseUrl(properties.getBaseUrl());
    }

    @Override
    public String getSupersetVersion() {
        if (StringUtils.isNotBlank(supersetVersion)) {
            return supersetVersion;
        }
        synchronized (this) {
            if (StringUtils.isNotBlank(supersetVersion)) {
                return supersetVersion;
            }
            String resolved = resolveSupersetVersion();
            if (StringUtils.isNotBlank(resolved)) {
                supersetVersion = resolved;
            }
            return supersetVersion;
        }
    }

    @Override
    public List<SupersetDatabaseInfo> listDatabases() {
        Map<String, Object> response = get(DATABASE_API + "?q=(page:0,page_size:500)");
        List<Map<String, Object>> result = resolveResultList(response);
        if (result == null) {
            return Collections.emptyList();
        }
        List<SupersetDatabaseInfo> databases = new ArrayList<>();
        for (Map<String, Object> item : result) {
            SupersetDatabaseInfo info = new SupersetDatabaseInfo();
            info.setId(parseLong(resolveValue(item, "id")));
            info.setName(resolveString(item, "database_name"));
            if (info.getId() != null || StringUtils.isNotBlank(info.getName())) {
                databases.add(info);
            }
        }
        return databases;
    }

    @Override
    public SupersetDatabaseInfo fetchDatabase(Long id) {
        if (id == null) {
            return null;
        }
        Map<String, Object> response = get(DATABASE_API + id);
        Map<String, Object> result = resolveResult(response);
        SupersetDatabaseInfo info = new SupersetDatabaseInfo();
        info.setId(parseLong(resolveValue(result, "id")));
        info.setName(resolveString(result, "database_name"));
        String sqlalchemyUri = resolveString(result, "sqlalchemy_uri");
        if (StringUtils.isBlank(sqlalchemyUri)) {
            sqlalchemyUri = resolveString(result, "sqlalchemy_uri_safe");
        }
        info.setSqlalchemyUri(sqlalchemyUri);
        info.setSchema(resolveString(result, "force_ctas_schema"));
        return info;
    }

    @Override
    public Long createDatabase(SupersetDatabaseInfo databaseInfo) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("database_name", databaseInfo.getName());
        payload.put("sqlalchemy_uri", databaseInfo.getSqlalchemyUri());
        Map<String, Object> response = post(DATABASE_API, payload);
        Long id = parseLong(resolveValue(response, "id"));
        if (id == null) {
            Map<String, Object> result = resolveResult(response);
            id = parseLong(resolveValue(result, "id"));
        }
        return id;
    }

    @Override
    public void updateDatabase(Long id, SupersetDatabaseInfo databaseInfo) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("database_name", databaseInfo.getName());
        payload.put("sqlalchemy_uri", databaseInfo.getSqlalchemyUri());
        put(DATABASE_API + id, payload);
    }

    @Override
    public List<SupersetDatasetInfo> listDatasets() {
        Map<String, Object> response = get(DATASET_API + "?q=(page:0,page_size:500)");
        List<Map<String, Object>> result = resolveResultList(response);
        if (result == null) {
            return Collections.emptyList();
        }
        List<SupersetDatasetInfo> datasets = new ArrayList<>();
        for (Map<String, Object> item : result) {
            if (item == null) {
                continue;
            }
            SupersetDatasetInfo info = new SupersetDatasetInfo();
            info.setId(parseLong(resolveValue(item, "id")));
            info.setDescription(resolveString(item, "description"));
            info.setTableName(resolveString(item, "table_name"));
            info.setSchema(resolveString(item, "schema"));
            info.setSql(resolveString(item, "sql"));
            Map<String, Object> database = resolveMap(item, "database");
            info.setDatabaseId(parseLong(resolveValue(database, "id")));
            if (info.getId() != null || StringUtils.isNotBlank(info.getTableName())) {
                datasets.add(info);
            }
        }
        return datasets;
    }

    @Override
    public SupersetDatasetInfo fetchDataset(Long id) {
        if (id == null) {
            return null;
        }
        Map<String, Object> response = get(DATASET_API + id);
        Map<String, Object> result = resolveResult(response);
        if (result == null) {
            return null;
        }
        SupersetDatasetInfo info = new SupersetDatasetInfo();
        info.setId(parseLong(resolveValue(result, "id")));
        info.setDescription(resolveString(result, "description"));
        info.setTableName(resolveString(result, "table_name"));
        info.setSchema(resolveString(result, "schema"));
        info.setSql(resolveString(result, "sql"));
        info.setMainDttmCol(resolveString(result, "main_dttm_col"));
        Map<String, Object> database = resolveMap(result, "database");
        info.setDatabaseId(parseLong(resolveValue(database, "id")));
        info.setColumns(parseColumns(resolveList(result, "columns")));
        info.setMetrics(parseMetrics(resolveList(result, "metrics")));
        return info;
    }

    @Override
    public Long createDataset(SupersetDatasetInfo datasetInfo) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("database", datasetInfo.getDatabaseId());
        payload.put("schema", datasetInfo.getSchema());
        payload.put("table_name", datasetInfo.getTableName());
        String sql = StringUtils.trimToNull(datasetInfo.getSql());
        if (StringUtils.isNotBlank(sql)) {
            payload.put("sql", sql);
        }
        payload.put("template_params", "{}");
        Map<String, Object> response = post(DATASET_API, payload);
        Long id = parseLong(resolveValue(response, "id"));
        if (id == null) {
            Map<String, Object> result = resolveResult(response);
            id = parseLong(resolveValue(result, "id"));
        }
        return id;
    }

    @Override
    public void updateDataset(Long id, SupersetDatasetInfo datasetInfo) {
        put(DATASET_API + id, buildDatasetUpdatePayload(datasetInfo));
    }

    @Override
    public void deleteDataset(Long id) {
        if (id == null) {
            return;
        }
        delete(DATASET_API + id);
    }

    @Override
    public void deleteDatasetColumn(Long datasetId, Long columnId) {
        if (datasetId == null || columnId == null) {
            return;
        }
        delete(DATASET_API + datasetId + "/column/" + columnId);
    }

    @Override
    public void deleteDatasetMetric(Long datasetId, Long metricId) {
        if (datasetId == null || metricId == null) {
            return;
        }
        delete(DATASET_API + datasetId + "/metric/" + metricId);
    }

    private Map<String, Object> get(String path) {
        return request(HttpMethod.GET, path, null);
    }

    private Map<String, Object> post(String path, Object body) {
        return request(HttpMethod.POST, path, body);
    }

    private Map<String, Object> put(String path, Object body) {
        return request(HttpMethod.PUT, path, body);
    }

    private Map<String, Object> delete(String path) {
        return request(HttpMethod.DELETE, path, null);
    }

    private Map<String, Object> request(HttpMethod method, String path, Object body) {
        AuthHeaders authHeaders = resolveAuthHeaders(method);
        log.debug("superset sync request, method={}, path={}, authMode={}", method, path,
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
                if (StringUtils.isNotBlank(properties.getApiKey())) {
                    log.warn("superset jwt request failed, fallback to api key: {}",
                            ex.getStatusCode());
                    return execute(method, path, body, buildHeaders());
                }
            }
            throw ex;
        }
    }

    private Map<String, Object> execute(HttpMethod method, String path, Object body,
            HttpHeaders headers) {
        String url = baseUrl + path;
        HttpEntity<String> entity = body == null ? new HttpEntity<>(headers)
                : new HttpEntity<>(JsonUtil.toString(body), headers);
        ResponseEntity<Object> response = restTemplate.exchange(url, method, entity, Object.class);
        captureSupersetVersion(response.getHeaders());
        log.debug("superset sync response, method={}, path={}, status={}", method, path,
                response.getStatusCode());
        return JsonUtil.objectToMap(response.getBody());
    }

    private AuthHeaders resolveAuthHeaders(HttpMethod method) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (!properties.isAuthEnabled()) {
            return new AuthHeaders(headers, AuthMode.NONE);
        }
        boolean jwtFirst =
                !AUTH_STRATEGY_API_KEY_FIRST.equalsIgnoreCase(properties.getAuthStrategy());
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

    private HttpHeaders buildHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (properties.isAuthEnabled() && StringUtils.isNotBlank(properties.getApiKey())) {
            headers.set("Authorization", "Bearer " + properties.getApiKey());
        }
        return headers;
    }

    private AuthHeaders tryBuildApiKeyHeaders(HttpHeaders headers) {
        if (StringUtils.isBlank(properties.getApiKey())) {
            return null;
        }
        headers.set("Authorization", "Bearer " + properties.getApiKey());
        return new AuthHeaders(headers, AuthMode.API_KEY);
    }

    private AuthHeaders tryBuildJwtHeaders(HttpMethod method) {
        if (StringUtils.isBlank(properties.getJwtUsername())
                || StringUtils.isBlank(properties.getJwtPassword())) {
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

    private void loginJwt() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("username", properties.getJwtUsername());
        payload.put("password", properties.getJwtPassword());
        payload.put("provider", StringUtils.defaultIfBlank(properties.getJwtProvider(), "db"));
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

    private boolean requiresCsrf(HttpMethod method) {
        return method != HttpMethod.GET;
    }

    private boolean canFallback(HttpStatusCodeException ex) {
        return ex.getStatusCode().value() == 401 || ex.getStatusCode().value() == 403;
    }

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

    private String resolveSupersetVersion() {
        return null;
    }

    private Map<String, Object> safeRequest(HttpMethod method, String path, Object body) {
        try {
            return request(method, path, body);
        } catch (Exception ex) {
            log.debug("superset version request failed, path={}", path, ex);
            return null;
        }
    }

    private String extractVersion(Map<String, Object> response) {
        if (response == null) {
            return null;
        }
        String version = resolveString(response, "version");
        if (StringUtils.isBlank(version)) {
            version = resolveString(response, "superset_version");
        }
        if (StringUtils.isBlank(version)) {
            version = resolveString(response, "version_string");
        }
        if (StringUtils.isBlank(version)) {
            Map<String, Object> result = resolveResult(response);
            if (result != null && result != response) {
                version = resolveString(result, "version");
                if (StringUtils.isBlank(version)) {
                    version = resolveString(result, "superset_version");
                }
                if (StringUtils.isBlank(version)) {
                    version = resolveString(result, "version_string");
                }
            }
        }
        return StringUtils.trimToNull(version);
    }

    private void captureSupersetVersion(HttpHeaders headers) {
        if (headers == null || StringUtils.isNotBlank(supersetVersion)) {
            return;
        }
        String version = headers.getFirst("X-Superset-Version");
        if (StringUtils.isBlank(version)) {
            version = headers.getFirst("Superset-Version");
        }
        if (StringUtils.isBlank(version)) {
            version = headers.getFirst("X-Server-Version");
        }
        if (StringUtils.isBlank(version)) {
            version = headers.getFirst("X-Version");
        }
        if (StringUtils.isNotBlank(version)) {
            supersetVersion = version.trim();
        }
    }

    private Map<String, Object> buildDatasetUpdatePayload(SupersetDatasetInfo datasetInfo) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("database_id", datasetInfo.getDatabaseId());
        payload.put("schema", datasetInfo.getSchema());
        payload.put("table_name", datasetInfo.getTableName());
        String sql = StringUtils.trimToNull(datasetInfo.getSql());
        if (StringUtils.isNotBlank(sql)) {
            payload.put("sql", sql);
        }
        payload.put("template_params", "{}");
        if (StringUtils.isNotBlank(datasetInfo.getMainDttmCol())) {
            payload.put("main_dttm_col", datasetInfo.getMainDttmCol());
        }
        if (datasetInfo.getColumns() != null && !datasetInfo.getColumns().isEmpty()) {
            payload.put("columns", datasetInfo.getColumns().stream()
                    .map(SupersetDatasetColumn::toPayload).collect(Collectors.toList()));
        }
        if (datasetInfo.getMetrics() != null && !datasetInfo.getMetrics().isEmpty()) {
            payload.put("metrics", datasetInfo.getMetrics().stream()
                    .map(SupersetDatasetMetric::toPayload).collect(Collectors.toList()));
        }
        return payload;
    }

    private List<SupersetDatasetColumn> parseColumns(List<Map<String, Object>> columns) {
        if (columns == null) {
            return Collections.emptyList();
        }
        List<SupersetDatasetColumn> result = new ArrayList<>();
        for (Map<String, Object> column : columns) {
            if (column == null) {
                continue;
            }
            SupersetDatasetColumn info = new SupersetDatasetColumn();
            info.setId(parseLong(resolveValue(column, "id")));
            info.setColumnName(resolveString(column, "column_name"));
            info.setExpression(resolveString(column, "expression"));
            info.setType(resolveString(column, "type"));
            info.setIsDttm(resolveBoolean(column, "is_dttm"));
            info.setFilterable(resolveBoolean(column, "filterable"));
            info.setGroupby(resolveBoolean(column, "groupby"));
            info.setIsActive(resolveBoolean(column, "is_active"));
            info.setVerboseName(resolveString(column, "verbose_name"));
            info.setDescription(resolveString(column, "description"));
            info.setPythonDateFormat(resolveString(column, "python_date_format"));
            if (StringUtils.isNotBlank(info.getColumnName()) || info.getId() != null) {
                result.add(info);
            }
        }
        return result;
    }

    private List<SupersetDatasetMetric> parseMetrics(List<Map<String, Object>> metrics) {
        if (metrics == null) {
            return Collections.emptyList();
        }
        List<SupersetDatasetMetric> result = new ArrayList<>();
        for (Map<String, Object> metric : metrics) {
            if (metric == null) {
                continue;
            }
            SupersetDatasetMetric info = new SupersetDatasetMetric();
            info.setId(parseLong(resolveValue(metric, "id")));
            info.setMetricName(resolveString(metric, "metric_name"));
            info.setExpression(resolveString(metric, "expression"));
            info.setMetricType(resolveString(metric, "metric_type"));
            info.setVerboseName(resolveString(metric, "verbose_name"));
            info.setDescription(resolveString(metric, "description"));
            if (StringUtils.isNotBlank(info.getMetricName()) || info.getId() != null) {
                result.add(info);
            }
        }
        return result;
    }

    private Map<String, Object> resolveResult(Map<String, Object> response) {
        Map<String, Object> result = resolveMap(response, "result");
        return result == null ? response : result;
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

    private String resolveString(Map<String, Object> map, String key) {
        Object value = resolveValue(map, key);
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    private Boolean resolveBoolean(Map<String, Object> map, String key) {
        Object value = resolveValue(map, key);
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(String.valueOf(value));
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

    private String normalizeBaseUrl(String baseUrl) {
        if (StringUtils.isBlank(baseUrl)) {
            return "";
        }
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
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
}
