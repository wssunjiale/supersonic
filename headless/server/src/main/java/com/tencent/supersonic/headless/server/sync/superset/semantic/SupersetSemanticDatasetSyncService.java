package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.MetaFilter;
import com.tencent.supersonic.headless.api.pojo.response.DataSetResp;
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.service.DataSetService;
import com.tencent.supersonic.headless.server.service.DatabaseService;
import com.tencent.supersonic.headless.server.service.SupersetDatasetRegistryService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatabaseInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetType;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncClient;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncProperties;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncResult;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncStats;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncTrigger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpStatusCodeException;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SupersetSemanticDatasetSyncService {

    private static final int NAME_LIMIT = 250;

    private final DataSetService dataSetService;
    private final DatabaseService databaseService;
    private final SupersetSemanticDatasetMapper mapper;
    private final SupersetSemanticDatasetRegistry registry;
    private final SupersetDatasetRegistryService registryService;
    private final SupersetSyncService supersetSyncService;
    private final SupersetSyncClient syncClient;
    private final SupersetSyncProperties properties;

    public SupersetSemanticDatasetSyncService(DataSetService dataSetService,
            DatabaseService databaseService, SupersetSemanticDatasetMapper mapper,
            SupersetSemanticDatasetRegistry registry,
            SupersetDatasetRegistryService registryService, SupersetSyncService supersetSyncService,
            SupersetSyncClient syncClient, SupersetSyncProperties properties) {
        this.dataSetService = dataSetService;
        this.databaseService = databaseService;
        this.mapper = mapper;
        this.registry = registry;
        this.registryService = registryService;
        this.supersetSyncService = supersetSyncService;
        this.syncClient = syncClient;
        this.properties = properties;
    }

    /**
     * Sync all Supersonic semantic datasets (DataSet) into Superset datasets. This is an on-demand
     * workflow and does not replace the existing chat-SQL dataset flow.
     */
    public SupersetSyncResult syncAllSemanticDatasets(User user) {
        long start = System.currentTimeMillis();
        SupersetSyncStats stats = new SupersetSyncStats();

        if (!properties.isEnabled()) {
            return SupersetSyncResult.success("Superset 未启用，跳过同步",
                    System.currentTimeMillis() - start, stats);
        }
        if (StringUtils.isBlank(properties.getBaseUrl())) {
            return SupersetSyncResult.failure("Superset 地址未配置", System.currentTimeMillis() - start,
                    stats);
        }

        User safeUser = user == null ? User.getDefaultUser() : user;
        List<DataSetResp> dataSets = dataSetService.getDataSetList(new MetaFilter());
        if (CollectionUtils.isEmpty(dataSets)) {
            return SupersetSyncResult.success("未找到 Supersonic DataSet，跳过同步",
                    System.currentTimeMillis() - start, stats);
        }

        List<SupersetDatasetDO> records = new ArrayList<>();
        for (DataSetResp dataSet : dataSets) {
            SemanticDatasetMapping mapping = mapper.buildMapping(dataSet, safeUser);
            if (mapping == null) {
                stats.incSkipped();
                continue;
            }
            SupersetDatasetDO record = registry.upsert(mapping, safeUser);
            if (record == null) {
                stats.incFailed();
                continue;
            }
            records.add(record);
        }

        if (records.isEmpty()) {
            return SupersetSyncResult.success("Supersonic DataSet 映射为空，跳过同步",
                    System.currentTimeMillis() - start, stats);
        }

        Set<Long> registryIds = records.stream().map(SupersetDatasetDO::getId)
                .filter(Objects::nonNull).collect(Collectors.toSet());
        SupersetSyncResult result =
                supersetSyncService.triggerDatasetSync(registryIds, SupersetSyncTrigger.MANUAL);
        if (result == null) {
            return SupersetSyncResult.failure("Superset 语义数据集同步失败",
                    System.currentTimeMillis() - start, stats);
        }
        String message = result.isSuccess() ? "Superset 语义数据集同步完成" : "Superset 语义数据集同步失败";
        return result.isSuccess()
                ? SupersetSyncResult.success(message, System.currentTimeMillis() - start,
                        result.getStats())
                : SupersetSyncResult.failure(message, System.currentTimeMillis() - start,
                        result.getStats());
    }

    private Map<Long, Long> ensureDatabaseMapping(Set<Long> databaseIds) {
        if (databaseIds == null || databaseIds.isEmpty()) {
            return Collections.emptyMap();
        }
        List<SupersetDatabaseInfo> supersetDatabases = syncClient.listDatabases();
        Map<String, SupersetDatabaseInfo> byName = supersetDatabases.stream()
                .filter(item -> item != null && StringUtils.isNotBlank(item.getName())).collect(
                        Collectors.toMap(SupersetDatabaseInfo::getName, item -> item, (l, r) -> l));
        Map<Long, Long> mapping = new HashMap<>();
        for (Long databaseId : databaseIds) {
            DatabaseResp db = databaseService.getDatabase(databaseId);
            if (db == null) {
                continue;
            }
            SupersetDatabaseInfo expected = buildDatabaseInfo(db);
            if (expected == null) {
                continue;
            }
            SupersetDatabaseInfo existing = byName.get(expected.getName());
            if (existing == null || existing.getId() == null) {
                try {
                    Long createdId = syncClient.createDatabase(expected);
                    if (createdId != null) {
                        mapping.put(databaseId, createdId);
                    }
                } catch (Exception ex) {
                    log.warn("superset semantic database create failed, dbId={}, name={}",
                            databaseId, expected.getName(), ex);
                }
                continue;
            }
            mapping.put(databaseId, existing.getId());
        }
        return mapping;
    }

    private SupersetDatabaseInfo buildDatabaseInfo(DatabaseResp databaseResp) {
        if (databaseResp == null) {
            return null;
        }
        String sqlalchemyUri = buildSqlalchemyUri(databaseResp);
        if (StringUtils.isBlank(sqlalchemyUri)) {
            return null;
        }
        SupersetDatabaseInfo info = new SupersetDatabaseInfo();
        info.setName(buildSupersetDatabaseName(databaseResp));
        info.setSqlalchemyUri(sqlalchemyUri);
        info.setSchema(resolveSchema(databaseResp));
        return info;
    }

    private String buildSupersetDatabaseName(DatabaseResp databaseResp) {
        String suffix = StringUtils.defaultString(databaseResp.getName(), "database");
        String name = String.format("supersonic_db_%s_%s", databaseResp.getId(), suffix);
        return truncateName(name);
    }

    private String truncateName(String name) {
        if (StringUtils.isBlank(name)) {
            return name;
        }
        if (name.length() <= NAME_LIMIT) {
            return name;
        }
        return name.substring(0, NAME_LIMIT);
    }

    private String datasetKey(SupersetDatasetInfo info) {
        String name = info == null ? null : info.getTableName();
        Long databaseId = info == null ? null : info.getDatabaseId();
        return String.format("%s:%s", databaseId, StringUtils.defaultString(name));
    }

    private SupersetDatasetInfo buildDatasetInfo(SupersetDatasetDO dataset, Long databaseId) {
        if (dataset == null || databaseId == null) {
            return null;
        }
        SupersetDatasetInfo info = new SupersetDatasetInfo();
        info.setId(dataset.getSupersetDatasetId());
        info.setDatabaseId(databaseId);
        info.setSchema(StringUtils.defaultIfBlank(dataset.getSchemaName(), null));

        String sqlText = StringUtils.trimToNull(dataset.getSqlText());
        if (StringUtils.isBlank(sqlText)) {
            sqlText = StringUtils.trimToNull(dataset.getNormalizedSql());
        }
        boolean virtual =
                SupersetDatasetType.VIRTUAL.name().equalsIgnoreCase(dataset.getDatasetType());
        String tableName = dataset.getTableName();
        if (StringUtils.isBlank(tableName) && StringUtils.isNotBlank(sqlText)) {
            virtual = true;
        }
        if (virtual) {
            tableName = StringUtils.defaultIfBlank(tableName, dataset.getDatasetName());
            if (StringUtils.isBlank(sqlText)) {
                log.warn("superset semantic dataset skip, sql missing, id={}, name={}",
                        dataset.getId(), dataset.getDatasetName());
                return null;
            }
            info.setSql(sqlText);
        } else {
            if (StringUtils.isBlank(tableName)) {
                return null;
            }
            info.setSql(null);
        }
        info.setTableName(tableName);
        info.setMainDttmCol(dataset.getMainDttmCol());
        info.setColumns(parseColumns(dataset.getColumns()));
        info.setMetrics(parseMetrics(dataset.getMetrics()));
        return info;
    }

    private List<SupersetDatasetColumn> parseColumns(String json) {
        return JsonUtil.toList(json, SupersetDatasetColumn.class);
    }

    private List<SupersetDatasetMetric> parseMetrics(String json) {
        return JsonUtil.toList(json, SupersetDatasetMetric.class);
    }

    private SupersetDatasetInfo mergeDatasetSchema(SupersetDatasetInfo expected,
            SupersetDatasetInfo current, boolean rebuild) {
        if (expected == null || current == null) {
            return expected;
        }
        expected.setId(current.getId());
        mergeColumns(expected, current, rebuild);
        mergeMetrics(expected, current, rebuild);
        return expected;
    }

    private void mergeColumns(SupersetDatasetInfo expected, SupersetDatasetInfo current,
            boolean rebuild) {
        if (CollectionUtils.isEmpty(expected.getColumns())) {
            expected.setColumns(
                    current.getColumns() == null ? Collections.emptyList() : current.getColumns());
            return;
        }
        Map<String, SupersetDatasetColumn> currentMap = toColumnMap(current.getColumns());
        List<SupersetDatasetColumn> merged = new ArrayList<>();
        Set<String> expectedKeys = new HashSet<>();
        for (SupersetDatasetColumn column : expected.getColumns()) {
            String key = normalizeName(column.getColumnName());
            expectedKeys.add(key);
            SupersetDatasetColumn existing = currentMap.get(key);
            if (existing != null) {
                column.setId(existing.getId());
            }
            merged.add(column);
        }
        if (rebuild) {
            removeOrphanColumns(current, expectedKeys);
        } else {
            appendOrphanColumns(merged, current.getColumns(), expectedKeys);
        }
        expected.setColumns(merged);
    }

    private void mergeMetrics(SupersetDatasetInfo expected, SupersetDatasetInfo current,
            boolean rebuild) {
        if (CollectionUtils.isEmpty(expected.getMetrics())) {
            expected.setMetrics(
                    current.getMetrics() == null ? Collections.emptyList() : current.getMetrics());
            return;
        }
        Map<String, SupersetDatasetMetric> currentMap = toMetricMap(current.getMetrics());
        List<SupersetDatasetMetric> merged = new ArrayList<>();
        Set<String> expectedKeys = new HashSet<>();
        for (SupersetDatasetMetric metric : expected.getMetrics()) {
            String key = normalizeName(metric.getMetricName());
            expectedKeys.add(key);
            SupersetDatasetMetric existing = currentMap.get(key);
            if (existing != null) {
                metric.setId(existing.getId());
            }
            merged.add(metric);
        }
        if (rebuild) {
            removeOrphanMetrics(current, expectedKeys);
        } else {
            appendOrphanMetrics(merged, current.getMetrics(), expectedKeys);
        }
        expected.setMetrics(merged);
    }

    private void removeOrphanColumns(SupersetDatasetInfo current, Set<String> expectedKeys) {
        if (current == null || current.getId() == null
                || CollectionUtils.isEmpty(current.getColumns())) {
            return;
        }
        for (SupersetDatasetColumn column : current.getColumns()) {
            String key = normalizeName(column.getColumnName());
            if (!expectedKeys.contains(key) && column.getId() != null) {
                syncClient.deleteDatasetColumn(current.getId(), column.getId());
            }
        }
    }

    private void removeOrphanMetrics(SupersetDatasetInfo current, Set<String> expectedKeys) {
        if (current == null || current.getId() == null
                || CollectionUtils.isEmpty(current.getMetrics())) {
            return;
        }
        for (SupersetDatasetMetric metric : current.getMetrics()) {
            String key = normalizeName(metric.getMetricName());
            if (!expectedKeys.contains(key) && metric.getId() != null) {
                syncClient.deleteDatasetMetric(current.getId(), metric.getId());
            }
        }
    }

    private void appendOrphanColumns(List<SupersetDatasetColumn> merged,
            List<SupersetDatasetColumn> currentColumns, Set<String> expectedKeys) {
        if (CollectionUtils.isEmpty(currentColumns)) {
            return;
        }
        for (SupersetDatasetColumn column : currentColumns) {
            String key = normalizeName(column.getColumnName());
            if (!expectedKeys.contains(key)) {
                merged.add(column);
            }
        }
    }

    private void appendOrphanMetrics(List<SupersetDatasetMetric> merged,
            List<SupersetDatasetMetric> currentMetrics, Set<String> expectedKeys) {
        if (CollectionUtils.isEmpty(currentMetrics)) {
            return;
        }
        for (SupersetDatasetMetric metric : currentMetrics) {
            String key = normalizeName(metric.getMetricName());
            if (!expectedKeys.contains(key)) {
                merged.add(metric);
            }
        }
    }

    private Map<String, SupersetDatasetColumn> toColumnMap(List<SupersetDatasetColumn> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetColumn> map = new HashMap<>();
        for (SupersetDatasetColumn column : columns) {
            if (column == null) {
                continue;
            }
            String key = normalizeName(column.getColumnName());
            if (StringUtils.isNotBlank(key) && !map.containsKey(key)) {
                map.put(key, column);
            }
        }
        return map;
    }

    private Map<String, SupersetDatasetMetric> toMetricMap(List<SupersetDatasetMetric> metrics) {
        if (CollectionUtils.isEmpty(metrics)) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetMetric> map = new HashMap<>();
        for (SupersetDatasetMetric metric : metrics) {
            if (metric == null) {
                continue;
            }
            String key = normalizeName(metric.getMetricName());
            if (StringUtils.isNotBlank(key) && !map.containsKey(key)) {
                map.put(key, metric);
            }
        }
        return map;
    }

    private String normalizeName(String name) {
        if (StringUtils.isBlank(name)) {
            return "";
        }
        return name.trim().replaceAll("\\s+", "").toLowerCase();
    }

    private String abbreviateErrorBody(HttpStatusCodeException ex) {
        if (ex == null) {
            return "";
        }
        String body = ex.getResponseBodyAsString();
        if (StringUtils.isBlank(body)) {
            return "";
        }
        String normalized = StringUtils.normalizeSpace(body);
        if (normalized.length() <= 500) {
            return normalized;
        }
        return normalized.substring(0, 500) + "...";
    }

    private String resolveSchema(DatabaseResp databaseResp) {
        if (databaseResp == null) {
            return null;
        }
        String schema = StringUtils.defaultIfBlank(databaseResp.getSchema(), null);
        if (StringUtils.isBlank(schema)
                && EngineType.POSTGRESQL.getName().equalsIgnoreCase(databaseResp.getType())) {
            return "public";
        }
        return StringUtils.defaultIfBlank(schema, null);
    }

    private String buildSqlalchemyUri(DatabaseResp databaseResp) {
        if (databaseResp == null || StringUtils.isBlank(databaseResp.getUrl())) {
            return null;
        }
        String jdbcUrl = databaseResp.getUrl().trim();
        if (!jdbcUrl.startsWith("jdbc:")) {
            return null;
        }
        String engine = resolveSqlalchemyEngine(databaseResp.getType());
        if (StringUtils.isBlank(engine)) {
            return null;
        }
        String rawUrl = jdbcUrl.substring(5);
        URI uri;
        try {
            uri = URI.create(rawUrl);
        } catch (Exception ex) {
            return null;
        }
        String host = uri.getHost();
        int port = uri.getPort();
        String dbName = databaseResp.getDatabase();
        if (StringUtils.isBlank(dbName) && StringUtils.isNotBlank(uri.getPath())) {
            dbName = uri.getPath().replaceFirst("/", "");
        }
        String query = sanitizeJdbcQuery(uri.getQuery(), databaseResp.getType());
        String user = encodeUriComponent(databaseResp.getUsername());
        String password = encodeUriComponent(safeDecryptPassword(databaseResp));
        StringBuilder builder = new StringBuilder();
        builder.append(engine).append("://");
        if (StringUtils.isNotBlank(user)) {
            builder.append(user);
            if (StringUtils.isNotBlank(password)) {
                builder.append(":").append(password);
            }
            builder.append("@");
        }
        if (StringUtils.isBlank(host)) {
            return null;
        }
        builder.append(host);
        if (port > 0) {
            builder.append(":").append(port);
        }
        if (StringUtils.isNotBlank(dbName)) {
            builder.append("/").append(dbName);
        }
        if (StringUtils.isNotBlank(query)) {
            builder.append("?").append(query);
        }
        return builder.toString();
    }

    private String safeDecryptPassword(DatabaseResp databaseResp) {
        if (databaseResp == null) {
            return "";
        }
        try {
            return databaseResp.passwordDecrypt();
        } catch (Exception ex) {
            return "";
        }
    }

    private String sanitizeJdbcQuery(String query, String type) {
        if (StringUtils.isBlank(query)) {
            return null;
        }
        if (!EngineType.POSTGRESQL.getName().equalsIgnoreCase(type)) {
            return query;
        }
        List<String> retained = new ArrayList<>();
        for (String part : query.split("&")) {
            if (StringUtils.isBlank(part)) {
                continue;
            }
            String[] pair = part.split("=", 2);
            String key = pair.length > 0 ? pair[0] : "";
            if (StringUtils.isBlank(key)) {
                continue;
            }
            if ("stringtype".equalsIgnoreCase(key)) {
                continue;
            }
            retained.add(part);
        }
        return retained.isEmpty() ? null : String.join("&", retained);
    }

    private String resolveSqlalchemyEngine(String type) {
        if (StringUtils.isBlank(type)) {
            return null;
        }
        if (EngineType.POSTGRESQL.getName().equalsIgnoreCase(type)) {
            return "postgresql+psycopg2";
        }
        if (EngineType.MYSQL.getName().equalsIgnoreCase(type)) {
            return "mysql+pymysql";
        }
        if (EngineType.CLICKHOUSE.getName().equalsIgnoreCase(type)) {
            return "clickhouse+native";
        }
        return null;
    }

    private String encodeUriComponent(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            return value;
        }
    }
}
