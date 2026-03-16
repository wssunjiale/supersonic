package com.tencent.supersonic.headless.server.sync.superset;

import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.service.DatabaseService;
import com.tencent.supersonic.headless.server.service.SupersetDatasetRegistryService;
import jakarta.annotation.PreDestroy;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SupersetSyncService {

    private static final int NAME_LIMIT = 250;

    private final SupersetSyncClient syncClient;
    private final SupersetSyncProperties properties;
    private final DatabaseService databaseService;
    private final SupersetDatasetRegistryService registryService;
    private final ScheduledExecutorService retryExecutor;
    private final AtomicBoolean databaseSyncRunning = new AtomicBoolean(false);
    private final AtomicBoolean datasetSyncRunning = new AtomicBoolean(false);

    public SupersetSyncService(SupersetSyncClient syncClient, SupersetSyncProperties properties,
            DatabaseService databaseService, SupersetDatasetRegistryService registryService) {
        this.syncClient = syncClient;
        this.properties = properties;
        this.databaseService = databaseService;
        this.registryService = registryService;
        this.retryExecutor = new ScheduledThreadPoolExecutor(2);
    }

    public SupersetSyncResult triggerDatabaseSync(Set<Long> databaseIds,
            SupersetSyncTrigger trigger) {
        return runWithRetry(SupersetSyncType.DATABASE, trigger,
                () -> syncDatabases(databaseIds, trigger));
    }

    public SupersetSyncResult triggerDatasetSync(Set<Long> datasetRegistryIds,
            SupersetSyncTrigger trigger) {
        return runWithRetry(SupersetSyncType.DATASET, trigger,
                () -> syncDatasets(datasetRegistryIds, trigger));
    }


    public void triggerFullSync(SupersetSyncTrigger trigger) {
        SupersetSyncResult dbResult = triggerDatabaseSync(Collections.emptySet(), trigger);
        log.info("superset database sync finished, success={}, message={}, stats={}",
                dbResult.isSuccess(), dbResult.getMessage(), dbResult.getStats());
        SupersetSyncResult datasetResult = triggerDatasetSync(Collections.emptySet(), trigger);
        log.info("superset dataset sync finished, success={}, message={}, stats={}",
                datasetResult.isSuccess(), datasetResult.getMessage(), datasetResult.getStats());
    }

    public SupersetDatasetInfo registerAndSyncDataset(SemanticParseInfo parseInfo, String sql,
            List<QueryColumn> queryColumns, User user) {
        if (parseInfo == null || StringUtils.isBlank(sql)) {
            return null;
        }
        if (!properties.isEnabled() || !properties.getSync().isEnabled()
                || StringUtils.isBlank(properties.getBaseUrl())) {
            return null;
        }
        SupersetDatasetDO record = registryService.registerDataset(parseInfo, sql, queryColumns,
                user);
        if (record == null) {
            return null;
        }
        SupersetSyncResult result = triggerDatasetSync(Collections.singleton(record.getId()),
                SupersetSyncTrigger.ON_DEMAND);
        if (!result.isSuccess()) {
            log.warn("superset dataset sync failed, id={}, message={}", record.getId(),
                    result.getMessage());
        }
        SupersetDatasetDO refreshed = registryService.getById(record.getId());
        SupersetDatasetInfo info = buildDatasetInfo(refreshed,
                resolveSupersetDatabaseId(refreshed == null ? null : refreshed.getDatabaseId()));
        if (info == null || info.getId() == null) {
            return null;
        }
        SupersetDatasetInfo remote = null;
        try {
            remote = syncClient.fetchDataset(info.getId());
        } catch (Exception ex) {
            log.warn("superset dataset fetch failed after sync, id={}, message={}", info.getId(),
                    ex.getMessage());
            log.debug("superset dataset fetch error", ex);
        }
        mergeDatasetInfoForChart(info, remote);
        return info;
    }

    public SupersetDatasetInfo resolveDatasetInfo(SupersetDatasetDO dataset) {
        if (dataset == null || dataset.getSupersetDatasetId() == null) {
            return null;
        }
        if (!properties.isEnabled() || StringUtils.isBlank(properties.getBaseUrl())) {
            return null;
        }
        Long supersetDatabaseId = resolveSupersetDatabaseId(dataset.getDatabaseId());
        SupersetDatasetInfo info = buildDatasetInfo(dataset, supersetDatabaseId);
        if (info == null || info.getId() == null) {
            return null;
        }
        SupersetDatasetInfo remote = null;
        try {
            remote = syncClient.fetchDataset(info.getId());
        } catch (Exception ex) {
            log.warn("superset dataset fetch failed for registry record, id={}, message={}",
                    info.getId(), ex.getMessage());
            log.debug("superset dataset fetch error", ex);
        }
        mergeDatasetInfoForChart(info, remote);
        return info;
    }


    private SupersetSyncResult runWithRetry(SupersetSyncType type, SupersetSyncTrigger trigger,
            Supplier<SupersetSyncResult> action) {
        SupersetSyncResult result = action.get();
        if (!result.isSuccess() && shouldScheduleRetry(trigger)) {
            scheduleRetry(type, action, 1);
        }
        return result;
    }

    private boolean shouldScheduleRetry(SupersetSyncTrigger trigger) {
        // Manual runs should return a deterministic result to the caller. Retries will still be
        // handled by the scheduled sync loop (retryable failures are persisted in registry).
        if (trigger == null) {
            return true;
        }
        return trigger != SupersetSyncTrigger.MANUAL;
    }

    private void scheduleRetry(SupersetSyncType type, Supplier<SupersetSyncResult> action,
            int attempt) {
        SupersetSyncProperties.Sync syncConfig = properties.getSync();
        if (!syncConfig.isEnabled() || attempt > syncConfig.getMaxRetries()) {
            return;
        }
        long delay = syncConfig.getRetryIntervalMs();
        retryExecutor.schedule(() -> {
            SupersetSyncResult result = action.get();
            if (!result.isSuccess()) {
                scheduleRetry(type, action, attempt + 1);
            } else {
                log.info("superset {} sync retry success, attempt={}", type.name().toLowerCase(),
                        attempt);
            }
        }, delay, TimeUnit.MILLISECONDS);
        log.warn("superset {} sync scheduled retry, attempt={}, delayMs={}",
                type.name().toLowerCase(), attempt, delay);
    }

    @PreDestroy
    public void shutdownRetryExecutor() {
        try {
            retryExecutor.shutdownNow();
        } catch (Exception ex) {
            log.debug("shutdown superset retry executor failed", ex);
        }
    }

    private SupersetSyncResult syncDatabases(Set<Long> databaseIds, SupersetSyncTrigger trigger) {
        long start = System.currentTimeMillis();
        if (!properties.isEnabled()) {
            return SupersetSyncResult.success("Superset 未启用，跳过同步",
                    System.currentTimeMillis() - start, new SupersetSyncStats());
        }
        if (!properties.getSync().isEnabled()) {
            return SupersetSyncResult.success("Superset 同步已关闭，跳过同步",
                    System.currentTimeMillis() - start, new SupersetSyncStats());
        }
        if (StringUtils.isBlank(properties.getBaseUrl())) {
            return SupersetSyncResult.failure("Superset 地址未配置", System.currentTimeMillis() - start,
                    new SupersetSyncStats());
        }
        if (!databaseSyncRunning.compareAndSet(false, true)) {
            return SupersetSyncResult.success("Superset 数据库同步已在执行中",
                    System.currentTimeMillis() - start, new SupersetSyncStats());
        }
        SupersetSyncStats stats = new SupersetSyncStats();
        try {
            log.debug("superset database sync start, trigger={}, ids={}", trigger, databaseIds);
            List<DatabaseResp> databases = databaseService.getDatabaseList(User.getDefaultUser());
            if (!databaseIds.isEmpty()) {
                databases = databases.stream().filter(db -> databaseIds.contains(db.getId()))
                        .collect(Collectors.toList());
            }
            List<SupersetDatabaseInfo> supersetDatabases = syncClient.listDatabases();
            Map<String, SupersetDatabaseInfo> supersetByName = supersetDatabases.stream()
                    .filter(item -> StringUtils.isNotBlank(item.getName()))
                    .collect(Collectors.toMap(SupersetDatabaseInfo::getName, item -> item,
                            (left, right) -> left));
            for (DatabaseResp database : databases) {
                DatabaseResp resolved = resolveDatabaseWithPassword(database);
                stats.incTotal();
                log.debug(
                        "superset database sync candidate, id={}, name={}, type={}, host={}, port={}, database={}, schema={}",
                        resolved.getId(), resolved.getName(), resolved.getType(),
                        safeHost(resolved), safePort(resolved), resolved.getDatabase(),
                        resolved.getSchema());
                log.debug("superset database sync credential, id={}, username={}, password={}",
                        resolved.getId(), resolved.getUsername(), safePassword(resolved));
                SupersetDatabaseInfo expected = buildDatabaseInfo(resolved);
                if (expected == null) {
                    stats.incSkipped();
                    continue;
                }
                log.debug("superset database sync expected, name={}, sqlalchemyUri={}",
                        expected.getName(), maskSqlalchemyUri(expected.getSqlalchemyUri()));
                SupersetDatabaseInfo existing = supersetByName.get(expected.getName());
                if (existing == null) {
                    log.debug("superset database not found, create required, name={}",
                            expected.getName());
                    try {
                        Long createdId = syncClient.createDatabase(expected);
                        if (createdId != null) {
                            log.debug("superset database created, name={}, id={}",
                                    expected.getName(), createdId);
                            stats.incCreated();
                        } else {
                            stats.incFailed();
                        }
                    } catch (HttpStatusCodeException ex) {
                        if (shouldIgnoreDuplicateCreate(ex)) {
                            log.warn(
                                    "superset database create ignored for superset 6.0.0, name={}, status={}, body={}",
                                    expected.getName(), ex.getStatusCode(),
                                    abbreviateErrorBody(ex));
                            stats.incSkipped();
                        } else {
                            throw ex;
                        }
                    }
                    continue;
                }
                log.debug("superset database exists, name={}, id={}", expected.getName(),
                        existing.getId());
                SupersetDatabaseInfo current = syncClient.fetchDatabase(existing.getId());
                if (!databaseMatches(current, expected)) {
                    syncClient.updateDatabase(existing.getId(), expected);
                    log.debug("superset database updated, name={}, id={}", expected.getName(),
                            existing.getId());
                    stats.incUpdated();
                } else {
                    stats.incSkipped();
                }
            }
            boolean success = stats.getFailed() == 0;
            String message = success ? "Superset 数据库同步完成" : "Superset 数据库同步失败";
            return success
                    ? SupersetSyncResult.success(message, System.currentTimeMillis() - start, stats)
                    : SupersetSyncResult.failure(message, System.currentTimeMillis() - start,
                            stats);
        } catch (Exception ex) {
            log.warn("superset database sync failed", ex);
            return SupersetSyncResult.failure("Superset 数据库同步异常",
                    System.currentTimeMillis() - start, stats);
        } finally {
            databaseSyncRunning.set(false);
        }
    }

    private SupersetSyncResult syncDatasets(Set<Long> datasetRegistryIds,
            SupersetSyncTrigger trigger) {
        long start = System.currentTimeMillis();
        if (!properties.isEnabled()) {
            return SupersetSyncResult.success("Superset 未启用，跳过同步",
                    System.currentTimeMillis() - start, new SupersetSyncStats());
        }
        if (!properties.getSync().isEnabled()) {
            return SupersetSyncResult.success("Superset 同步已关闭，跳过同步",
                    System.currentTimeMillis() - start, new SupersetSyncStats());
        }
        if (StringUtils.isBlank(properties.getBaseUrl())) {
            return SupersetSyncResult.failure("Superset 地址未配置", System.currentTimeMillis() - start,
                    new SupersetSyncStats());
        }
        if (!datasetSyncRunning.compareAndSet(false, true)) {
            return SupersetSyncResult.success("Superset 数据集同步已在执行中",
                    System.currentTimeMillis() - start, new SupersetSyncStats());
        }
        SupersetSyncStats stats = new SupersetSyncStats();
        try {
            log.debug("superset dataset sync start, trigger={}, registryIds={}", trigger,
                    datasetRegistryIds);
            boolean forceSync = trigger == SupersetSyncTrigger.MANUAL && datasetRegistryIds != null
                    && !datasetRegistryIds.isEmpty();
            List<SupersetDatasetDO> datasets = registryService.listForSync(datasetRegistryIds);
            if (datasets.isEmpty()) {
                return SupersetSyncResult.success("Superset 数据集同步完成",
                        System.currentTimeMillis() - start, stats);
            }
            Set<Long> databaseIds = datasets.stream().map(SupersetDatasetDO::getDatabaseId)
                    .filter(Objects::nonNull).collect(Collectors.toSet());
            Map<Long, Long> databaseMapping = syncDatabasesAndBuildMapping(databaseIds);
            List<SupersetDatasetInfo> supersetDatasets = syncClient.listDatasets();
            Map<Long, SupersetDatasetInfo> datasetIdMap =
                    supersetDatasets.stream().filter(item -> item != null && item.getId() != null)
                            .collect(Collectors.toMap(SupersetDatasetInfo::getId, item -> item,
                                    (left, right) -> left));
            Map<String, SupersetDatasetInfo> datasetMap = supersetDatasets.stream()
                    .filter(item -> StringUtils.isNotBlank(item.getTableName())).collect(Collectors
                            .toMap(this::datasetKey, item -> item, (left, right) -> left));
            for (SupersetDatasetDO dataset : datasets) {
                stats.incTotal();
                Date attemptAt = new Date();
                if (!forceSync && !shouldSyncDataset(dataset)) {
                    stats.incSkipped();
                    continue;
                }
                if (dataset.getId() != null) {
                    registryService.updateSyncAttempt(dataset.getId(), attemptAt);
                }
                Long supersetDatabaseId = databaseMapping.get(dataset.getDatabaseId());
                if (supersetDatabaseId == null) {
                    stats.incFailed();
                    markDatasetSyncFailed(dataset, SupersetDatasetSyncErrorType.FATAL, attemptAt,
                            "Superset 数据库映射缺失");
                    continue;
                }
                SupersetDatasetInfo expected = buildDatasetInfo(dataset, supersetDatabaseId);
                if (expected == null) {
                    stats.incSkipped();
                    continue;
                }
                try {
                    String key = datasetKey(expected);
                    SupersetDatasetInfo existing = dataset.getSupersetDatasetId() == null ? null
                            : datasetIdMap.get(dataset.getSupersetDatasetId());
                    if (existing == null) {
                        existing = datasetMap.get(key);
                    }
                    if (existing == null) {
                        Long createdId = syncClient.createDataset(expected);
                        if (createdId == null) {
                            stats.incFailed();
                            markDatasetSyncFailed(dataset, SupersetDatasetSyncErrorType.RETRYABLE,
                                    attemptAt, "Superset dataset create failed (null id)");
                            continue;
                        }
                        expected.setId(createdId);
                        if (shouldUpdateDataset(expected)) {
                            SupersetDatasetInfo current = syncClient.fetchDataset(createdId);
                            if (current == null) {
                                log.warn(
                                        "superset dataset fetch failed after create, skip update, id={}",
                                        createdId);
                            } else {
                                SupersetDatasetInfo merged =
                                        mergeDatasetSchema(expected, current, true);
                                syncClient.updateDataset(createdId, merged);
                            }
                        }
                        stats.incCreated();
                        registryService.updateSyncInfo(dataset.getId(), createdId, new Date());
                        continue;
                    }

                    expected.setId(existing.getId());
                    SupersetDatasetInfo current = syncClient.fetchDataset(existing.getId());
                    SupersetDatasetInfo merged = mergeDatasetSchema(expected, current, false);
                    if (current == null || !datasetMatches(current, merged, false)) {
                        syncClient.updateDataset(existing.getId(), merged);
                        stats.incUpdated();
                    } else {
                        stats.incSkipped();
                    }
                    registryService.updateSyncInfo(dataset.getId(), existing.getId(), new Date());
                } catch (HttpStatusCodeException ex) {
                    if (shouldIgnoreDuplicateCreate(ex)) {
                        log.warn(
                                "superset dataset create ignored for superset 6.0.0, name={}, status={}, body={}",
                                expected.getTableName(), ex.getStatusCode(),
                                abbreviateErrorBody(ex));
                        SupersetDatasetInfo resolved = resolveDatasetByKey(expected);
                        if (resolved != null && resolved.getId() != null) {
                            registryService.updateSyncInfo(dataset.getId(), resolved.getId(),
                                    new Date());
                            stats.incUpdated();
                        } else {
                            stats.incFailed();
                            markDatasetSyncFailed(dataset, SupersetDatasetSyncErrorType.RETRYABLE,
                                    attemptAt,
                                    String.format(
                                            "Superset create ignored, need reconcile, status=%s, body=%s",
                                            ex.getStatusCode(), abbreviateErrorBody(ex)));
                        }
                    } else {
                        stats.incFailed();
                        SupersetDatasetSyncErrorType errorType =
                                isRetryableStatus(ex) ? SupersetDatasetSyncErrorType.RETRYABLE
                                        : SupersetDatasetSyncErrorType.FATAL;
                        markDatasetSyncFailed(dataset, errorType, attemptAt, String.format(
                                "status=%s, body=%s", ex.getStatusCode(), abbreviateErrorBody(ex)));
                    }
                } catch (Exception ex) {
                    stats.incFailed();
                    SupersetDatasetSyncErrorType errorType =
                            isRetryableException(ex) ? SupersetDatasetSyncErrorType.RETRYABLE
                                    : SupersetDatasetSyncErrorType.FATAL;
                    markDatasetSyncFailed(dataset, errorType, attemptAt, ex.getMessage());
                }
            }
            boolean success = stats.getFailed() == 0;
            String message = success ? "Superset 数据集同步完成" : "Superset 数据集同步失败";
            return success
                    ? SupersetSyncResult.success(message, System.currentTimeMillis() - start, stats)
                    : SupersetSyncResult.failure(message, System.currentTimeMillis() - start,
                            stats);
        } catch (Exception ex) {
            log.warn("superset dataset sync failed", ex);
            return SupersetSyncResult.failure("Superset 数据集同步异常",
                    System.currentTimeMillis() - start, stats);
        } finally {
            datasetSyncRunning.set(false);
        }
    }

    private Map<Long, Long> syncDatabasesAndBuildMapping(Set<Long> databaseIds) {
        log.debug("superset database mapping start, ids={}", databaseIds);
        List<DatabaseResp> databases = databaseService.getDatabaseList(User.getDefaultUser());
        if (!databaseIds.isEmpty()) {
            databases = databases.stream().filter(db -> databaseIds.contains(db.getId())).toList();
        }
        List<SupersetDatabaseInfo> supersetDatabases = syncClient.listDatabases();
        Map<String, SupersetDatabaseInfo> supersetByName = supersetDatabases.stream()
                .filter(item -> StringUtils.isNotBlank(item.getName())).collect(Collectors
                        .toMap(SupersetDatabaseInfo::getName, item -> item, (left, right) -> left));
        Map<Long, Long> mapping = new HashMap<>();
        for (DatabaseResp database : databases) {
            DatabaseResp resolved = resolveDatabaseWithPassword(database);
            log.debug(
                    "superset database mapping candidate, id={}, name={}, type={}, host={}, port={}, database={}, schema={}",
                    resolved.getId(), resolved.getName(), resolved.getType(), safeHost(resolved),
                    safePort(resolved), resolved.getDatabase(), resolved.getSchema());
            log.debug("superset database mapping credential, id={}, username={}, password={}",
                    resolved.getId(), resolved.getUsername(), safePassword(resolved));
            SupersetDatabaseInfo expected = buildDatabaseInfo(resolved);
            if (expected == null) {
                continue;
            }
            log.debug("superset database mapping expected, name={}, sqlalchemyUri={}",
                    expected.getName(), maskSqlalchemyUri(expected.getSqlalchemyUri()));
            SupersetDatabaseInfo existing = supersetByName.get(expected.getName());
            if (existing == null) {
                log.debug("superset database not found, create required, name={}",
                        expected.getName());
                try {
                    Long createdId = syncClient.createDatabase(expected);
                    if (createdId != null) {
                        log.debug("superset database created, name={}, id={}", expected.getName(),
                                createdId);
                        mapping.put(database.getId(), createdId);
                    }
                } catch (HttpStatusCodeException ex) {
                    if (shouldIgnoreDuplicateCreate(ex)) {
                        log.warn(
                                "superset database create ignored for superset 6.0.0, name={}, status={}, body={}",
                                expected.getName(), ex.getStatusCode(), abbreviateErrorBody(ex));
                    } else {
                        throw ex;
                    }
                }
                continue;
            }
            log.debug("superset database exists, name={}, id={}", expected.getName(),
                    existing.getId());
            SupersetDatabaseInfo current = syncClient.fetchDatabase(existing.getId());
            if (!databaseMatches(current, expected)) {
                syncClient.updateDatabase(existing.getId(), expected);
                log.debug("superset database updated, name={}, id={}", expected.getName(),
                        existing.getId());
            }
            mapping.put(database.getId(), existing.getId());
        }
        return mapping;
    }

    private SupersetDatabaseInfo buildDatabaseInfo(DatabaseResp databaseResp) {
        String sqlalchemyUri = buildSqlalchemyUri(databaseResp);
        if (StringUtils.isBlank(sqlalchemyUri)) {
            log.warn("superset database skip, unsupported jdbc url: {}", databaseResp.getUrl());
            return null;
        }
        SupersetDatabaseInfo info = new SupersetDatabaseInfo();
        info.setName(buildSupersetDatabaseName(databaseResp));
        info.setSqlalchemyUri(sqlalchemyUri);
        info.setSchema(resolveSchema(databaseResp));
        return info;
    }

    private SupersetDatasetInfo buildDatasetInfo(SupersetDatasetDO dataset, Long databaseId) {
        if (dataset == null || databaseId == null) {
            return null;
        }
        SupersetDatasetInfo info = new SupersetDatasetInfo();
        info.setId(dataset.getSupersetDatasetId());
        info.setDatabaseId(databaseId);
        info.setSchema(StringUtils.defaultIfBlank(dataset.getSchemaName(), null));
        info.setDescription(StringUtils.trimToNull(dataset.getDatasetDesc()));
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
                log.warn("superset dataset skip, sql missing, id={}, name={}", dataset.getId(),
                        dataset.getDatasetName());
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

    private void mergeDatasetInfoForChart(SupersetDatasetInfo target, SupersetDatasetInfo source) {
        if (target == null || source == null) {
            return;
        }
        if (target.getDatabaseId() == null && source.getDatabaseId() != null) {
            target.setDatabaseId(source.getDatabaseId());
        }
        if (StringUtils.isBlank(target.getSchema()) && StringUtils.isNotBlank(source.getSchema())) {
            target.setSchema(source.getSchema());
        }
        if (StringUtils.isBlank(target.getMainDttmCol())
                && StringUtils.isNotBlank(source.getMainDttmCol())) {
            target.setMainDttmCol(source.getMainDttmCol());
        }
        if (!CollectionUtils.isEmpty(source.getColumns())) {
            target.setColumns(source.getColumns());
        }
        if (!CollectionUtils.isEmpty(source.getMetrics())) {
            target.setMetrics(source.getMetrics());
        }
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

    private boolean shouldUpdateDataset(SupersetDatasetInfo expected) {
        if (expected == null) {
            return false;
        }
        return !CollectionUtils.isEmpty(expected.getColumns())
                || !CollectionUtils.isEmpty(expected.getMetrics())
                || StringUtils.isNotBlank(expected.getMainDttmCol());
    }

    private boolean shouldSyncDataset(SupersetDatasetDO dataset) {
        if (dataset == null) {
            return false;
        }
        if (SupersetDatasetSyncState.PENDING.name().equalsIgnoreCase(dataset.getSyncState())) {
            return true;
        }
        if (SupersetDatasetSyncState.FAILED.name().equalsIgnoreCase(dataset.getSyncState())
                && SupersetDatasetSyncErrorType.RETRYABLE.name()
                        .equalsIgnoreCase(dataset.getSyncErrorType())) {
            Date nextRetryAt = dataset.getNextRetryAt();
            return nextRetryAt == null || !nextRetryAt.after(new Date());
        }
        if (dataset.getSupersetDatasetId() == null) {
            return true;
        }
        if (dataset.getSyncedAt() == null) {
            return true;
        }
        if (dataset.getUpdatedAt() == null) {
            return false;
        }
        return dataset.getUpdatedAt().after(dataset.getSyncedAt());
    }

    private void markDatasetSyncFailed(SupersetDatasetDO dataset, SupersetDatasetSyncErrorType type,
            Date attemptAt, String message) {
        if (dataset == null || dataset.getId() == null) {
            return;
        }
        int retryCount = (dataset.getRetryCount() == null ? 0 : dataset.getRetryCount());
        int nextRetryCount = retryCount + 1;
        Date nextRetryAt = null;
        if (SupersetDatasetSyncErrorType.RETRYABLE == type) {
            nextRetryAt = computeNextRetryAt(nextRetryCount);
        }
        registryService.markSyncFailed(dataset.getId(), type.name(), message, attemptAt,
                nextRetryAt, nextRetryCount);
    }

    private boolean isRetryableStatus(HttpStatusCodeException ex) {
        if (ex == null || ex.getStatusCode() == null) {
            return false;
        }
        int code = ex.getStatusCode().value();
        return code == 429 || (code >= 500 && code < 600);
    }

    private boolean isRetryableException(Exception ex) {
        if (ex == null) {
            return false;
        }
        String name = ex.getClass().getName();
        return name.contains("ResourceAccessException") || name.contains("ConnectException")
                || name.contains("SocketTimeoutException") || name.contains("UnknownHostException");
    }

    private Date computeNextRetryAt(int retryCount) {
        long base = properties.getSync().getRetryIntervalMs();
        long maxDelayMs = Math.max(base, TimeUnit.MINUTES.toMillis(30));
        int exp = Math.max(0, Math.min(10, retryCount - 1));
        long delay = base * (1L << exp);
        delay = Math.min(delay, maxDelayMs);
        return new Date(System.currentTimeMillis() + delay);
    }

    private SupersetDatasetInfo resolveDatasetByKey(SupersetDatasetInfo expected) {
        try {
            if (expected == null) {
                return null;
            }
            String key = datasetKey(expected);
            for (SupersetDatasetInfo item : syncClient.listDatasets()) {
                if (item == null) {
                    continue;
                }
                if (key.equals(datasetKey(item))) {
                    return item;
                }
            }
        } catch (Exception ex) {
            log.debug("resolve dataset by key failed", ex);
        }
        return null;
    }

    private Long resolveSupersetDatabaseId(Long databaseId) {
        if (databaseId == null) {
            return null;
        }
        Map<Long, Long> mapping = syncDatabasesAndBuildMapping(Collections.singleton(databaseId));
        return mapping.get(databaseId);
    }

    private List<SupersetDatasetColumn> parseColumns(String columnsJson) {
        if (StringUtils.isBlank(columnsJson)) {
            return Collections.emptyList();
        }
        try {
            List<SupersetDatasetColumn> columns =
                    JsonUtil.toList(columnsJson, SupersetDatasetColumn.class);
            return columns == null ? Collections.emptyList() : columns;
        } catch (Exception ex) {
            log.warn("superset dataset columns parse failed");
            return Collections.emptyList();
        }
    }

    private List<SupersetDatasetMetric> parseMetrics(String metricsJson) {
        if (StringUtils.isBlank(metricsJson)) {
            return Collections.emptyList();
        }
        try {
            List<SupersetDatasetMetric> metrics =
                    JsonUtil.toList(metricsJson, SupersetDatasetMetric.class);
            return metrics == null ? Collections.emptyList() : metrics;
        } catch (Exception ex) {
            log.warn("superset dataset metrics parse failed");
            return Collections.emptyList();
        }
    }

    private String normalizeName(String name) {
        return StringUtils.lowerCase(StringUtils.trimToEmpty(name));
    }

    private String normalizeType(String type) {
        if (StringUtils.isBlank(type)) {
            return null;
        }
        return type.trim().toUpperCase();
    }

    private String normalizeExpression(String expr) {
        if (StringUtils.isBlank(expr)) {
            return "";
        }
        String normalized = expr.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return StringUtils.normalizeSpace(normalized);
    }

    private Map<String, SupersetDatasetColumn> toColumnMap(List<SupersetDatasetColumn> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetColumn> map = new HashMap<>();
        for (SupersetDatasetColumn column : columns) {
            if (column == null || StringUtils.isBlank(column.getColumnName())) {
                continue;
            }
            map.put(normalizeName(column.getColumnName()), column);
        }
        return map;
    }

    private Map<String, SupersetDatasetMetric> toMetricMap(List<SupersetDatasetMetric> metrics) {
        if (CollectionUtils.isEmpty(metrics)) {
            return Collections.emptyMap();
        }
        Map<String, SupersetDatasetMetric> map = new HashMap<>();
        for (SupersetDatasetMetric metric : metrics) {
            if (metric == null || StringUtils.isBlank(metric.getMetricName())) {
                continue;
            }
            map.put(normalizeName(metric.getMetricName()), metric);
        }
        return map;
    }

    private boolean columnsMatch(List<SupersetDatasetColumn> currentColumns,
            List<SupersetDatasetColumn> expectedColumns, boolean rebuild) {
        if (CollectionUtils.isEmpty(expectedColumns)) {
            return true;
        }
        Map<String, SupersetDatasetColumn> currentMap = toColumnMap(currentColumns);
        Set<String> expectedKeys = new HashSet<>();
        for (SupersetDatasetColumn expected : expectedColumns) {
            String key = normalizeName(expected.getColumnName());
            expectedKeys.add(key);
            SupersetDatasetColumn current = currentMap.get(key);
            if (!columnMatches(current, expected)) {
                return false;
            }
        }
        if (rebuild && !CollectionUtils.isEmpty(currentMap)) {
            return currentMap.keySet().stream().allMatch(expectedKeys::contains);
        }
        return true;
    }

    private boolean metricsMatch(List<SupersetDatasetMetric> currentMetrics,
            List<SupersetDatasetMetric> expectedMetrics, boolean rebuild) {
        if (CollectionUtils.isEmpty(expectedMetrics)) {
            return true;
        }
        Map<String, SupersetDatasetMetric> currentMap = toMetricMap(currentMetrics);
        Set<String> expectedKeys = new HashSet<>();
        for (SupersetDatasetMetric expected : expectedMetrics) {
            String key = normalizeName(expected.getMetricName());
            expectedKeys.add(key);
            SupersetDatasetMetric current = currentMap.get(key);
            if (!metricMatches(current, expected)) {
                return false;
            }
        }
        if (rebuild && !CollectionUtils.isEmpty(currentMap)) {
            return currentMap.keySet().stream().allMatch(expectedKeys::contains);
        }
        return true;
    }

    private boolean columnMatches(SupersetDatasetColumn current, SupersetDatasetColumn expected) {
        if (current == null || expected == null) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(normalizeExpression(current.getExpression()),
                normalizeExpression(expected.getExpression()))) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(StringUtils.defaultString(current.getType()),
                StringUtils.defaultString(expected.getType()))) {
            return false;
        }
        if (expected.getIsDttm() != null
                && !Objects.equals(expected.getIsDttm(), current.getIsDttm())) {
            return false;
        }
        if (expected.getFilterable() != null
                && !Objects.equals(expected.getFilterable(), current.getFilterable())) {
            return false;
        }
        if (expected.getGroupby() != null
                && !Objects.equals(expected.getGroupby(), current.getGroupby())) {
            return false;
        }
        if (StringUtils.isNotBlank(expected.getVerboseName()) && !StringUtils
                .equalsIgnoreCase(expected.getVerboseName(), current.getVerboseName())) {
            return false;
        }
        if (StringUtils.isNotBlank(expected.getDescription()) && !StringUtils
                .equalsIgnoreCase(expected.getDescription(), current.getDescription())) {
            return false;
        }
        if (StringUtils.isNotBlank(expected.getPythonDateFormat()) && !StringUtils
                .equalsIgnoreCase(expected.getPythonDateFormat(), current.getPythonDateFormat())) {
            return false;
        }
        return true;
    }

    private boolean metricMatches(SupersetDatasetMetric current, SupersetDatasetMetric expected) {
        if (current == null || expected == null) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(normalizeExpression(current.getExpression()),
                normalizeExpression(expected.getExpression()))) {
            return false;
        }
        if (StringUtils.isNotBlank(expected.getMetricType()) && !StringUtils
                .equalsIgnoreCase(expected.getMetricType(), current.getMetricType())) {
            return false;
        }
        if (StringUtils.isNotBlank(expected.getVerboseName()) && !StringUtils
                .equalsIgnoreCase(expected.getVerboseName(), current.getVerboseName())) {
            return false;
        }
        if (StringUtils.isNotBlank(expected.getDescription()) && !StringUtils
                .equalsIgnoreCase(expected.getDescription(), current.getDescription())) {
            return false;
        }
        return true;
    }

    private boolean databaseMatches(SupersetDatabaseInfo current, SupersetDatabaseInfo expected) {
        if (current == null || expected == null) {
            return false;
        }
        String currentUri = normalizeSqlalchemyUri(current.getSqlalchemyUri());
        String expectedUri = normalizeSqlalchemyUri(expected.getSqlalchemyUri());
        if (!StringUtils.equalsIgnoreCase(currentUri, expectedUri)) {
            return false;
        }
        return true;
    }

    private boolean datasetMatches(SupersetDatasetInfo current, SupersetDatasetInfo expected,
            boolean rebuild) {
        if (current == null || expected == null) {
            return false;
        }
        if (!Objects.equals(current.getDatabaseId(), expected.getDatabaseId())) {
            return false;
        }
        String currentSchema = StringUtils.defaultString(current.getSchema());
        String expectedSchema = StringUtils.defaultString(expected.getSchema());
        if (!StringUtils.equalsIgnoreCase(currentSchema, expectedSchema)) {
            return false;
        }
        String expectedDesc = StringUtils.trimToNull(expected.getDescription());
        if (StringUtils.isNotBlank(expectedDesc) && !StringUtils.equalsIgnoreCase(
                StringUtils.defaultString(current.getDescription()).trim(), expectedDesc)) {
            return false;
        }
        String currentSql = normalizeSql(current.getSql());
        String expectedSql = normalizeSql(expected.getSql());
        if (!StringUtils.equalsIgnoreCase(currentSql, expectedSql)) {
            return false;
        }
        if (StringUtils.isNotBlank(expected.getMainDttmCol()) && !StringUtils.equalsIgnoreCase(
                StringUtils.defaultString(current.getMainDttmCol()),
                StringUtils.defaultString(expected.getMainDttmCol()))) {
            return false;
        }
        if (!columnsMatch(current.getColumns(), expected.getColumns(), rebuild)) {
            return false;
        }
        return metricsMatch(current.getMetrics(), expected.getMetrics(), rebuild);
    }

    private String datasetKey(SupersetDatasetInfo info) {
        String name = info == null ? null : info.getTableName();
        Long databaseId = info == null ? null : info.getDatabaseId();
        return String.format("%s:%s", databaseId, StringUtils.defaultString(name));
    }

    private String buildSupersetDatabaseName(DatabaseResp databaseResp) {
        String suffix = StringUtils.defaultString(databaseResp.getName(), "database");
        String name = String.format("supersonic_db_%s_%s", databaseResp.getId(), suffix);
        return truncateName(name);
    }

    private String truncateName(String name) {
        if (name.length() <= NAME_LIMIT) {
            return name;
        }
        return name.substring(0, NAME_LIMIT);
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
            log.warn("superset sqlalchemy uri parse failed: {}", jdbcUrl, ex);
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
        String password = encodeUriComponent(databaseResp.passwordDecrypt());
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

    private String safeHost(DatabaseResp databaseResp) {
        if (databaseResp == null || StringUtils.isBlank(databaseResp.getUrl())) {
            return "";
        }
        return databaseResp.getHost();
    }

    private String safePort(DatabaseResp databaseResp) {
        if (databaseResp == null || StringUtils.isBlank(databaseResp.getUrl())) {
            return "";
        }
        return databaseResp.getPort();
    }

    private String safePassword(DatabaseResp databaseResp) {
        if (databaseResp == null) {
            return "";
        }
        try {
            return databaseResp.passwordDecrypt();
        } catch (Exception ex) {
            log.warn("superset database password decrypt failed, id={}", databaseResp.getId(), ex);
            return "";
        }
    }

    private DatabaseResp resolveDatabaseWithPassword(DatabaseResp databaseResp) {
        if (databaseResp == null || databaseResp.getId() == null) {
            return databaseResp;
        }
        try {
            DatabaseResp resolved =
                    databaseService.getDatabase(databaseResp.getId(), User.getDefaultUser());
            return resolved == null ? databaseResp : resolved;
        } catch (Exception ex) {
            log.debug("superset database load failed, id={}", databaseResp.getId(), ex);
            return databaseResp;
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

    private String normalizeSqlalchemyUri(String uri) {
        if (StringUtils.isBlank(uri)) {
            return "";
        }
        String normalized = uri.trim();
        int schemeIndex = normalized.indexOf("://");
        int atIndex = normalized.indexOf('@');
        if (schemeIndex >= 0 && atIndex > schemeIndex) {
            String prefix = normalized.substring(0, schemeIndex + 3);
            String credentials = normalized.substring(schemeIndex + 3, atIndex);
            int colonIndex = credentials.indexOf(':');
            if (colonIndex > 0) {
                String userOnly = credentials.substring(0, colonIndex);
                normalized = prefix + userOnly + "@" + normalized.substring(atIndex + 1);
            }
        }
        return StringUtils.normalizeSpace(normalized);
    }

    private String maskSqlalchemyUri(String uri) {
        return normalizeSqlalchemyUri(uri);
    }

    private String normalizeSql(String sql) {
        if (StringUtils.isBlank(sql)) {
            return "";
        }
        String normalized = sql.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return StringUtils.normalizeSpace(normalized);
    }

    private boolean shouldIgnoreDuplicateCreate(HttpStatusCodeException ex) {
        if (ex == null || ex.getStatusCode() == null || ex.getStatusCode().value() != 422) {
            return false;
        }
        return isDuplicateCreateError(ex);
    }

    private boolean isDuplicateCreateError(HttpStatusCodeException ex) {
        String body = ex.getResponseBodyAsString();
        if (StringUtils.isBlank(body)) {
            return false;
        }
        return StringUtils.containsIgnoreCase(body, "already exists");
    }

    private String normalizeSupersetVersion(String version) {
        if (StringUtils.isBlank(version)) {
            return null;
        }
        String normalized = version.trim();
        if (normalized.startsWith("v") || normalized.startsWith("V")) {
            normalized = normalized.substring(1);
        }
        int spaceIndex = normalized.indexOf(' ');
        if (spaceIndex > 0) {
            normalized = normalized.substring(0, spaceIndex);
        }
        return StringUtils.trimToNull(normalized);
    }

    private String abbreviateErrorBody(HttpStatusCodeException ex) {
        if (ex == null) {
            return "";
        }
        String body = ex.getResponseBodyAsString();
        if (StringUtils.isBlank(body)) {
            return "";
        }
        return StringUtils.abbreviate(body, 200);
    }

    private String encodeUriComponent(String value) {
        if (StringUtils.isBlank(value)) {
            return "";
        }
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
