package com.tencent.supersonic.headless.server.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.tencent.supersonic.common.jsqlparser.SqlNormalizeHelper;
import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.common.pojo.exception.InvalidPermissionException;
import com.tencent.supersonic.common.util.BeanMapper;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.common.util.MD5Util;
import com.tencent.supersonic.common.util.PageUtils;
import com.tencent.supersonic.common.util.StringUtil;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.request.SupersetDatasetQueryReq;
import com.tencent.supersonic.headless.api.pojo.response.DataSetResp;
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;
import com.tencent.supersonic.headless.api.pojo.response.ModelResp;
import com.tencent.supersonic.headless.api.pojo.response.SupersetDatasetResp;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.persistence.mapper.SupersetDatasetMapper;
import com.tencent.supersonic.headless.server.service.DataSetService;
import com.tencent.supersonic.headless.server.service.DatabaseService;
import com.tencent.supersonic.headless.server.service.ModelService;
import com.tencent.supersonic.headless.server.service.SupersetDatasetRegistryService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSourceType;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSyncErrorType;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSyncState;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetType;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SupersetDatasetRegistryServiceImpl
        extends ServiceImpl<SupersetDatasetMapper, SupersetDatasetDO>
        implements SupersetDatasetRegistryService {

    private static final int NAME_LIMIT = 250;

    private final DataSetService dataSetService;
    private final ModelService modelService;
    private final DatabaseService databaseService;

    public SupersetDatasetRegistryServiceImpl(DataSetService dataSetService,
            ModelService modelService, DatabaseService databaseService) {
        this.dataSetService = dataSetService;
        this.modelService = modelService;
        this.databaseService = databaseService;
    }

    @Override
    public SupersetDatasetDO registerDataset(SemanticParseInfo parseInfo, String sql,
            List<QueryColumn> queryColumns, User user) {
        if (parseInfo == null || StringUtils.isBlank(sql)) {
            return null;
        }
        String normalizedSql = SqlNormalizeHelper.normalizeSql(sql);
        if (StringUtils.isBlank(normalizedSql)) {
            return null;
        }
        String sqlHash = MD5Util.getMD5(normalizedSql, false, MD5Util.BIT32);
        SupersetDatasetType datasetType = resolveDatasetType(normalizedSql);
        SupersetDatasetDO existing = getBySqlHash(sqlHash);

        Long databaseId = resolveDatabaseId(parseInfo);
        DatabaseResp databaseResp =
                databaseId == null ? null : databaseService.getDatabase(databaseId);
        Table table = resolvePrimaryTable(normalizedSql);
        String datasetName = buildDatasetName(parseInfo, sqlHash);
        String tableName = resolveTableName(table, datasetType, datasetName);
        if (datasetType == SupersetDatasetType.PHYSICAL && StringUtils.isBlank(tableName)) {
            log.debug("superset dataset fallback to virtual, table unresolved, sqlHash={}",
                    sqlHash);
            datasetType = SupersetDatasetType.VIRTUAL;
            tableName = datasetName;
        }
        String schemaName = resolveSchemaName(table, databaseResp);

        if (existing == null && datasetType == SupersetDatasetType.PHYSICAL && databaseId != null
                && StringUtils.isNotBlank(tableName)) {
            SupersetDatasetDO physical = getByPhysicalTable(databaseId, schemaName, tableName);
            if (physical != null) {
                return physical;
            }
        }
        String datasetDesc = buildDatasetDesc(parseInfo, sqlHash);
        String tags = buildDatasetTags(parseInfo, datasetType, sqlHash);
        DatasetSchemaSpec schemaSpec = buildDatasetSchema(parseInfo, normalizedSql, queryColumns);
        List<SupersetDatasetColumn> columns = schemaSpec.getColumns();
        List<SupersetDatasetMetric> metrics = schemaSpec.getMetrics();
        log.debug("superset dataset register schema, sqlHash={}, queryColumns={}, columns={}, metrics={}",
                sqlHash,
                queryColumns == null ? Collections.emptyList()
                        : queryColumns.stream().map(QueryColumn::getBizName)
                                .collect(Collectors.toList()),
                columns.stream().map(SupersetDatasetColumn::getColumnName)
                        .collect(Collectors.toList()),
                metrics.stream().map(SupersetDatasetMetric::getMetricName)
                        .collect(Collectors.toList()));
        String mainDttmCol = resolveMainDttmCol(columns);

        SupersetDatasetDO record = existing == null ? new SupersetDatasetDO() : existing;
        boolean changed = applyChanges(record, datasetName, datasetDesc, tags, normalizedSql, sql,
                sqlHash, datasetType, databaseId, schemaName, tableName, mainDttmCol,
                parseInfo.getDataSetId(), columns, metrics);

        User safeUser = user == null ? User.getDefaultUser() : user;
        Date now = new Date();
        if (existing == null) {
            record.setCreatedAt(now);
            record.setCreatedBy(safeUser.getName());
            record.setUpdatedAt(now);
            record.setUpdatedBy(safeUser.getName());
            record.setSyncedAt(null);
            record.setSourceType(SupersetDatasetSourceType.CHAT_SQL.name());
            record.setSyncState(SupersetDatasetSyncState.PENDING.name());
            record.setSyncAttemptAt(null);
            record.setNextRetryAt(null);
            record.setRetryCount(0);
            record.setSyncErrorType(null);
            record.setSyncErrorMsg(null);
            save(record);
        } else if (changed) {
            record.setUpdatedAt(now);
            record.setUpdatedBy(safeUser.getName());
            record.setSyncedAt(null);
            record.setSourceType(StringUtils.defaultIfBlank(record.getSourceType(),
                    SupersetDatasetSourceType.CHAT_SQL.name()));
            record.setSyncState(SupersetDatasetSyncState.PENDING.name());
            record.setSyncAttemptAt(null);
            record.setNextRetryAt(null);
            record.setRetryCount(0);
            record.setSyncErrorType(null);
            record.setSyncErrorMsg(null);
            updateById(record);
        }
        return record;
    }

    @Override
    public SupersetDatasetDO getBySqlHash(String sqlHash) {
        if (StringUtils.isBlank(sqlHash)) {
            return null;
        }
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDatasetDO::getSqlHash, sqlHash);
        return getOne(wrapper);
    }

    @Override
    public SupersetDatasetDO getById(Long id) {
        return super.getById(id);
    }

    @Override
    public List<SupersetDatasetDO> listAvailablePersistentDatasets(Long dataSetId) {
        if (dataSetId == null) {
            return Collections.emptyList();
        }
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDatasetDO::getDataSetId, dataSetId)
                .eq(SupersetDatasetDO::getSyncState, SupersetDatasetSyncState.SUCCESS.name())
                .isNotNull(SupersetDatasetDO::getSupersetDatasetId)
                .and(w -> w.eq(SupersetDatasetDO::getSourceType,
                        SupersetDatasetSourceType.SEMANTIC_DATASET.name()).or()
                        .eq(SupersetDatasetDO::getDatasetType, SupersetDatasetType.PHYSICAL.name()));
        List<SupersetDatasetDO> records = list(wrapper);
        if (CollectionUtils.isEmpty(records)) {
            return Collections.emptyList();
        }
        return records.stream().filter(Objects::nonNull)
                .filter(record -> Objects.equals(dataSetId, record.getDataSetId()))
                .filter(record -> record.getSupersetDatasetId() != null)
                .filter(record -> SupersetDatasetSyncState.SUCCESS.name()
                        .equalsIgnoreCase(record.getSyncState()))
                .filter(this::isPersistentDatasetCandidate)
                .sorted(Comparator.comparingInt(this::resolvePersistentDatasetPriority)
                        .thenComparing(SupersetDatasetDO::getUpdatedAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(SupersetDatasetDO::getCreatedAt,
                                Comparator.nullsLast(Comparator.reverseOrder())))
                .collect(Collectors.toList());
    }

    @Override
    public SupersetDatasetDO getByPhysicalTable(Long databaseId, String schemaName,
            String tableName) {
        if (databaseId == null || StringUtils.isBlank(tableName)) {
            return null;
        }
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDatasetDO::getDatabaseId, databaseId)
                .eq(SupersetDatasetDO::getDatasetType, SupersetDatasetType.PHYSICAL.name())
                .eq(SupersetDatasetDO::getTableName, tableName);
        if (StringUtils.isNotBlank(schemaName)) {
            wrapper.eq(SupersetDatasetDO::getSchemaName, schemaName);
        }
        return getOne(wrapper);
    }

    @Override
    public List<SupersetDatasetDO> listForSync(Set<Long> ids) {
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        if (!CollectionUtils.isEmpty(ids)) {
            wrapper.in(SupersetDatasetDO::getId, ids);
            return list(wrapper);
        }
        Date now = new Date();
        wrapper.and(w -> w
                .eq(SupersetDatasetDO::getSyncState, SupersetDatasetSyncState.PENDING.name())
                .or(inner -> inner
                        .eq(SupersetDatasetDO::getSyncState, SupersetDatasetSyncState.FAILED.name())
                        .eq(SupersetDatasetDO::getSyncErrorType,
                                SupersetDatasetSyncErrorType.RETRYABLE.name())
                        .and(t -> t.isNull(SupersetDatasetDO::getNextRetryAt).or()
                                .le(SupersetDatasetDO::getNextRetryAt, now)))
                .or(inner -> inner.isNull(SupersetDatasetDO::getSyncState)
                        .and(t -> t.isNull(SupersetDatasetDO::getSyncedAt).or()
                                .isNull(SupersetDatasetDO::getSupersetDatasetId))));
        return list(wrapper);
    }

    @Override
    public void updateSyncInfo(Long id, Long supersetDatasetId, Date syncedAt) {
        if (id == null) {
            return;
        }
        LambdaUpdateWrapper<SupersetDatasetDO> wrapper = new LambdaUpdateWrapper<>();
        wrapper.eq(SupersetDatasetDO::getId, id)
                .set(SupersetDatasetDO::getSupersetDatasetId, supersetDatasetId)
                .set(SupersetDatasetDO::getSyncedAt, syncedAt)
                .set(SupersetDatasetDO::getSyncState, SupersetDatasetSyncState.SUCCESS.name())
                .set(SupersetDatasetDO::getSyncAttemptAt, syncedAt)
                .set(SupersetDatasetDO::getNextRetryAt, null)
                .set(SupersetDatasetDO::getRetryCount, 0)
                .set(SupersetDatasetDO::getSyncErrorType, null)
                .set(SupersetDatasetDO::getSyncErrorMsg, null);
        update(wrapper);
    }

    @Override
    public void updateSyncAttempt(Long id, Date attemptAt) {
        if (id == null) {
            return;
        }
        LambdaUpdateWrapper<SupersetDatasetDO> wrapper = new LambdaUpdateWrapper<>();
        wrapper.eq(SupersetDatasetDO::getId, id).set(SupersetDatasetDO::getSyncAttemptAt,
                attemptAt);
        update(wrapper);
    }

    @Override
    public void markSyncFailed(Long id, String errorType, String errorMsg, Date attemptAt,
            Date nextRetryAt, Integer retryCount) {
        if (id == null) {
            return;
        }
        String safeType = StringUtils.defaultIfBlank(errorType,
                SupersetDatasetSyncErrorType.RETRYABLE.name());
        LambdaUpdateWrapper<SupersetDatasetDO> wrapper = new LambdaUpdateWrapper<>();
        wrapper.eq(SupersetDatasetDO::getId, id)
                .set(SupersetDatasetDO::getSyncState, SupersetDatasetSyncState.FAILED.name())
                .set(SupersetDatasetDO::getSyncErrorType, safeType)
                .set(SupersetDatasetDO::getSyncErrorMsg, StringUtils.abbreviate(errorMsg, 1000))
                .set(SupersetDatasetDO::getSyncAttemptAt, attemptAt)
                .set(SupersetDatasetDO::getNextRetryAt, nextRetryAt)
                .set(SupersetDatasetDO::getRetryCount, retryCount);
        update(wrapper);
    }

    @Override
    public void markSyncPending(Long id, User user) {
        if (id == null) {
            return;
        }
        User safeUser = user == null ? User.getDefaultUser() : user;
        Date now = new Date();
        LambdaUpdateWrapper<SupersetDatasetDO> wrapper = new LambdaUpdateWrapper<>();
        wrapper.eq(SupersetDatasetDO::getId, id)
                .set(SupersetDatasetDO::getSyncState, SupersetDatasetSyncState.PENDING.name())
                .set(SupersetDatasetDO::getSyncAttemptAt, null)
                .set(SupersetDatasetDO::getNextRetryAt, null)
                .set(SupersetDatasetDO::getRetryCount, 0)
                .set(SupersetDatasetDO::getSyncErrorType, null)
                .set(SupersetDatasetDO::getSyncErrorMsg, null)
                .set(SupersetDatasetDO::getSyncedAt, null).set(SupersetDatasetDO::getUpdatedAt, now)
                .set(SupersetDatasetDO::getUpdatedBy, safeUser.getName());
        update(wrapper);
    }

    @Override
    public PageInfo<SupersetDatasetResp> querySupersetDataset(SupersetDatasetQueryReq queryReq,
            User user) {
        assertAdmin(user);
        SupersetDatasetQueryReq request =
                queryReq == null ? new SupersetDatasetQueryReq() : queryReq;
        PageInfo<SupersetDatasetDO> pageInfo =
                PageHelper.startPage(request.getCurrent(), request.getPageSize())
                        .doSelectPageInfo(() -> querySupersetDatasets(request));
        PageInfo<SupersetDatasetResp> result = PageUtils.pageInfo2PageInfoVo(pageInfo);
        result.setList(pageInfo.getList().stream().map(this::convert).collect(Collectors.toList()));
        return result;
    }

    @Override
    public void deleteSupersetDataset(Long id, User user) {
        assertAdmin(user);
        if (id == null) {
            return;
        }
        removeById(id);
    }

    @Override
    public void deleteSupersetDatasetBatch(List<Long> ids, User user) {
        assertAdmin(user);
        if (CollectionUtils.isEmpty(ids)) {
            return;
        }
        removeByIds(ids);
    }

    private List<SupersetDatasetDO> querySupersetDatasets(SupersetDatasetQueryReq request) {
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = buildQueryWrapper(request);
        return list(wrapper);
    }

    protected LambdaQueryWrapper<SupersetDatasetDO> buildQueryWrapper(
            SupersetDatasetQueryReq request) {
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        if (StringUtils.isNotBlank(request.getDatasetName())) {
            wrapper.like(SupersetDatasetDO::getDatasetName, request.getDatasetName());
        }
        if (StringUtils.isNotBlank(request.getDatasetType())) {
            wrapper.eq(SupersetDatasetDO::getDatasetType, request.getDatasetType());
        }
        if (request.getDatabaseId() != null) {
            wrapper.eq(SupersetDatasetDO::getDatabaseId, request.getDatabaseId());
        }
        if (request.getDataSetId() != null) {
            wrapper.eq(SupersetDatasetDO::getDataSetId, request.getDataSetId());
        }
        if (StringUtils.isNotBlank(request.getSqlHash())) {
            wrapper.eq(SupersetDatasetDO::getSqlHash, request.getSqlHash());
        }
        if (request.getSupersetDatasetId() != null) {
            wrapper.eq(SupersetDatasetDO::getSupersetDatasetId, request.getSupersetDatasetId());
        }
        if (StringUtils.isNotBlank(request.getCreatedBy())) {
            wrapper.eq(SupersetDatasetDO::getCreatedBy, request.getCreatedBy());
        }
        if (StringUtils.isNotBlank(request.getSourceType())) {
            wrapper.eq(SupersetDatasetDO::getSourceType, request.getSourceType());
        }
        if (StringUtils.isNotBlank(request.getSyncState())) {
            wrapper.eq(SupersetDatasetDO::getSyncState, request.getSyncState());
        }
        if (request.getSynced() != null) {
            if (Boolean.TRUE.equals(request.getSynced())) {
                wrapper.isNotNull(SupersetDatasetDO::getSyncedAt);
            } else {
                wrapper.isNull(SupersetDatasetDO::getSyncedAt);
            }
        }
        if (request.getNeedSync() != null) {
            if (Boolean.TRUE.equals(request.getNeedSync())) {
                wrapper.and(w -> w.isNull(SupersetDatasetDO::getSyncState)
                        .and(inner -> inner.isNull(SupersetDatasetDO::getSyncedAt).or()
                                .isNull(SupersetDatasetDO::getSupersetDatasetId))
                        .or().in(SupersetDatasetDO::getSyncState,
                                List.of(SupersetDatasetSyncState.PENDING.name(),
                                        SupersetDatasetSyncState.FAILED.name())));
            } else {
                wrapper.eq(SupersetDatasetDO::getSyncState,
                        SupersetDatasetSyncState.SUCCESS.name());
            }
        }
        wrapper.orderByDesc(SupersetDatasetDO::getUpdatedAt, SupersetDatasetDO::getCreatedAt);
        return wrapper;
    }

    private SupersetDatasetResp convert(SupersetDatasetDO record) {
        SupersetDatasetResp resp = new SupersetDatasetResp();
        BeanMapper.mapper(record, resp);
        return resp;
    }

    private boolean isPersistentDatasetCandidate(SupersetDatasetDO record) {
        if (record == null) {
            return false;
        }
        if (SupersetDatasetSourceType.SEMANTIC_DATASET.name()
                .equalsIgnoreCase(record.getSourceType())) {
            return true;
        }
        return SupersetDatasetType.PHYSICAL.name().equalsIgnoreCase(record.getDatasetType());
    }

    private int resolvePersistentDatasetPriority(SupersetDatasetDO record) {
        if (record == null) {
            return Integer.MAX_VALUE;
        }
        if (SupersetDatasetSourceType.SEMANTIC_DATASET.name()
                .equalsIgnoreCase(record.getSourceType())) {
            return 0;
        }
        if (SupersetDatasetType.PHYSICAL.name().equalsIgnoreCase(record.getDatasetType())) {
            return 1;
        }
        return 2;
    }

    private void assertAdmin(User user) {
        if (user == null || !user.isSuperAdmin()) {
            throw new InvalidPermissionException("仅管理员可以操作 Superset 数据集注册表");
        }
    }

    private boolean applyChanges(SupersetDatasetDO record, String datasetName, String datasetDesc,
            String tags, String normalizedSql, String sqlText, String sqlHash,
            SupersetDatasetType datasetType, Long databaseId, String schemaName, String tableName,
            String mainDttmCol, Long dataSetId, List<SupersetDatasetColumn> columns,
            List<SupersetDatasetMetric> metrics) {
        boolean changed = false;
        if (!Objects.equals(record.getDatasetName(), datasetName)) {
            record.setDatasetName(datasetName);
            changed = true;
        }
        if (!Objects.equals(record.getDatasetDesc(), datasetDesc)) {
            record.setDatasetDesc(datasetDesc);
            changed = true;
        }
        if (!Objects.equals(record.getTags(), tags)) {
            record.setTags(tags);
            changed = true;
        }
        if (!Objects.equals(record.getNormalizedSql(), normalizedSql)) {
            record.setNormalizedSql(normalizedSql);
            changed = true;
        }
        if (!Objects.equals(record.getSqlText(), sqlText)) {
            record.setSqlText(sqlText);
            changed = true;
        }
        if (!Objects.equals(record.getSqlHash(), sqlHash)) {
            record.setSqlHash(sqlHash);
            changed = true;
        }
        String typeName = datasetType == null ? null : datasetType.name();
        if (!Objects.equals(record.getDatasetType(), typeName)) {
            record.setDatasetType(typeName);
            changed = true;
        }
        if (!Objects.equals(record.getDatabaseId(), databaseId)) {
            record.setDatabaseId(databaseId);
            changed = true;
        }
        if (!Objects.equals(record.getSchemaName(), schemaName)) {
            record.setSchemaName(schemaName);
            changed = true;
        }
        if (!Objects.equals(record.getTableName(), tableName)) {
            record.setTableName(tableName);
            changed = true;
        }
        if (!Objects.equals(record.getMainDttmCol(), mainDttmCol)) {
            record.setMainDttmCol(mainDttmCol);
            changed = true;
        }
        if (!Objects.equals(record.getDataSetId(), dataSetId)) {
            record.setDataSetId(dataSetId);
            changed = true;
        }
        String columnsJson = JsonUtil.toString(columns);
        String metricsJson = JsonUtil.toString(metrics);
        if (!Objects.equals(record.getColumns(), columnsJson)) {
            record.setColumns(columnsJson);
            changed = true;
        }
        if (!Objects.equals(record.getMetrics(), metricsJson)) {
            record.setMetrics(metricsJson);
            changed = true;
        }
        return changed;
    }

    private SupersetDatasetType resolveDatasetType(String normalizedSql) {
        return SupersetDatasetType.VIRTUAL;
    }

    private Long resolveDatabaseId(SemanticParseInfo parseInfo) {
        Long dataSetId = parseInfo == null ? null : parseInfo.getDataSetId();
        if (dataSetId == null) {
            return null;
        }
        DataSetResp dataSetResp = dataSetService.getDataSet(dataSetId);
        if (dataSetResp == null) {
            return null;
        }
        Long modelId = resolvePreferredModelId(dataSetResp);
        if (modelId == null) {
            return null;
        }
        ModelResp modelResp = modelService.getModel(modelId);
        return modelResp == null ? null : modelResp.getDatabaseId();
    }

    private Long resolvePreferredModelId(DataSetResp dataSetResp) {
        if (dataSetResp == null) {
            return null;
        }
        List<Long> includeAllModels = dataSetResp.getAllIncludeAllModels();
        if (includeAllModels != null && !includeAllModels.isEmpty()) {
            return includeAllModels.get(0);
        }
        List<Long> modelIds = dataSetResp.getAllModels();
        if (modelIds == null || modelIds.isEmpty()) {
            return null;
        }
        if (modelIds.size() > 1) {
            log.debug(
                    "superset dataset registry resolved by first model, dataSetId={}, modelIds={}",
                    dataSetResp.getId(), modelIds);
        }
        return modelIds.get(0);
    }

    private Table resolvePrimaryTable(String sql) {
        try {
            return SqlSelectHelper.getTable(sql);
        } catch (Exception ex) {
            log.debug("resolve primary table failed", ex);
            return null;
        }
    }

    private String resolveTableName(Table table, SupersetDatasetType datasetType,
            String datasetName) {
        if (datasetType == SupersetDatasetType.PHYSICAL) {
            if (table == null || StringUtils.isBlank(table.getName())) {
                return null;
            }
            return StringUtil.replaceBackticks(table.getName());
        }
        return datasetName;
    }

    private String resolveSchemaName(Table table, DatabaseResp databaseResp) {
        String schema = table == null ? null : table.getSchemaName();
        if (StringUtils.isBlank(schema) && databaseResp != null) {
            schema = databaseResp.getSchema();
            if (StringUtils.isBlank(schema)
                    && EngineType.POSTGRESQL.getName().equalsIgnoreCase(databaseResp.getType())) {
                schema = "public";
            }
        }
        return StringUtils.defaultIfBlank(schema, null);
    }

    private String buildDatasetName(SemanticParseInfo parseInfo, String sqlHash) {
        String metricPart = joinNames(parseInfo == null ? null : parseInfo.getMetrics(), 3);
        String dimensionPart = joinNames(parseInfo == null ? null : parseInfo.getDimensions(), 3);
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotBlank(metricPart)) {
            builder.append("指标").append(metricPart);
        }
        if (StringUtils.isNotBlank(dimensionPart)) {
            if (builder.length() > 0) {
                builder.append("·");
            }
            builder.append("维度").append(dimensionPart);
        }
        if (builder.length() == 0) {
            builder.append("对话查询数据集");
        }
        String hashSuffix =
                StringUtils.isBlank(sqlHash) ? null : StringUtils.substring(sqlHash, 0, 6);
        if (StringUtils.isNotBlank(hashSuffix)) {
            builder.append("·").append(hashSuffix);
        }
        return truncateName(builder.toString());
    }

    private String buildDatasetDesc(SemanticParseInfo parseInfo, String sqlHash) {
        String metricPart = joinNames(parseInfo == null ? null : parseInfo.getMetrics(), 5);
        String dimensionPart = joinNames(parseInfo == null ? null : parseInfo.getDimensions(), 5);
        int filterCount = 0;
        if (parseInfo != null) {
            filterCount += parseInfo.getDimensionFilters() == null ? 0
                    : parseInfo.getDimensionFilters().size();
            filterCount +=
                    parseInfo.getMetricFilters() == null ? 0 : parseInfo.getMetricFilters().size();
        }
        StringBuilder builder = new StringBuilder("对话生成数据集");
        if (StringUtils.isNotBlank(sqlHash)) {
            builder.append("，SQL Hash: ").append(sqlHash);
        }
        if (StringUtils.isNotBlank(metricPart)) {
            builder.append("，指标: ").append(metricPart);
        }
        if (StringUtils.isNotBlank(dimensionPart)) {
            builder.append("，维度: ").append(dimensionPart);
        }
        if (filterCount > 0) {
            builder.append("，筛选: ").append(filterCount).append("项");
        }
        return builder.toString();
    }

    private String buildDatasetTags(SemanticParseInfo parseInfo, SupersetDatasetType datasetType,
            String sqlHash) {
        List<String> tags = new ArrayList<>();
        tags.add("supersonic");
        tags.add("chat");
        if (parseInfo != null && parseInfo.getDataSetId() != null) {
            tags.add("datasetId:" + parseInfo.getDataSetId());
        }
        if (datasetType != null) {
            tags.add(datasetType.name().toLowerCase());
        }
        if (StringUtils.isNotBlank(sqlHash)) {
            tags.add("sqlHash:" + StringUtils.substring(sqlHash, 0, 8));
        }
        return JsonUtil.toString(tags);
    }

    private DatasetSchemaSpec buildDatasetSchema(SemanticParseInfo parseInfo, String sql,
            List<QueryColumn> queryColumns) {
        List<DatasetOutputField> outputFields = buildOutputFields(parseInfo, sql, queryColumns);
        if (!outputFields.isEmpty()) {
            return new DatasetSchemaSpec(buildDatasetColumns(outputFields),
                    buildDatasetMetrics(outputFields));
        }
        if (!CollectionUtils.isEmpty(queryColumns)) {
            List<DatasetOutputField> queryFields = buildQueryColumnOutputFields(queryColumns);
            if (!queryFields.isEmpty()) {
                return new DatasetSchemaSpec(buildDatasetColumns(queryFields),
                        buildDatasetMetrics(queryFields));
            }
        }
        return new DatasetSchemaSpec(buildDatasetColumns(parseInfo), buildDatasetMetrics(parseInfo));
    }

    private List<DatasetOutputField> buildOutputFields(SemanticParseInfo parseInfo, String sql,
            List<QueryColumn> queryColumns) {
        List<SelectItem<?>> selectItems = resolveTopLevelSelectItems(sql);
        if (CollectionUtils.isEmpty(selectItems)) {
            return Collections.emptyList();
        }
        Map<String, SchemaElement> metricLookup =
                buildSchemaElementLookup(parseInfo == null ? null : parseInfo.getMetrics());
        Map<String, SchemaElement> dimensionLookup =
                buildSchemaElementLookup(parseInfo == null ? null : parseInfo.getDimensions());
        Map<String, QueryColumn> queryColumnLookup = buildQueryColumnLookup(queryColumns);
        List<DatasetOutputField> outputFields = new ArrayList<>();
        for (int i = 0; i < selectItems.size(); i++) {
            SelectItem<?> selectItem = selectItems.get(i);
            if (selectItem == null || selectItem.getExpression() == null) {
                continue;
            }
            QueryColumn positionalColumn = resolveQueryColumnByIndex(queryColumns, i);
            String outputName = resolveOutputName(selectItem, positionalColumn);
            if (StringUtils.isBlank(outputName)) {
                continue;
            }
            QueryColumn queryColumn = positionalColumn == null
                    ? queryColumnLookup.get(normalizeName(outputName))
                    : positionalColumn;
            Set<String> sourceFields = resolveSourceFields(selectItem.getExpression());
            SchemaElement metricElement =
                    resolveMatchedElement(metricLookup, outputName, sourceFields);
            SchemaElement dimensionElement =
                    resolveMatchedElement(dimensionLookup, outputName, sourceFields);
            boolean isTime = isTimeField(dimensionElement, queryColumn);
            boolean numeric = metricElement != null || isNumericQueryColumn(queryColumn)
                    || isLikelyNumericExpression(selectItem.getExpression());
            boolean groupBy =
                    metricElement == null && (isTime || dimensionElement != null || !numeric);

            DatasetOutputField outputField = new DatasetOutputField();
            outputField.setOutputName(outputName);
            outputField.setVerboseName(resolveVerboseName(queryColumn, metricElement,
                    dimensionElement, outputField.getOutputName()));
            outputField.setDescription(resolveDescription(queryColumn, metricElement,
                    dimensionElement));
            outputField.setDttm(isTime);
            outputField.setGroupby(groupBy);
            outputField.setFilterable(groupBy);
            outputField.setMetricCandidate(!groupBy && numeric);
            outputField.setMatchedMetric(metricElement != null);
            outputField.setAggregate(metricElement == null ? null : metricElement.getDefaultAgg());
            outputField.setType(isTime ? "DATE" : (numeric ? "NUMBER" : "STRING"));
            outputFields.add(outputField);
        }
        return outputFields;
    }

    private List<DatasetOutputField> buildQueryColumnOutputFields(List<QueryColumn> queryColumns) {
        if (CollectionUtils.isEmpty(queryColumns)) {
            return Collections.emptyList();
        }
        List<DatasetOutputField> outputFields = new ArrayList<>();
        for (QueryColumn queryColumn : queryColumns) {
            if (queryColumn == null || StringUtils.isBlank(queryColumn.getBizName())) {
                continue;
            }
            boolean isTime = isTimeQueryColumn(queryColumn);
            boolean numeric = isNumericQueryColumn(queryColumn);
            boolean groupBy = !numeric || isTime;
            DatasetOutputField outputField = new DatasetOutputField();
            outputField.setOutputName(queryColumn.getBizName());
            outputField.setVerboseName(StringUtils.defaultIfBlank(queryColumn.getName(),
                    queryColumn.getBizName()));
            outputField.setDescription(queryColumn.getComment());
            outputField.setDttm(isTime);
            outputField.setGroupby(groupBy);
            outputField.setFilterable(groupBy);
            outputField.setMetricCandidate(!groupBy && numeric);
            outputField.setMatchedMetric(numeric);
            outputField.setAggregate("SUM");
            outputField.setType(isTime ? "DATE" : (numeric ? "NUMBER" : "STRING"));
            outputFields.add(outputField);
        }
        return outputFields;
    }

    private List<SupersetDatasetColumn> buildDatasetColumns(SemanticParseInfo parseInfo) {
        if (parseInfo == null) {
            return new ArrayList<>();
        }
        List<SupersetDatasetColumn> columns = new ArrayList<>();
        if (!CollectionUtils.isEmpty(parseInfo.getDimensions())) {
            for (SchemaElement element : parseInfo.getDimensions()) {
                String name = resolveSchemaElementColumnName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                SupersetDatasetColumn column = new SupersetDatasetColumn();
                column.setColumnName(name);
                column.setVerboseName(element.getName());
                column.setDescription(element.getDescription());
                column.setGroupby(true);
                column.setFilterable(true);
                if (element.isPartitionTime()) {
                    column.setIsDttm(true);
                    column.setType("DATE");
                } else {
                    column.setIsDttm(false);
                    column.setType("STRING");
                }
                columns.add(column);
            }
        }
        if (!CollectionUtils.isEmpty(parseInfo.getMetrics())) {
            for (SchemaElement element : parseInfo.getMetrics()) {
                String name = resolveSchemaElementColumnName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                SupersetDatasetColumn column = new SupersetDatasetColumn();
                column.setColumnName(name);
                column.setVerboseName(element.getName());
                column.setDescription(element.getDescription());
                column.setGroupby(false);
                column.setFilterable(false);
                column.setType("NUMBER");
                columns.add(column);
            }
        }
        return columns
                .stream().collect(Collectors.toMap(SupersetDatasetColumn::getColumnName,
                        item -> item, (left, right) -> left))
                .values().stream().collect(Collectors.toList());
    }

    private List<SupersetDatasetColumn> buildDatasetColumns(List<DatasetOutputField> outputFields) {
        if (CollectionUtils.isEmpty(outputFields)) {
            return Collections.emptyList();
        }
        Map<String, SupersetDatasetColumn> columns = new LinkedHashMap<>();
        for (DatasetOutputField outputField : outputFields) {
            if (outputField == null || StringUtils.isBlank(outputField.getOutputName())) {
                continue;
            }
            SupersetDatasetColumn column = new SupersetDatasetColumn();
            column.setColumnName(outputField.getOutputName());
            column.setVerboseName(StringUtils.defaultIfBlank(outputField.getVerboseName(),
                    outputField.getOutputName()));
            column.setDescription(outputField.getDescription());
            column.setGroupby(outputField.isGroupby());
            column.setFilterable(outputField.isFilterable());
            column.setIsDttm(outputField.isDttm());
            column.setType(outputField.getType());
            columns.putIfAbsent(normalizeName(outputField.getOutputName()), column);
        }
        return new ArrayList<>(columns.values());
    }

    private List<SupersetDatasetMetric> buildDatasetMetrics(SemanticParseInfo parseInfo) {
        if (parseInfo == null || CollectionUtils.isEmpty(parseInfo.getMetrics())) {
            return new ArrayList<>();
        }
        List<SupersetDatasetMetric> metrics = new ArrayList<>();
        for (SchemaElement element : parseInfo.getMetrics()) {
            String name = resolveSchemaElementColumnName(element);
            if (StringUtils.isBlank(name)) {
                continue;
            }
            SupersetDatasetMetric metric = new SupersetDatasetMetric();
            metric.setMetricName(name);
            String agg = StringUtils.defaultIfBlank(element.getDefaultAgg(), "SUM").toUpperCase();
            metric.setExpression(agg + "(" + name + ")");
            metric.setMetricType("SQL");
            metric.setVerboseName(element.getName());
            metric.setDescription(element.getDescription());
            metrics.add(metric);
        }
        return metrics;
    }

    private List<SupersetDatasetMetric> buildDatasetMetrics(List<DatasetOutputField> outputFields) {
        if (CollectionUtils.isEmpty(outputFields)) {
            return Collections.emptyList();
        }
        List<DatasetOutputField> selectedMetrics = outputFields.stream()
                .filter(field -> field != null && field.isMatchedMetric() && field.isMetricCandidate())
                .collect(Collectors.toList());
        if (selectedMetrics.isEmpty()) {
            selectedMetrics = outputFields.stream()
                    .filter(field -> field != null && field.isMetricCandidate())
                    .collect(Collectors.toList());
        }
        Map<String, SupersetDatasetMetric> metrics = new LinkedHashMap<>();
        for (DatasetOutputField outputField : selectedMetrics) {
            if (StringUtils.isBlank(outputField.getOutputName())) {
                continue;
            }
            SupersetDatasetMetric metric = new SupersetDatasetMetric();
            metric.setMetricName(outputField.getOutputName());
            metric.setExpression(buildMetricExpression(outputField.getOutputName(),
                    outputField.getAggregate()));
            metric.setMetricType("SQL");
            metric.setVerboseName(StringUtils.defaultIfBlank(outputField.getVerboseName(),
                    outputField.getOutputName()));
            metric.setDescription(outputField.getDescription());
            metrics.putIfAbsent(normalizeName(outputField.getOutputName()), metric);
        }
        return new ArrayList<>(metrics.values());
    }

    private String resolveSchemaElementColumnName(SchemaElement element) {
        if (element == null) {
            return null;
        }
        if (StringUtils.isNotBlank(element.getBizName())) {
            return element.getBizName();
        }
        return element.getName();
    }

    private List<SelectItem<?>> resolveTopLevelSelectItems(String sql) {
        if (StringUtils.isBlank(sql)) {
            return Collections.emptyList();
        }
        try {
            Select select = SqlSelectHelper.getSelect(sql);
            if (select instanceof PlainSelect) {
                return ((PlainSelect) select).getSelectItems();
            }
            if (select instanceof SetOperationList) {
                List<Select> selects = ((SetOperationList) select).getSelects();
                if (!CollectionUtils.isEmpty(selects) && selects.get(0) instanceof PlainSelect) {
                    return ((PlainSelect) selects.get(0)).getSelectItems();
                }
            }
        } catch (Exception ex) {
            log.debug("resolve top level select items failed", ex);
        }
        return Collections.emptyList();
    }

    private Map<String, SchemaElement> buildSchemaElementLookup(Set<SchemaElement> elements) {
        Map<String, SchemaElement> lookup = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(elements)) {
            return lookup;
        }
        for (SchemaElement element : elements) {
            if (element == null) {
                continue;
            }
            addLookupEntry(lookup, element.getBizName(), element);
            addLookupEntry(lookup, element.getName(), element);
        }
        return lookup;
    }

    private Map<String, QueryColumn> buildQueryColumnLookup(List<QueryColumn> queryColumns) {
        Map<String, QueryColumn> lookup = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(queryColumns)) {
            return lookup;
        }
        for (QueryColumn queryColumn : queryColumns) {
            if (queryColumn == null) {
                continue;
            }
            addLookupEntry(lookup, queryColumn.getBizName(), queryColumn);
            addLookupEntry(lookup, queryColumn.getName(), queryColumn);
        }
        return lookup;
    }

    private <T> void addLookupEntry(Map<String, T> lookup, String key, T value) {
        if (lookup == null || value == null || StringUtils.isBlank(key)) {
            return;
        }
        lookup.putIfAbsent(normalizeName(key), value);
    }

    private QueryColumn resolveQueryColumnByIndex(List<QueryColumn> queryColumns, int index) {
        if (CollectionUtils.isEmpty(queryColumns) || index < 0 || index >= queryColumns.size()) {
            return null;
        }
        return queryColumns.get(index);
    }

    private String resolveOutputName(SelectItem<?> selectItem, QueryColumn positionalColumn) {
        if (selectItem == null) {
            return null;
        }
        if (selectItem.getAlias() != null && StringUtils.isNotBlank(selectItem.getAlias().getName())) {
            return StringUtil.replaceBackticks(selectItem.getAlias().getName());
        }
        if (positionalColumn != null && StringUtils.isNotBlank(positionalColumn.getBizName())) {
            return positionalColumn.getBizName();
        }
        if (selectItem.getExpression() != null) {
            String expression = StringUtil.replaceBackticks(selectItem.getExpression().toString());
            if (StringUtils.isNotBlank(expression)) {
                return expression;
            }
        }
        return null;
    }

    private Set<String> resolveSourceFields(Expression expression) {
        Set<String> sourceFields = new LinkedHashSet<>();
        if (expression == null) {
            return sourceFields;
        }
        try {
            SqlSelectHelper.getFieldsFromExpr(expression, sourceFields);
        } catch (Exception ex) {
            log.debug("resolve source fields failed, expression={}", expression, ex);
        }
        return sourceFields;
    }

    private SchemaElement resolveMatchedElement(Map<String, SchemaElement> lookup, String outputName,
            Set<String> sourceFields) {
        if (lookup == null || lookup.isEmpty()) {
            return null;
        }
        SchemaElement matched = lookup.get(normalizeName(outputName));
        if (matched != null || CollectionUtils.isEmpty(sourceFields)) {
            return matched;
        }
        for (String sourceField : sourceFields) {
            matched = lookup.get(normalizeName(sourceField));
            if (matched != null) {
                return matched;
            }
        }
        return null;
    }

    private boolean isTimeField(SchemaElement dimensionElement, QueryColumn queryColumn) {
        return (dimensionElement != null && dimensionElement.isPartitionTime())
                || isTimeQueryColumn(queryColumn);
    }

    private boolean isTimeQueryColumn(QueryColumn queryColumn) {
        if (queryColumn == null) {
            return false;
        }
        String showType = StringUtils.defaultString(queryColumn.getShowType());
        return "DATE".equalsIgnoreCase(showType) || "TIME".equalsIgnoreCase(showType)
                || isTimeType(queryColumn.getType());
    }

    private boolean isNumericQueryColumn(QueryColumn queryColumn) {
        if (queryColumn == null) {
            return false;
        }
        String showType = StringUtils.defaultString(queryColumn.getShowType());
        return "NUMBER".equalsIgnoreCase(showType) || isNumericType(queryColumn.getType());
    }

    private boolean isLikelyNumericExpression(Expression expression) {
        if (expression == null) {
            return false;
        }
        String text = StringUtils.lowerCase(StringUtil.replaceBackticks(expression.toString()));
        return text.contains("sum(") || text.contains("avg(") || text.contains("count(")
                || text.contains("max(") || text.contains("min(") || text.contains("rank(")
                || text.contains("row_number(") || text.contains("dense_rank(")
                || text.contains("percent_rank(");
    }

    private String resolveVerboseName(QueryColumn queryColumn, SchemaElement metricElement,
            SchemaElement dimensionElement, String outputName) {
        if (queryColumn != null && StringUtils.isNotBlank(queryColumn.getName())) {
            return queryColumn.getName();
        }
        if (metricElement != null && StringUtils.isNotBlank(metricElement.getName())) {
            return metricElement.getName();
        }
        if (dimensionElement != null && StringUtils.isNotBlank(dimensionElement.getName())) {
            return dimensionElement.getName();
        }
        return outputName;
    }

    private String resolveDescription(QueryColumn queryColumn, SchemaElement metricElement,
            SchemaElement dimensionElement) {
        if (queryColumn != null && StringUtils.isNotBlank(queryColumn.getComment())) {
            return queryColumn.getComment();
        }
        if (metricElement != null && StringUtils.isNotBlank(metricElement.getDescription())) {
            return metricElement.getDescription();
        }
        if (dimensionElement != null && StringUtils.isNotBlank(dimensionElement.getDescription())) {
            return dimensionElement.getDescription();
        }
        return null;
    }

    private String buildMetricExpression(String metricName, String aggregate) {
        String agg = StringUtils.defaultIfBlank(aggregate, "SUM").toUpperCase();
        return agg + "(" + metricName + ")";
    }

    private String normalizeName(String name) {
        return StringUtils.lowerCase(StringUtils.trimToEmpty(name));
    }

    private boolean isNumericType(String type) {
        String normalized = StringUtils.defaultString(type).toUpperCase();
        return normalized.contains("INT") || normalized.contains("LONG")
                || normalized.contains("DOUBLE") || normalized.contains("FLOAT")
                || normalized.contains("DECIMAL") || normalized.contains("NUMBER")
                || normalized.contains("NUMERIC") || normalized.contains("BIGINT")
                || normalized.contains("SHORT");
    }

    private boolean isTimeType(String type) {
        String normalized = StringUtils.defaultString(type).toUpperCase();
        return normalized.contains("DATE") || normalized.contains("TIME")
                || normalized.contains("TIMESTAMP");
    }

    private String resolveMainDttmCol(List<SupersetDatasetColumn> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return null;
        }
        for (SupersetDatasetColumn column : columns) {
            if (column != null && Boolean.TRUE.equals(column.getIsDttm())
                    && StringUtils.isNotBlank(column.getColumnName())) {
                return column.getColumnName();
            }
        }
        return null;
    }

    private String resolveSchemaElementName(SchemaElement element) {
        if (element == null) {
            return null;
        }
        if (StringUtils.isNotBlank(element.getBizName())) {
            return element.getBizName();
        }
        return element.getName();
    }

    private String joinNames(Set<SchemaElement> elements, int limit) {
        if (CollectionUtils.isEmpty(elements)) {
            return "";
        }
        List<String> names = elements.stream().map(this::resolveSchemaElementName)
                .filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
        if (names.isEmpty()) {
            return "";
        }
        if (names.size() > limit) {
            names = names.subList(0, limit);
        }
        return String.join("、", names);
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

    private static class DatasetSchemaSpec {
        private final List<SupersetDatasetColumn> columns;
        private final List<SupersetDatasetMetric> metrics;

        private DatasetSchemaSpec(List<SupersetDatasetColumn> columns,
                List<SupersetDatasetMetric> metrics) {
            this.columns = columns == null ? Collections.emptyList() : columns;
            this.metrics = metrics == null ? Collections.emptyList() : metrics;
        }

        private List<SupersetDatasetColumn> getColumns() {
            return columns;
        }

        private List<SupersetDatasetMetric> getMetrics() {
            return metrics;
        }
    }

    private static class DatasetOutputField {
        private String outputName;
        private String verboseName;
        private String description;
        private String type;
        private boolean dttm;
        private boolean groupby;
        private boolean filterable;
        private boolean metricCandidate;
        private boolean matchedMetric;
        private String aggregate;

        private String getOutputName() {
            return outputName;
        }

        private void setOutputName(String outputName) {
            this.outputName = outputName;
        }

        private String getVerboseName() {
            return verboseName;
        }

        private void setVerboseName(String verboseName) {
            this.verboseName = verboseName;
        }

        private String getDescription() {
            return description;
        }

        private void setDescription(String description) {
            this.description = description;
        }

        private String getType() {
            return type;
        }

        private void setType(String type) {
            this.type = type;
        }

        private boolean isDttm() {
            return dttm;
        }

        private void setDttm(boolean dttm) {
            this.dttm = dttm;
        }

        private boolean isGroupby() {
            return groupby;
        }

        private void setGroupby(boolean groupby) {
            this.groupby = groupby;
        }

        private boolean isFilterable() {
            return filterable;
        }

        private void setFilterable(boolean filterable) {
            this.filterable = filterable;
        }

        private boolean isMetricCandidate() {
            return metricCandidate;
        }

        private void setMetricCandidate(boolean metricCandidate) {
            this.metricCandidate = metricCandidate;
        }

        private boolean isMatchedMetric() {
            return matchedMetric;
        }

        private void setMatchedMetric(boolean matchedMetric) {
            this.matchedMetric = matchedMetric;
        }

        private String getAggregate() {
            return aggregate;
        }

        private void setAggregate(String aggregate) {
            this.aggregate = aggregate;
        }
    }
}
