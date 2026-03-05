package com.tencent.supersonic.headless.server.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.tencent.supersonic.common.jsqlparser.SqlNormalizeHelper;
import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
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
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetType;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.schema.Table;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
    public SupersetDatasetDO registerDataset(SemanticParseInfo parseInfo, String sql, User user) {
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
        List<SupersetDatasetColumn> columns = buildDatasetColumns(parseInfo);
        List<SupersetDatasetMetric> metrics = buildDatasetMetrics(parseInfo);
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
            save(record);
        } else if (changed) {
            record.setUpdatedAt(now);
            record.setUpdatedBy(safeUser.getName());
            record.setSyncedAt(null);
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
        }
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
                .set(SupersetDatasetDO::getSyncedAt, syncedAt);
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
        if (request.getSynced() != null) {
            if (Boolean.TRUE.equals(request.getSynced())) {
                wrapper.isNotNull(SupersetDatasetDO::getSyncedAt);
            } else {
                wrapper.isNull(SupersetDatasetDO::getSyncedAt);
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

    private String resolveSchemaElementColumnName(SchemaElement element) {
        if (element == null) {
            return null;
        }
        if (StringUtils.isNotBlank(element.getBizName())) {
            return element.getBizName();
        }
        return element.getName();
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
}
