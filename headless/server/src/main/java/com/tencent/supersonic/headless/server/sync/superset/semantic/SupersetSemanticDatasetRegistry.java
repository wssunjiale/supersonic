package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.tencent.supersonic.common.jsqlparser.SqlNormalizeHelper;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.persistence.mapper.SupersetDatasetMapper;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSourceType;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSyncState;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Objects;

@Service
@Slf4j
public class SupersetSemanticDatasetRegistry {

    private final SupersetDatasetMapper mapper;

    public SupersetSemanticDatasetRegistry(SupersetDatasetMapper mapper) {
        this.mapper = mapper;
    }

    public SupersetDatasetDO upsert(SemanticDatasetMapping mapping, User user) {
        if (mapping == null || StringUtils.isBlank(mapping.getSqlHash())) {
            return null;
        }
        SupersetDatasetDO existing = getBySqlHash(mapping.getSqlHash());
        SupersetDatasetDO record = existing == null ? new SupersetDatasetDO() : existing;

        String normalizedSql = SqlNormalizeHelper.normalizeSql(mapping.getSupersetSql());

        boolean changed = false;
        changed |= apply(record.getSqlHash(), mapping.getSqlHash(), record::setSqlHash);
        changed |= apply(record.getSqlText(), mapping.getSupersetSql(), record::setSqlText);
        changed |= apply(record.getNormalizedSql(), normalizedSql, record::setNormalizedSql);
        changed |= apply(record.getDatasetName(), mapping.getDataSetName(), record::setDatasetName);
        changed |= apply(record.getDatasetDesc(), mapping.getDatasetDesc(), record::setDatasetDesc);
        changed |= apply(record.getTags(), mapping.getTagsJson(), record::setTags);
        changed |= apply(record.getDatasetType(), SupersetDatasetType.VIRTUAL.name(),
                record::setDatasetType);
        changed |= apply(record.getDataSetId(), mapping.getDataSetId(), record::setDataSetId);
        changed |= apply(record.getDatabaseId(), mapping.getDatabaseId(), record::setDatabaseId);
        changed |= apply(record.getSchemaName(), mapping.getSchemaName(), record::setSchemaName);
        changed |=
                apply(record.getTableName(), mapping.getSupersetTableName(), record::setTableName);
        changed |= apply(record.getMainDttmCol(), mapping.getMainDttmCol(), record::setMainDttmCol);
        changed |= apply(record.getColumns(), JsonUtil.toString(mapping.getColumns()),
                record::setColumns);
        changed |= apply(record.getMetrics(), JsonUtil.toString(mapping.getMetrics()),
                record::setMetrics);
        changed |= apply(record.getSourceType(), SupersetDatasetSourceType.SEMANTIC_DATASET.name(),
                record::setSourceType);

        User safeUser = user == null ? User.getDefaultUser() : user;
        Date now = new Date();
        if (existing == null) {
            record.setCreatedAt(now);
            record.setCreatedBy(safeUser.getName());
            record.setUpdatedAt(now);
            record.setUpdatedBy(safeUser.getName());
            record.setSyncedAt(null);
            record.setSyncState(SupersetDatasetSyncState.PENDING.name());
            record.setSyncAttemptAt(null);
            record.setNextRetryAt(null);
            record.setRetryCount(0);
            record.setSyncErrorType(null);
            record.setSyncErrorMsg(null);
            mapper.insert(record);
            return record;
        }
        if (changed) {
            record.setUpdatedAt(now);
            record.setUpdatedBy(safeUser.getName());
            record.setSyncedAt(null);
            record.setSyncState(SupersetDatasetSyncState.PENDING.name());
            record.setSyncAttemptAt(null);
            record.setNextRetryAt(null);
            record.setRetryCount(0);
            record.setSyncErrorType(null);
            record.setSyncErrorMsg(null);
            mapper.updateById(record);
        }
        return record;
    }

    public SupersetDatasetDO getBySqlHash(String sqlHash) {
        if (StringUtils.isBlank(sqlHash)) {
            return null;
        }
        LambdaQueryWrapper<SupersetDatasetDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDatasetDO::getSqlHash, sqlHash);
        return mapper.selectOne(wrapper);
    }

    private boolean apply(String current, String next, java.util.function.Consumer<String> setter) {
        String a = StringUtils.defaultString(current);
        String b = StringUtils.defaultString(next);
        if (!Objects.equals(a, b)) {
            setter.accept(next);
            return true;
        }
        return false;
    }

    private boolean apply(Long current, Long next, java.util.function.Consumer<Long> setter) {
        if (!Objects.equals(current, next)) {
            setter.accept(next);
            return true;
        }
        return false;
    }
}
