package com.tencent.supersonic.headless.server.service;

import com.github.pagehelper.PageInfo;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.request.SupersetDatasetQueryReq;
import com.tencent.supersonic.headless.api.pojo.response.SupersetDatasetResp;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;

import java.util.Date;
import java.util.List;
import java.util.Set;

public interface SupersetDatasetRegistryService {

    SupersetDatasetDO registerDataset(SemanticParseInfo parseInfo, String sql, User user);

    SupersetDatasetDO getBySqlHash(String sqlHash);

    SupersetDatasetDO getByPhysicalTable(Long databaseId, String schemaName, String tableName);

    SupersetDatasetDO getById(Long id);

    List<SupersetDatasetDO> listForSync(Set<Long> ids);

    void updateSyncInfo(Long id, Long supersetDatasetId, Date syncedAt);

    void updateSyncAttempt(Long id, Date attemptAt);

    void markSyncFailed(Long id, String errorType, String errorMsg, Date attemptAt,
            Date nextRetryAt, Integer retryCount);

    void markSyncPending(Long id, User user);

    PageInfo<SupersetDatasetResp> querySupersetDataset(SupersetDatasetQueryReq queryReq, User user);

    void deleteSupersetDataset(Long id, User user);

    void deleteSupersetDatasetBatch(List<Long> ids, User user);
}
