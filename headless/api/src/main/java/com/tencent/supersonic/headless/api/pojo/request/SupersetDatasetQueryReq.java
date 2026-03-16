package com.tencent.supersonic.headless.api.pojo.request;

import com.tencent.supersonic.common.pojo.PageBaseReq;
import lombok.Data;

@Data
public class SupersetDatasetQueryReq extends PageBaseReq {

    private String datasetName;

    private String datasetType;

    private Long databaseId;

    private Long dataSetId;

    private String sqlHash;

    private Long supersetDatasetId;

    private String createdBy;

    private Boolean synced;

    /**
     * Registry source type filter (e.g. SEMANTIC_DATASET / CHAT_SQL).
     */
    private String sourceType;

    /**
     * Whether only return datasets that need sync (PENDING/FAILED). If null, backend may apply a
     * default strategy depending on UI.
     */
    private Boolean needSync;

    /**
     * Sync state filter (PENDING/SUCCESS/FAILED).
     */
    private String syncState;
}
