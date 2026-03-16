package com.tencent.supersonic.headless.api.pojo.response;

import lombok.Data;

import java.util.Date;

@Data
public class SupersetDatasetResp {

    private Long id;

    private String sqlHash;

    private String datasetName;

    private String datasetDesc;

    private String tags;

    private String datasetType;

    private Long dataSetId;

    private Long databaseId;

    private String schemaName;

    private String tableName;

    private String mainDttmCol;

    private Long supersetDatasetId;

    private String sourceType;

    private String syncState;

    private Date syncAttemptAt;

    private Date nextRetryAt;

    private Integer retryCount;

    private String syncErrorType;

    private String syncErrorMsg;

    private Date createdAt;

    private String createdBy;

    private Date updatedAt;

    private String updatedBy;

    private Date syncedAt;
}
