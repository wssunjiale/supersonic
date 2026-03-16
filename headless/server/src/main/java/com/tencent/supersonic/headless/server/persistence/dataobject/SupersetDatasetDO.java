package com.tencent.supersonic.headless.server.persistence.dataobject;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

@Data
@TableName("s2_superset_dataset")
public class SupersetDatasetDO {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String sqlHash;

    private String sqlText;

    private String normalizedSql;

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

    private String columns;

    private String metrics;

    /**
     * Dataset registry source type (e.g. chat SQL, semantic dataset mapping).
     */
    private String sourceType;

    /**
     * Sync state for superset dataset sync job.
     */
    private String syncState;

    /**
     * Last sync attempt time.
     */
    private Date syncAttemptAt;

    /**
     * Next retry time for retryable failures.
     */
    private Date nextRetryAt;

    /**
     * Retry count for consecutive failures.
     */
    private Integer retryCount;

    /**
     * Last sync error type (retryable/fatal).
     */
    private String syncErrorType;

    /**
     * Last sync error message.
     */
    private String syncErrorMsg;

    private Date createdAt;

    private String createdBy;

    private Date updatedAt;

    private String updatedBy;

    private Date syncedAt;
}
