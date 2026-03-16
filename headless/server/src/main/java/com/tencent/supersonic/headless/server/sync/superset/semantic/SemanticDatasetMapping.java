package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class SemanticDatasetMapping {

    private Long dataSetId;
    private String dataSetName;
    private String dataSetBizName;
    private Long databaseId;
    private String schemaName;

    /** Stable table name in Superset (used as dataset key). */
    private String supersetTableName;

    /** Physical SQL that Superset virtual dataset should use. */
    private String supersetSql;

    private String sqlHash;

    private String datasetDesc;
    private String tagsJson;

    private String mainDttmCol;
    private List<SupersetDatasetColumn> columns;
    private List<SupersetDatasetMetric> metrics;
}
