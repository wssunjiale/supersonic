package com.tencent.supersonic.headless.server.sync.superset;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SupersetDatasetInfo {

    private Long id;
    private Long databaseId;
    private String schema;
    private String tableName;
    /**
     * Superset description.
     */
    private String description;
    private String sql;
    private String mainDttmCol;
    private List<SupersetDatasetColumn> columns = new ArrayList<>();
    private List<SupersetDatasetMetric> metrics = new ArrayList<>();
}
