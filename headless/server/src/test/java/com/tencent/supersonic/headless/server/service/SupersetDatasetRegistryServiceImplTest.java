package com.tencent.supersonic.headless.server.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.exception.InvalidPermissionException;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.request.SupersetDatasetQueryReq;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.service.impl.SupersetDatasetRegistryServiceImpl;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSourceType;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetSyncState;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class SupersetDatasetRegistryServiceImplTest {

    @Test
    public void querySupersetDatasetShouldRejectNonAdmin() {
        SupersetDatasetRegistryServiceImpl service = buildService();
        User user = User.get(1L, "user");
        Assertions.assertThrows(InvalidPermissionException.class,
                () -> service.querySupersetDataset(new SupersetDatasetQueryReq(), user));
    }

    @Test
    public void deleteSupersetDatasetShouldRejectNonAdmin() {
        SupersetDatasetRegistryServiceImpl service = buildService();
        User user = User.get(1L, "user");
        Assertions.assertThrows(InvalidPermissionException.class,
                () -> service.deleteSupersetDataset(1L, user));
    }

    @Test
    public void deleteSupersetDatasetBatchShouldRejectNonAdmin() {
        SupersetDatasetRegistryServiceImpl service = buildService();
        User user = User.get(1L, "user");
        Assertions.assertThrows(InvalidPermissionException.class,
                () -> service.deleteSupersetDatasetBatch(Arrays.asList(1L, 2L), user));
    }

    @Test
    public void buildQueryWrapperShouldIncludeFilters() {
        TestableSupersetDatasetRegistryServiceImpl service = buildTestableService();
        SupersetDatasetQueryReq req = new SupersetDatasetQueryReq();
        req.setDatasetName("demo");
        req.setDatasetType("PHYSICAL");
        req.setDatabaseId(10L);
        req.setDataSetId(20L);
        req.setSqlHash("hash-1");
        req.setSupersetDatasetId(30L);
        req.setCreatedBy("alice");
        req.setSynced(true);

        LambdaQueryWrapper<SupersetDatasetDO> wrapper = service.buildWrapper(req);
        Map<String, Object> params = wrapper.getParamNameValuePairs();

        Assertions.assertNotNull(wrapper);
        Assertions.assertNotNull(params);
    }

    @Test
    public void listAvailablePersistentDatasetsShouldFilterAndSortByPriority() {
        SeededSupersetDatasetRegistryServiceImpl service = buildSeededService(Arrays.asList(
                buildRegistryRecord(10L, 101L, SupersetDatasetSourceType.SEMANTIC_DATASET.name(),
                        SupersetDatasetType.VIRTUAL.name(),
                        SupersetDatasetSyncState.SUCCESS.name(), new Date(1000L)),
                buildRegistryRecord(10L, 102L, SupersetDatasetSourceType.CHAT_SQL.name(),
                        SupersetDatasetType.PHYSICAL.name(),
                        SupersetDatasetSyncState.SUCCESS.name(), new Date(2000L)),
                buildRegistryRecord(10L, 103L, SupersetDatasetSourceType.CHAT_SQL.name(),
                        SupersetDatasetType.VIRTUAL.name(),
                        SupersetDatasetSyncState.SUCCESS.name(), new Date(3000L)),
                buildRegistryRecord(10L, 104L, SupersetDatasetSourceType.SEMANTIC_DATASET.name(),
                        SupersetDatasetType.VIRTUAL.name(),
                        SupersetDatasetSyncState.FAILED.name(), new Date(4000L)),
                buildRegistryRecord(11L, 105L, SupersetDatasetSourceType.SEMANTIC_DATASET.name(),
                        SupersetDatasetType.VIRTUAL.name(),
                        SupersetDatasetSyncState.SUCCESS.name(), new Date(5000L)),
                buildRegistryRecord(10L, null, SupersetDatasetSourceType.SEMANTIC_DATASET.name(),
                        SupersetDatasetType.VIRTUAL.name(),
                        SupersetDatasetSyncState.SUCCESS.name(), new Date(6000L))));

        List<SupersetDatasetDO> records = service.listAvailablePersistentDatasets(10L);

        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(101L, records.get(0).getSupersetDatasetId());
        Assertions.assertEquals(SupersetDatasetSourceType.SEMANTIC_DATASET.name(),
                records.get(0).getSourceType());
        Assertions.assertEquals(102L, records.get(1).getSupersetDatasetId());
        Assertions.assertEquals(SupersetDatasetType.PHYSICAL.name(),
                records.get(1).getDatasetType());
    }

    @Test
    public void registerDatasetShouldUseFinalQueryColumnsForSupersetSchema() {
        CapturingSupersetDatasetRegistryServiceImpl service = buildCapturingService();
        service.registerDataset(buildParseInfo(), buildTopMetricSql(),
                Arrays.asList(buildQueryColumn("department", "STRING", "部门", "CATEGORY"),
                        buildQueryColumn("_总访问次数", "BIGINT", "_总访问次数", "NUMBER"),
                        buildQueryColumn("_排名", "BIGINT", "_排名", "NUMBER")),
                User.getDefaultUser());

        SupersetDatasetDO saved = service.getCapturedRecord();
        Assertions.assertNotNull(saved);
        List<SupersetDatasetColumn> columns =
                JsonUtil.toList(saved.getColumns(), SupersetDatasetColumn.class);
        List<SupersetDatasetMetric> metrics =
                JsonUtil.toList(saved.getMetrics(), SupersetDatasetMetric.class);

        Assertions.assertEquals(columns.stream().map(SupersetDatasetColumn::getColumnName).toList(),
                Arrays.asList("department", "_总访问次数", "_排名"));
        Assertions.assertEquals(metrics.stream().map(SupersetDatasetMetric::getMetricName).toList(),
                Collections.singletonList("_总访问次数"));
        Assertions.assertEquals(metrics.get(0).getExpression(), "SUM(_总访问次数)");
    }

    @Test
    public void registerDatasetShouldPreferFinalSqlAliasesOverSemanticQueryColumns() {
        CapturingSupersetDatasetRegistryServiceImpl service = buildCapturingService();
        service.registerDataset(buildParseInfo(), buildTopMetricSql(),
                Arrays.asList(buildQueryColumn("department", "STRING", "部门", "CATEGORY"),
                        buildQueryColumn("pv", "BIGINT", "访问次数", "NUMBER")),
                User.getDefaultUser());

        SupersetDatasetDO saved = service.getCapturedRecord();
        Assertions.assertNotNull(saved);
        List<SupersetDatasetColumn> columns =
                JsonUtil.toList(saved.getColumns(), SupersetDatasetColumn.class);
        List<SupersetDatasetMetric> metrics =
                JsonUtil.toList(saved.getMetrics(), SupersetDatasetMetric.class);

        Assertions.assertEquals(columns.stream().map(SupersetDatasetColumn::getColumnName).toList(),
                Arrays.asList("department", "_总访问次数", "_排名"));
        Assertions.assertEquals(metrics.stream().map(SupersetDatasetMetric::getMetricName).toList(),
                Collections.singletonList("_总访问次数"));
        Assertions.assertEquals(metrics.get(0).getExpression(), "SUM(_总访问次数)");
    }

    @Test
    public void registerDatasetShouldFallbackToFinalSqlAliasesWhenQueryColumnsMissing() {
        CapturingSupersetDatasetRegistryServiceImpl service = buildCapturingService();
        service.registerDataset(buildParseInfo(), buildTopMetricSql(), Collections.emptyList(),
                User.getDefaultUser());

        SupersetDatasetDO saved = service.getCapturedRecord();
        Assertions.assertNotNull(saved);
        List<SupersetDatasetColumn> columns =
                JsonUtil.toList(saved.getColumns(), SupersetDatasetColumn.class);
        List<SupersetDatasetMetric> metrics =
                JsonUtil.toList(saved.getMetrics(), SupersetDatasetMetric.class);

        Assertions.assertEquals(columns.stream().map(SupersetDatasetColumn::getColumnName).toList(),
                Arrays.asList("department", "_总访问次数", "_排名"));
        Assertions.assertEquals(metrics.stream().map(SupersetDatasetMetric::getMetricName).toList(),
                Collections.singletonList("_总访问次数"));
    }

    @Test
    public void registerDatasetShouldUseFinalColumnsForCteSql() {
        CapturingSupersetDatasetRegistryServiceImpl service = buildCapturingService();
        service.registerDataset(buildParseInfo(), buildLiveCteSql(),
                Arrays.asList(buildQueryColumn("department", "varchar", "部门", "CATEGORY"),
                        buildQueryColumn("_总访问次数", "int8", "_总访问次数", "NUMBER"),
                        buildQueryColumn("_排名", "int8", "_排名", "NUMBER")),
                User.getDefaultUser());

        SupersetDatasetDO saved = service.getCapturedRecord();
        Assertions.assertNotNull(saved);
        List<SupersetDatasetColumn> columns =
                JsonUtil.toList(saved.getColumns(), SupersetDatasetColumn.class);
        List<SupersetDatasetMetric> metrics =
                JsonUtil.toList(saved.getMetrics(), SupersetDatasetMetric.class);

        Assertions.assertEquals(columns.stream().map(SupersetDatasetColumn::getColumnName).toList(),
                Arrays.asList("department", "_总访问次数", "_排名"));
        Assertions.assertEquals(metrics.stream().map(SupersetDatasetMetric::getMetricName).toList(),
                Collections.singletonList("_总访问次数"));
        Assertions.assertEquals(metrics.get(0).getExpression(), "SUM(_总访问次数)");
    }

    private SupersetDatasetRegistryServiceImpl buildService() {
        DataSetService dataSetService = Mockito.mock(DataSetService.class);
        ModelService modelService = Mockito.mock(ModelService.class);
        DatabaseService databaseService = Mockito.mock(DatabaseService.class);
        return new SupersetDatasetRegistryServiceImpl(dataSetService, modelService,
                databaseService);
    }

    private TestableSupersetDatasetRegistryServiceImpl buildTestableService() {
        DataSetService dataSetService = Mockito.mock(DataSetService.class);
        ModelService modelService = Mockito.mock(ModelService.class);
        DatabaseService databaseService = Mockito.mock(DatabaseService.class);
        return new TestableSupersetDatasetRegistryServiceImpl(dataSetService, modelService,
                databaseService);
    }

    private CapturingSupersetDatasetRegistryServiceImpl buildCapturingService() {
        DataSetService dataSetService = Mockito.mock(DataSetService.class);
        ModelService modelService = Mockito.mock(ModelService.class);
        DatabaseService databaseService = Mockito.mock(DatabaseService.class);
        return new CapturingSupersetDatasetRegistryServiceImpl(dataSetService, modelService,
                databaseService);
    }

    private SeededSupersetDatasetRegistryServiceImpl buildSeededService(
            List<SupersetDatasetDO> records) {
        DataSetService dataSetService = Mockito.mock(DataSetService.class);
        ModelService modelService = Mockito.mock(ModelService.class);
        DatabaseService databaseService = Mockito.mock(DatabaseService.class);
        return new SeededSupersetDatasetRegistryServiceImpl(dataSetService, modelService,
                databaseService, records);
    }

    private SemanticParseInfo buildParseInfo() {
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.setDimensions(new LinkedHashSet<>(Collections.singletonList(
                SchemaElement.builder().name("部门").bizName("department")
                        .type(SchemaElementType.DIMENSION).description("部门").build())));
        parseInfo.setMetrics(new LinkedHashSet<>(Collections.singletonList(
                SchemaElement.builder().name("总访问次数").bizName("pv")
                        .type(SchemaElementType.METRIC).defaultAgg("SUM")
                        .description("总访问次数").build())));
        return parseInfo;
    }

    private QueryColumn buildQueryColumn(String bizName, String type, String name, String showType) {
        QueryColumn queryColumn = new QueryColumn();
        queryColumn.setBizName(bizName);
        queryColumn.setType(type);
        queryColumn.setName(name);
        queryColumn.setShowType(showType);
        return queryColumn;
    }

    private String buildTopMetricSql() {
        return "SELECT department, pv AS _总访问次数, "
                + "RANK() OVER (ORDER BY pv DESC) AS _排名 "
                + "FROM (SELECT department, SUM(pv) AS pv FROM s2 "
                + "WHERE ds >= '2026-02-01' GROUP BY department) ranked";
    }

    private String buildLiveCteSql() {
        return "WITH t_1 AS (SELECT t2.imp_date, t3.department "
                + "FROM (SELECT * FROM s2_pv_uv_statis) AS t2 "
                + "LEFT JOIN (SELECT * FROM s2_user_department) AS t3 "
                + "ON t2.user_name = t3.user_name), "
                + "department_visits AS (SELECT department, count(1) AS _总访问次数 "
                + "FROM t_1 WHERE imp_date >= '2026-02-14' AND imp_date <= '2026-03-15' "
                + "GROUP BY department), "
                + "ranked_departments AS (SELECT department, _总访问次数, "
                + "ROW_NUMBER() OVER (ORDER BY _总访问次数 DESC) AS _排名 "
                + "FROM department_visits) "
                + "SELECT department, _总访问次数, _排名 "
                + "FROM ranked_departments WHERE _排名 <= 3 ORDER BY _排名 LIMIT 1000";
    }

    private SupersetDatasetDO buildRegistryRecord(Long dataSetId, Long supersetDatasetId,
            String sourceType, String datasetType, String syncState, Date updatedAt) {
        SupersetDatasetDO record = new SupersetDatasetDO();
        record.setDataSetId(dataSetId);
        record.setSupersetDatasetId(supersetDatasetId);
        record.setSourceType(sourceType);
        record.setDatasetType(datasetType);
        record.setSyncState(syncState);
        record.setUpdatedAt(updatedAt);
        record.setCreatedAt(updatedAt);
        return record;
    }

    private static class TestableSupersetDatasetRegistryServiceImpl
            extends SupersetDatasetRegistryServiceImpl {
        private TestableSupersetDatasetRegistryServiceImpl(DataSetService dataSetService,
                ModelService modelService, DatabaseService databaseService) {
            super(dataSetService, modelService, databaseService);
        }

        private LambdaQueryWrapper<SupersetDatasetDO> buildWrapper(SupersetDatasetQueryReq req) {
            return buildQueryWrapper(req);
        }
    }

    private static class CapturingSupersetDatasetRegistryServiceImpl
            extends SupersetDatasetRegistryServiceImpl {
        private SupersetDatasetDO capturedRecord;

        private CapturingSupersetDatasetRegistryServiceImpl(DataSetService dataSetService,
                ModelService modelService, DatabaseService databaseService) {
            super(dataSetService, modelService, databaseService);
        }

        @Override
        public SupersetDatasetDO getBySqlHash(String sqlHash) {
            return null;
        }

        @Override
        public SupersetDatasetDO getByPhysicalTable(Long databaseId, String schemaName,
                String tableName) {
            return null;
        }

        @Override
        public boolean save(SupersetDatasetDO entity) {
            this.capturedRecord = entity;
            return true;
        }

        private SupersetDatasetDO getCapturedRecord() {
            return capturedRecord;
        }
    }

    private static class SeededSupersetDatasetRegistryServiceImpl
            extends SupersetDatasetRegistryServiceImpl {
        private final List<SupersetDatasetDO> seededRecords;

        private SeededSupersetDatasetRegistryServiceImpl(DataSetService dataSetService,
                ModelService modelService, DatabaseService databaseService,
                List<SupersetDatasetDO> seededRecords) {
            super(dataSetService, modelService, databaseService);
            this.seededRecords = seededRecords == null ? Collections.emptyList()
                    : new ArrayList<>(seededRecords);
        }

        @Override
        public List<SupersetDatasetDO> list(Wrapper<SupersetDatasetDO> queryWrapper) {
            return seededRecords;
        }
    }
}
