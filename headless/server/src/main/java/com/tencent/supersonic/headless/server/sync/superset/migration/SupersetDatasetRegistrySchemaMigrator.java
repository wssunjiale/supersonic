package com.tencent.supersonic.headless.server.sync.superset.migration;

import javax.sql.DataSource;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Ensure Supersonic metadata DB schema for {@code s2_superset_dataset} is compatible with the
 * current Superset dataset registry sync logic.
 *
 * <p>
 * This migrator is idempotent and supports PostgreSQL/MySQL-like dialects.
 * </p>
 */
@Component
@Slf4j
public class SupersetDatasetRegistrySchemaMigrator {

    public static final String TABLE_NAME = "s2_superset_dataset";

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    public SupersetDatasetRegistrySchemaMigrator(DataSource dataSource) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public MigrationReport migrate(boolean dryRun) {
        MigrationReport report = new MigrationReport();
        report.setDryRun(dryRun);
        report.setTableName(TABLE_NAME);

        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData meta = connection.getMetaData();
            DbDialect dialect = DbDialect.detect(meta);
            report.setDialect(dialect.name());
            report.setDatabaseProductName(
                    safe(meta == null ? null : meta.getDatabaseProductName()));

            TableRef tableRef = resolveTableRef(connection, meta);
            if (tableRef == null) {
                tableRef = defaultTableRef(connection, dialect);
            }
            report.setTableCatalog(tableRef.getCatalog());
            report.setTableSchema(tableRef.getSchema());

            boolean existed = tableExists(meta, tableRef);
            if (!existed) {
                String create = buildCreateTableSql(dialect, tableRef);
                report.addPlanned("create_table", create);
                if (!dryRun) {
                    jdbcTemplate.execute(create);
                    report.addExecuted("create_table", create);
                }
            }

            if (!existed && dryRun) {
                // In dry-run mode, creating the table would include all required columns, so we
                // avoid emitting redundant ADD COLUMN statements.
                for (IndexSpec index : requiredIndexes()) {
                    report.addPlanned("create_index:" + index.getName(),
                            buildCreateIndexSql(dialect, tableRef, index));
                }
                return report;
            }

            Set<String> existingColumns = readColumnNames(meta, tableRef);
            List<ColumnSpec> required = requiredColumns();
            for (ColumnSpec spec : required) {
                if (spec == null || StringUtils.isBlank(spec.getName())) {
                    continue;
                }
                if (existingColumns.contains(normalize(spec.getName()))) {
                    continue;
                }
                String ddl = buildAddColumnSql(dialect, tableRef, spec);
                report.addPlanned("add_column:" + spec.getName(), ddl);
                if (!dryRun) {
                    jdbcTemplate.execute(ddl);
                    report.addExecuted("add_column:" + spec.getName(), ddl);
                }
            }

            // Indexes: optimize PENDING/FAILED(RETRYABLE) picking + UI filtering by source_type.
            Set<String> existingIndexes = readIndexNames(meta, tableRef);
            for (IndexSpec index : requiredIndexes()) {
                if (index == null || StringUtils.isBlank(index.getName())) {
                    continue;
                }
                String key = normalize(index.getName());
                if (existingIndexes.contains(key)) {
                    continue;
                }
                String ddl = buildCreateIndexSql(dialect, tableRef, index);
                report.addPlanned("create_index:" + index.getName(), ddl);
                if (!dryRun) {
                    jdbcTemplate.execute(ddl);
                    report.addExecuted("create_index:" + index.getName(), ddl);
                }
            }
        } catch (Exception ex) {
            log.warn("migrate superset dataset registry schema failed", ex);
            report.addError("exception",
                    ex.getClass().getSimpleName() + ": " + safe(ex.getMessage()));
        }
        return report;
    }

    private boolean tableExists(DatabaseMetaData meta, TableRef tableRef) {
        if (meta == null || tableRef == null) {
            return false;
        }
        try (ResultSet rs = meta.getTables(tableRef.getCatalog(), tableRef.getSchema(),
                tableRef.getTable(), new String[] {"TABLE"})) {
            return rs != null && rs.next();
        } catch (Exception ex) {
            return false;
        }
    }

    private TableRef resolveTableRef(Connection connection, DatabaseMetaData meta) {
        if (meta == null) {
            return null;
        }
        String catalog = safeCatalog(connection);
        List<TableRef> candidates = new ArrayList<>();
        candidates.addAll(findTables(meta, catalog, null, TABLE_NAME));
        candidates.addAll(findTables(meta, null, null, TABLE_NAME));
        if (candidates.isEmpty()) {
            candidates.addAll(findTables(meta, catalog, null, TABLE_NAME.toUpperCase(Locale.ROOT)));
            candidates.addAll(findTables(meta, null, null, TABLE_NAME.toUpperCase(Locale.ROOT)));
        }
        if (candidates.isEmpty()) {
            return null;
        }
        // Prefer "public" when using PostgreSQL.
        for (TableRef ref : candidates) {
            if ("public".equalsIgnoreCase(ref.getSchema())) {
                return ref;
            }
        }
        return candidates.get(0);
    }

    private TableRef defaultTableRef(Connection connection, DbDialect dialect) {
        TableRef ref = new TableRef();
        ref.setTable(TABLE_NAME);
        if (dialect == DbDialect.POSTGRES) {
            String schema = null;
            try {
                schema = safe(connection == null ? null : connection.getSchema());
            } catch (Exception ignored) {
                // ignore
            }
            ref.setSchema(StringUtils.defaultIfBlank(schema, "public"));
        } else {
            ref.setCatalog(safeCatalog(connection));
        }
        return ref;
    }

    private List<TableRef> findTables(DatabaseMetaData meta, String catalog, String schema,
            String tableName) {
        List<TableRef> list = new ArrayList<>();
        if (meta == null || StringUtils.isBlank(tableName)) {
            return list;
        }
        try (ResultSet rs = meta.getTables(catalog, schema, tableName, new String[] {"TABLE"})) {
            while (rs != null && rs.next()) {
                String tableCat = rs.getString("TABLE_CAT");
                String tableSchem = rs.getString("TABLE_SCHEM");
                String table = rs.getString("TABLE_NAME");
                if (StringUtils.isBlank(table)) {
                    continue;
                }
                TableRef ref = new TableRef();
                ref.setCatalog(safe(tableCat));
                ref.setSchema(safe(tableSchem));
                ref.setTable(safe(table));
                list.add(ref);
            }
        } catch (Exception ignored) {
            // ignore
        }
        return list;
    }

    private Set<String> readColumnNames(DatabaseMetaData meta, TableRef tableRef) {
        Set<String> set = new HashSet<>();
        if (meta == null || tableRef == null) {
            return set;
        }
        try (ResultSet rs = meta.getColumns(tableRef.getCatalog(), tableRef.getSchema(),
                tableRef.getTable(), null)) {
            while (rs != null && rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                set.add(normalize(name));
            }
        } catch (Exception ignored) {
            // ignore
        }
        return set;
    }

    private Set<String> readIndexNames(DatabaseMetaData meta, TableRef tableRef) {
        Set<String> set = new HashSet<>();
        if (meta == null || tableRef == null) {
            return set;
        }
        try (ResultSet rs = meta.getIndexInfo(tableRef.getCatalog(), tableRef.getSchema(),
                tableRef.getTable(), false, false)) {
            while (rs != null && rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if (StringUtils.isBlank(indexName)) {
                    continue;
                }
                // Skip implicit primary index name and statistics pseudo index.
                if ("PRIMARY".equalsIgnoreCase(indexName)
                        || "statistics".equalsIgnoreCase(indexName)) {
                    continue;
                }
                set.add(normalize(indexName));
            }
        } catch (Exception ignored) {
            // ignore
        }
        return set;
    }

    private String buildAddColumnSql(DbDialect dialect, TableRef tableRef, ColumnSpec spec) {
        String table = qualify(dialect, tableRef);
        String type = dialect == DbDialect.POSTGRES ? spec.getPostgresType() : spec.getMysqlType();
        StringBuilder sb = new StringBuilder();
        if (dialect == DbDialect.POSTGRES) {
            sb.append("ALTER TABLE ").append(table).append(" ADD COLUMN IF NOT EXISTS ");
        } else {
            sb.append("ALTER TABLE ").append(table).append(" ADD COLUMN ");
        }
        sb.append(quoteIdent(dialect, spec.getName())).append(" ").append(type);
        if (StringUtils.isNotBlank(spec.getDefaultSql())) {
            sb.append(" DEFAULT ").append(spec.getDefaultSql());
        }
        return sb.toString();
    }

    private String buildCreateIndexSql(DbDialect dialect, TableRef tableRef, IndexSpec index) {
        String table = qualify(dialect, tableRef);
        String indexName = quoteIdent(dialect, index.getName());
        String cols = index.getColumns() == null ? ""
                : String.join(", ", index.getColumns().stream().filter(StringUtils::isNotBlank)
                        .map(col -> quoteIdent(dialect, col)).toList());
        if (StringUtils.isBlank(cols)) {
            cols = quoteIdent(dialect, "id");
        }
        if (dialect == DbDialect.POSTGRES) {
            return "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + table + " (" + cols + ")";
        }
        return "CREATE INDEX " + indexName + " ON " + table + " (" + cols + ")";
    }

    private String buildCreateTableSql(DbDialect dialect, TableRef tableRef) {
        String table = qualify(dialect, tableRef);
        if (dialect == DbDialect.POSTGRES) {
            return """
                    CREATE TABLE IF NOT EXISTS %s (
                        id BIGSERIAL PRIMARY KEY,
                        sql_hash varchar(64) NOT NULL,
                        sql_text text DEFAULT NULL,
                        normalized_sql text DEFAULT NULL,
                        dataset_name varchar(255) DEFAULT NULL,
                        dataset_desc text DEFAULT NULL,
                        tags text DEFAULT NULL,
                        dataset_type varchar(20) DEFAULT NULL,
                        data_set_id bigint DEFAULT NULL,
                        database_id bigint DEFAULT NULL,
                        schema_name varchar(255) DEFAULT NULL,
                        table_name varchar(255) DEFAULT NULL,
                        main_dttm_col varchar(255) DEFAULT NULL,
                        superset_dataset_id bigint DEFAULT NULL,
                        columns text DEFAULT NULL,
                        metrics text DEFAULT NULL,
                        source_type varchar(50) DEFAULT NULL,
                        sync_state varchar(20) DEFAULT NULL,
                        sync_attempt_at timestamp DEFAULT NULL,
                        next_retry_at timestamp DEFAULT NULL,
                        retry_count integer DEFAULT 0,
                        sync_error_type varchar(20) DEFAULT NULL,
                        sync_error_msg text DEFAULT NULL,
                        created_at timestamp DEFAULT NULL,
                        created_by varchar(100) DEFAULT NULL,
                        updated_at timestamp DEFAULT NULL,
                        updated_by varchar(100) DEFAULT NULL,
                        synced_at timestamp DEFAULT NULL
                    )
                    """.formatted(table).trim();
        }
        return """
                CREATE TABLE IF NOT EXISTS %s (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    sql_hash varchar(64) NOT NULL,
                    sql_text longtext DEFAULT NULL,
                    normalized_sql longtext DEFAULT NULL,
                    dataset_name varchar(255) DEFAULT NULL,
                    dataset_desc text DEFAULT NULL,
                    tags text DEFAULT NULL,
                    dataset_type varchar(20) DEFAULT NULL,
                    data_set_id bigint DEFAULT NULL,
                    database_id bigint DEFAULT NULL,
                    schema_name varchar(255) DEFAULT NULL,
                    table_name varchar(255) DEFAULT NULL,
                    main_dttm_col varchar(255) DEFAULT NULL,
                    superset_dataset_id bigint DEFAULT NULL,
                    columns longtext DEFAULT NULL,
                    metrics longtext DEFAULT NULL,
                    source_type varchar(50) DEFAULT NULL,
                    sync_state varchar(20) DEFAULT NULL,
                    sync_attempt_at datetime DEFAULT NULL,
                    next_retry_at datetime DEFAULT NULL,
                    retry_count int DEFAULT 0,
                    sync_error_type varchar(20) DEFAULT NULL,
                    sync_error_msg text DEFAULT NULL,
                    created_at datetime DEFAULT NULL,
                    created_by varchar(100) DEFAULT NULL,
                    updated_at datetime DEFAULT NULL,
                    updated_by varchar(100) DEFAULT NULL,
                    synced_at datetime DEFAULT NULL,
                    PRIMARY KEY (id)
                )
                """.formatted(table).trim();
    }

    private List<ColumnSpec> requiredColumns() {
        List<ColumnSpec> specs = new ArrayList<>();
        specs.add(new ColumnSpec("source_type", "varchar(50)", "varchar(50)", null));
        specs.add(new ColumnSpec("sync_state", "varchar(20)", "varchar(20)", null));
        specs.add(new ColumnSpec("sync_attempt_at", "datetime", "timestamp", null));
        specs.add(new ColumnSpec("next_retry_at", "datetime", "timestamp", null));
        specs.add(new ColumnSpec("retry_count", "int", "integer", "0"));
        specs.add(new ColumnSpec("sync_error_type", "varchar(20)", "varchar(20)", null));
        specs.add(new ColumnSpec("sync_error_msg", "text", "text", null));
        return specs;
    }

    private List<IndexSpec> requiredIndexes() {
        List<IndexSpec> list = new ArrayList<>();
        list.add(new IndexSpec("idx_s2_superset_dataset_sql_hash", List.of("sql_hash")));
        list.add(new IndexSpec("idx_s2_superset_dataset_sync_pick",
                List.of("sync_state", "sync_error_type", "next_retry_at")));
        list.add(new IndexSpec("idx_s2_superset_dataset_source_sync",
                List.of("source_type", "sync_state", "next_retry_at")));
        return list;
    }

    private String qualify(DbDialect dialect, TableRef tableRef) {
        if (tableRef == null || StringUtils.isBlank(tableRef.getTable())) {
            return quoteIdent(dialect, TABLE_NAME);
        }
        if (dialect == DbDialect.POSTGRES) {
            if (StringUtils.isNotBlank(tableRef.getSchema())) {
                return quoteIdent(dialect, tableRef.getSchema()) + "."
                        + quoteIdent(dialect, tableRef.getTable());
            }
            return quoteIdent(dialect, tableRef.getTable());
        }
        if (StringUtils.isNotBlank(tableRef.getCatalog())) {
            return quoteIdent(dialect, tableRef.getCatalog()) + "."
                    + quoteIdent(dialect, tableRef.getTable());
        }
        return quoteIdent(dialect, tableRef.getTable());
    }

    private String quoteIdent(DbDialect dialect, String ident) {
        String safe = safe(ident);
        if (StringUtils.isBlank(safe)) {
            return safe;
        }
        if (dialect == DbDialect.POSTGRES) {
            return "\"" + safe.replace("\"", "\"\"") + "\"";
        }
        return "`" + safe.replace("`", "``") + "`";
    }

    private String safe(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String safeCatalog(Connection connection) {
        if (connection == null) {
            return null;
        }
        try {
            return safe(connection.getCatalog());
        } catch (Exception ignored) {
            return null;
        }
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private enum DbDialect {
        POSTGRES, MYSQL, OTHER;

        static DbDialect detect(DatabaseMetaData meta) {
            try {
                String name = meta == null ? ""
                        : StringUtils.defaultString(meta.getDatabaseProductName());
                String lower = name.toLowerCase(Locale.ROOT);
                if (lower.contains("postgres")) {
                    return POSTGRES;
                }
                if (lower.contains("mysql") || lower.contains("mariadb")) {
                    return MYSQL;
                }
            } catch (Exception ignored) {
                // ignore
            }
            return OTHER;
        }
    }

    @Data
    private static class TableRef {
        private String catalog;
        private String schema;
        private String table;
    }

    @Data
    private static class ColumnSpec {
        private final String name;
        private final String mysqlType;
        private final String postgresType;
        private final String defaultSql;
    }

    @Data
    private static class IndexSpec {
        private final String name;
        private final List<String> columns;
    }

    @Data
    public static class MigrationReport {
        private boolean dryRun;
        private String dialect;
        private String databaseProductName;
        private String tableName;
        private String tableCatalog;
        private String tableSchema;

        private List<Item> planned = new ArrayList<>();
        private List<Item> executed = new ArrayList<>();
        private List<Item> errors = new ArrayList<>();

        public void addPlanned(String action, String sql) {
            planned.add(new Item(action, sql));
        }

        public void addExecuted(String action, String sql) {
            executed.add(new Item(action, sql));
        }

        public void addError(String code, String message) {
            errors.add(new Item(code, message));
        }

        @Data
        public static class Item {
            private final String action;
            private final String detail;

            public Item(String action, String detail) {
                this.action = action;
                this.detail = detail;
            }
        }
    }
}
