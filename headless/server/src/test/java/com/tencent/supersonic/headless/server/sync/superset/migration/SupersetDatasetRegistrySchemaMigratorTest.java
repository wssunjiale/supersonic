package com.tencent.supersonic.headless.server.sync.superset.migration;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class SupersetDatasetRegistrySchemaMigratorTest {

    @Test
    public void migrateAddsMissingColumnsIdempotently() throws Exception {
        DataSource dataSource = buildH2MySqlMode();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS s2_superset_dataset (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    sql_hash varchar(64) NOT NULL,
                    created_at TIMESTAMP DEFAULT NULL,
                    updated_at TIMESTAMP DEFAULT NULL,
                    PRIMARY KEY (id)
                )
                """.trim());

        SupersetDatasetRegistrySchemaMigrator migrator =
                new SupersetDatasetRegistrySchemaMigrator(dataSource);
        SupersetDatasetRegistrySchemaMigrator.MigrationReport first = migrator.migrate(false);
        Assert.assertNotNull(first);
        Assert.assertTrue(first.getErrors() == null || first.getErrors().isEmpty(),
                "first migrate should not have errors");

        // Required columns should exist after migration.
        Set<String> cols = readColumns(dataSource, "s2_superset_dataset");
        Assert.assertTrue(cols.contains("source_type"));
        Assert.assertTrue(cols.contains("sync_state"));
        Assert.assertTrue(cols.contains("sync_attempt_at"));
        Assert.assertTrue(cols.contains("next_retry_at"));
        Assert.assertTrue(cols.contains("retry_count"));
        Assert.assertTrue(cols.contains("sync_error_type"));
        Assert.assertTrue(cols.contains("sync_error_msg"));

        // Second migrate should be idempotent (no errors, and usually no executed DDL).
        SupersetDatasetRegistrySchemaMigrator.MigrationReport second = migrator.migrate(false);
        Assert.assertNotNull(second);
        Assert.assertTrue(second.getErrors() == null || second.getErrors().isEmpty(),
                "second migrate should not have errors");
    }

    private DataSource buildH2MySqlMode() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl(
                "jdbc:h2:mem:s2_superset_dataset_migrator;MODE=MySQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }

    private Set<String> readColumns(DataSource dataSource, String table) throws Exception {
        Set<String> result = new HashSet<>();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData meta = connection.getMetaData();
            try (ResultSet rs = meta.getColumns(connection.getCatalog(), null, table, null)) {
                while (rs != null && rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    if (name == null) {
                        continue;
                    }
                    result.add(name.trim().toLowerCase(Locale.ROOT));
                }
            }
        }
        return result;
    }
}
