package com.tencent.supersonic.chat.server.plugin.build.superset;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Locale;

@Component
@Slf4j
public class SupersetDashboardRegistrySchemaInitializer implements ApplicationRunner {

    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    public SupersetDashboardRegistrySchemaInitializer(JdbcTemplate jdbcTemplate,
            DataSource dataSource) {
        this.jdbcTemplate = jdbcTemplate;
        this.dataSource = dataSource;
    }

    @Override
    public void run(ApplicationArguments args) {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            String productName = metaData == null ? "" : metaData.getDatabaseProductName();
            String normalized = productName == null ? "" : productName.toLowerCase(Locale.ROOT);
            String ddl = normalized.contains("postgres") ? buildPostgresDdl() : buildMysqlDdl();
            jdbcTemplate.execute(ddl);
            log.debug("superset dashboard registry schema ready");
        } catch (Exception ex) {
            log.warn("superset dashboard registry schema init failed", ex);
        }
    }

    private String buildPostgresDdl() {
        return """
                CREATE TABLE IF NOT EXISTS s2_superset_dashboard (
                    id BIGSERIAL PRIMARY KEY,
                    plugin_id BIGINT NOT NULL,
                    dashboard_id BIGINT NOT NULL,
                    title VARCHAR(255) DEFAULT NULL,
                    embedded_id VARCHAR(255) DEFAULT NULL,
                    owner_id BIGINT DEFAULT NULL,
                    owner_name VARCHAR(100) DEFAULT NULL,
                    deleted INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NULL,
                    created_by VARCHAR(100) DEFAULT NULL,
                    updated_at TIMESTAMP DEFAULT NULL,
                    updated_by VARCHAR(100) DEFAULT NULL
                )
                """;
    }

    private String buildMysqlDdl() {
        return """
                CREATE TABLE IF NOT EXISTS s2_superset_dashboard (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    plugin_id BIGINT NOT NULL,
                    dashboard_id BIGINT NOT NULL,
                    title VARCHAR(255) DEFAULT NULL,
                    embedded_id VARCHAR(255) DEFAULT NULL,
                    owner_id BIGINT DEFAULT NULL,
                    owner_name VARCHAR(100) DEFAULT NULL,
                    deleted INT DEFAULT 0,
                    created_at DATETIME DEFAULT NULL,
                    created_by VARCHAR(100) DEFAULT NULL,
                    updated_at DATETIME DEFAULT NULL,
                    updated_by VARCHAR(100) DEFAULT NULL,
                    PRIMARY KEY (id)
                )
                """;
    }
}
