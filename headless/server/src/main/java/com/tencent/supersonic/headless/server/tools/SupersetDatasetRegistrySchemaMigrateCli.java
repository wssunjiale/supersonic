package com.tencent.supersonic.headless.server.tools;

import javax.sql.DataSource;

import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.server.sync.superset.migration.SupersetDatasetRegistrySchemaMigrator;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Migrate/validate Supersonic metadata DB schema for table {@code s2_superset_dataset}.
 *
 * <p>
 * This CLI does not require starting the Supersonic server.
 * </p>
 *
 * <p>
 * Args:
 * <ul>
 * <li>--dry-run: only print planned DDL</li>
 * <li>--jdbc-url=... (optional)</li>
 * <li>--username=... (optional)</li>
 * <li>--password=... (optional)</li>
 * </ul>
 *
 * Env fallback (compatible with standalone configs):
 * <ul>
 * <li>S2_DB_TYPE: postgresql|mysql (default postgresql)</li>
 * <li>S2_DB_HOST (default 10.0.12.252)</li>
 * <li>S2_DB_PORT (default 5439 or 3306)</li>
 * <li>S2_DB_DATABASE (default supersonic)</li>
 * <li>S2_DB_USER (default supersonic)</li>
 * <li>S2_DB_PASSWORD (default postgres)</li>
 * <li>S2_DB_JDBC_URL (optional)</li>
 * </ul>
 * </p>
 */
public class SupersetDatasetRegistrySchemaMigrateCli {

    public static void main(String[] args) {
        Map<String, String> parsed = parseArgs(args);
        boolean dryRun = parsed.containsKey("dry-run") || parsed.containsKey("print");

        String jdbcUrl =
                firstNonBlank(parsed.get("jdbc-url"), getenv("S2_DB_JDBC_URL", null), null);
        String username =
                firstNonBlank(parsed.get("username"), getenv("S2_DB_USER", "supersonic"), null);
        String password =
                firstNonBlank(parsed.get("password"), getenv("S2_DB_PASSWORD", "postgres"), null);

        if (StringUtils.isBlank(jdbcUrl)) {
            String dbType = getenv("S2_DB_TYPE", "postgresql").toLowerCase(Locale.ROOT);
            String host = getenv("S2_DB_HOST", "10.0.12.252");
            String database = getenv("S2_DB_DATABASE", "supersonic");
            if (dbType.contains("mysql")) {
                String port = getenv("S2_DB_PORT", "3306");
                jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database
                        + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false&allowMultiQueries=true&allowPublicKeyRetrieval=true";
            } else {
                String port = getenv("S2_DB_PORT", "5439");
                jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + database
                        + "?stringtype=unspecified";
            }
        }

        DataSource dataSource = buildDataSource(jdbcUrl, username, password);
        SupersetDatasetRegistrySchemaMigrator migrator =
                new SupersetDatasetRegistrySchemaMigrator(dataSource);

        SupersetDatasetRegistrySchemaMigrator.MigrationReport report = migrator.migrate(dryRun);
        System.out.println(JsonUtil.toString(report));
    }

    private static DataSource buildDataSource(String jdbcUrl, String username, String password) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(jdbcUrl);
        ds.setUsername(username);
        ds.setPassword(password);
        // Driver may be auto-detected by JDBC SPI. For MySQL, explicitly set when available.
        String driverClass = resolveDriverClass(jdbcUrl);
        if (StringUtils.isNotBlank(driverClass)) {
            try {
                Class.forName(driverClass);
                ds.setDriverClassName(driverClass);
            } catch (Exception ignored) {
                // ignore, rely on auto-detection
            }
        }
        return ds;
    }

    private static String resolveDriverClass(String jdbcUrl) {
        if (StringUtils.isBlank(jdbcUrl)) {
            return null;
        }
        String url = jdbcUrl.toLowerCase(Locale.ROOT);
        if (url.startsWith("jdbc:postgresql:")) {
            return "org.postgresql.Driver";
        }
        if (url.startsWith("jdbc:mysql:")) {
            return "com.mysql.cj.jdbc.Driver";
        }
        return null;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        if (args == null) {
            return map;
        }
        for (int i = 0; i < args.length; i++) {
            String raw = args[i];
            if (StringUtils.isBlank(raw)) {
                continue;
            }
            String item = raw.trim();
            if ("--dry-run".equals(item) || "--print".equals(item)) {
                map.put(item.substring(2), "true");
                continue;
            }
            if (!item.startsWith("--")) {
                continue;
            }
            String[] pair = item.substring(2).split("=", 2);
            if (pair.length == 2) {
                map.put(pair[0], pair[1]);
                continue;
            }
            if (pair.length == 1 && i + 1 < args.length) {
                String next = args[i + 1];
                if (next != null && !next.startsWith("--")) {
                    map.put(pair[0], next);
                    i++;
                }
            }
        }
        return map;
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static String firstNonBlank(String a, String b, String c) {
        if (StringUtils.isNotBlank(a)) {
            return a.trim();
        }
        if (StringUtils.isNotBlank(b)) {
            return b.trim();
        }
        if (StringUtils.isNotBlank(c)) {
            return c.trim();
        }
        return null;
    }
}
