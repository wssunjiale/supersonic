package com.tencent.supersonic.headless.server.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Inspect current Supersonic metadata DB schema for table {@code s2_superset_dataset}.
 *
 * <p>
 * This CLI is read-only and does not require starting the Supersonic server.
 * </p>
 *
 * <p>
 * Connection information is taken from env vars (with defaults matching standalone config):
 * <ul>
 * <li>S2_DB_HOST (default 10.0.12.252)</li>
 * <li>S2_DB_PORT (default 5439)</li>
 * <li>S2_DB_DATABASE (default supersonic)</li>
 * <li>S2_DB_USER (default supersonic)</li>
 * <li>S2_DB_PASSWORD (default postgres)</li>
 * </ul>
 * </p>
 */
public class SupersonicMetaDbSchemaInspectCli {

    public static void main(String[] args) throws Exception {
        String host = getenv("S2_DB_HOST", "10.0.12.252");
        String port = getenv("S2_DB_PORT", "5439");
        String database = getenv("S2_DB_DATABASE", "supersonic");
        String username = getenv("S2_DB_USER", "supersonic");
        String password = getenv("S2_DB_PASSWORD", "postgres");

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database
                + "?stringtype=unspecified";

        String sql = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 's2_superset_dataset'
                ORDER BY ordinal_position
                """;

        try (Connection connection = DriverManager.getConnection(url, username, password);
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2));
            }
        }
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }
}
