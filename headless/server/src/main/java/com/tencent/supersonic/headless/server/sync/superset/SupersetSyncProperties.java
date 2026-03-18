package com.tencent.supersonic.headless.server.sync.superset;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "s2.superset")
public class SupersetSyncProperties {

    private boolean enabled = true;

    private String baseUrl;

    private boolean authEnabled = false;

    private String authStrategy = "JWT_FIRST";

    private String apiKey;

    private String jwtUsername;

    private String jwtPassword;

    private String jwtProvider = "db";

    private Integer timeoutSeconds = 30;

    private String datasourceType = "table";

    private Sync sync = new Sync();

    @Data
    public static class Sync {
        private boolean enabled = true;
        private long intervalMs = 60 * 60 * 1000L;
        private long datasetIntervalMs = 60 * 1000L;
        private long retryIntervalMs = 60 * 1000L;
        private int maxRetries = 3;
        private boolean rebuild = false;
        /**
         * Whether auto-migrate Supersonic metadata DB schema for
         * {@code s2_superset_dataset} on
         * startup. When enabled, Supersonic will attempt to add missing columns/indexes
         * required by
         * Superset dataset registry syncing.
         */
        private boolean autoMigrateRegistrySchema = true;
    }
}
