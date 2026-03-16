package com.tencent.supersonic.headless.server.sync.superset.migration;

import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Best-effort initializer to ensure Supersonic metadata DB schema for table {@code
 * s2_superset_dataset} is compatible with the current Superset dataset registry logic.
 *
 * <p>
 * This runs once on application startup when Superset sync is enabled and {@code
 * s2.superset.sync.auto-migrate-registry-schema=true}.
 * </p>
 */
@Component
@Slf4j
public class SupersetDatasetRegistrySchemaInitializer implements ApplicationRunner {

    private final SupersetSyncProperties properties;
    private final SupersetDatasetRegistrySchemaMigrator migrator;

    public SupersetDatasetRegistrySchemaInitializer(SupersetSyncProperties properties,
            SupersetDatasetRegistrySchemaMigrator migrator) {
        this.properties = properties;
        this.migrator = migrator;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (properties == null || !properties.isEnabled() || properties.getSync() == null
                || !properties.getSync().isEnabled()
                || !properties.getSync().isAutoMigrateRegistrySchema()) {
            return;
        }
        try {
            SupersetDatasetRegistrySchemaMigrator.MigrationReport report = migrator.migrate(false);
            int executed = report == null || report.getExecuted() == null ? 0
                    : report.getExecuted().size();
            int planned =
                    report == null || report.getPlanned() == null ? 0 : report.getPlanned().size();
            int errors =
                    report == null || report.getErrors() == null ? 0 : report.getErrors().size();
            if (errors > 0) {
                log.warn(
                        "superset dataset registry schema auto-migrate finished with errors, executed={}, planned={}, errors={}",
                        executed, planned, errors);
            } else if (executed > 0) {
                log.info("superset dataset registry schema auto-migrate applied, executed={}",
                        executed);
            } else {
                log.debug("superset dataset registry schema already compatible");
            }
        } catch (Exception ex) {
            log.warn("superset dataset registry schema auto-migrate failed", ex);
        }
    }
}
