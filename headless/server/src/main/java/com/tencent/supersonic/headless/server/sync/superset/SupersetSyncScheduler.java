package com.tencent.supersonic.headless.server.sync.superset;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Slf4j
@Component
public class SupersetSyncScheduler {

    private final SupersetSyncService supersetSyncService;
    private final SupersetSyncProperties properties;

    public SupersetSyncScheduler(SupersetSyncService supersetSyncService,
            SupersetSyncProperties properties) {
        this.supersetSyncService = supersetSyncService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${s2.superset.sync.interval-ms:3600000}")
    public void scheduleSync() {
        if (!properties.isEnabled() || !properties.getSync().isEnabled()) {
            return;
        }
        log.debug("superset scheduled sync triggered");
        supersetSyncService.triggerFullSync(SupersetSyncTrigger.SCHEDULED);
    }

    @Scheduled(fixedDelayString = "${s2.superset.sync.dataset-interval-ms:60000}")
    public void scheduleDatasetSync() {
        if (!properties.isEnabled() || !properties.getSync().isEnabled()) {
            return;
        }
        log.debug("superset scheduled dataset sync triggered");
        supersetSyncService.triggerDatasetSync(Collections.emptySet(),
                SupersetSyncTrigger.SCHEDULED);
    }
}
