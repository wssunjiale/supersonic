package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.headless.server.persistence.dataobject.SupersetDatasetDO;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncProperties;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncTrigger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Slf4j
@Component
public class SupersetSemanticDatasetChangedListener {

    private final SupersetSemanticDatasetMapper mapper;
    private final SupersetSemanticDatasetRegistry registry;
    private final SupersetSyncService supersetSyncService;
    private final SupersetSyncProperties properties;

    public SupersetSemanticDatasetChangedListener(SupersetSemanticDatasetMapper mapper,
            SupersetSemanticDatasetRegistry registry, SupersetSyncService supersetSyncService,
            SupersetSyncProperties properties) {
        this.mapper = mapper;
        this.registry = registry;
        this.supersetSyncService = supersetSyncService;
        this.properties = properties;
    }

    @Async("eventExecutor")
    @EventListener
    public void onSemanticDatasetChanged(SupersetSemanticDatasetChangedEvent event) {
        if (event == null || event.getDataSetId() == null) {
            return;
        }
        if (!properties.isEnabled() || StringUtils.isBlank(properties.getBaseUrl())) {
            return;
        }
        User safeUser = event.getUser() == null ? User.getDefaultUser() : event.getUser();
        try {
            SemanticDatasetMapping mapping = mapper.buildMapping(event.getDataSetId(), safeUser);
            if (mapping == null) {
                return;
            }
            SupersetDatasetDO record = registry.upsert(mapping, safeUser);
            if (record == null || record.getId() == null) {
                return;
            }
            supersetSyncService.triggerDatasetSync(Collections.singleton(record.getId()),
                    SupersetSyncTrigger.EVENT);
        } catch (Exception ex) {
            log.warn("superset semantic dataset change sync failed, dataSetId={}",
                    event.getDataSetId(), ex);
        }
    }
}
