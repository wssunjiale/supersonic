package com.tencent.supersonic.headless.server.rest;

import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncResult;
import com.tencent.supersonic.headless.server.sync.superset.semantic.SupersetSemanticDatasetSyncService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/semantic/superset")
public class SupersetSemanticDatasetSyncController {

    private final SupersetSemanticDatasetSyncService semanticDatasetSyncService;

    public SupersetSemanticDatasetSyncController(
            SupersetSemanticDatasetSyncService semanticDatasetSyncService) {
        this.semanticDatasetSyncService = semanticDatasetSyncService;
    }

    /**
     * Sync Supersonic semantic datasets (DataSet) into Superset datasets.
     *
     * This endpoint is intentionally separated from the existing dataset sync that is used by
     * chat-generated SQL datasets. It can take over when needed, without overwriting the old
     * workflow.
     */
    @PostMapping("/sync/semantic-datasets")
    public SupersetSyncResult syncSemanticDatasets() {
        return semanticDatasetSyncService.syncAllSemanticDatasets(User.getDefaultUser());
    }
}
