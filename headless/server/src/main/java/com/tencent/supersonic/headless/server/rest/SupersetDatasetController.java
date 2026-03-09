package com.tencent.supersonic.headless.server.rest;

import com.github.pagehelper.PageInfo;
import com.tencent.supersonic.auth.api.authentication.utils.UserHolder;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.headless.api.pojo.request.MetaBatchReq;
import com.tencent.supersonic.headless.api.pojo.request.SupersetDatasetQueryReq;
import com.tencent.supersonic.headless.api.pojo.response.SupersetDatasetResp;
import com.tencent.supersonic.headless.server.service.SupersetDatasetRegistryService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncResult;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncService;
import com.tencent.supersonic.headless.server.sync.superset.SupersetSyncTrigger;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/semantic/superset/datasets")
public class SupersetDatasetController {

    private final SupersetDatasetRegistryService registryService;
    private final SupersetSyncService supersetSyncService;

    public SupersetDatasetController(SupersetDatasetRegistryService registryService,
            SupersetSyncService supersetSyncService) {
        this.registryService = registryService;
        this.supersetSyncService = supersetSyncService;
    }

    @PostMapping("/query")
    public PageInfo<SupersetDatasetResp> query(@RequestBody SupersetDatasetQueryReq queryReq,
            HttpServletRequest request, HttpServletResponse response) {
        User user = UserHolder.findUser(request, response);
        return registryService.querySupersetDataset(queryReq, user);
    }

    @PostMapping("/{id}/sync")
    public SupersetSyncResult syncOne(@PathVariable("id") Long id, HttpServletRequest request,
            HttpServletResponse response) {
        User user = UserHolder.findUser(request, response);
        registryService.markSyncPending(id, user);
        return supersetSyncService.triggerDatasetSync(java.util.Collections.singleton(id),
                SupersetSyncTrigger.MANUAL);
    }

    @PostMapping("/syncBatch")
    public SupersetSyncResult syncBatch(@RequestBody MetaBatchReq batchReq,
            HttpServletRequest request, HttpServletResponse response) {
        User user = UserHolder.findUser(request, response);
        java.util.Set<Long> ids =
                batchReq == null || batchReq.getIds() == null ? java.util.Collections.emptySet()
                        : new java.util.HashSet<>(batchReq.getIds());
        for (Long id : ids) {
            registryService.markSyncPending(id, user);
        }
        return supersetSyncService.triggerDatasetSync(ids, SupersetSyncTrigger.MANUAL);
    }

    @DeleteMapping("/{id}")
    public boolean delete(@PathVariable("id") Long id, HttpServletRequest request,
            HttpServletResponse response) {
        User user = UserHolder.findUser(request, response);
        registryService.deleteSupersetDataset(id, user);
        return true;
    }

    @PostMapping("/deleteBatch")
    public boolean deleteBatch(@RequestBody MetaBatchReq batchReq, HttpServletRequest request,
            HttpServletResponse response) {
        User user = UserHolder.findUser(request, response);
        registryService.deleteSupersetDatasetBatch(batchReq.getIds(), user);
        return true;
    }
}
