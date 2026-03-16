package com.tencent.supersonic.chat.server.service;

import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetDashboardInfo;
import com.tencent.supersonic.common.pojo.User;

import java.util.List;

public interface SupersetDashboardRegistryService {

    SupersetDashboardInfo registerManualDashboard(Long pluginId, SupersetDashboardInfo dashboard,
            User user);

    List<SupersetDashboardInfo> listManualDashboards(Long pluginId, User user);

    SupersetDashboardInfo getManualDashboard(Long pluginId, Long dashboardId);

    void markDeleted(Long pluginId, Long dashboardId, User user);
}
