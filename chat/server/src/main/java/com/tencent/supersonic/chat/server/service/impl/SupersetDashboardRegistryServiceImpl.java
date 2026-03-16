package com.tencent.supersonic.chat.server.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.tencent.supersonic.chat.server.persistence.dataobject.SupersetDashboardDO;
import com.tencent.supersonic.chat.server.persistence.mapper.SupersetDashboardMapper;
import com.tencent.supersonic.chat.server.plugin.build.superset.SupersetDashboardInfo;
import com.tencent.supersonic.chat.server.service.SupersetDashboardRegistryService;
import com.tencent.supersonic.common.pojo.User;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class SupersetDashboardRegistryServiceImpl
        extends ServiceImpl<SupersetDashboardMapper, SupersetDashboardDO>
        implements SupersetDashboardRegistryService {

    private static final int NOT_DELETED = 0;
    private static final int DELETED = 1;

    @Override
    public SupersetDashboardInfo registerManualDashboard(Long pluginId,
            SupersetDashboardInfo dashboard, User user) {
        if (pluginId == null || dashboard == null || dashboard.getId() == null) {
            return dashboard;
        }
        Date now = new Date();
        User safeUser = user == null ? User.getDefaultUser() : user;
        SupersetDashboardDO existing = getRecord(pluginId, dashboard.getId());
        if (existing == null) {
            existing = new SupersetDashboardDO();
            existing.setPluginId(pluginId);
            existing.setDashboardId(dashboard.getId());
            existing.setCreatedAt(now);
            existing.setCreatedBy(safeUser.getName());
        }
        existing.setTitle(dashboard.getTitle());
        existing.setEmbeddedId(dashboard.getEmbeddedId());
        existing.setOwnerId(safeUser.getId());
        existing.setOwnerName(
                StringUtils.defaultIfBlank(safeUser.getName(), safeUser.getDisplayName()));
        existing.setDeleted(NOT_DELETED);
        existing.setUpdatedAt(now);
        existing.setUpdatedBy(safeUser.getName());
        if (existing.getId() == null) {
            save(existing);
        } else {
            updateById(existing);
        }
        return toDashboardInfo(existing);
    }

    @Override
    public List<SupersetDashboardInfo> listManualDashboards(Long pluginId, User user) {
        if (pluginId == null || user == null) {
            return Collections.emptyList();
        }
        LambdaQueryWrapper<SupersetDashboardDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDashboardDO::getPluginId, pluginId)
                .eq(SupersetDashboardDO::getDeleted, NOT_DELETED)
                .orderByDesc(SupersetDashboardDO::getCreatedAt, SupersetDashboardDO::getId);
        if (!user.isSuperAdmin()) {
            wrapper.eq(SupersetDashboardDO::getOwnerId, user.getId());
        }
        return list(wrapper).stream().map(this::toDashboardInfo).collect(Collectors.toList());
    }

    @Override
    public SupersetDashboardInfo getManualDashboard(Long pluginId, Long dashboardId) {
        SupersetDashboardDO record = getRecord(pluginId, dashboardId);
        return toDashboardInfo(record);
    }

    @Override
    public void markDeleted(Long pluginId, Long dashboardId, User user) {
        if (pluginId == null || dashboardId == null) {
            return;
        }
        User safeUser = user == null ? User.getDefaultUser() : user;
        LambdaUpdateWrapper<SupersetDashboardDO> wrapper = new LambdaUpdateWrapper<>();
        wrapper.eq(SupersetDashboardDO::getPluginId, pluginId)
                .eq(SupersetDashboardDO::getDashboardId, dashboardId)
                .eq(SupersetDashboardDO::getDeleted, NOT_DELETED)
                .set(SupersetDashboardDO::getDeleted, DELETED)
                .set(SupersetDashboardDO::getUpdatedAt, new Date())
                .set(SupersetDashboardDO::getUpdatedBy, safeUser.getName());
        update(wrapper);
    }

    private SupersetDashboardDO getRecord(Long pluginId, Long dashboardId) {
        if (pluginId == null || dashboardId == null) {
            return null;
        }
        LambdaQueryWrapper<SupersetDashboardDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SupersetDashboardDO::getPluginId, pluginId)
                .eq(SupersetDashboardDO::getDashboardId, dashboardId)
                .eq(SupersetDashboardDO::getDeleted, NOT_DELETED).last("limit 1");
        return getOne(wrapper, false);
    }

    private SupersetDashboardInfo toDashboardInfo(SupersetDashboardDO record) {
        if (record == null) {
            return null;
        }
        SupersetDashboardInfo info = new SupersetDashboardInfo();
        info.setId(record.getDashboardId());
        info.setTitle(record.getTitle());
        info.setEmbeddedId(record.getEmbeddedId());
        info.setOwnerId(record.getOwnerId());
        info.setOwnerName(record.getOwnerName());
        return info;
    }
}
