package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.tencent.supersonic.chat.server.plugin.ChatPlugin;
import com.tencent.supersonic.chat.server.service.PluginService;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Component
@Slf4j
public class SupersetPluginInitializer implements ApplicationRunner {

    private static final String SUPERSET_TYPE = "SUPERSET";
    private static final String DEFAULT_NAME = "Superset";

    private final PluginService pluginService;
    private final SupersetPluginProperties properties;

    public SupersetPluginInitializer(PluginService pluginService,
            SupersetPluginProperties properties) {
        this.pluginService = pluginService;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isEnabled()) {
            return;
        }
        if (StringUtils.isBlank(properties.getBaseUrl())) {
            return;
        }
        SupersetPluginConfig config = buildConfig();
        Optional<ChatPlugin> existing = findSupersetPlugin(pluginService.getPluginList());
        if (existing.isPresent()) {
            ChatPlugin plugin = existing.get();
            applyDefaults(plugin);
            plugin.setConfig(JsonUtil.toString(config));
            pluginService.updatePlugin(plugin, User.getDefaultUser());
            return;
        }
        ChatPlugin plugin = new ChatPlugin();
        plugin.setType(SUPERSET_TYPE);
        plugin.setName(DEFAULT_NAME);
        plugin.setDataSetList(Collections.singletonList(-1L));
        plugin.setPattern("superset");
        plugin.setConfig(JsonUtil.toString(config));
        pluginService.createPlugin(plugin, User.getDefaultUser());
        log.info("Superset plugin initialized");
    }

    private Optional<ChatPlugin> findSupersetPlugin(List<ChatPlugin> plugins) {
        if (plugins == null) {
            return Optional.empty();
        }
        return plugins.stream().filter(plugin -> SUPERSET_TYPE.equalsIgnoreCase(plugin.getType()))
                .findFirst();
    }

    private void applyDefaults(ChatPlugin plugin) {
        plugin.setType(SUPERSET_TYPE);
        plugin.setName(StringUtils.defaultIfBlank(plugin.getName(), DEFAULT_NAME));
        plugin.setDataSetList(Collections.singletonList(-1L));
    }

    private SupersetPluginConfig buildConfig() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setEnabled(true);
        config.setAuthEnabled(properties.isAuthEnabled());
        config.setBaseUrl(properties.getBaseUrl());
        config.setJwtUsername(properties.getJwtUsername());
        config.setJwtPassword(properties.getJwtPassword());
        config.setJwtProvider(properties.getJwtProvider());
        config.setTimeoutSeconds(properties.getTimeoutSeconds());
        config.setDatasourceType(properties.getDatasourceType());
        config.setVizType(properties.getVizType());
        config.setVizTypeLlmEnabled(properties.isVizTypeLlmEnabled());
        config.setVizTypeLlmTopN(properties.getVizTypeLlmTopN());
        config.setVizTypeAllowList(properties.getVizTypeAllowList());
        config.setVizTypeDenyList(properties.getVizTypeDenyList());
        config.setFormData(properties.getFormData());
        config.setHeight(properties.getHeight());
        config.setGuestTokenUserUsername(properties.getGuestTokenUserUsername());
        config.setGuestTokenUserFirstName(properties.getGuestTokenUserFirstName());
        config.setGuestTokenUserLastName(properties.getGuestTokenUserLastName());
        return config;
    }
}
