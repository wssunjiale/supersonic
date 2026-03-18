package com.tencent.supersonic.demo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.supersonic.chat.server.agent.Agent;
import com.tencent.supersonic.chat.server.agent.AgentToolType;
import com.tencent.supersonic.chat.server.plugin.ChatPlugin;
import com.tencent.supersonic.chat.server.plugin.build.ParamOption;
import com.tencent.supersonic.chat.server.plugin.build.WebBase;
import com.tencent.supersonic.chat.server.plugin.build.agentservice.AgentServiceQuery;
import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.enums.AppModule;
import com.tencent.supersonic.common.util.ChatAppManager;
import com.tencent.supersonic.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j
@Order(4)
public class S2MetricPlannerDemo extends S2BaseDemo {

    private static final String PLUGIN_NAME = "metric_planner插件";
    private static final String PLUGIN_SERVICE_NAME = "metric_planner";
    private static final String AGENT_NAME = "metric_planner智能体";

    @Override
    public void doRun() {
        try {
            ChatPlugin plugin = addMetricPlannerPlugin();
            Long pluginId = plugin != null ? plugin.getId() : null;
            addMetricPlannerAgent(pluginId);
        } catch (Exception e) {
            log.error("Failed to add metric planner demo data", e);
        }
    }

    @Override
    protected boolean checkNeedToRun() {
        boolean agentExists = agentService.getAgents().stream()
                .anyMatch(agent -> AGENT_NAME.equals(agent.getName()));
        boolean pluginExists = pluginService.getPluginList().stream()
                .anyMatch(plugin -> PLUGIN_NAME.equals(plugin.getName()));
        return !(agentExists && pluginExists);
    }

    private ChatPlugin addMetricPlannerPlugin() {
        ChatPlugin exists = findPluginByName(PLUGIN_NAME);
        if (exists != null) {
            return exists;
        }

        ChatPlugin plugin = new ChatPlugin();
        plugin.setType(AgentServiceQuery.QUERY_MODE);
        plugin.setName(PLUGIN_NAME);
        plugin.setParseModeConfig(buildParseModeConfig());
        plugin.setConfig(buildPluginConfig());
        pluginService.createPlugin(plugin, defaultUser);
        return findPluginByName(PLUGIN_NAME);
    }

    private void addMetricPlannerAgent(Long pluginId) {
        if (pluginId == null) {
            log.warn("{} plugin id missing, skip agent creation", PLUGIN_NAME);
            return;
        }
        boolean agentExists = agentService.getAgents().stream()
                .anyMatch(agent -> AGENT_NAME.equals(agent.getName()));
        if (agentExists) {
            return;
        }
        Agent agent = new Agent();
        agent.setName(AGENT_NAME);
        agent.setDescription(String.format("通过调用 %s 插件完成指标规划相关对话", PLUGIN_NAME));
        agent.setStatus(1);
        agent.setEnableSearch(1);
        agent.setEnableFeedback(1);
        agent.setExamples(Collections.emptyList());
        agent.setToolConfig(buildToolConfig(pluginId));
        agent.setChatAppConfig(buildChatAppConfig());
        agentService.createAgent(agent, defaultUser);
    }

    private ChatPlugin findPluginByName(String name) {
        Optional<ChatPlugin> pluginOptional = pluginService.getPluginList().stream()
                .filter(plugin -> name.equals(plugin.getName())).findFirst();
        return pluginOptional.orElse(null);
    }

    private String buildParseModeConfig() {
        Map<String, Object> parseConfig = new HashMap<>();
        parseConfig.put("name", PLUGIN_SERVICE_NAME);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("type", "object");
        parameters.put("properties", new HashMap<>());
        parameters.put("required", Lists.newArrayList());
        parseConfig.put("parameters", parameters);
        parseConfig.put("examples", Lists.newArrayList());
        return JsonUtil.toString(parseConfig);
    }

    private String buildPluginConfig() {
        WebBase webBase = new WebBase();
        webBase.setUrl("http://10.0.12.253:8000/chat");
        ParamOption paramOption = new ParamOption();
        paramOption.setParamType(ParamOption.ParamType.FORWARD);
        paramOption.setKey("height");
        webBase.setParamOptions(Collections.singletonList(paramOption));
        return JsonUtil.toString(webBase);
    }

    private String buildToolConfig(Long pluginId) {
        Map<String, Object> toolConfig = new HashMap<>();
        Map<String, Object> pluginTool = new HashMap<>();
        pluginTool.put("id", PLUGIN_SERVICE_NAME);
        pluginTool.put("type", AgentToolType.PLUGIN.name());
        pluginTool.put("name", PLUGIN_NAME);
        pluginTool.put("plugins", Collections.singletonList(pluginId));
        pluginTool.put("exampleQuestions", Collections.emptyList());
        pluginTool.put("metricOptions", Collections.emptyList());
        toolConfig.put("tools", Collections.singletonList(pluginTool));
        toolConfig.put("simpleMode", false);
        toolConfig.put("debugMode", true);
        return JsonUtil.toString(toolConfig);
    }

    private Map<String, ChatApp> buildChatAppConfig() {
        Map<String, ChatApp> chatAppConfig = Maps.newHashMap(ChatAppManager.getAllApps(AppModule.CHAT));
        chatAppConfig.values().forEach(app -> app.setChatModelId(demoChatModel.getId()));
        Optional.ofNullable(chatAppConfig.get("SMALL_TALK")).ifPresent(app -> app.setEnable(true));
        Optional.ofNullable(chatAppConfig.get("S2SQL_PARSER")).ifPresent(app -> app.setEnable(false));
        return chatAppConfig;
    }
}
