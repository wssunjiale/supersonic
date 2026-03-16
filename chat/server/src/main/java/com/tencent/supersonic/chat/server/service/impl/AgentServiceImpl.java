package com.tencent.supersonic.chat.server.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.tencent.supersonic.auth.api.authentication.service.UserService;
import com.tencent.supersonic.chat.api.pojo.request.ChatMemoryFilter;
import com.tencent.supersonic.chat.api.pojo.request.ChatParseReq;
import com.tencent.supersonic.chat.server.agent.Agent;
import com.tencent.supersonic.chat.server.agent.VisualConfig;
import com.tencent.supersonic.chat.server.persistence.dataobject.AgentDO;
import com.tencent.supersonic.chat.server.persistence.mapper.AgentDOMapper;
import com.tencent.supersonic.chat.server.pojo.ChatMemory;
import com.tencent.supersonic.chat.server.service.AgentService;
import com.tencent.supersonic.chat.server.service.ChatQueryService;
import com.tencent.supersonic.chat.server.service.MemoryService;
import com.tencent.supersonic.common.config.ChatModel;
import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.pojo.enums.AppModule;
import com.tencent.supersonic.common.pojo.enums.AuthType;
import com.tencent.supersonic.common.service.ChatModelService;
import com.tencent.supersonic.common.util.ChatAppManager;
import com.tencent.supersonic.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AgentServiceImpl extends ServiceImpl<AgentDOMapper, AgentDO> implements AgentService {

    @Autowired
    private MemoryService memoryService;

    @Autowired
    @Lazy
    private ChatQueryService chatQueryService;

    @Autowired
    private ChatModelService chatModelService;

    @Autowired
    private UserService userService;

    @Autowired
    @Qualifier("chatExecutor")
    private ThreadPoolExecutor executor;

    @Override
    public List<Agent> getAgents(User user, AuthType authType) {
        return getAgentDOList().stream().map(this::convert)
                .filter(agent -> filterByAuth(agent, user, authType)).collect(Collectors.toList());
    }

    private boolean filterByAuth(Agent agent, User user, AuthType authType) {
        Set<String> orgIds = userService.getUserAllOrgId(user.getName());

        if (user.isSuperAdmin() || agent.openToAll()
                || user.getName().equals(agent.getCreatedBy())) {
            return true;
        }
        authType = authType == null ? AuthType.VIEWER : authType;
        switch (authType) {
            case ADMIN:
                return checkAdminPermission(orgIds, user, agent);
            case VIEWER:
            default:
                return checkViewPermission(orgIds, user, agent);
        }
    }

    @Override
    public List<Agent> getAgents() {
        return getAgentDOList().stream().map(this::convert).collect(Collectors.toList());
    }

    @Override
    public Agent createAgent(Agent agent, User user) {
        agent.createdBy(user.getName());
        AgentDO agentDO = convert(agent);
        save(agentDO);
        agent.setId(agentDO.getId());
        executeAgentExamplesAsync(agent);
        return agent;
    }

    @Override
    public Agent updateAgent(Agent agent, User user) {
        agent.updatedBy(user.getName());
        updateById(convert(agent));
        executeAgentExamplesAsync(agent);
        return agent;
    }

    @Override
    public Agent getAgent(Integer id) {
        if (id == null) {
            return null;
        }
        return convert(getById(id));
    }

    @Override
    public void deleteAgent(Integer id) {
        removeById(id);
    }

    /**
     * the example in the agent will be executed by default, if the result is correct, it will be
     * put into memory as a reference for LLM
     *
     * @param agent
     */
    private void executeAgentExamplesAsync(Agent agent) {
        executor.execute(() -> doExecuteAgentExamples(agent));
    }

    private synchronized void doExecuteAgentExamples(Agent agent) {
        if (!agent.containsDatasetTool() || !agent.enableMemoryReview()
                || CollectionUtils.isEmpty(agent.getExamples())) {
            return;
        }

        List<String> examples = agent.getExamples();
        ChatMemoryFilter chatMemoryFilter =
                ChatMemoryFilter.builder().agentId(agent.getId()).questions(examples).build();
        List<String> memoriesExisted = memoryService.getMemories(chatMemoryFilter).stream()
                .map(ChatMemory::getQuestion).collect(Collectors.toList());
        for (String example : examples) {
            if (memoriesExisted.contains(example)) {
                continue;
            }
            try {
                chatQueryService
                        .parseAndExecute(ChatParseReq.builder().chatId(-1).agentId(agent.getId())
                                .queryText(example).user(User.getDefaultUser()).build());
            } catch (Exception e) {
                log.warn("agent:{} example execute failed:{}", agent.getName(), example);
            }
        }
    }

    private List<AgentDO> getAgentDOList() {
        return list();
    }

    private Agent convert(AgentDO agentDO) {
        if (agentDO == null) {
            return null;
        }
        Agent agent = new Agent();
        BeanUtils.copyProperties(agentDO, agent);
        agent.setToolConfig(agentDO.getToolConfig());
        agent.setExamples(JsonUtil.toList(agentDO.getExamples(), String.class));
        agent.setChatAppConfig(mergeChatAppConfigs(
                JsonUtil.toMap(agentDO.getChatModelConfig(), String.class, ChatApp.class)));
        agent.setVisualConfig(JsonUtil.toObject(agentDO.getVisualConfig(), VisualConfig.class));
        agent.getChatAppConfig().values().forEach(c -> {
            if (c.isEnable()) {// 优化，减少访问数据库的次数
                ChatModel chatModel = chatModelService.getChatModel(c.getChatModelId());
                if (Objects.nonNull(chatModel)) {
                    c.setChatModelConfig(chatModel.getConfig());
                }
            }
        });
        agent.setAdmins(JsonUtil.toList(agentDO.getAdmin(), String.class));
        agent.setViewers(JsonUtil.toList(agentDO.getViewer(), String.class));
        agent.setAdminOrgs(JsonUtil.toList(agentDO.getAdminOrg(), String.class));
        agent.setViewOrgs(JsonUtil.toList(agentDO.getViewOrg(), String.class));
        agent.setIsOpen(agentDO.getIsOpen());
        return agent;
    }

    private AgentDO convert(Agent agent) {
        AgentDO agentDO = new AgentDO();
        BeanUtils.copyProperties(agent, agentDO);
        agentDO.setToolConfig(agent.getToolConfig());
        agentDO.setExamples(JsonUtil.toString(agent.getExamples()));
        agentDO.setChatModelConfig(JsonUtil.toString(normalizeChatAppConfigs(agent.getChatAppConfig())));
        agentDO.setVisualConfig(JsonUtil.toString(agent.getVisualConfig()));
        agentDO.setAdmin(JsonUtil.toString(agent.getAdmins()));
        agentDO.setViewer(JsonUtil.toString(agent.getViewers()));
        agentDO.setAdminOrg(JsonUtil.toString(agent.getAdminOrgs()));
        agentDO.setViewOrg(JsonUtil.toString(agent.getViewOrgs()));
        agentDO.setIsOpen(agent.getIsOpen());
        if (agentDO.getStatus() == null) {
            agentDO.setStatus(1);
        }
        return agentDO;
    }

    Map<String, ChatApp> mergeChatAppConfigs(Map<String, ChatApp> configuredApps) {
        if (CollectionUtils.isEmpty(configuredApps)) {
            return Collections.emptyMap();
        }
        Map<String, ChatApp> merged = new LinkedHashMap<>();
        configuredApps.forEach((key, configuredApp) -> {
            ChatApp defaultApp =
                    ChatAppManager.getAllApps(AppModule.CHAT).get(key);
            merged.put(key, mergeChatApp(defaultApp, configuredApp));
        });
        return merged;
    }

    Map<String, ChatApp> normalizeChatAppConfigs(Map<String, ChatApp> configuredApps) {
        if (CollectionUtils.isEmpty(configuredApps)) {
            return Collections.emptyMap();
        }
        Map<String, ChatApp> normalized = new LinkedHashMap<>();
        configuredApps.forEach((key, configuredApp) -> {
            if (configuredApp == null) {
                return;
            }
            ChatApp app = copyChatApp(configuredApp);
            if (StringUtils.isBlank(app.getPrompt())) {
                app.setPrompt(null);
            }
            normalized.put(key, app);
        });
        return normalized;
    }

    private ChatApp mergeChatApp(ChatApp defaultApp, ChatApp configuredApp) {
        if (defaultApp == null) {
            return copyChatApp(configuredApp);
        }
        ChatApp merged = copyChatApp(defaultApp);
        if (configuredApp == null) {
            return merged;
        }
        if (StringUtils.isNotBlank(configuredApp.getName())) {
            merged.setName(configuredApp.getName());
        }
        if (StringUtils.isNotBlank(configuredApp.getDescription())) {
            merged.setDescription(configuredApp.getDescription());
        }
        if (StringUtils.isNotBlank(configuredApp.getPrompt())) {
            merged.setPrompt(configuredApp.getPrompt());
        }
        merged.setEnable(configuredApp.isEnable());
        if (configuredApp.getChatModelId() != null) {
            merged.setChatModelId(configuredApp.getChatModelId());
        }
        if (configuredApp.getChatModelConfig() != null) {
            merged.setChatModelConfig(configuredApp.getChatModelConfig());
        }
        if (configuredApp.getAppModule() != null) {
            merged.setAppModule(configuredApp.getAppModule());
        }
        return merged;
    }

    private ChatApp copyChatApp(ChatApp source) {
        if (source == null) {
            return null;
        }
        ChatApp target = new ChatApp();
        target.setName(source.getName());
        target.setDescription(source.getDescription());
        target.setPrompt(source.getPrompt());
        target.setEnable(source.isEnable());
        target.setChatModelId(source.getChatModelId());
        target.setChatModelConfig(source.getChatModelConfig());
        target.setAppModule(source.getAppModule());
        return target;
    }

    private boolean checkAdminPermission(Set<String> orgIds, User user, Agent agent) {
        List<String> admins = agent.getAdmins();
        List<String> adminOrgs = agent.getAdminOrgs();
        if (user.isSuperAdmin()) {
            return true;
        }
        if (admins.contains(user.getName()) || agent.getCreatedBy().equals(user.getName())) {
            return true;
        }
        if (CollectionUtils.isEmpty(adminOrgs)) {
            return false;
        }
        for (String orgId : orgIds) {
            if (adminOrgs.contains(orgId)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkViewPermission(Set<String> orgIds, User user, Agent agent) {
        if (checkAdminPermission(orgIds, user, agent)) {
            return true;
        }
        List<String> viewers = agent.getViewers();
        List<String> viewOrgs = agent.getViewOrgs();
        if (agent.openToAll()) {
            return true;
        }
        if (viewers.contains(user.getName())) {
            return true;
        }
        if (CollectionUtils.isEmpty(viewOrgs)) {
            return false;
        }
        for (String orgId : orgIds) {
            if (viewOrgs.contains(orgId)) {
                return true;
            }
        }
        return false;
    }

}
