package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.chat.server.agent.Agent;
import com.tencent.supersonic.common.config.ChatModel;
import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.ChatModelConfig;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.service.ChatModelService;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.input.Prompt;
import dev.langchain4j.model.input.PromptTemplate;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.provider.ModelProvider;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SupersetVizTypeSelector {

    private static final String DEFAULT_VIZ_TYPE = "table";
    private static final List<String> VIZTYPE_RESOURCES = Arrays
            .asList("superset-spec/catalog/viztypes.json", "viztype.json", "docs/viztype.json");
    private static final VizTypeCatalog VIZTYPE_CATALOG = VizTypeCatalog.load();
    private static final String LLM_PROMPT = "" + "#Role: You are a Superset chart type selector.\n"
            + "#Task: Rank the most suitable Superset viz_type candidates for this chart request.\n"
            + "#Rules:\n" + "1. ONLY choose from selection_payload.candidate_viztypes.\n"
            + "2. Local prefilter already removed obviously impossible types, but you must still rank by fitness.\n"
            + "3. Consider user_question, executed_sql, sql_summary, query_result, data_profile, dataset_context, semantic_context, intent_profile and signals together.\n"
            + "4. Prioritize candidates whose required_dataset_roles and required_slots can be satisfied by the dataset and the query intent.\n"
            + "5. Penalize candidates that require unavailable roles such as time, metric, latitude/longitude, source/target.\n"
            + "6. Treat selection_payload.intent_profile.preferred_table_viz_type as a strong local intent signal when it is present.\n"
            + "7. For explicit detail/list/raw-row requests, prefer plain `table`. Use `pivot_table_v2` only for cross-dimensional aggregated tables. Use `time_table` or `time_pivot` only for time-series report/comparison intent, not merely because a time column exists.\n"
            + "8. Prefer charts that directly express the question intent. Plain table is allowed to be the best answer; do not demote it just because it is table-like.\n"
            + "9. Return JSON only, no extra text.\n"
            + "10. Output fields: ranked_candidates (array, max {{top_n}} items).\n"
            + "11. Each ranked candidate object should include: viz_type, chart_name, score, reason, matched_signals, missing_signals.\n"
            + "12. Also include backward-compatible fields: viz_type, chart_name, alternatives.\n"
            + "#SelectionPayload: {{selection_payload}}\n" + "#Response:";
    private static final Set<String> NUMBER_TYPES =
            Stream.of("INT", "INTEGER", "BIGINT", "LONG", "FLOAT", "DOUBLE", "DECIMAL", "NUMBER")
                    .collect(Collectors.toSet());
    private static final Set<String> DATE_TYPES =
            Stream.of("DATE", "DATETIME", "TIMESTAMP").collect(Collectors.toSet());
    private static final int MAX_SAMPLE_ROWS = 200;
    private static final int MAX_SAMPLE_VALUES = 5;

    public static String select(QueryResult queryResult) {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(false);
        return select(config, queryResult, null);
    }

    public static String select(SupersetPluginConfig config, QueryResult queryResult,
            String queryText) {
        return select(config, queryResult, queryText, null);
    }

    public static String select(SupersetPluginConfig config, QueryResult queryResult,
            String queryText, Agent agent) {
        List<VizTypeItem> candidates =
                selectCandidates(config, queryResult, queryText, agent, null, null, null);
        if (candidates.isEmpty()) {
            return DEFAULT_VIZ_TYPE;
        }
        return StringUtils.defaultIfBlank(candidates.get(0).getVizType(), DEFAULT_VIZ_TYPE);
    }

    public static List<VizTypeItem> selectCandidates(SupersetPluginConfig config,
            QueryResult queryResult, String queryText, Agent agent) {
        return selectCandidates(config, queryResult, queryText, agent, null, null, null);
    }

    public static List<VizTypeItem> selectCandidates(SupersetPluginConfig config,
            QueryResult queryResult, String queryText, Agent agent, String executedSql,
            SemanticParseInfo parseInfo, SupersetDatasetInfo datasetInfo) {
        if (queryResult == null || queryResult.getQueryColumns() == null) {
            return Collections.singletonList(resolveFallbackItem(DEFAULT_VIZ_TYPE, null));
        }
        List<QueryColumn> columns = queryResult.getQueryColumns();
        List<Map<String, Object>> results = queryResult.getQueryResults();
        if (columns.isEmpty()) {
            return Collections.singletonList(resolveFallbackItem(DEFAULT_VIZ_TYPE, null));
        }
        SupersetPluginConfig safeConfig = config == null ? new SupersetPluginConfig() : config;
        List<VizTypeItem> candidates =
                ensurePlainTableCandidate(filterCandidates(safeConfig, VIZTYPE_CATALOG.getItems()));

        DecisionContext decisionContext = buildDecisionContext(columns, results);
        SupersetVizSelectionPromptBuilder.SelectionSignals selectionSignals =
                toSelectionSignals(decisionContext);
        SupersetVizSelectionPromptBuilder.IntentProfile intentProfile =
                SupersetVizSelectionPromptBuilder.buildIntentProfile(queryText, executedSql,
                        parseInfo, selectionSignals);
        candidates = prefilterCandidates(queryResult, decisionContext, datasetInfo, candidates);
        int topN = resolveTopN(safeConfig);
        List<String> ordered = new ArrayList<>();

        Map<String, String> nameOverrides = Collections.emptyMap();
        if (safeConfig.isVizTypeLlmEnabled()) {
            Optional<LlmSelection> llmSelection =
                    selectByLlmCandidates(safeConfig, queryResult, queryText, candidates,
                            decisionContext, agent, executedSql, parseInfo, datasetInfo);
            if (llmSelection.isPresent()) {
                LlmSelection selection = llmSelection.get();
                List<String> guarded =
                        guardLlmCandidates(selection.getOrdered(), decisionContext, candidates);
                guarded = applyIntentOverrides(guarded, intentProfile, candidates, topN);
                addOrderedCandidates(ordered, guarded, topN);
                if (!selection.getNames().isEmpty()) {
                    nameOverrides = selection.getNames();
                }
            }
        }

        if (ordered.size() < topN) {
            Optional<String> heuristicChoice =
                    selectByHeuristics(decisionContext, results, candidates);
            heuristicChoice.ifPresent(choice -> addOrderedCandidate(ordered, choice, topN));
        }

        if (ordered.size() < topN) {
            Optional<String> hardChoice = selectByHardRules(decisionContext, candidates);
            hardChoice.ifPresent(choice -> addOrderedCandidate(ordered, choice, topN));
        }

        List<String> expanded = expandCandidates(ordered, decisionContext, candidates);
        List<String> diversified = diversifyCandidates(expanded, candidates, topN);
        diversified = applyIntentOverrides(diversified, intentProfile, candidates, topN);
        if (diversified.isEmpty()) {
            String fallback = resolveFallback(candidates, decisionContext);
            addOrderedCandidate(diversified, fallback, topN);
        }

        List<VizTypeItem> resolved = resolveCandidateItems(diversified, candidates, nameOverrides);
        if (resolved.isEmpty()) {
            return Collections.singletonList(resolveFallbackItem(DEFAULT_VIZ_TYPE, candidates));
        }
        return ensureTableCandidate(resolved, candidates);
    }

    private static boolean isNumber(QueryColumn column) {
        if (column == null) {
            return false;
        }
        if ("NUMBER".equalsIgnoreCase(column.getShowType())) {
            return true;
        }
        String type = StringUtils.upperCase(column.getType());
        return StringUtils.isNotBlank(type) && NUMBER_TYPES.contains(type);
    }

    private static boolean isDate(QueryColumn column) {
        if (column == null) {
            return false;
        }
        if ("DATE".equalsIgnoreCase(column.getShowType())) {
            return true;
        }
        String type = StringUtils.upperCase(column.getType());
        return StringUtils.isNotBlank(type) && DATE_TYPES.contains(type);
    }

    private static boolean isCategory(QueryColumn column) {
        if (column == null) {
            return false;
        }
        return "CATEGORY".equalsIgnoreCase(column.getShowType());
    }

    private static Optional<String> selectByHardRules(DecisionContext context,
            List<VizTypeItem> candidates) {
        if (context == null) {
            return Optional.empty();
        }
        if (context.getMetricCount() == 1 && context.getColumnCount() == 1
                && context.getRowCount() == 1) {
            return resolveCandidateByName("Big Number", candidates)
                    .or(() -> resolveCandidateByName("Big Number with Trendline", candidates))
                    .or(() -> resolveCandidateByName("Big Number Total", candidates));
        }
        return Optional.empty();
    }

    private static List<String> resolvePreferredCandidates(DecisionContext context,
            List<VizTypeItem> candidates) {
        if (context == null || candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> preferred = new ArrayList<>();
        if (context.getMetricCount() == 1 && context.getColumnCount() == 1
                && context.getRowCount() == 1) {
            preferred.add("Big Number");
            preferred.add("Big Number with Trendline");
            preferred.add("Big Number Total");
        } else if (context.getTimeCount() >= 1 && context.getMetricCount() > 1) {
            preferred.add("Mixed Chart");
            preferred.add("Line Chart");
            preferred.add("Smooth Line");
            preferred.add("Area Chart");
            preferred.add("Generic Chart");
        } else if (context.getTimeCount() >= 1 && context.getMetricCount() >= 1) {
            preferred.add("Line Chart");
            preferred.add("Smooth Line");
            preferred.add("Area Chart");
            preferred.add("Generic Chart");
            preferred.add("Mixed Chart");
        } else if (context.getDimensionCount() >= 1 && context.getMetricCount() == 1) {
            preferred.add("Bar Chart");
            preferred.add("Pie Chart");
            preferred.add("Partition Chart");
        } else if (context.getDimensionCount() >= 1 && context.getMetricCount() > 1) {
            preferred.add("Pivot Table");
        }
        return resolveCandidates(preferred, candidates);
    }

    private static List<String> expandCandidates(List<String> ordered, DecisionContext context,
            List<VizTypeItem> candidates) {
        List<String> expanded = new ArrayList<>();
        addOrderedCandidates(expanded, ordered, Integer.MAX_VALUE);
        addOrderedCandidates(expanded, resolvePreferredCandidates(context, candidates),
                Integer.MAX_VALUE);
        if (candidates != null) {
            for (VizTypeItem item : candidates) {
                if (item == null || StringUtils.isBlank(item.getVizType())
                        || isTableCandidate(item)) {
                    continue;
                }
                addOrderedCandidate(expanded, item.getVizType(), Integer.MAX_VALUE);
            }
        }
        return expanded;
    }

    private static List<String> diversifyCandidates(List<String> ordered,
            List<VizTypeItem> candidates, int limit) {
        if (ordered == null || ordered.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> diversified = new ArrayList<>();
        String anchor = null;
        for (String candidate : ordered) {
            if (StringUtils.isNotBlank(candidate)) {
                anchor = candidate;
                break;
            }
        }
        if (StringUtils.isBlank(anchor)) {
            addOrderedCandidates(diversified, ordered, limit);
            return diversified;
        }
        addOrderedCandidate(diversified, anchor, limit);
        String anchorCategory = resolveCategoryByVizType(anchor, candidates);
        String anchorProfile = resolveProfileByVizType(anchor, candidates);
        for (String candidate : ordered) {
            if (diversified.size() >= limit) {
                break;
            }
            if (StringUtils.isBlank(candidate) || StringUtils.equalsIgnoreCase(candidate, anchor)) {
                continue;
            }
            if (isRelatedCandidate(candidate, anchorCategory, anchorProfile, candidates)) {
                addOrderedCandidate(diversified, candidate, limit);
            }
        }
        for (String candidate : ordered) {
            if (diversified.size() >= limit) {
                break;
            }
            addOrderedCandidate(diversified, candidate, limit);
        }
        return diversified;
    }

    private static String resolveCategoryByVizType(String vizType, List<VizTypeItem> candidates) {
        if (StringUtils.isBlank(vizType)) {
            return null;
        }
        if (candidates != null) {
            for (VizTypeItem item : candidates) {
                if (item != null && StringUtils.equalsIgnoreCase(item.getVizType(), vizType)) {
                    return item.getCategory();
                }
            }
        }
        return resolveCategory(vizType);
    }

    private static String resolveProfileByVizType(String vizType, List<VizTypeItem> candidates) {
        if (StringUtils.isBlank(vizType)) {
            return null;
        }
        VizTypeItem item = resolveItemByVizType(vizType, candidates);
        if (item == null || item.getFormDataRules() == null) {
            return null;
        }
        return item.getFormDataRules().getProfile();
    }

    private static boolean isRelatedCandidate(String vizType, String anchorCategory,
            String anchorProfile, List<VizTypeItem> candidates) {
        if (StringUtils.isBlank(vizType)) {
            return false;
        }
        if (StringUtils.isNotBlank(anchorProfile)) {
            String profile = resolveProfileByVizType(vizType, candidates);
            if (StringUtils.isNotBlank(profile)
                    && StringUtils.equalsIgnoreCase(anchorProfile, profile)) {
                return true;
            }
        }
        if (StringUtils.isNotBlank(anchorCategory)) {
            String category = resolveCategoryByVizType(vizType, candidates);
            return StringUtils.isNotBlank(category)
                    && StringUtils.equalsIgnoreCase(anchorCategory, category);
        }
        return false;
    }

    private static Optional<String> selectByHeuristics(DecisionContext context,
            List<Map<String, Object>> results, List<VizTypeItem> candidates) {
        if (context == null) {
            return Optional.empty();
        }
        int metricCount = context.getMetricCount();
        int timeCount = context.getTimeCount();
        int dimensionCount = context.getDimensionCount();
        if (timeCount >= 1 && metricCount >= 1 && dimensionCount <= 1) {
            return resolveCandidateByName("Line Chart", candidates)
                    .or(() -> resolveCandidateByName("Smooth Line", candidates))
                    .or(() -> resolveCandidateByName("Area Chart", candidates))
                    .or(() -> resolveCandidateByName("Generic Chart", candidates));
        }

        if (timeCount >= 1 && metricCount > 1) {
            return resolveCandidateByName("Mixed Chart", candidates)
                    .or(() -> resolveCandidateByName("Generic Chart", candidates))
                    .or(() -> resolveCandidateByName("Line Chart", candidates));
        }

        if (dimensionCount >= 1 && metricCount == 1) {
            if (results != null && !results.isEmpty() && results.size() <= 10) {
                return resolveCandidateByName("Pie Chart", candidates)
                        .or(() -> resolveCandidateByName("Partition Chart", candidates))
                        .or(() -> resolveCandidateByName("Bar Chart", candidates));
            }
            return resolveCandidateByName("Bar Chart", candidates);
        }

        if (dimensionCount >= 1 && metricCount > 1) {
            return resolveCandidateByName("Pivot Table", candidates);
        }

        return Optional.empty();
    }

    private static Optional<LlmSelection> selectByLlmCandidates(SupersetPluginConfig config,
            QueryResult queryResult, String queryText, List<VizTypeItem> candidates,
            DecisionContext decisionContext, Agent agent, String executedSql,
            SemanticParseInfo parseInfo, SupersetDatasetInfo datasetInfo) {
        Integer chatModelId = resolveChatModelId(config, agent);
        if (chatModelId == null) {
            throw new IllegalStateException("superset viztype llm chat model id missing");
        }
        ChatModelConfig chatModelConfig = resolveChatModelConfig(chatModelId);
        if (chatModelConfig == null) {
            throw new IllegalStateException("superset viztype llm chat model config missing");
        }
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        int topN = resolveTopN(config);
        String promptText = StringUtils.defaultIfBlank(config.getVizTypeLlmPrompt(), LLM_PROMPT);
        String selectionPayload = SupersetVizSelectionPromptBuilder.buildJsonPayload(queryText,
                executedSql, queryResult, parseInfo, datasetInfo,
                toSelectionSignals(decisionContext), candidates);
        Map<String, Object> variables = new HashMap<>();
        variables.put("selection_payload", selectionPayload);
        variables.put("top_n", topN);

        Prompt prompt = PromptTemplate.from(promptText).apply(variables);
        ChatLanguageModel chatLanguageModel = ModelProvider.getChatModel(chatModelConfig);
        Response<AiMessage> response = chatLanguageModel.generate(prompt.toUserMessage());
        String answer =
                response == null || response.content() == null ? null : response.content().text();
        log.info("superset viztype llm req:\n{} \nresp:\n{}", prompt.text(), answer);
        LlmSelection resolved =
                resolveLlmSelectionFromModelResponse(answer, candidates, resolveTopN(config));
        return resolved == null || resolved.getOrdered().isEmpty() ? Optional.empty()
                : Optional.of(resolved);
    }

    static Integer resolveChatModelId(SupersetPluginConfig config, Agent agent) {
        if (config != null && config.getVizTypeLlmChatModelId() != null) {
            return config.getVizTypeLlmChatModelId();
        }
        return resolveAgentChatModelId(agent);
    }

    private static Integer resolveAgentChatModelId(Agent agent) {
        if (agent == null) {
            return null;
        }
        Map<String, ChatApp> chatAppConfig = agent.getChatAppConfig();
        if (chatAppConfig == null || chatAppConfig.isEmpty()) {
            return null;
        }
        Integer enabledChatModelId = resolveChatModelIdFromApps(chatAppConfig, true);
        if (enabledChatModelId != null) {
            return enabledChatModelId;
        }
        return resolveChatModelIdFromApps(chatAppConfig, false);
    }

    private static Integer resolveChatModelIdFromApps(Map<String, ChatApp> chatAppConfig,
            boolean enabledOnly) {
        if (chatAppConfig == null || chatAppConfig.isEmpty()) {
            return null;
        }
        ChatApp preferred = chatAppConfig.get("S2SQL_PARSER");
        if (isChatAppUsable(preferred, enabledOnly)) {
            return preferred.getChatModelId();
        }
        List<String> keys = new ArrayList<>(chatAppConfig.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            ChatApp app = chatAppConfig.get(key);
            if (isChatAppUsable(app, enabledOnly)) {
                return app.getChatModelId();
            }
        }
        return null;
    }

    private static boolean isChatAppUsable(ChatApp app, boolean enabledOnly) {
        if (app == null || app.getChatModelId() == null) {
            return false;
        }
        return !enabledOnly || app.isEnable();
    }

    private static ChatModelConfig resolveChatModelConfig(Integer chatModelId) {
        if (chatModelId == null) {
            return null;
        }
        ChatModelService chatModelService = ContextUtils.getBean(ChatModelService.class);
        if (chatModelService == null) {
            return null;
        }
        ChatModel chatModel = chatModelService.getChatModel(chatModelId);
        return chatModel == null ? null : chatModel.getConfig();
    }

    static String resolveFromModelResponse(String response, List<VizTypeItem> candidates) {
        LlmSelection resolved =
                resolveLlmSelectionFromModelResponse(response, candidates, resolveTopN(null));
        return resolved == null || resolved.getOrdered().isEmpty() ? null
                : resolved.getOrdered().get(0);
    }

    static List<String> resolveCandidatesFromModelResponse(String response,
            List<VizTypeItem> candidates) {
        LlmSelection resolved =
                resolveLlmSelectionFromModelResponse(response, candidates, resolveTopN(null));
        return resolved == null ? Collections.emptyList() : resolved.getOrdered();
    }

    private static LlmSelection resolveLlmSelectionFromModelResponse(String response,
            List<VizTypeItem> candidates) {
        return resolveLlmSelectionFromModelResponse(response, candidates, resolveTopN(null));
    }

    private static LlmSelection resolveLlmSelectionFromModelResponse(String response,
            List<VizTypeItem> candidates, int maxCandidates) {
        int resolvedMaxCandidates = maxCandidates <= 0 ? resolveTopN(null) : maxCandidates;
        if (StringUtils.isBlank(response) || candidates == null || candidates.isEmpty()) {
            return null;
        }
        String payload = extractJsonPayload(response);
        if (StringUtils.isBlank(payload)) {
            return null;
        }
        JSONObject json;
        try {
            json = JSONObject.parseObject(payload);
        } catch (Exception ex) {
            log.warn("superset viztype llm response parse failed", ex);
            return null;
        }
        List<LlmChoice> ordered = new ArrayList<>();
        JSONArray rankedCandidates = json.getJSONArray("ranked_candidates");
        if (rankedCandidates == null) {
            rankedCandidates = json.getJSONArray("rankedCandidates");
        }
        if (rankedCandidates != null) {
            ordered.addAll(resolveChoices(rankedCandidates));
        }
        String primary =
                StringUtils.defaultIfBlank(json.getString("viz_type"), json.getString("vizType"));
        String primaryName = resolveChartName(json);
        if (StringUtils.isNotBlank(primary)) {
            ordered.add(new LlmChoice(primary, primaryName));
        }
        JSONArray alternatives = json.getJSONArray("alternatives");
        ordered.addAll(resolveChoices(alternatives));
        return resolveLlmSelection(ordered, candidates, resolvedMaxCandidates);
    }

    private static List<LlmChoice> resolveChoices(JSONArray values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<LlmChoice> ordered = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
                continue;
            }
            if (value instanceof JSONObject) {
                JSONObject item = (JSONObject) value;
                String vizType = StringUtils.defaultIfBlank(item.getString("viz_type"),
                        item.getString("vizType"));
                String name = resolveChartName(item);
                if (StringUtils.isNotBlank(vizType)) {
                    ordered.add(new LlmChoice(vizType, name));
                }
            } else {
                String vizType = String.valueOf(value);
                if (StringUtils.isNotBlank(vizType)) {
                    ordered.add(new LlmChoice(vizType, null));
                }
            }
        }
        return ordered;
    }

    private static String resolveChartName(JSONObject json) {
        if (json == null) {
            return null;
        }
        String name = json.getString("chart_name");
        if (StringUtils.isBlank(name)) {
            name = json.getString("chartName");
        }
        if (StringUtils.isBlank(name)) {
            name = json.getString("name");
        }
        if (StringUtils.isBlank(name)) {
            name = json.getString("title");
        }
        return StringUtils.trimToNull(name);
    }

    private static LlmSelection resolveLlmSelection(List<LlmChoice> ordered,
            List<VizTypeItem> candidates) {
        return resolveLlmSelection(ordered, candidates, resolveTopN(null));
    }

    private static LlmSelection resolveLlmSelection(List<LlmChoice> ordered,
            List<VizTypeItem> candidates, int maxCandidates) {
        if (ordered == null || ordered.isEmpty()) {
            return null;
        }
        int resolvedMaxCandidates = maxCandidates <= 0 ? resolveTopN(null) : maxCandidates;
        Map<String, String> lookup = buildLookup(candidates);
        List<String> resolved = new ArrayList<>();
        Map<String, String> names = new HashMap<>();
        for (LlmChoice choice : ordered) {
            if (choice == null || StringUtils.isBlank(choice.getVizType())) {
                continue;
            }
            String resolvedValue = resolveCandidateValue(choice.getVizType(), candidates, lookup);
            if (StringUtils.isBlank(resolvedValue) || resolved.contains(resolvedValue)) {
                continue;
            }
            resolved.add(resolvedValue);
            if (StringUtils.isNotBlank(choice.getChartName())
                    && !names.containsKey(resolvedValue)) {
                names.put(resolvedValue, choice.getChartName());
            }
            if (resolved.size() >= resolvedMaxCandidates) {
                break;
            }
        }
        if (resolved.isEmpty()) {
            return null;
        }
        return new LlmSelection(resolved, names);
    }

    private static List<String> resolveCandidates(List<String> ordered,
            List<VizTypeItem> candidates) {
        if (ordered == null || ordered.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, String> lookup = buildLookup(candidates);
        List<String> resolved = new ArrayList<>();
        for (String candidate : ordered) {
            String resolvedValue = resolveCandidateValue(candidate, candidates, lookup);
            if (StringUtils.isNotBlank(resolvedValue) && !resolved.contains(resolvedValue)) {
                resolved.add(resolvedValue);
            }
        }
        return resolved;
    }

    private static String resolveCandidate(List<String> ordered, List<VizTypeItem> candidates) {
        if (ordered == null || ordered.isEmpty()) {
            return null;
        }
        List<String> resolved = resolveCandidates(ordered, candidates);
        return resolved.isEmpty() ? null : resolved.get(0);
    }

    private static Map<String, String> buildLookup(List<VizTypeItem> candidates) {
        Map<String, String> lookup = new HashMap<>();
        for (VizTypeItem item : candidates) {
            if (StringUtils.isNotBlank(item.getVizType())) {
                lookup.put(normalize(item.getVizType()), item.getVizType());
            }
            if (StringUtils.isNotBlank(item.getName())) {
                lookup.put(normalize(item.getName()), item.getVizType());
            }
            if (StringUtils.isNotBlank(item.getVizKey())) {
                lookup.put(normalize(item.getVizKey()), item.getVizType());
            }
        }
        return lookup;
    }

    private static String resolveCandidateValue(String value, List<VizTypeItem> candidates,
            Map<String, String> lookup) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        String normalized = normalize(value);
        if (lookup.containsKey(normalized)) {
            return lookup.get(normalized);
        }
        int leftParen = normalized.indexOf('(');
        int rightParen = normalized.indexOf(')');
        if (leftParen >= 0 && rightParen > leftParen) {
            String inner = normalized.substring(leftParen + 1, rightParen).trim();
            String direct = lookup.get(inner);
            if (StringUtils.isNotBlank(direct)) {
                return direct;
            }
        }
        for (VizTypeItem item : candidates) {
            if (item == null) {
                continue;
            }
            String vizType = item.getVizType();
            String name = item.getName();
            if (StringUtils.isNotBlank(vizType) && normalized.contains(normalize(vizType))) {
                return vizType;
            }
            if (StringUtils.isNotBlank(name) && normalized.contains(normalize(name))) {
                return item.getVizType();
            }
        }
        return null;
    }

    private static String normalize(String value) {
        return StringUtils.lowerCase(StringUtils.trimToEmpty(value), Locale.ROOT);
    }

    private static String extractJsonPayload(String response) {
        int start = response.indexOf('{');
        int end = response.lastIndexOf('}');
        if (start < 0 || end <= start) {
            return null;
        }
        return response.substring(start, end + 1);
    }

    private static String buildDataProfile(QueryResult queryResult) {
        Map<String, Object> profile = new LinkedHashMap<>();
        List<QueryColumn> columns =
                queryResult == null ? Collections.emptyList() : queryResult.getQueryColumns();
        List<Map<String, Object>> rows =
                queryResult == null ? Collections.emptyList() : queryResult.getQueryResults();
        List<Map<String, Object>> sampleRows = sampleRows(rows);
        profile.put("rowCount", sampleRows.size());
        if (columns != null && !columns.isEmpty()) {
            List<Map<String, Object>> columnProfiles = new ArrayList<>();
            List<Map<String, Object>> normalizedRows = normalizeRows(sampleRows);
            for (QueryColumn column : columns) {
                columnProfiles.add(buildColumnProfile(column, normalizedRows));
            }
            profile.put("columns", columnProfiles);
        }
        if (queryResult != null && queryResult.getAggregateInfo() != null
                && queryResult.getAggregateInfo().getMetricInfos() != null
                && !queryResult.getAggregateInfo().getMetricInfos().isEmpty()) {
            profile.put("aggregateInfo", queryResult.getAggregateInfo().getMetricInfos());
        }
        return JsonUtil.toString(profile);
    }

    private static DecisionContext buildDecisionContext(List<QueryColumn> columns,
            List<Map<String, Object>> rows) {
        DecisionContext context = new DecisionContext();
        context.setColumnCount(columns == null ? 0 : columns.size());
        List<Map<String, Object>> sampleRows = sampleRows(rows);
        context.setRowCount(sampleRows.size());
        if (columns == null || columns.isEmpty()) {
            return context;
        }
        List<Map<String, Object>> normalizedRows = normalizeRows(sampleRows);
        int metricCount = 0;
        int timeCount = 0;
        int dimensionCount = 0;
        boolean inferred = false;
        boolean timeGrainDetected = false;
        for (QueryColumn column : columns) {
            ColumnSignals signals = buildColumnSignals(column, normalizedRows);
            inferred = inferred || signals.isInferred();
            timeGrainDetected = timeGrainDetected || signals.isTimeGrainDetected();
            if (signals.isTime()) {
                timeCount++;
                continue;
            }
            if (signals.isMetric()) {
                metricCount++;
                continue;
            }
            dimensionCount++;
        }
        context.setMetricCount(metricCount);
        context.setTimeCount(timeCount);
        context.setDimensionCount(dimensionCount);
        context.setInferredFromValues(inferred);
        context.setTimeGrainDetected(timeGrainDetected);
        return context;
    }

    private static ColumnSignals buildColumnSignals(QueryColumn column,
            List<Map<String, Object>> rows) {
        ColumnSignals signals = new ColumnSignals();
        if (column == null) {
            return signals;
        }
        boolean category = isCategory(column);
        boolean time = isDate(column);
        boolean metric = !time && !category && isNumber(column);
        String[] keys = resolveKeys(column);
        List<Object> values = new ArrayList<>();
        int numericCount = 0;
        if (rows != null) {
            for (Map<String, Object> row : rows) {
                Object value = resolveValue(row, keys);
                if (value == null) {
                    continue;
                }
                values.add(value);
                if (!time) {
                    Double numeric = tryParseNumber(value);
                    if (numeric != null) {
                        numericCount++;
                    }
                }
            }
        }
        if (!time) {
            String grain = detectTimeGrain(values);
            if (StringUtils.isNotBlank(grain)) {
                time = true;
                signals.setTimeGrainDetected(true);
                signals.setInferred(true);
                metric = false;
            }
        }
        if (!time && !metric && !category && numericCount > 0) {
            metric = true;
            signals.setInferred(true);
        }
        signals.setTime(time);
        signals.setMetric(metric);
        return signals;
    }

    private static String buildDecisionSignals(DecisionContext context) {
        if (context == null) {
            return "{}";
        }
        Map<String, Object> signals = new LinkedHashMap<>();
        signals.put("columnCount", context.getColumnCount());
        signals.put("rowCount", context.getRowCount());
        signals.put("metricCount", context.getMetricCount());
        signals.put("timeCount", context.getTimeCount());
        signals.put("dimensionCount", context.getDimensionCount());
        signals.put("timeGrainDetected", context.isTimeGrainDetected());
        signals.put("inferredFromValues", context.isInferredFromValues());
        return JsonUtil.toString(signals);
    }

    private static SupersetVizSelectionPromptBuilder.SelectionSignals toSelectionSignals(
            DecisionContext context) {
        SupersetVizSelectionPromptBuilder.SelectionSignals signals =
                new SupersetVizSelectionPromptBuilder.SelectionSignals();
        if (context == null) {
            return signals;
        }
        signals.setColumnCount(context.getColumnCount());
        signals.setRowCount(context.getRowCount());
        signals.setMetricCount(context.getMetricCount());
        signals.setTimeCount(context.getTimeCount());
        signals.setDimensionCount(context.getDimensionCount());
        signals.setInferredFromValues(context.isInferredFromValues());
        signals.setTimeGrainDetected(context.isTimeGrainDetected());
        return signals;
    }

    private static List<VizTypeItem> prefilterCandidates(QueryResult queryResult,
            DecisionContext context, SupersetDatasetInfo datasetInfo,
            List<VizTypeItem> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }
        SelectionAvailability availability =
                buildSelectionAvailability(queryResult, context, datasetInfo);
        List<VizTypeItem> filtered =
                candidates.stream().filter(item -> isCandidateFeasible(item, availability))
                        .collect(Collectors.toList());
        return filtered.isEmpty() ? candidates : filtered;
    }

    private static boolean isCandidateFeasible(VizTypeItem item,
            SelectionAvailability availability) {
        if (item == null || isTableCandidate(item) || item.getSelectionSummary() == null) {
            return true;
        }
        List<SelectionSlot> requiredSlots = item.getSelectionSummary().getRequiredSlots();
        if (requiredSlots == null || requiredSlots.isEmpty()) {
            return true;
        }
        for (SelectionSlot slot : requiredSlots) {
            if (slot == null || !slot.isRequired()) {
                continue;
            }
            List<String> acceptedRoles = slot.getAcceptedDatasetRoles();
            if (acceptedRoles == null || acceptedRoles.isEmpty()) {
                continue;
            }
            boolean hasStrongRole =
                    acceptedRoles.stream().anyMatch(SelectionAvailability::isStrongRole);
            if (!hasStrongRole) {
                continue;
            }
            boolean matched = acceptedRoles.stream().anyMatch(availability::supportsRole);
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    private static SelectionAvailability buildSelectionAvailability(QueryResult queryResult,
            DecisionContext context, SupersetDatasetInfo datasetInfo) {
        SelectionAvailability availability = new SelectionAvailability();
        availability
                .setHasMetric(context != null && context.getMetricCount() > 0 || datasetInfo != null
                        && datasetInfo.getMetrics() != null && !datasetInfo.getMetrics().isEmpty());
        availability.setHasTime(context != null && context.getTimeCount() > 0 || datasetInfo != null
                && (StringUtils.isNotBlank(datasetInfo.getMainDttmCol()) || hasDatasetColumn(
                        datasetInfo, name -> Boolean.TRUE.equals(name.getIsDttm()))));
        availability.setHasDimension(context != null && context.getDimensionCount() > 0
                || datasetInfo != null && hasDatasetColumn(datasetInfo,
                        column -> Boolean.TRUE.equals(column.getGroupby())));
        List<String> names = collectFieldNames(queryResult, datasetInfo);
        availability.setHasLatitude(matchesAny(names, "latitude", "lat"));
        availability.setHasLongitude(matchesAny(names, "longitude", "lng", "lon"));
        availability.setHasRegion(matchesAny(names, "region", "country", "province", "state",
                "city", "area", "district"));
        availability.setHasSource(matchesAny(names, "source", "from", "src"));
        availability.setHasTarget(matchesAny(names, "target", "to", "dst", "dest"));
        availability.setHasStart(matchesAny(names, "start", "begin"));
        availability.setHasEnd(matchesAny(names, "end", "finish"));
        return availability;
    }

    private static List<String> collectFieldNames(QueryResult queryResult,
            SupersetDatasetInfo datasetInfo) {
        List<String> names = new ArrayList<>();
        if (queryResult != null && queryResult.getQueryColumns() != null) {
            for (QueryColumn column : queryResult.getQueryColumns()) {
                if (column == null) {
                    continue;
                }
                if (StringUtils.isNotBlank(column.getName())) {
                    names.add(normalize(column.getName()));
                }
                if (StringUtils.isNotBlank(column.getBizName())) {
                    names.add(normalize(column.getBizName()));
                }
            }
        }
        if (datasetInfo != null && datasetInfo.getColumns() != null) {
            for (com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn column : datasetInfo
                    .getColumns()) {
                if (column != null && StringUtils.isNotBlank(column.getColumnName())) {
                    names.add(normalize(column.getColumnName()));
                }
            }
        }
        return names;
    }

    private static boolean matchesAny(List<String> names, String... tokens) {
        if (names == null || names.isEmpty() || tokens == null) {
            return false;
        }
        for (String name : names) {
            for (String token : tokens) {
                if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(token)
                        && name.contains(token)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasDatasetColumn(SupersetDatasetInfo datasetInfo,
            java.util.function.Predicate<com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn> predicate) {
        if (datasetInfo == null || datasetInfo.getColumns() == null || predicate == null) {
            return false;
        }
        for (com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn column : datasetInfo
                .getColumns()) {
            if (column != null && predicate.test(column)) {
                return true;
            }
        }
        return false;
    }

    private static String guardLlmChoice(String llmChoice, DecisionContext context,
            List<VizTypeItem> candidates) {
        if (StringUtils.isBlank(llmChoice)) {
            return null;
        }
        if (shouldAvoidTable(context, candidates) && isTableVizType(llmChoice)) {
            String preferred = resolvePreferredCandidate(context, candidates);
            if (StringUtils.isNotBlank(preferred)) {
                return preferred;
            }
        }
        return llmChoice;
    }

    private static List<String> guardLlmCandidates(List<String> llmChoices, DecisionContext context,
            List<VizTypeItem> candidates) {
        if (llmChoices == null || llmChoices.isEmpty()) {
            return Collections.emptyList();
        }
        if (!shouldAvoidTable(context, candidates)) {
            return llmChoices;
        }
        List<String> filtered = llmChoices.stream().filter(StringUtils::isNotBlank)
                .filter(choice -> !isTableVizType(choice)).collect(Collectors.toList());
        return filtered.isEmpty() ? llmChoices : filtered;
    }

    private static String resolvePreferredCandidate(DecisionContext context,
            List<VizTypeItem> candidates) {
        if (context == null) {
            return null;
        }
        int metricCount = context.getMetricCount();
        int timeCount = context.getTimeCount();
        int dimensionCount = context.getDimensionCount();
        List<String> preferred = new ArrayList<>();
        if (timeCount >= 1 && metricCount >= 1) {
            preferred.add("Line Chart");
            preferred.add("Smooth Line");
            preferred.add("Area Chart");
            preferred.add("Generic Chart");
            preferred.add("Mixed Chart");
        } else if (dimensionCount >= 1 && metricCount == 1) {
            preferred.add("Bar Chart");
            preferred.add("Pie Chart");
            preferred.add("Partition Chart");
        }
        return resolveCandidate(preferred, candidates);
    }

    private static int resolveTopN(SupersetPluginConfig config) {
        int topN = config == null || config.getVizTypeLlmTopN() == null ? 3
                : config.getVizTypeLlmTopN();
        if (topN <= 0) {
            topN = 1;
        }
        return Math.min(3, topN);
    }

    private static void addOrderedCandidates(List<String> ordered, List<String> candidates,
            int limit) {
        if (candidates == null || candidates.isEmpty()) {
            return;
        }
        for (String candidate : candidates) {
            addOrderedCandidate(ordered, candidate, limit);
        }
    }

    private static void addOrderedCandidate(List<String> ordered, String candidate, int limit) {
        if (StringUtils.isBlank(candidate) || ordered.size() >= limit) {
            return;
        }
        if (!ordered.contains(candidate)) {
            ordered.add(candidate);
        }
    }

    private static List<VizTypeItem> resolveCandidateItems(List<String> ordered,
            List<VizTypeItem> candidates, Map<String, String> nameOverrides) {
        if (ordered == null || ordered.isEmpty()) {
            return Collections.emptyList();
        }
        List<VizTypeItem> resolved = new ArrayList<>();
        for (String candidate : ordered) {
            VizTypeItem item = resolveItemByVizType(candidate, candidates);
            if (item != null && StringUtils.isNotBlank(item.getVizType())
                    && resolved.stream().noneMatch(existing -> StringUtils
                            .equalsIgnoreCase(existing.getVizType(), item.getVizType()))) {
                VizTypeItem resolvedItem = item;
                String override =
                        nameOverrides == null ? null : nameOverrides.get(item.getVizType());
                if (StringUtils.isNotBlank(override)) {
                    resolvedItem = cloneItem(item);
                    resolvedItem.setLlmName(override);
                }
                resolved.add(resolvedItem);
            }
        }
        return resolved;
    }

    private static VizTypeItem cloneItem(VizTypeItem item) {
        if (item == null) {
            return null;
        }
        VizTypeItem clone = new VizTypeItem();
        clone.setVizKey(item.getVizKey());
        clone.setVizType(item.getVizType());
        clone.setName(item.getName());
        clone.setCategory(item.getCategory());
        clone.setDescription(item.getDescription());
        clone.setSourcePath(item.getSourcePath());
        clone.setFormDataRules(item.getFormDataRules());
        clone.setSelectionSummary(item.getSelectionSummary());
        clone.setSelectionPromptCard(item.getSelectionPromptCard());
        return clone;
    }

    private static List<VizTypeItem> ensureTableCandidate(List<VizTypeItem> resolved,
            List<VizTypeItem> candidates) {
        if (resolved == null || resolved.isEmpty()) {
            return resolved;
        }
        for (VizTypeItem item : resolved) {
            if (item != null && StringUtils.equalsIgnoreCase(DEFAULT_VIZ_TYPE, item.getVizType())) {
                return resolved;
            }
        }
        List<VizTypeItem> withTable = new ArrayList<>(resolved);
        VizTypeItem table = resolveFallbackItem(DEFAULT_VIZ_TYPE, candidates);
        if (table != null) {
            withTable.add(table);
        }
        return withTable;
    }

    private static List<VizTypeItem> ensurePlainTableCandidate(List<VizTypeItem> candidates) {
        List<VizTypeItem> safeCandidates =
                candidates == null ? new ArrayList<>() : new ArrayList<>(candidates);
        for (VizTypeItem item : safeCandidates) {
            if (item != null && StringUtils.equalsIgnoreCase(DEFAULT_VIZ_TYPE, item.getVizType())) {
                return safeCandidates;
            }
        }
        VizTypeItem table = resolveFallbackItem(DEFAULT_VIZ_TYPE, null);
        if (table != null) {
            safeCandidates.add(table);
        }
        return safeCandidates;
    }

    private static List<String> applyIntentOverrides(List<String> ordered,
            SupersetVizSelectionPromptBuilder.IntentProfile intentProfile,
            List<VizTypeItem> candidates, int limit) {
        if (ordered == null) {
            return Collections.emptyList();
        }
        if (intentProfile == null || !StringUtils.equalsIgnoreCase(DEFAULT_VIZ_TYPE,
                intentProfile.getPreferredTableVizType())) {
            return ordered;
        }
        String table = resolveCandidate(Collections.singletonList(DEFAULT_VIZ_TYPE), candidates);
        if (StringUtils.isBlank(table)) {
            return ordered;
        }
        List<String> overridden = new ArrayList<>();
        addOrderedCandidate(overridden, table, limit);
        addOrderedCandidates(overridden, ordered, limit);
        return overridden;
    }

    public static VizTypeItem resolveItemByVizType(String vizType, List<VizTypeItem> candidates) {
        if (StringUtils.isBlank(vizType)) {
            return null;
        }
        List<VizTypeItem> safeCandidates =
                candidates == null ? VIZTYPE_CATALOG.getItems() : candidates;
        if (safeCandidates != null) {
            for (VizTypeItem item : safeCandidates) {
                if (item != null && StringUtils
                        .equalsIgnoreCase(StringUtils.trimToEmpty(item.getVizType()), vizType)) {
                    return item;
                }
            }
        }
        VizTypeItem fallback = new VizTypeItem();
        fallback.setVizType(vizType);
        fallback.setName(vizType);
        return fallback;
    }

    public static String resolveCategory(String vizType) {
        if (StringUtils.isBlank(vizType)) {
            return null;
        }
        List<VizTypeItem> items = VIZTYPE_CATALOG.getItems();
        if (items == null || items.isEmpty()) {
            return null;
        }
        for (VizTypeItem item : items) {
            if (item != null && StringUtils.equalsIgnoreCase(item.getVizType(), vizType)) {
                return item.getCategory();
            }
        }
        return null;
    }

    private static VizTypeItem resolveFallbackItem(String vizType, List<VizTypeItem> candidates) {
        VizTypeItem resolved = resolveItemByVizType(vizType, candidates);
        if (resolved != null) {
            return resolved;
        }
        VizTypeItem fallback = new VizTypeItem();
        fallback.setVizType(vizType);
        fallback.setName(vizType);
        return fallback;
    }

    private static boolean shouldAvoidTable(DecisionContext context, List<VizTypeItem> candidates) {
        if (context == null || !hasNonTableCandidate(candidates)) {
            return false;
        }
        if (isDetailLikeContext(context)) {
            return false;
        }
        if (context.getTimeCount() >= 1 && context.getMetricCount() >= 1) {
            return true;
        }
        return context.getMetricCount() == 1 && context.getDimensionCount() >= 1;
    }

    private static boolean isDetailLikeContext(DecisionContext context) {
        if (context == null) {
            return false;
        }
        return context.getColumnCount() >= 4 || context.getDimensionCount() >= 2;
    }

    private static boolean hasNonTableCandidate(List<VizTypeItem> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return false;
        }
        for (VizTypeItem item : candidates) {
            if (!isTableCandidate(item)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isTableCandidate(VizTypeItem item) {
        if (item == null) {
            return false;
        }
        return isTableVizType(item.getVizType()) || isTableVizType(item.getName())
                || isTableVizType(item.getVizKey());
    }

    private static boolean isTableVizType(String value) {
        return StringUtils.isNotBlank(value) && normalize(value).contains("table");
    }

    private static List<Map<String, Object>> sampleRows(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        if (rows.size() <= MAX_SAMPLE_ROWS) {
            return rows;
        }
        return rows.subList(0, MAX_SAMPLE_ROWS);
    }

    private static List<Map<String, Object>> normalizeRows(List<Map<String, Object>> rows) {
        List<Map<String, Object>> normalized = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> normalizedRow = new HashMap<>();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (entry.getKey() != null) {
                    normalizedRow.put(normalize(entry.getKey()), entry.getValue());
                }
            }
            normalized.add(normalizedRow);
        }
        return normalized;
    }

    private static Map<String, Object> buildColumnProfile(QueryColumn column,
            List<Map<String, Object>> rows) {
        Map<String, Object> profile = new LinkedHashMap<>();
        if (column == null) {
            return profile;
        }
        profile.put("name", column.getName());
        profile.put("bizName", column.getBizName());
        profile.put("type", column.getType());
        profile.put("showType", column.getShowType());
        profile.put("role", resolveRole(column));
        String[] keys = resolveKeys(column);
        List<Object> values = new ArrayList<>();
        int nullCount = 0;
        Map<String, Long> frequency = new HashMap<>();
        DoubleSummaryStatistics numericStats = new DoubleSummaryStatistics();
        for (Map<String, Object> row : rows) {
            Object value = resolveValue(row, keys);
            if (value == null) {
                nullCount++;
                continue;
            }
            values.add(value);
            String stringValue = String.valueOf(value);
            frequency.put(stringValue, frequency.getOrDefault(stringValue, 0L) + 1L);
            Double numeric = tryParseNumber(value);
            if (numeric != null) {
                numericStats.accept(numeric);
            }
        }
        int sampleSize = rows == null ? 0 : rows.size();
        profile.put("nullRate", sampleSize == 0 ? 0 : (double) nullCount / sampleSize);
        profile.put("distinctCount", frequency.size());
        profile.put("sampleValues", values.stream().filter(Objects::nonNull)
                .limit(MAX_SAMPLE_VALUES).collect(Collectors.toList()));
        if (numericStats.getCount() > 0) {
            Map<String, Object> numericSummary = new LinkedHashMap<>();
            numericSummary.put("min", numericStats.getMin());
            numericSummary.put("max", numericStats.getMax());
            numericSummary.put("avg", numericStats.getAverage());
            profile.put("numericStats", numericSummary);
        }
        String grain = detectTimeGrain(values);
        if (StringUtils.isNotBlank(grain)) {
            profile.put("timeGrain", grain);
        }
        if (!frequency.isEmpty()) {
            List<Map<String, Object>> topValues = frequency.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(MAX_SAMPLE_VALUES).map(entry -> {
                        Map<String, Object> item = new LinkedHashMap<>();
                        item.put("value", entry.getKey());
                        item.put("count", entry.getValue());
                        return item;
                    }).collect(Collectors.toList());
            profile.put("topValues", topValues);
        }
        return profile;
    }

    private static String resolveRole(QueryColumn column) {
        if (column == null) {
            return "UNKNOWN";
        }
        if (isDate(column)) {
            return "TIME";
        }
        if (isNumber(column)) {
            return "METRIC";
        }
        return "DIMENSION";
    }

    private static String[] resolveKeys(QueryColumn column) {
        return new String[] {normalize(column.getName()), normalize(column.getBizName()),
                        normalize(column.getNameEn())};
    }

    private static Object resolveValue(Map<String, Object> row, String[] keys) {
        if (row == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            if (StringUtils.isBlank(key)) {
                continue;
            }
            if (row.containsKey(key)) {
                return row.get(key);
            }
        }
        return null;
    }

    private static Double tryParseNumber(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception ex) {
            return null;
        }
    }

    private static String detectTimeGrain(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        TreeMap<Integer, String> grains = new TreeMap<>(Comparator.naturalOrder());
        for (Object value : values) {
            String grain = detectTimeGrain(value);
            if (StringUtils.isNotBlank(grain)) {
                grains.put(grainRank(grain), grain);
            }
        }
        if (grains.isEmpty()) {
            return null;
        }
        return grains.get(grains.lastKey());
    }

    private static String detectTimeGrain(Object value) {
        if (value == null) {
            return null;
        }
        String raw = String.valueOf(value).trim();
        if (raw.matches("^\\d{4}$")) {
            return "YEAR";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}$")) {
            return "MONTH";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}$")) {
            return "DAY";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}[ T]\\d{1,2}$")) {
            return "HOUR";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}[ T]\\d{1,2}:\\d{1,2}$")) {
            return "MINUTE";
        }
        if (raw.matches("^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}[ T]\\d{1,2}:\\d{1,2}:\\d{1,2}$")) {
            return "SECOND";
        }
        if (value instanceof Number) {
            String digits = String.valueOf(((Number) value).longValue());
            if (digits.length() == 10) {
                return "SECOND";
            }
            if (digits.length() == 13) {
                return "MILLISECOND";
            }
        }
        return null;
    }

    private static int grainRank(String grain) {
        if ("YEAR".equalsIgnoreCase(grain)) {
            return 1;
        }
        if ("MONTH".equalsIgnoreCase(grain)) {
            return 2;
        }
        if ("DAY".equalsIgnoreCase(grain)) {
            return 3;
        }
        if ("HOUR".equalsIgnoreCase(grain)) {
            return 4;
        }
        if ("MINUTE".equalsIgnoreCase(grain)) {
            return 5;
        }
        if ("SECOND".equalsIgnoreCase(grain)) {
            return 6;
        }
        if ("MILLISECOND".equalsIgnoreCase(grain)) {
            return 7;
        }
        return 0;
    }

    private static List<VizTypeItem> filterCandidates(SupersetPluginConfig config,
            List<VizTypeItem> items) {
        if (items == null) {
            return Collections.emptyList();
        }
        List<String> allowList = config.getVizTypeAllowList();
        List<String> denyList = config.getVizTypeDenyList();
        List<VizTypeItem> filtered = new ArrayList<>(items);
        if (allowList != null && !allowList.isEmpty()) {
            Set<String> allowNormalized = allowList.stream().filter(StringUtils::isNotBlank)
                    .map(SupersetVizTypeSelector::normalize).collect(Collectors.toSet());
            filtered = filtered.stream()
                    .filter(item -> allowNormalized.contains(normalize(item.getVizType()))
                            || allowNormalized.contains(normalize(item.getName()))
                            || allowNormalized.contains(normalize(item.getVizKey())))
                    .collect(Collectors.toList());
        }
        if (denyList != null && !denyList.isEmpty()) {
            Set<String> denyNormalized = denyList.stream().filter(StringUtils::isNotBlank)
                    .map(SupersetVizTypeSelector::normalize).collect(Collectors.toSet());
            filtered = filtered.stream()
                    .filter(item -> !denyNormalized.contains(normalize(item.getVizType()))
                            && !denyNormalized.contains(normalize(item.getName()))
                            && !denyNormalized.contains(normalize(item.getVizKey())))
                    .collect(Collectors.toList());
        }
        return filtered;
    }

    private static Optional<String> resolveCandidateByName(String name,
            List<VizTypeItem> candidates) {
        if (StringUtils.isBlank(name) || candidates == null) {
            return Optional.empty();
        }
        return candidates.stream().filter(item -> name.equalsIgnoreCase(item.getName()))
                .map(VizTypeItem::getVizType).filter(StringUtils::isNotBlank).findFirst();
    }

    private static String resolveFallback(List<VizTypeItem> candidates, DecisionContext context) {
        if (shouldAvoidTable(context, candidates)) {
            String preferred = resolvePreferredCandidate(context, candidates);
            if (StringUtils.isNotBlank(preferred)) {
                return preferred;
            }
            if (candidates != null) {
                for (VizTypeItem item : candidates) {
                    if (item != null && !isTableCandidate(item)
                            && StringUtils.isNotBlank(item.getVizType())) {
                        return item.getVizType();
                    }
                }
            }
        }
        String candidate =
                resolveCandidate(Collections.singletonList(DEFAULT_VIZ_TYPE), candidates);
        if (StringUtils.isNotBlank(candidate)) {
            return candidate;
        }
        return candidates == null || candidates.isEmpty() ? DEFAULT_VIZ_TYPE
                : StringUtils.defaultIfBlank(candidates.get(0).getVizType(), DEFAULT_VIZ_TYPE);
    }

    private static String buildCandidateSummary(List<VizTypeItem> candidates) {
        List<Map<String, Object>> summary = new ArrayList<>();
        for (VizTypeItem item : candidates) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("vizType", item.getVizType());
            entry.put("name", item.getName());
            entry.put("category", item.getCategory());
            entry.put("description", item.getDescription());
            summary.add(entry);
        }
        return JsonUtil.toString(summary);
    }

    @Data
    private static class DecisionContext {
        private int columnCount;
        private int rowCount;
        private int metricCount;
        private int timeCount;
        private int dimensionCount;
        private boolean inferredFromValues;
        private boolean timeGrainDetected;
    }

    @Data
    private static class ColumnSignals {
        private boolean metric;
        private boolean time;
        private boolean inferred;
        private boolean timeGrainDetected;
    }

    @Data
    private static class SelectionAvailability {
        private boolean hasMetric;
        private boolean hasTime;
        private boolean hasDimension;
        private boolean hasLatitude;
        private boolean hasLongitude;
        private boolean hasRegion;
        private boolean hasSource;
        private boolean hasTarget;
        private boolean hasStart;
        private boolean hasEnd;

        private boolean supportsRole(String role) {
            String normalized = normalize(role);
            switch (normalized) {
                case "metric":
                case "saved_metric":
                case "adhoc_metric":
                    return hasMetric;
                case "time_dimension":
                case "time_grain":
                case "start_time":
                case "end_time":
                    return hasTime;
                case "dimension":
                case "category_dimension":
                case "series_dimension":
                case "entity_dimension":
                    return hasDimension;
                case "latitude":
                    return hasLatitude;
                case "longitude":
                    return hasLongitude;
                case "region_dimension":
                    return hasRegion;
                case "source":
                case "source_dimension":
                    return hasSource;
                case "target":
                case "target_dimension":
                    return hasTarget;
                case "start":
                    return hasStart;
                case "end":
                    return hasEnd;
                case "filter_context":
                    return true;
                default:
                    return false;
            }
        }

        private static boolean isStrongRole(String role) {
            String normalized = normalize(role);
            return Arrays.asList("metric", "saved_metric", "adhoc_metric", "time_dimension",
                    "time_grain", "dimension", "category_dimension", "series_dimension",
                    "entity_dimension", "latitude", "longitude", "region_dimension", "source",
                    "source_dimension", "target", "target_dimension", "start", "end", "start_time",
                    "end_time").contains(normalized);
        }
    }

    private static class LlmChoice {
        private final String vizType;
        private final String chartName;

        private LlmChoice(String vizType, String chartName) {
            this.vizType = vizType;
            this.chartName = chartName;
        }

        private String getVizType() {
            return vizType;
        }

        private String getChartName() {
            return chartName;
        }
    }

    private static class LlmSelection {
        private final List<String> ordered;
        private final Map<String, String> names;

        private LlmSelection(List<String> ordered, Map<String, String> names) {
            this.ordered = ordered == null ? Collections.emptyList() : ordered;
            this.names = names == null ? Collections.emptyMap() : names;
        }

        private List<String> getOrdered() {
            return ordered;
        }

        private Map<String, String> getNames() {
            return names;
        }
    }

    @Data
    public static class VizTypeItem {
        private String vizKey;
        private String vizType;
        private String name;
        private String llmName;
        private String category;
        private String description;
        private String sourcePath;
        private FormDataRules formDataRules;
        private SelectionSummary selectionSummary;
        private Map<String, Object> selectionPromptCard = Collections.emptyMap();
    }

    @Data
    public static class FormDataRules {
        private String profile;
        private List<String> required = Collections.emptyList();
        private List<List<FormDataField>> requiredAnyOf = Collections.emptyList();
        private List<FormDataField> fields = Collections.emptyList();
        private List<FormDataSource> sources = Collections.emptyList();
    }

    @Data
    public static class FormDataField {
        private String key;
        private String type;
        private String description;
        private String source;
    }

    @Data
    public static class FormDataSource {
        private String type;
        private String name;
        private String url;
        private String path;
        private String note;
    }

    @Data
    public static class SelectionSummary {
        private String category;
        private List<String> tags = Collections.emptyList();
        private String descriptionZh;
        private String visualShapeZh;
        private List<String> useCasesZh = Collections.emptyList();
        private List<SelectionSlot> requiredSlots = Collections.emptyList();
        private List<SelectionSlot> optionalSlots = Collections.emptyList();
        private List<String> requiredDatasetRoles = Collections.emptyList();
        private List<String> optionalDatasetRoles = Collections.emptyList();
        private Map<String, List<String>> datasetRoleInventory = Collections.emptyMap();
        private List<String> nonBindingControlNames = Collections.emptyList();
        private List<String> embeddingRuntimeFieldNames = Collections.emptyList();
        private List<String> minimalTemplateKeys = Collections.emptyList();
        private List<String> recommendedTemplateKeys = Collections.emptyList();
        private Map<String, Object> structuralFlags = Collections.emptyMap();
        private List<String> selectionHints = Collections.emptyList();
    }

    @Data
    public static class SelectionSlot {
        private String fieldName;
        private boolean required;
        private String slotKind;
        private List<String> acceptedDatasetRoles = Collections.emptyList();
        private String cardinality;
        private Object placeholder;
        private List<String> sourceCategories = Collections.emptyList();
        private List<String> embeddingModes = Collections.emptyList();
        private List<String> descriptions = Collections.emptyList();
    }

    @Data
    public static class VizTypeCatalog {
        private String source;
        private String generatedAt;
        private List<VizTypeItem> items = Collections.emptyList();

        public Optional<String> resolveByName(String name) {
            if (StringUtils.isBlank(name) || items == null) {
                return Optional.empty();
            }
            return items.stream().filter(item -> name.equalsIgnoreCase(item.getName()))
                    .map(VizTypeItem::getVizType).filter(StringUtils::isNotBlank).findFirst();
        }

        public List<VizTypeItem> getItems() {
            return items == null ? Collections.emptyList() : items;
        }

        public static VizTypeCatalog load() {
            String payload = readPayload();
            if (StringUtils.isBlank(payload)) {
                return new VizTypeCatalog();
            }
            try {
                return parsePayload(payload);
            } catch (Exception ex) {
                log.warn("superset viztype catalog load failed", ex);
                return new VizTypeCatalog();
            }
        }

        private static VizTypeCatalog parsePayload(String payload) {
            JSONObject root = JSONObject.parseObject(payload);
            if (root == null) {
                return new VizTypeCatalog();
            }
            JSONArray itemsArray = root.getJSONArray("items");
            if (itemsArray != null) {
                return JsonUtil.toObject(payload, VizTypeCatalog.class);
            }
            JSONArray viztypesArray = root.getJSONArray("viztypes");
            if (viztypesArray == null) {
                viztypesArray = root.getJSONArray("pairs");
            }
            if (viztypesArray == null) {
                return new VizTypeCatalog();
            }
            VizTypeCatalog catalog = new VizTypeCatalog();
            catalog.setSource(StringUtils.defaultIfBlank(root.getString("source"),
                    "superset-spec/catalog/viztypes.json"));
            catalog.setGeneratedAt(StringUtils.defaultIfBlank(root.getString("generatedAt"),
                    root.getString("generated_at")));
            List<VizTypeItem> items = new ArrayList<>();
            for (int i = 0; i < viztypesArray.size(); i++) {
                JSONObject item = viztypesArray.getJSONObject(i);
                VizTypeItem resolved = resolveCatalogItem(item);
                if (resolved != null && StringUtils.isNotBlank(resolved.getVizType())) {
                    items.add(resolved);
                }
            }
            catalog.setItems(items);
            return catalog;
        }

        private static VizTypeItem resolveCatalogItem(JSONObject item) {
            if (item == null) {
                return null;
            }
            JSONObject originalOnlineEntry = item.getJSONObject("original_online_entry");
            JSONObject selectionSummary = item.getJSONObject("selection_summary");
            JSONObject formDataRules = item.getJSONObject("form_data_rules");
            VizTypeItem resolved = new VizTypeItem();
            String vizType = StringUtils.defaultIfBlank(item.getString("vizType"),
                    item.getString("viztype"));
            resolved.setVizType(vizType);
            resolved.setName(firstNonBlank(item.getString("name"),
                    originalOnlineEntry == null ? null : originalOnlineEntry.getString("name"),
                    vizType));
            resolved.setLlmName(firstNonBlank(item.getString("llmName"), resolved.getName()));
            resolved.setVizKey(firstNonBlank(item.getString("vizKey"),
                    originalOnlineEntry == null ? null : originalOnlineEntry.getString("vizKey"),
                    resolved.getName()));
            resolved.setCategory(firstNonBlank(item.getString("category"),
                    originalOnlineEntry == null ? null : originalOnlineEntry.getString("category"),
                    originalOnlineEntry == null ? null
                            : originalOnlineEntry.getString("pluginCategory")));
            resolved.setDescription(firstNonBlank(item.getString("description"),
                    item.getString("详细描述"),
                    selectionSummary == null ? null : selectionSummary.getString("description_zh"),
                    originalOnlineEntry == null ? null : originalOnlineEntry.getString("详细描述"),
                    originalOnlineEntry == null ? null
                            : originalOnlineEntry.getString("description")));
            resolved.setSourcePath(firstNonBlank(item.getString("sourcePath"),
                    item.getString("summary"), originalOnlineEntry == null ? null
                            : originalOnlineEntry.getString("sourcePath")));
            resolved.setFormDataRules(resolveFormDataRules(formDataRules));
            resolved.setSelectionSummary(resolveSelectionSummary(selectionSummary));
            resolved.setSelectionPromptCard(
                    toObjectMap(item.getJSONObject("selection_prompt_card")));
            return resolved;
        }

        private static FormDataRules resolveFormDataRules(JSONObject json) {
            if (json == null || json.isEmpty()) {
                return null;
            }
            FormDataRules rules = new FormDataRules();
            rules.setProfile(firstNonBlank(json.getString("profile"), json.getString("type")));
            rules.setRequired(toStringList(json.getJSONArray("required")));
            JSONArray requiredAnyOf = json.getJSONArray("requiredAnyOf");
            if (requiredAnyOf == null) {
                requiredAnyOf = json.getJSONArray("required_any_of");
            }
            rules.setRequiredAnyOf(resolveFormDataFieldGroups(requiredAnyOf));
            rules.setFields(resolveFormDataFields(json.getJSONArray("fields")));
            rules.setSources(resolveFormDataSources(json.getJSONArray("sources")));
            return rules;
        }

        private static List<List<FormDataField>> resolveFormDataFieldGroups(JSONArray array) {
            if (array == null || array.isEmpty()) {
                return Collections.emptyList();
            }
            List<List<FormDataField>> groups = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                Object value = array.get(i);
                List<FormDataField> group = new ArrayList<>();
                if (value instanceof JSONArray) {
                    group.addAll(resolveFormDataFields((JSONArray) value));
                } else if (value instanceof JSONObject) {
                    FormDataField field = resolveFormDataField((JSONObject) value);
                    if (field != null) {
                        group.add(field);
                    }
                } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                    FormDataField field = new FormDataField();
                    field.setKey((String) value);
                    group.add(field);
                }
                if (!group.isEmpty()) {
                    groups.add(group);
                }
            }
            return groups;
        }

        private static List<FormDataField> resolveFormDataFields(JSONArray array) {
            if (array == null || array.isEmpty()) {
                return Collections.emptyList();
            }
            List<FormDataField> fields = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                Object value = array.get(i);
                if (value instanceof JSONObject) {
                    FormDataField field = resolveFormDataField((JSONObject) value);
                    if (field != null) {
                        fields.add(field);
                    }
                } else if (value instanceof String && StringUtils.isNotBlank((String) value)) {
                    FormDataField field = new FormDataField();
                    field.setKey((String) value);
                    fields.add(field);
                }
            }
            return fields;
        }

        private static FormDataField resolveFormDataField(JSONObject json) {
            if (json == null || json.isEmpty()) {
                return null;
            }
            FormDataField field = new FormDataField();
            field.setKey(firstNonBlank(json.getString("key"), json.getString("field_name")));
            field.setType(firstNonBlank(json.getString("type"), json.getString("slot_kind")));
            field.setDescription(
                    firstNonBlank(json.getString("description"), json.getString("note")));
            field.setSource(json.getString("source"));
            return StringUtils.isBlank(field.getKey()) ? null : field;
        }

        private static List<FormDataSource> resolveFormDataSources(JSONArray array) {
            if (array == null || array.isEmpty()) {
                return Collections.emptyList();
            }
            List<FormDataSource> sources = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                JSONObject item = array.getJSONObject(i);
                if (item == null) {
                    continue;
                }
                FormDataSource source = new FormDataSource();
                source.setType(item.getString("type"));
                source.setName(item.getString("name"));
                source.setUrl(item.getString("url"));
                source.setPath(item.getString("path"));
                source.setNote(item.getString("note"));
                sources.add(source);
            }
            return sources;
        }

        private static SelectionSummary resolveSelectionSummary(JSONObject json) {
            if (json == null) {
                return null;
            }
            SelectionSummary summary = new SelectionSummary();
            summary.setCategory(json.getString("category"));
            summary.setTags(toStringList(json.getJSONArray("tags")));
            summary.setDescriptionZh(json.getString("description_zh"));
            summary.setVisualShapeZh(json.getString("visual_shape_zh"));
            summary.setUseCasesZh(toStringList(json.getJSONArray("use_cases_zh")));
            summary.setRequiredSlots(resolveSelectionSlots(json.getJSONArray("required_slots")));
            summary.setOptionalSlots(resolveSelectionSlots(json.getJSONArray("optional_slots")));
            summary.setRequiredDatasetRoles(
                    toStringList(json.getJSONArray("required_dataset_roles")));
            summary.setOptionalDatasetRoles(
                    toStringList(json.getJSONArray("optional_dataset_roles")));
            summary.setDatasetRoleInventory(
                    toStringListMap(json.getJSONObject("dataset_role_inventory")));
            summary.setNonBindingControlNames(
                    toStringList(json.getJSONArray("non_binding_control_names")));
            summary.setEmbeddingRuntimeFieldNames(
                    toStringList(json.getJSONArray("embedding_runtime_field_names")));
            summary.setMinimalTemplateKeys(
                    toStringList(json.getJSONArray("minimal_template_keys")));
            summary.setRecommendedTemplateKeys(
                    toStringList(json.getJSONArray("recommended_template_keys")));
            summary.setStructuralFlags(toObjectMap(json.getJSONObject("structural_flags")));
            summary.setSelectionHints(toStringList(json.getJSONArray("selection_hints")));
            return summary;
        }

        private static List<SelectionSlot> resolveSelectionSlots(JSONArray array) {
            if (array == null || array.isEmpty()) {
                return Collections.emptyList();
            }
            List<SelectionSlot> slots = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                JSONObject item = array.getJSONObject(i);
                if (item == null) {
                    continue;
                }
                SelectionSlot slot = new SelectionSlot();
                slot.setFieldName(item.getString("field_name"));
                slot.setRequired(Boolean.TRUE.equals(item.getBoolean("required")));
                slot.setSlotKind(item.getString("slot_kind"));
                slot.setAcceptedDatasetRoles(
                        toStringList(item.getJSONArray("accepted_dataset_roles")));
                slot.setCardinality(item.getString("cardinality"));
                slot.setPlaceholder(item.get("placeholder"));
                slot.setSourceCategories(toStringList(item.getJSONArray("source_categories")));
                slot.setEmbeddingModes(toStringList(item.getJSONArray("embedding_modes")));
                slot.setDescriptions(toStringList(item.getJSONArray("descriptions")));
                slots.add(slot);
            }
            return slots;
        }

        private static List<String> toStringList(JSONArray array) {
            if (array == null || array.isEmpty()) {
                return Collections.emptyList();
            }
            List<String> values = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                String value = array.getString(i);
                if (StringUtils.isNotBlank(value)) {
                    values.add(value);
                }
            }
            return values;
        }

        private static Map<String, Object> toObjectMap(JSONObject json) {
            if (json == null || json.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<String, Object> resolved = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : json.entrySet()) {
                resolved.put(entry.getKey(), entry.getValue());
            }
            return resolved;
        }

        private static Map<String, List<String>> toStringListMap(JSONObject json) {
            if (json == null || json.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<String, List<String>> resolved = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : json.entrySet()) {
                if (entry.getValue() instanceof JSONArray) {
                    resolved.put(entry.getKey(), toStringList((JSONArray) entry.getValue()));
                }
            }
            return resolved;
        }

        private static String firstNonBlank(String... values) {
            if (values == null) {
                return null;
            }
            for (String value : values) {
                if (StringUtils.isNotBlank(value)) {
                    return value;
                }
            }
            return null;
        }

        private static String readPayload() {
            for (String resource : VIZTYPE_RESOURCES) {
                try (InputStream stream = SupersetVizTypeSelector.class.getClassLoader()
                        .getResourceAsStream(resource)) {
                    if (stream != null) {
                        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                    }
                } catch (IOException ex) {
                    log.warn("superset viztype resource read failed: {}", resource, ex);
                }
                Path fallback = resolveFallbackPath(resource);
                if (fallback == null) {
                    continue;
                }
                try {
                    return Files.readString(fallback, StandardCharsets.UTF_8);
                } catch (IOException ex) {
                    log.warn("superset viztype file read failed: {}", fallback, ex);
                }
            }
            log.warn("superset viztype resource missing: {}", VIZTYPE_RESOURCES);
            return null;
        }

        private static Path resolveFallbackPath(String resource) {
            Path current = Path.of("").toAbsolutePath().normalize();
            while (current != null) {
                Path candidate = current.resolve(resource);
                if (Files.exists(candidate)) {
                    return candidate;
                }
                current = current.getParent();
            }
            return null;
        }
    }
}
