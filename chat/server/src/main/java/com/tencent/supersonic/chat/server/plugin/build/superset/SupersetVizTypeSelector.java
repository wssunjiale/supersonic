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
    private static final String VIZTYPE_RESOURCE = "docs/viztype.json";
    private static final VizTypeCatalog VIZTYPE_CATALOG = VizTypeCatalog.load();
    private static final String LLM_PROMPT = "" + "#Role: You are a Superset chart type selector.\n"
            + "#Task: Choose the best Superset viz_type from the available list.\n" + "#Rules:\n"
            + "1. ONLY select from #AvailableVizTypes.\n"
            + "2. Consider #UserInstruction, #DataProfile, and #Signals.\n"
            + "3. Use Signals.metricCount/timeCount/dimensionCount/rowCount to infer chart shape.\n"
            + "4. If timeCount>=1 and metricCount>=1, prefer Line/Smooth/Area/Generic (or Mixed when metricCount>1).\n"
            + "5. If dimensionCount>=1 and metricCount==1, prefer Bar; allow Pie/Partition only when rowCount<=10.\n"
            + "6. Avoid table-type charts (Table/Time-series Table/Pivot Table) unless no other candidate fits.\n"
            + "7. Return JSON only, no extra text.\n"
            + "8. Output fields: viz_type, alternatives (array), reason.\n"
            + "9. alternatives length should be {{top_n}} and ordered by preference.\n"
            + "#UserInstruction: {{instruction}}\n" + "#DataProfile: {{data_profile}}\n"
            + "#Signals: {{signals}}\n" + "#AvailableVizTypes: {{candidates}}\n" + "#Response:";
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
        List<VizTypeItem> candidates = selectCandidates(config, queryResult, queryText, agent);
        if (candidates.isEmpty()) {
            return DEFAULT_VIZ_TYPE;
        }
        return StringUtils.defaultIfBlank(candidates.get(0).getVizType(), DEFAULT_VIZ_TYPE);
    }

    public static List<VizTypeItem> selectCandidates(SupersetPluginConfig config,
            QueryResult queryResult, String queryText, Agent agent) {
        if (queryResult == null || queryResult.getQueryColumns() == null) {
            return Collections.singletonList(resolveFallbackItem(DEFAULT_VIZ_TYPE, null));
        }
        List<QueryColumn> columns = queryResult.getQueryColumns();
        List<Map<String, Object>> results = queryResult.getQueryResults();
        if (columns.isEmpty()) {
            return Collections.singletonList(resolveFallbackItem(DEFAULT_VIZ_TYPE, null));
        }
        SupersetPluginConfig safeConfig = config == null ? new SupersetPluginConfig() : config;
        List<VizTypeItem> candidates = filterCandidates(safeConfig, VIZTYPE_CATALOG.getItems());

        DecisionContext decisionContext = buildDecisionContext(columns, results);
        int topN = resolveTopN(safeConfig);
        List<String> ordered = new ArrayList<>();

        if (safeConfig.isVizTypeLlmEnabled()) {
            Optional<List<String>> llmCandidates = selectByLlmCandidates(safeConfig, queryResult,
                    queryText, candidates, decisionContext, agent);
            if (llmCandidates.isPresent()) {
                List<String> guarded =
                        guardLlmCandidates(llmCandidates.get(), decisionContext, candidates);
                addOrderedCandidates(ordered, guarded, topN);
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

        if (ordered.isEmpty()) {
            String fallback = resolveFallback(candidates, decisionContext);
            addOrderedCandidate(ordered, fallback, topN);
        }

        List<VizTypeItem> resolved = resolveCandidateItems(ordered, candidates);
        if (resolved.isEmpty()) {
            return Collections.singletonList(resolveFallbackItem(DEFAULT_VIZ_TYPE, candidates));
        }
        return resolved;
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
            if (results != null && results.size() <= 10) {
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

    private static Optional<List<String>> selectByLlmCandidates(SupersetPluginConfig config,
            QueryResult queryResult, String queryText, List<VizTypeItem> candidates,
            DecisionContext decisionContext, Agent agent) {
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
        String dataProfile = buildDataProfile(queryResult);
        String candidateSummary = buildCandidateSummary(candidates);
        String signals = buildDecisionSignals(decisionContext);
        Map<String, Object> variables = new HashMap<>();
        variables.put("instruction", StringUtils.defaultString(queryText));
        variables.put("data_profile", dataProfile);
        variables.put("signals", signals);
        variables.put("candidates", candidateSummary);
        variables.put("top_n", topN);

        Prompt prompt = PromptTemplate.from(promptText).apply(variables);
        ChatLanguageModel chatLanguageModel = ModelProvider.getChatModel(chatModelConfig);
        Response<AiMessage> response = chatLanguageModel.generate(prompt.toUserMessage());
        String answer =
                response == null || response.content() == null ? null : response.content().text();
        log.info("superset viztype llm req:\n{} \nresp:\n{}", prompt.text(), answer);
        List<String> resolved = resolveCandidatesFromModelResponse(answer, candidates);
        return resolved.isEmpty() ? Optional.empty() : Optional.of(resolved);
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
        List<String> resolved = resolveCandidatesFromModelResponse(response, candidates);
        return resolved.isEmpty() ? null : resolved.get(0);
    }

    static List<String> resolveCandidatesFromModelResponse(String response,
            List<VizTypeItem> candidates) {
        if (StringUtils.isBlank(response) || candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }
        String payload = extractJsonPayload(response);
        if (StringUtils.isBlank(payload)) {
            return Collections.emptyList();
        }
        JSONObject json;
        try {
            json = JSONObject.parseObject(payload);
        } catch (Exception ex) {
            log.warn("superset viztype llm response parse failed", ex);
            return Collections.emptyList();
        }
        List<String> ordered = new ArrayList<>();
        String primary =
                StringUtils.defaultIfBlank(json.getString("viz_type"), json.getString("vizType"));
        if (StringUtils.isNotBlank(primary)) {
            ordered.add(primary);
        }
        JSONArray alternatives = json.getJSONArray("alternatives");
        if (alternatives != null) {
            for (int i = 0; i < alternatives.size(); i++) {
                String value = alternatives.getString(i);
                if (StringUtils.isNotBlank(value)) {
                    ordered.add(value);
                }
            }
        }
        return resolveCandidates(ordered, candidates);
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
        int topN = config.getVizTypeLlmTopN() == null ? 3 : config.getVizTypeLlmTopN();
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
            List<VizTypeItem> candidates) {
        if (ordered == null || ordered.isEmpty()) {
            return Collections.emptyList();
        }
        List<VizTypeItem> resolved = new ArrayList<>();
        for (String candidate : ordered) {
            VizTypeItem item = resolveItemByVizType(candidate, candidates);
            if (item != null && StringUtils.isNotBlank(item.getVizType())
                    && resolved.stream().noneMatch(existing -> StringUtils
                            .equalsIgnoreCase(existing.getVizType(), item.getVizType()))) {
                resolved.add(item);
            }
        }
        return resolved;
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
        if (context.getTimeCount() >= 1 && context.getMetricCount() >= 1) {
            return true;
        }
        return context.getMetricCount() == 1 && context.getDimensionCount() >= 1;
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
    public static class VizTypeItem {
        private String vizKey;
        private String vizType;
        private String name;
        private String category;
        private String description;
        private String sourcePath;
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
                return JsonUtil.toObject(payload, VizTypeCatalog.class);
            } catch (Exception ex) {
                log.warn("superset viztype catalog load failed", ex);
                return new VizTypeCatalog();
            }
        }

        private static String readPayload() {
            try (InputStream stream = SupersetVizTypeSelector.class.getClassLoader()
                    .getResourceAsStream(VIZTYPE_RESOURCE)) {
                if (stream != null) {
                    return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } catch (IOException ex) {
                log.warn("superset viztype resource read failed", ex);
            }
            Path fallback = Path.of(VIZTYPE_RESOURCE);
            if (!Files.exists(fallback)) {
                return null;
            }
            try {
                return Files.readString(fallback, StandardCharsets.UTF_8);
            } catch (IOException ex) {
                log.warn("superset viztype file read failed: {}", fallback, ex);
                return null;
            }
        }
    }
}
