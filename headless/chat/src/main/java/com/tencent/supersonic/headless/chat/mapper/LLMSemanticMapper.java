package com.tencent.supersonic.headless.chat.mapper;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.tencent.supersonic.common.pojo.ChatApp;
import com.tencent.supersonic.common.pojo.enums.AppModule;
import com.tencent.supersonic.common.util.ChatAppManager;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.DataSetSchema;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementMatch;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.input.Prompt;
import dev.langchain4j.model.input.PromptTemplate;
import dev.langchain4j.provider.ModelProvider;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * LLM-driven mapper (方案2): 1) Based on full metric semantics per dataset, select ONE best dataset.
 * 2) Based on full field semantics within the selected dataset, select metrics/dimensions.
 *
 * It is enabled by config {@code s2.mapper.strategy=LLM}. If LLM fails, it falls back to the legacy
 * mapping results produced by other mappers.
 */
@Slf4j
public class LLMSemanticMapper extends BaseMapper {

    private static final Logger keyPipelineLog = LoggerFactory.getLogger("keyPipeline");

    public static final String APP_KEY_DATASET_SELECTOR = "LLM_DATASET_SELECTOR";
    public static final String APP_KEY_FIELD_SELECTOR = "LLM_FIELD_SELECTOR";

    private static final String DATASET_SELECTOR_PROMPT =
            """
                    #Role: You are a senior analytics engineer. You are selecting the best dataset for a user question.
                    #Task: You will be given a user Question and multiple candidate Datasets. Each dataset contains ALL Metrics (with rich semantics).
                    #Rules:
                    1. Metrics are the MOST important signals. Prefer datasets whose metrics semantics match the Question.
                    2. Only select ONE datasetId from the provided candidates.
                    3. Do NOT hallucinate datasetIds or metricIds.
                    4. Output MUST be strict JSON (no markdown), exactly in the schema:
                       {"dataSetId": <number>, "matchedMetricIds": [<number>...], "confidence": <0-1>, "reason": "<short>"}
                    5. confidence should reflect certainty given the provided info.
                    #Question: {{question}}
                    #Datasets: {{datasets}}
                    #Output:
                    """;

    private static final String FIELD_SELECTOR_PROMPT =
            """
                    #Role: You are a senior analytics engineer. You are selecting schema fields for semantic SQL generation.
                    #Task: Given a Question and a Dataset schema, select the best Metrics and Dimensions needed to answer the Question.
                    #Rules:
                    1. Only select from the provided field lists; do NOT hallucinate.
                    2. Prefer human-readable dimensions for grouping/display (avoid *_id/*_key when a name/title exists).
                    3. Output MUST be strict JSON (no markdown), exactly in the schema:
                       {"metricIds":[<number>...], "dimensionIds":[<number>...], "confidence": <0-1>, "reason":"<short>"}
                    #Question: {{question}}
                    #Dataset: {{dataset}}
                    #SelectedDatasetMetricsHint: {{matchedMetricIds}}
                    #Output:
                    """;

    private static final Pattern FIRST_NUMBER_PATTERN = Pattern.compile("(\\d+)");

    public LLMSemanticMapper() {
        // Register default chat-app prompts (users can still provide overrides via chatAppConfig).
        ChatAppManager.register(APP_KEY_DATASET_SELECTOR,
                ChatApp.builder().name("LLM数据集选择").description("通过大模型选择最合适的数据集")
                        .prompt(DATASET_SELECTOR_PROMPT).enable(true).appModule(AppModule.CHAT)
                        .build());
        ChatAppManager.register(APP_KEY_FIELD_SELECTOR,
                ChatApp.builder().name("LLM字段选择").description("通过大模型选择最合适的指标与维度")
                        .prompt(FIELD_SELECTOR_PROMPT).enable(true).appModule(AppModule.CHAT)
                        .build());
    }

    @Override
    protected boolean accept(ChatQueryContext chatQueryContext) {
        if (chatQueryContext == null || chatQueryContext.getRequest() == null) {
            return false;
        }
        // Only enable when explicitly configured.
        MapperConfig mapperConfig = ContextUtils.getBean(MapperConfig.class);
        String strategy = mapperConfig.getParameterValue(MapperConfig.MAPPER_STRATEGY);
        return "LLM".equalsIgnoreCase(StringUtils.trimToEmpty(strategy));
    }

    @Override
    public void doMap(ChatQueryContext chatQueryContext) {
        try {
            if (chatQueryContext == null || chatQueryContext.getSemanticSchema() == null) {
                return;
            }
            Set<Long> candidateDataSetIds = chatQueryContext.getRequest().getDataSetIds();
            if (CollectionUtils.isEmpty(candidateDataSetIds)) {
                // If caller doesn't specify datasets, legacy behavior may involve all datasets.
                // This LLM mapper intentionally does not take over in that case.
                return;
            }
            Map<Long, DataSetSchema> dataSetSchemaMap =
                    chatQueryContext.getSemanticSchema().getDataSetSchemaMap();
            if (dataSetSchemaMap == null || dataSetSchemaMap.isEmpty()) {
                return;
            }

            // 1) Select dataset using ALL metrics per dataset.
            DataSetSelection selection =
                    selectBestDataSet(chatQueryContext, candidateDataSetIds, dataSetSchemaMap);
            if (selection == null || selection.getDataSetId() == null
                    || !candidateDataSetIds.contains(selection.getDataSetId())) {
                return; // fallback to legacy mapping
            }

            DataSetSchema selected = dataSetSchemaMap.get(selection.getDataSetId());
            if (selected == null) {
                return;
            }

            // 2) Select fields within the chosen dataset.
            FieldSelection fieldSelection =
                    selectFields(chatQueryContext, selected, selection.getMatchedMetricIds());
            if (fieldSelection == null || CollectionUtils.isEmpty(fieldSelection.getMetricIds())
                    && CollectionUtils.isEmpty(fieldSelection.getDimensionIds())) {
                return; // fallback to legacy mapping
            }

            overwriteMapInfo(chatQueryContext, selected, fieldSelection, selection);
        } catch (Exception e) {
            // Any failure should not break the overall workflow.
            log.error("LLMSemanticMapper failed, fallback to legacy mapping", e);
        }
    }

    private DataSetSelection selectBestDataSet(ChatQueryContext ctx, Set<Long> candidateDataSetIds,
            Map<Long, DataSetSchema> dataSetSchemaMap) {
        String question = StringUtils.defaultString(ctx.getRequest().getQueryText());
        List<DataSetSchema> candidates = candidateDataSetIds.stream().map(dataSetSchemaMap::get)
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            return null;
        }

        String datasetsStr = buildDatasetsSummary(candidates);
        Map<String, Object> vars = new HashMap<>();
        vars.put("question", question);
        vars.put("datasets", datasetsStr);

        ChatApp chatApp = resolveChatApp(ctx, APP_KEY_DATASET_SELECTOR);
        Prompt prompt = PromptTemplate.from(chatApp.getPrompt()).apply(vars);
        ChatLanguageModel model = resolveChatModel(ctx, chatApp);
        String resp = model.generate(prompt.toUserMessage().singleText());

        keyPipelineLog.info("LLM_DATASET_SELECTOR modelReq:\n{} \nmodelResp:\n{}", prompt.text(),
                resp);

        DataSetSelection selection = tryParseJson(resp, DataSetSelection.class);
        if (selection != null && selection.getDataSetId() != null) {
            if (selection.getMatchedMetricIds() == null) {
                selection.setMatchedMetricIds(new ArrayList<>());
            }
            return selection;
        }

        // Fallback: pick first number if it's a valid datasetId.
        Long dataSetId = extractFirstLong(resp);
        if (dataSetId != null && candidateDataSetIds.contains(dataSetId)) {
            DataSetSelection fallback = new DataSetSelection();
            fallback.setDataSetId(dataSetId);
            fallback.setMatchedMetricIds(new ArrayList<>());
            fallback.setConfidence(0.0);
            fallback.setReason("fallback_parse");
            return fallback;
        }
        return null;
    }

    private FieldSelection selectFields(ChatQueryContext ctx, DataSetSchema selected,
            List<Long> matchedMetricIds) {
        String question = StringUtils.defaultString(ctx.getRequest().getQueryText());
        String datasetStr = buildSingleDatasetSchema(selected);

        Map<String, Object> vars = new HashMap<>();
        vars.put("question", question);
        vars.put("dataset", datasetStr);
        vars.put("matchedMetricIds", matchedMetricIds == null ? "[]" : matchedMetricIds.toString());

        ChatApp chatApp = resolveChatApp(ctx, APP_KEY_FIELD_SELECTOR);
        Prompt prompt = PromptTemplate.from(chatApp.getPrompt()).apply(vars);
        ChatLanguageModel model = resolveChatModel(ctx, chatApp);
        String resp = model.generate(prompt.toUserMessage().singleText());

        keyPipelineLog.info("LLM_FIELD_SELECTOR modelReq:\n{} \nmodelResp:\n{}", prompt.text(),
                resp);

        FieldSelection selection = tryParseJson(resp, FieldSelection.class);
        if (selection == null) {
            return null;
        }
        if (selection.getMetricIds() == null) {
            selection.setMetricIds(new ArrayList<>());
        }
        if (selection.getDimensionIds() == null) {
            selection.setDimensionIds(new ArrayList<>());
        }
        // Keep only ids that exist in schema (defensive).
        Map<Long, SchemaElement> metricMap = selected.getMetrics().stream().filter(Objects::nonNull)
                .collect(Collectors.toMap(SchemaElement::getId, Function.identity(), (a, b) -> a));
        Map<Long, SchemaElement> dimMap = selected.getDimensions().stream().filter(Objects::nonNull)
                .collect(Collectors.toMap(SchemaElement::getId, Function.identity(), (a, b) -> a));
        selection.setMetricIds(selection.getMetricIds().stream().filter(metricMap::containsKey)
                .distinct().collect(Collectors.toList()));
        selection.setDimensionIds(selection.getDimensionIds().stream().filter(dimMap::containsKey)
                .distinct().collect(Collectors.toList()));
        return selection;
    }

    private void overwriteMapInfo(ChatQueryContext ctx, DataSetSchema selected,
            FieldSelection fieldSelection, DataSetSelection dataSetSelection) {
        Map<Long, SchemaElement> metricMap = selected.getMetrics().stream().filter(Objects::nonNull)
                .collect(Collectors.toMap(SchemaElement::getId, Function.identity(), (a, b) -> a));
        Map<Long, SchemaElement> dimMap = selected.getDimensions().stream().filter(Objects::nonNull)
                .collect(Collectors.toMap(SchemaElement::getId, Function.identity(), (a, b) -> a));

        List<SchemaElementMatch> matches = new ArrayList<>();

        // Add dataset element (helps debugging; resolver will be deterministic because only one
        // dataset remains).
        if (selected.getDataSet() != null) {
            matches.add(buildMatch(selected.getDataSet(), ctx.getRequest().getQueryText(), 0.9));
        }

        for (Long metricId : fieldSelection.getMetricIds()) {
            SchemaElement metric = metricMap.get(metricId);
            if (metric != null) {
                matches.add(buildMatch(metric, ctx.getRequest().getQueryText(), 0.9));
            }
        }

        for (Long dimId : fieldSelection.getDimensionIds()) {
            SchemaElement dim = dimMap.get(dimId);
            if (dim != null) {
                matches.add(buildMatch(dim, ctx.getRequest().getQueryText(), 0.9));
            }
        }

        // Ensure the most important matched metric(s) are included, even if the 2nd step missed
        // them.
        if (!CollectionUtils.isEmpty(dataSetSelection.getMatchedMetricIds())) {
            Set<Long> already = fieldSelection.getMetricIds().stream().collect(Collectors.toSet());
            for (Long metricId : dataSetSelection.getMatchedMetricIds()) {
                if (already.contains(metricId)) {
                    continue;
                }
                SchemaElement metric = metricMap.get(metricId);
                if (metric != null) {
                    matches.add(buildMatch(metric, ctx.getRequest().getQueryText(), 0.85));
                }
            }
        }

        // Overwrite mapInfo to make dataset selection deterministic and prevent legacy results from
        // leaking in.
        ctx.getMapInfo().getDataSetElementMatches().clear();
        ctx.getMapInfo().setMatchedElements(selected.getDataSetId(), matches);
    }

    private static SchemaElementMatch buildMatch(SchemaElement element, String question,
            double similarity) {
        return SchemaElementMatch.builder().element(element).word(element.getName())
                .detectWord(StringUtils.left(StringUtils.defaultString(question), 64))
                .similarity(similarity).frequency(0L).offset(0.0).llmMatched(true).build();
    }

    private static String buildDatasetsSummary(List<DataSetSchema> candidates) {
        StringBuilder sb = new StringBuilder();
        for (DataSetSchema ds : candidates) {
            SchemaElement dataSet = ds.getDataSet();
            String dsName = dataSet == null ? "" : StringUtils.defaultString(dataSet.getName());
            String dsBizName =
                    dataSet == null ? "" : StringUtils.defaultString(dataSet.getBizName());

            sb.append("{\"dataSetId\":").append(ds.getDataSetId());
            sb.append(",\"name\":\"").append(escape(dsName)).append("\"");
            if (StringUtils.isNotBlank(dsBizName)) {
                sb.append(",\"bizName\":\"").append(escape(dsBizName)).append("\"");
            }
            sb.append(",\"databaseType\":\"")
                    .append(escape(StringUtils.defaultString(ds.getDatabaseType()))).append("\"");
            sb.append(",\"databaseVersion\":\"")
                    .append(escape(StringUtils.defaultString(ds.getDatabaseVersion())))
                    .append("\"");

            // List ALL metrics with semantics (most important).
            sb.append(",\"metrics\":[");
            List<SchemaElement> metrics = ds.getMetrics().stream().filter(Objects::nonNull)
                    .sorted(Comparator.comparing(m -> StringUtils.defaultString(m.getBizName())))
                    .collect(Collectors.toList());
            for (int i = 0; i < metrics.size(); i++) {
                SchemaElement m = metrics.get(i);
                sb.append(buildFieldJson(m));
                if (i < metrics.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");

            // Lightweight dims signal (do NOT dump all dims here to keep prompt size under
            // control).
            sb.append(",\"dimensionCount\":")
                    .append(ds.getDimensions() == null ? 0 : ds.getDimensions().size());
            sb.append(",\"metricCount\":").append(metrics.size());
            sb.append("}\n");
        }
        return sb.toString();
    }

    private static String buildSingleDatasetSchema(DataSetSchema ds) {
        SchemaElement dataSet = ds.getDataSet();
        String dsName = dataSet == null ? "" : StringUtils.defaultString(dataSet.getName());
        String dsBizName = dataSet == null ? "" : StringUtils.defaultString(dataSet.getBizName());

        StringBuilder sb = new StringBuilder();
        sb.append("{\"dataSetId\":").append(ds.getDataSetId());
        sb.append(",\"name\":\"").append(escape(dsName)).append("\"");
        if (StringUtils.isNotBlank(dsBizName)) {
            sb.append(",\"bizName\":\"").append(escape(dsBizName)).append("\"");
        }
        sb.append(",\"metrics\":[");
        List<SchemaElement> metrics = ds.getMetrics().stream().filter(Objects::nonNull)
                .sorted(Comparator.comparing(m -> StringUtils.defaultString(m.getBizName())))
                .collect(Collectors.toList());
        for (int i = 0; i < metrics.size(); i++) {
            sb.append(buildFieldJson(metrics.get(i)));
            if (i < metrics.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]");

        sb.append(",\"dimensions\":[");
        List<SchemaElement> dims = ds.getDimensions().stream().filter(Objects::nonNull)
                .sorted(Comparator.comparing(d -> StringUtils.defaultString(d.getBizName())))
                .collect(Collectors.toList());
        for (int i = 0; i < dims.size(); i++) {
            sb.append(buildFieldJson(dims.get(i)));
            if (i < dims.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        sb.append("}");
        return sb.toString();
    }

    private static String buildFieldJson(SchemaElement e) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"id\":").append(e.getId());
        sb.append(",\"type\":\"").append(escape(String.valueOf(e.getType()))).append("\"");
        sb.append(",\"name\":\"").append(escape(StringUtils.defaultString(e.getName())))
                .append("\"");
        if (StringUtils.isNotBlank(e.getBizName())) {
            sb.append(",\"bizName\":\"").append(escape(e.getBizName())).append("\"");
        }
        if (StringUtils.isNotBlank(e.getDescription())) {
            sb.append(",\"description\":\"")
                    .append(escape(StringUtils.left(e.getDescription(), 300))).append("\"");
        }
        if (!CollectionUtils.isEmpty(e.getAlias())) {
            sb.append(",\"alias\":[");
            for (int i = 0; i < e.getAlias().size(); i++) {
                sb.append("\"")
                        .append(escape(StringUtils.left(String.valueOf(e.getAlias().get(i)), 60)))
                        .append("\"");
                if (i < e.getAlias().size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
        }
        if (StringUtils.isNotBlank(e.getDefaultAgg())) {
            sb.append(",\"defaultAgg\":\"").append(escape(e.getDefaultAgg())).append("\"");
        }
        if (StringUtils.isNotBlank(e.getDataFormatType())) {
            sb.append(",\"dataFormatType\":\"").append(escape(e.getDataFormatType())).append("\"");
        }
        if (e.getUseCnt() != null) {
            sb.append(",\"useCnt\":").append(e.getUseCnt());
        }
        sb.append("}");
        return sb.toString();
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
                .replace("\r", "\\r").replace("\t", "\\t");
    }

    private static <T> T tryParseJson(String text, Class<T> clazz) {
        if (StringUtils.isBlank(text) || clazz == null) {
            return null;
        }
        try {
            return JsonUtil.toObject(text, clazz);
        } catch (Exception e) {
            return null;
        }
    }

    private static Long extractFirstLong(String text) {
        if (StringUtils.isBlank(text)) {
            return null;
        }
        Matcher m = FIRST_NUMBER_PATTERN.matcher(text);
        if (!m.find()) {
            return null;
        }
        try {
            return Long.parseLong(m.group(1));
        } catch (Exception e) {
            return null;
        }
    }

    private static ChatApp resolveChatApp(ChatQueryContext ctx, String appKey) {
        Map<String, ChatApp> cfg = ctx.getRequest().getChatAppConfig();
        if (cfg != null && cfg.containsKey(appKey) && cfg.get(appKey) != null
                && StringUtils.isNotBlank(cfg.get(appKey).getPrompt())) {
            return cfg.get(appKey);
        }
        return ChatAppManager.getApp(appKey).orElse(
                ChatApp.builder().prompt("").enable(true).appModule(AppModule.CHAT).build());
    }

    private static ChatLanguageModel resolveChatModel(ChatQueryContext ctx, ChatApp chatApp) {
        // Prefer chatAppConfig's explicit model config; otherwise fall back to default.
        if (chatApp != null && chatApp.getChatModelConfig() != null) {
            return ModelProvider.getChatModel(chatApp.getChatModelConfig());
        }
        // If user provided S2SQL_PARSER model config, reuse it.
        Map<String, ChatApp> cfg = ctx.getRequest().getChatAppConfig();
        if (cfg != null) {
            ChatApp s2sql = cfg.get("S2SQL_PARSER");
            if (s2sql != null && s2sql.getChatModelConfig() != null) {
                return ModelProvider.getChatModel(s2sql.getChatModelConfig());
            }
        }
        return ModelProvider.getChatModel();
    }

    @Data
    public static class DataSetSelection {
        @JsonAlias({"datasetId", "data_set_id"})
        private Long dataSetId;
        @JsonAlias({"matchedMetricIds", "matched_metrics", "metricIds"})
        private List<Long> matchedMetricIds;
        private Double confidence;
        private String reason;
    }

    @Data
    public static class FieldSelection {
        @JsonAlias({"metricIds", "metrics"})
        private List<Long> metricIds;
        @JsonAlias({"dimensionIds", "dimensions"})
        private List<Long> dimensionIds;
        private Double confidence;
        private String reason;
    }
}
