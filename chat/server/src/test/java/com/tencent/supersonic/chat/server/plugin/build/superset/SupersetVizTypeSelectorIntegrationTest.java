package com.tencent.supersonic.chat.server.plugin.build.superset;

import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.common.config.ChatModel;
import com.tencent.supersonic.common.pojo.ChatModelConfig;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.pojo.User;
import com.tencent.supersonic.common.service.ChatModelService;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetColumn;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetInfo;
import com.tencent.supersonic.headless.server.sync.superset.SupersetDatasetMetric;
import dev.langchain4j.provider.DifyModelFactory;
import dev.langchain4j.provider.OllamaModelFactory;
import dev.langchain4j.provider.OpenAiModelFactory;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.StaticApplicationContext;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SupersetVizTypeSelectorIntegrationTest {

    private static final String DEFAULT_DB_HOST = "10.0.12.252";
    private static final String DEFAULT_DB_PORT = "5439";
    private static final String DEFAULT_DB_NAME = "supersonic";
    private static final String DEFAULT_DB_USER = "supersonic";
    private static final int DEFAULT_CHAT_MODEL_ID = 2;

    private ApplicationContext originalContext;
    private Integer chatModelId;

    @BeforeAll
    void setUp() throws Exception {
        registerModelFactories();
        chatModelId = Integer.getInteger("superset.it.chatModelId", DEFAULT_CHAT_MODEL_ID);
        ChatModelConfig chatModelConfig = loadChatModelConfigFromProperty();
        if (chatModelConfig == null) {
            String password = firstNonBlank(System.getProperty("superset.it.db.password"),
                    System.getenv("S2_DB_PASSWORD"));
            Assumptions.assumeTrue(StringUtils.isNotBlank(password),
                    "requires superset.it.chatModelConfigBase64 or db password");
            chatModelConfig = loadChatModelConfig(chatModelId, password);
        }
        Assumptions.assumeTrue(chatModelConfig != null,
                "requires chat model config in s2_chat_model");

        originalContext = currentContext();
        StaticApplicationContext context = new StaticApplicationContext();
        context.getBeanFactory().registerSingleton("chatModelService",
                new StubChatModelService(chatModelId, chatModelConfig));
        setContext(context);
    }

    @AfterAll
    void tearDown() throws Exception {
        setContext(originalContext);
    }

    @Test
    void testRealLlmSelectsTimeseriesChartsForTrendQuestion() {
        QueryResult queryResult = new QueryResult();
        queryResult.setQuerySql(
                "select ds, sum(sales) as sales from orders group by ds order by ds limit 30");
        queryResult.setQueryColumns(Arrays.asList(buildQueryColumn("ds", "ds", "DATE", "DATE"),
                buildQueryColumn("sales", "sales", "NUMBER", "DOUBLE")));
        queryResult.setQueryResults(Arrays.asList(row("ds", "2024-01-01", "sales", 120.5),
                row("ds", "2024-01-02", "sales", 138.2), row("ds", "2024-01-03", "sales", 156.7)));

        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(schemaElement("sales", "sales", SchemaElementType.METRIC));
        parseInfo.getDimensions().add(schemaElement("ds", "ds", SchemaElementType.DATE));

        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setId(101L);
        datasetInfo.setDatabaseId(1L);
        datasetInfo.setSchema("analytics");
        datasetInfo.setTableName("orders");
        datasetInfo.setMainDttmCol("ds");
        datasetInfo.setColumns(Arrays.asList(buildDatasetColumn("ds", "DATE", true, true),
                buildDatasetColumn("sales", "DOUBLE", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("sales", "SUM(sales)")));

        List<SupersetVizTypeSelector.VizTypeItem> candidates =
                SupersetVizTypeSelector.selectCandidates(buildLlmConfig(), queryResult, "按日期看销售趋势",
                        null, queryResult.getQuerySql(), parseInfo, datasetInfo);
        List<String> vizTypes = toVizTypes(candidates);
        System.out.println("trend ranked candidates = " + vizTypes);

        Assertions.assertFalse(candidates.isEmpty());
        Assertions.assertEquals("timeseries", candidates.get(0).getCategory(),
                "趋势场景的首选图表应当属于 timeseries 类别");
        Assertions.assertTrue(vizTypes.contains("echarts_timeseries_line"), "趋势场景的候选结果应包含折线图");
    }

    @Test
    void testRealLlmSelectsPieOrBarForCategoryBreakdown() {
        QueryResult queryResult = new QueryResult();
        queryResult.setQuerySql(
                "select category, sum(sales) as sales from orders group by category order by sales desc");
        queryResult.setQueryColumns(
                Arrays.asList(buildQueryColumn("category", "category", "CATEGORY", "STRING"),
                        buildQueryColumn("sales", "sales", "NUMBER", "DOUBLE")));
        queryResult.setQueryResults(Arrays.asList(row("category", "手机", "sales", 320.0),
                row("category", "电脑", "sales", 210.0), row("category", "耳机", "sales", 95.0)));

        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getMetrics().add(schemaElement("sales", "sales", SchemaElementType.METRIC));
        parseInfo.getDimensions()
                .add(schemaElement("category", "category", SchemaElementType.DIMENSION));

        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setId(102L);
        datasetInfo.setDatabaseId(1L);
        datasetInfo.setSchema("analytics");
        datasetInfo.setTableName("orders");
        datasetInfo.setColumns(Arrays.asList(buildDatasetColumn("category", "STRING", true, false),
                buildDatasetColumn("sales", "DOUBLE", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("sales", "SUM(sales)")));

        List<SupersetVizTypeSelector.VizTypeItem> candidates =
                SupersetVizTypeSelector.selectCandidates(buildLlmConfig(), queryResult, "看各品类销售额占比",
                        null, queryResult.getQuerySql(), parseInfo, datasetInfo);
        List<String> vizTypes = toVizTypes(candidates);
        System.out.println("share ranked candidates = " + vizTypes);

        Assertions.assertFalse(candidates.isEmpty());
        Assertions.assertNotEquals("table", candidates.get(0).getVizType(), "占比场景不应把表格放在首位");
        Assertions.assertTrue(vizTypes.contains("pie") || vizTypes.contains("bar"),
                "占比场景的候选结果应包含 pie 或 bar");
    }

    @Test
    void testRealLlmSelectsTableForDetailQuestion() {
        QueryResult queryResult = new QueryResult();
        queryResult.setQuerySql(
                "select order_id, ds, category, user_name, sales from orders order by ds desc limit 20");
        queryResult.setQueryColumns(
                Arrays.asList(buildQueryColumn("order_id", "order_id", "CATEGORY", "BIGINT"),
                        buildQueryColumn("ds", "ds", "DATE", "DATE"),
                        buildQueryColumn("category", "category", "CATEGORY", "STRING"),
                        buildQueryColumn("user_name", "user_name", "CATEGORY", "STRING"),
                        buildQueryColumn("sales", "sales", "NUMBER", "DOUBLE")));
        queryResult.setQueryResults(Arrays.asList(
                row("order_id", 1L, "ds", "2024-01-03", "category", "手机", "user_name", "张三",
                        "sales", 120.5),
                row("order_id", 2L, "ds", "2024-01-03", "category", "电脑", "user_name", "李四",
                        "sales", 210.0)));

        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.getDimensions()
                .add(schemaElement("order_id", "order_id", SchemaElementType.DIMENSION));
        parseInfo.getDimensions().add(schemaElement("ds", "ds", SchemaElementType.DATE));
        parseInfo.getDimensions()
                .add(schemaElement("category", "category", SchemaElementType.DIMENSION));
        parseInfo.getDimensions()
                .add(schemaElement("user_name", "user_name", SchemaElementType.DIMENSION));
        parseInfo.getMetrics().add(schemaElement("sales", "sales", SchemaElementType.METRIC));

        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setId(103L);
        datasetInfo.setDatabaseId(1L);
        datasetInfo.setSchema("analytics");
        datasetInfo.setTableName("orders");
        datasetInfo.setMainDttmCol("ds");
        datasetInfo.setColumns(Arrays.asList(buildDatasetColumn("order_id", "BIGINT", true, false),
                buildDatasetColumn("ds", "DATE", true, true),
                buildDatasetColumn("category", "STRING", true, false),
                buildDatasetColumn("user_name", "STRING", true, false),
                buildDatasetColumn("sales", "DOUBLE", false, false)));
        datasetInfo.setMetrics(Collections.singletonList(buildMetric("sales", "SUM(sales)")));

        List<SupersetVizTypeSelector.VizTypeItem> candidates =
                SupersetVizTypeSelector.selectCandidates(buildLlmConfig(), queryResult, "把订单明细列出来",
                        null, queryResult.getQuerySql(), parseInfo, datasetInfo);
        List<String> vizTypes = toVizTypes(candidates);
        System.out.println("detail ranked candidates = " + vizTypes);

        Assertions.assertFalse(candidates.isEmpty());
        Assertions.assertEquals("table", candidates.get(0).getVizType(), "明细场景应优先选择 table");
    }

    private SupersetPluginConfig buildLlmConfig() {
        SupersetPluginConfig config = new SupersetPluginConfig();
        config.setVizTypeLlmEnabled(true);
        config.setVizTypeLlmTopN(3);
        config.setVizTypeLlmChatModelId(chatModelId);
        return config;
    }

    private ChatModelConfig loadChatModelConfig(Integer modelId, String password) throws Exception {
        String host = firstNonBlank(System.getProperty("superset.it.db.host"),
                System.getenv("S2_DB_HOST"), DEFAULT_DB_HOST);
        String port = firstNonBlank(System.getProperty("superset.it.db.port"),
                System.getenv("S2_DB_PORT"), DEFAULT_DB_PORT);
        String database = firstNonBlank(System.getProperty("superset.it.db.name"),
                System.getenv("S2_DB_DATABASE"), DEFAULT_DB_NAME);
        String user = firstNonBlank(System.getProperty("superset.it.db.user"),
                System.getenv("S2_DB_USER"), DEFAULT_DB_USER);
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s?stringtype=unspecified", host,
                port, database);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                PreparedStatement statement = connection
                        .prepareStatement("select config from s2_chat_model where id = ?")) {
            statement.setInt(1, modelId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return JsonUtil.toObject(resultSet.getString(1), ChatModelConfig.class);
            }
        }
    }

    private ChatModelConfig loadChatModelConfigFromProperty() {
        String encoded = firstNonBlank(System.getProperty("superset.it.chatModelConfigBase64"),
                System.getenv("SUPERSET_IT_CHAT_MODEL_CONFIG_B64"));
        if (StringUtils.isBlank(encoded)) {
            return null;
        }
        String json = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        return JsonUtil.toObject(json, ChatModelConfig.class);
    }

    private static void registerModelFactories() throws Exception {
        new OpenAiModelFactory().afterPropertiesSet();
        new OllamaModelFactory().afterPropertiesSet();
        new DifyModelFactory().afterPropertiesSet();
    }

    private static ApplicationContext currentContext() throws Exception {
        Field field = ContextUtils.class.getDeclaredField("context");
        field.setAccessible(true);
        return (ApplicationContext) field.get(null);
    }

    private static void setContext(ApplicationContext context) throws Exception {
        Field field = ContextUtils.class.getDeclaredField("context");
        field.setAccessible(true);
        field.set(null, context);
    }

    private static QueryColumn buildQueryColumn(String name, String bizName, String showType,
            String type) {
        QueryColumn column = new QueryColumn();
        column.setName(name);
        column.setBizName(bizName);
        column.setShowType(showType);
        column.setType(type);
        return column;
    }

    private static SchemaElement schemaElement(String name, String bizName,
            SchemaElementType type) {
        return SchemaElement.builder().name(name).bizName(bizName).type(type).build();
    }

    private static SupersetDatasetColumn buildDatasetColumn(String name, String type,
            boolean groupby, boolean isDttm) {
        SupersetDatasetColumn column = new SupersetDatasetColumn();
        column.setColumnName(name);
        column.setType(type);
        column.setGroupby(groupby);
        column.setIsDttm(isDttm);
        column.setFilterable(true);
        return column;
    }

    private static SupersetDatasetMetric buildMetric(String name, String expression) {
        SupersetDatasetMetric metric = new SupersetDatasetMetric();
        metric.setMetricName(name);
        metric.setExpression(expression);
        metric.setMetricType("DOUBLE");
        return metric;
    }

    private static List<String> toVizTypes(List<SupersetVizTypeSelector.VizTypeItem> candidates) {
        return candidates.stream().map(SupersetVizTypeSelector.VizTypeItem::getVizType)
                .collect(Collectors.toList());
    }

    private static Map<String, Object> row(Object... values) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            row.put(String.valueOf(values[i]), values[i + 1]);
        }
        return row;
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

    private static class StubChatModelService implements ChatModelService {

        private final Map<Integer, ChatModel> chatModels = new LinkedHashMap<>();

        StubChatModelService(Integer chatModelId, ChatModelConfig config) {
            ChatModel chatModel = new ChatModel();
            chatModel.setId(chatModelId);
            chatModel.setConfig(config);
            chatModels.put(chatModelId, chatModel);
        }

        @Override
        public List<ChatModel> getChatModels(User user) {
            return new ArrayList<>(chatModels.values());
        }

        @Override
        public ChatModel getChatModel(Integer id) {
            return chatModels.get(id);
        }

        @Override
        public ChatModel createChatModel(ChatModel chatModel, User user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChatModel updateChatModel(ChatModel chatModel, User user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteChatModel(Integer id, User user) {
            throw new UnsupportedOperationException();
        }
    }
}
