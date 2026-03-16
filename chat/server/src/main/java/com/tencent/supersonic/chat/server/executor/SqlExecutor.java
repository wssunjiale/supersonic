package com.tencent.supersonic.chat.server.executor;

import com.tencent.supersonic.chat.api.pojo.enums.MemoryStatus;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.chat.server.pojo.ChatContext;
import com.tencent.supersonic.chat.server.pojo.ChatMemory;
import com.tencent.supersonic.chat.server.pojo.ExecuteContext;
import com.tencent.supersonic.chat.server.service.ChatContextService;
import com.tencent.supersonic.chat.server.service.MemoryService;
import com.tencent.supersonic.chat.server.util.ResultFormatter;
import com.tencent.supersonic.common.pojo.QueryColumn;
import com.tencent.supersonic.common.pojo.Text2SQLExemplar;
import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.common.util.JsonUtil;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.request.QuerySqlReq;
import com.tencent.supersonic.headless.api.pojo.response.QueryState;
import com.tencent.supersonic.headless.api.pojo.response.SemanticQueryResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticTranslateResp;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMSqlQuery;
import com.tencent.supersonic.headless.server.facade.service.SemanticLayerService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class SqlExecutor implements ChatQueryExecutor {

    @Override
    public boolean accept(ExecuteContext executeContext) {
        return true;
    }

    @SneakyThrows
    @Override
    public QueryResult execute(ExecuteContext executeContext) {
        QueryResult queryResult = doExecute(executeContext);

        if (queryResult != null) {
            String textResult = ResultFormatter.transform2TextNew(queryResult.getQueryColumns(),
                    queryResult.getQueryResults());
            queryResult.setTextResult(textResult);

            if (queryResult.getQueryState().equals(QueryState.SUCCESS)
                    && queryResult.getQueryMode().equals(LLMSqlQuery.QUERY_MODE)) {
                Text2SQLExemplar exemplar =
                        JsonUtil.toObject(
                                JsonUtil.toString(executeContext.getParseInfo().getProperties()
                                        .get(Text2SQLExemplar.PROPERTY_KEY)),
                                Text2SQLExemplar.class);

                MemoryService memoryService = ContextUtils.getBean(MemoryService.class);
                memoryService.createMemory(ChatMemory.builder().queryId(queryResult.getQueryId())
                        .agentId(executeContext.getAgent().getId()).status(MemoryStatus.PENDING)
                        .question(exemplar.getQuestion()).sideInfo(exemplar.getSideInfo())
                        .dbSchema(exemplar.getDbSchema()).s2sql(exemplar.getSql())
                        .createdBy(executeContext.getRequest().getUser().getName())
                        .updatedBy(executeContext.getRequest().getUser().getName())
                        .createdAt(new Date()).build());
            }
        }

        return queryResult;
    }

    @SneakyThrows
    private QueryResult doExecute(ExecuteContext executeContext) {
        SemanticLayerService semanticLayer = ContextUtils.getBean(SemanticLayerService.class);
        ChatContextService chatContextService = ContextUtils.getBean(ChatContextService.class);

        ChatContext chatCtx =
                chatContextService.getOrCreateContext(executeContext.getRequest().getChatId());
        SemanticParseInfo parseInfo = executeContext.getParseInfo();
        if (Objects.isNull(parseInfo.getSqlInfo())
                || StringUtils.isBlank(parseInfo.getSqlInfo().getCorrectedS2SQL())) {
            return null;
        }

        if (shouldSkipSqlExecution(executeContext, parseInfo)) {
            QueryResult queryResult = buildSupersetPlaceholderResult(executeContext, parseInfo,
                    semanticLayer, chatCtx, chatContextService);
            if (queryResult != null) {
                return queryResult;
            }
        }

        // 使用querySQL，它已经包含了所有修正（包括物理SQL修正）
        String finalSql = StringUtils.isNotBlank(parseInfo.getSqlInfo().getQuerySQL())
                ? parseInfo.getSqlInfo().getQuerySQL()
                : parseInfo.getSqlInfo().getCorrectedS2SQL();

        QuerySqlReq sqlReq = QuerySqlReq.builder().sql(finalSql).build();

        sqlReq.setSqlInfo(parseInfo.getSqlInfo());
        sqlReq.setDataSetId(parseInfo.getDataSetId());

        long startTime = System.currentTimeMillis();
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryId(executeContext.getRequest().getQueryId());
        queryResult.setChatContext(parseInfo);
        queryResult.setQueryMode(parseInfo.getQueryMode());
        SemanticQueryResp queryResp =
                semanticLayer.queryByReq(sqlReq, executeContext.getRequest().getUser());
        queryResult.setQueryTimeCost(System.currentTimeMillis() - startTime);
        if (queryResp != null) {
            queryResult.setQueryAuthorization(queryResp.getQueryAuthorization());
            queryResult.setQuerySql(finalSql);
            queryResult.setQueryResults(queryResp.getResultList());
            queryResult.setQueryColumns(queryResp.getColumns());
            queryResult.setErrorMsg(queryResp.getErrorMsg());
            QueryState queryState = resolveQueryState(queryResp);
            queryResult.setQueryState(queryState);
            if (QueryState.SUCCESS.equals(queryState)) {
                chatCtx.setParseInfo(parseInfo);
                chatContextService.updateContext(chatCtx);
            }
        } else {
            queryResult.setQueryState(QueryState.INVALID);
        }

        return queryResult;
    }

    static QueryState resolveQueryState(SemanticQueryResp queryResp) {
        if (queryResp == null || StringUtils.isNotBlank(queryResp.getErrorMsg())) {
            return QueryState.INVALID;
        }
        return QueryState.SUCCESS;
    }

    /**
     * 判断是否需要跳过 Supersonic SQL 执行。
     *
     * Args: executeContext: 执行上下文。 parseInfo: 语义解析信息。
     *
     * Returns: true 表示跳过 SQL 执行。
     */
    private boolean shouldSkipSqlExecution(ExecuteContext executeContext,
            SemanticParseInfo parseInfo) {
        return false;
    }

    /**
     * 构建 Superset 绘图用的占位查询结果。
     *
     * Args: executeContext: 执行上下文。 parseInfo: 语义解析信息。 chatCtx: 对话上下文。 chatContextService: 对话上下文服务。
     *
     * Returns: 占位 QueryResult。
     */
    private QueryResult buildSupersetPlaceholderResult(ExecuteContext executeContext,
            SemanticParseInfo parseInfo, SemanticLayerService semanticLayer, ChatContext chatCtx,
            ChatContextService chatContextService) {
        if (executeContext == null || parseInfo == null) {
            return null;
        }
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryId(executeContext.getRequest().getQueryId());
        queryResult.setChatContext(parseInfo);
        queryResult.setQueryMode(parseInfo.getQueryMode());
        String translatedSql = resolveTranslatedSql(executeContext, parseInfo, semanticLayer);
        if (StringUtils.isNotBlank(translatedSql)) {
            queryResult.setQuerySql(translatedSql);
            if (parseInfo.getSqlInfo() != null) {
                parseInfo.getSqlInfo().setQuerySQL(translatedSql);
            }
        }
        queryResult.setQueryResults(Collections.emptyList());
        queryResult.setQueryColumns(buildQueryColumns(parseInfo));
        queryResult.setQueryState(QueryState.SUCCESS);
        queryResult.setQueryTimeCost(0L);
        if (chatCtx != null) {
            chatCtx.setParseInfo(parseInfo);
            chatContextService.updateContext(chatCtx);
        }
        return queryResult;
    }

    private String resolveTranslatedSql(ExecuteContext executeContext, SemanticParseInfo parseInfo,
            SemanticLayerService semanticLayer) {
        if (parseInfo == null || parseInfo.getSqlInfo() == null || semanticLayer == null) {
            return null;
        }
        String s2sql = parseInfo.getSqlInfo().getCorrectedS2SQL();
        if (StringUtils.isBlank(s2sql)) {
            return null;
        }
        QuerySqlReq sqlReq = QuerySqlReq.builder().sql(s2sql).build();
        sqlReq.setDataSetId(parseInfo.getDataSetId());
        try {
            SemanticTranslateResp resp =
                    semanticLayer.translate(sqlReq, executeContext.getRequest() == null ? null
                            : executeContext.getRequest().getUser());
            if (resp == null || !resp.isOk() || StringUtils.isBlank(resp.getQuerySQL())) {
                log.warn("superset translate failed, dataSetId={}, ok={}, sqlPresent={}",
                        parseInfo.getDataSetId(), resp != null && resp.isOk(),
                        resp != null && StringUtils.isNotBlank(resp.getQuerySQL()));
                return null;
            }
            return resp.getQuerySQL();
        } catch (Exception ex) {
            log.warn("superset translate exception, dataSetId={}", parseInfo.getDataSetId(), ex);
            return null;
        }
    }

    /**
     * 基于解析结果构建 QueryColumn 列表。
     *
     * Args: parseInfo: 语义解析信息。
     *
     * Returns: QueryColumn 列表。
     */
    private List<QueryColumn> buildQueryColumns(SemanticParseInfo parseInfo) {
        if (parseInfo == null) {
            return Collections.emptyList();
        }
        Map<String, QueryColumn> columnMap = new LinkedHashMap<>();
        if (parseInfo.getDimensions() != null) {
            for (SchemaElement element : parseInfo.getDimensions()) {
                String name = resolveSchemaElementName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                QueryColumn column = new QueryColumn();
                column.setName(name);
                column.setBizName(name);
                if (element != null && element.isPartitionTime()) {
                    column.setType("DATE");
                    column.setShowType("DATE");
                } else {
                    column.setType("STRING");
                    column.setShowType("CATEGORY");
                }
                columnMap.putIfAbsent(normalizeName(name), column);
            }
        }
        if (parseInfo.getMetrics() != null) {
            for (SchemaElement element : parseInfo.getMetrics()) {
                String name = resolveSchemaElementName(element);
                if (StringUtils.isBlank(name)) {
                    continue;
                }
                QueryColumn column = new QueryColumn();
                column.setName(name);
                column.setBizName(name);
                column.setType("NUMBER");
                column.setShowType("NUMBER");
                columnMap.putIfAbsent(normalizeName(name), column);
            }
        }
        return new ArrayList<>(columnMap.values());
    }

    /**
     * 解析 SchemaElement 的显示名称。
     *
     * Args: element: 语义元素。
     *
     * Returns: 业务名优先的字段名。
     */
    private String resolveSchemaElementName(SchemaElement element) {
        if (element == null) {
            return null;
        }
        if (StringUtils.isNotBlank(element.getBizName())) {
            return element.getBizName();
        }
        return element.getName();
    }

    /**
     * 统一字段名的比较形式。
     *
     * Args: name: 字段名。
     *
     * Returns: 归一化后的字段名。
     */
    private String normalizeName(String name) {
        return StringUtils.lowerCase(StringUtils.trimToEmpty(name));
    }
}
