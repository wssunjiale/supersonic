package com.tencent.supersonic.chat.server.parser;

import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.SqlInfo;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMSqlQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NL2SQLParserTest {

    @Test
    public void containsTranslatorUnsafeWindowAggShouldDetectAggregateInsideWindowOrderBy() {
        SemanticParseInfo parseInfo = buildParseInfo(
                "WITH department_visits AS (SELECT department, count(1) AS _总访问次数, "
                        + "ROW_NUMBER() OVER (ORDER BY SUM(pv) DESC) AS _排名 "
                        + "FROM t_1 GROUP BY department) "
                        + "SELECT department, _总访问次数, _排名 FROM department_visits");

        Assertions.assertTrue(NL2SQLParser.containsTranslatorUnsafeWindowAgg(parseInfo));
    }

    @Test
    public void containsTranslatorUnsafeWindowAggShouldAllowRankingByProjectedAlias() {
        SemanticParseInfo parseInfo = buildParseInfo(
                "WITH department_visits AS (SELECT department, count(1) AS _总访问次数 "
                        + "FROM t_1 GROUP BY department), "
                        + "ranked_departments AS (SELECT department, _总访问次数, "
                        + "ROW_NUMBER() OVER (ORDER BY _总访问次数 DESC) AS _排名 "
                        + "FROM department_visits) "
                        + "SELECT department, _总访问次数, _排名 FROM ranked_departments");

        Assertions.assertFalse(NL2SQLParser.containsTranslatorUnsafeWindowAgg(parseInfo));
    }

    private SemanticParseInfo buildParseInfo(String querySql) {
        SemanticParseInfo parseInfo = new SemanticParseInfo();
        parseInfo.setQueryMode(LLMSqlQuery.QUERY_MODE);
        SqlInfo sqlInfo = new SqlInfo();
        sqlInfo.setQuerySQL(querySql);
        parseInfo.setSqlInfo(sqlInfo);
        return parseInfo;
    }
}
