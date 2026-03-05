package com.tencent.supersonic.chat.server.processor.execute;

import com.tencent.supersonic.chat.api.pojo.request.ChatExecuteReq;
import com.tencent.supersonic.chat.api.pojo.response.QueryResult;
import com.tencent.supersonic.chat.server.pojo.ExecuteContext;
import com.tencent.supersonic.headless.api.pojo.response.QueryState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class DataInterpretProcessorTest {

    @Test
    public void testNoDataSummaryChinese() {
        DataInterpretProcessor processor = new DataInterpretProcessor();

        ExecuteContext executeContext =
                new ExecuteContext(ChatExecuteReq.builder().queryText("近3年各产品的销售总额是多少").build());
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryState(QueryState.SUCCESS);
        queryResult.setQueryResults(Collections.emptyList());
        executeContext.setResponse(queryResult);

        processor.process(executeContext);

        Assertions.assertNotNull(queryResult.getTextSummary());
        Assertions.assertTrue(queryResult.getTextSummary().contains("未返回"));
    }

    @Test
    public void testNoDataSummaryEnglish() {
        DataInterpretProcessor processor = new DataInterpretProcessor();

        ExecuteContext executeContext = new ExecuteContext(ChatExecuteReq.builder()
                .queryText("What is the total sales of each product in last 3 years?").build());
        QueryResult queryResult = new QueryResult();
        queryResult.setQueryState(QueryState.SUCCESS);
        queryResult.setQueryResults(Collections.emptyList());
        executeContext.setResponse(queryResult);

        processor.process(executeContext);

        Assertions.assertNotNull(queryResult.getTextSummary());
        Assertions
                .assertTrue(queryResult.getTextSummary().startsWith("This query returned no data"));
    }
}
