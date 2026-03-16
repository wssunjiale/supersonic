package com.tencent.supersonic.chat.server.executor;

import com.tencent.supersonic.headless.api.pojo.response.QueryState;
import com.tencent.supersonic.headless.api.pojo.response.SemanticQueryResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlExecutorTest {

    @Test
    public void resolveQueryStateShouldReturnSuccessWhenExecutionHasNoError() {
        Assertions.assertEquals(QueryState.SUCCESS,
                SqlExecutor.resolveQueryState(new SemanticQueryResp()));
    }

    @Test
    public void resolveQueryStateShouldReturnInvalidWhenExecutionReportsError() {
        SemanticQueryResp queryResp = new SemanticQueryResp();
        queryResp.setErrorMsg("bad SQL grammar");

        Assertions.assertEquals(QueryState.INVALID, SqlExecutor.resolveQueryState(queryResp));
    }

    @Test
    public void resolveQueryStateShouldReturnInvalidForNullResponse() {
        Assertions.assertEquals(QueryState.INVALID, SqlExecutor.resolveQueryState(null));
    }
}
