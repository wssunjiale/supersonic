package com.tencent.supersonic.headless.server.sync.superset;

import com.tencent.supersonic.common.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class SupersetSyncApiClientTest {

    @Test
    void createDatasetDoesNotSendDescription() throws Exception {
        SupersetSyncApiClient client = new SupersetSyncApiClient(buildProperties());
        MockRestServiceServer server = bindServer(client);
        SupersetDatasetInfo datasetInfo = buildDatasetInfo();

        server.expect(requestTo("http://localhost:8088/api/v1/dataset/"))
                .andExpect(method(HttpMethod.POST)).andExpect(request -> {
                    Map<String, Object> payload = readPayload(request);
                    Assertions.assertFalse(payload.containsKey("description"));
                    Assertions.assertEquals(11L, ((Number) payload.get("database")).longValue());
                    Assertions.assertEquals("public", payload.get("schema"));
                    Assertions.assertEquals("chat_dataset", payload.get("table_name"));
                    Assertions.assertEquals("select 1", payload.get("sql"));
                    Assertions.assertEquals("{}", payload.get("template_params"));
                }).andRespond(withSuccess("{\"id\":123}", MediaType.APPLICATION_JSON));

        Assertions.assertEquals(Long.valueOf(123), client.createDataset(datasetInfo));
        server.verify();
    }

    @Test
    void updateDatasetDoesNotSendTopLevelDescription() throws Exception {
        SupersetSyncApiClient client = new SupersetSyncApiClient(buildProperties());
        MockRestServiceServer server = bindServer(client);
        SupersetDatasetInfo datasetInfo = buildDatasetInfo();
        datasetInfo.setMainDttmCol("event_time");

        server.expect(requestTo("http://localhost:8088/api/v1/dataset/123"))
                .andExpect(method(HttpMethod.PUT)).andExpect(request -> {
                    Map<String, Object> payload = readPayload(request);
                    Assertions.assertEquals(11L, ((Number) payload.get("database_id")).longValue());
                    Assertions.assertFalse(payload.containsKey("description"));
                    Assertions.assertEquals("event_time", payload.get("main_dttm_col"));
                }).andRespond(withSuccess("{}", MediaType.APPLICATION_JSON));

        client.updateDataset(123L, datasetInfo);
        server.verify();
    }

    private MockRestServiceServer bindServer(SupersetSyncApiClient client) {
        RestTemplate restTemplate =
                (RestTemplate) ReflectionTestUtils.getField(client, "restTemplate");
        return MockRestServiceServer.bindTo(restTemplate).build();
    }

    private SupersetSyncProperties buildProperties() {
        SupersetSyncProperties properties = new SupersetSyncProperties();
        properties.setBaseUrl("http://localhost:8088");
        properties.setAuthEnabled(false);
        return properties;
    }

    private SupersetDatasetInfo buildDatasetInfo() {
        SupersetDatasetInfo datasetInfo = new SupersetDatasetInfo();
        datasetInfo.setDatabaseId(11L);
        datasetInfo.setSchema("public");
        datasetInfo.setTableName("chat_dataset");
        datasetInfo.setDescription("chat dataset");
        datasetInfo.setSql("select 1");
        return datasetInfo;
    }

    private Map<String, Object> readPayload(
            org.springframework.http.client.ClientHttpRequest request) {
        String body = ((MockClientHttpRequest) request).getBodyAsString();
        return JsonUtil.toMap(body, String.class, Object.class);
    }
}
