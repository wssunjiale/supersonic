package com.tencent.supersonic.headless.chat.mapper;

import com.google.common.collect.Sets;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementMatch;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.request.QueryNLReq;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class MapFilterTest {

    @Test
    void filterShouldKeepTwoCharacterMetricSelectedByLlm() {
        QueryNLReq request = new QueryNLReq();
        request.setQueryText("利润最高的品牌top 3");
        request.setDataSetIds(Sets.newHashSet(2L));

        ChatQueryContext queryCtx = new ChatQueryContext(request);
        queryCtx.getMapInfo().setMatchedElements(2L, new ArrayList<>(List.of(
                match(dataset(2L, "企业数据集"), 0.9, true),
                match(metric(2L, 9L, "利润"), 0.9, true),
                match(dimension(2L, 14L, "品牌名称"), 0.9, true))));

        MapFilter.filter(queryCtx);

        Assertions.assertEquals(List.of("企业数据集", "利润", "品牌名称"),
                queryCtx.getMapInfo().getMatchedElements(2L).stream()
                        .map(match -> match.getElement().getName()).toList());
    }

    private static SchemaElementMatch match(SchemaElement element, double similarity,
            boolean llmMatched) {
        return SchemaElementMatch.builder().element(element).detectWord("利润最高的品牌top 3")
                .word(element.getName()).similarity(similarity).llmMatched(llmMatched).build();
    }

    private static SchemaElement dataset(Long id, String name) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setDataSetId(id);
        schemaElement.setId(id);
        schemaElement.setName(name);
        schemaElement.setType(SchemaElementType.DATASET);
        return schemaElement;
    }

    private static SchemaElement metric(Long dataSetId, Long id, String name) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setDataSetId(dataSetId);
        schemaElement.setId(id);
        schemaElement.setName(name);
        schemaElement.setType(SchemaElementType.METRIC);
        return schemaElement;
    }

    private static SchemaElement dimension(Long dataSetId, Long id, String name) {
        SchemaElement schemaElement = new SchemaElement();
        schemaElement.setDataSetId(dataSetId);
        schemaElement.setId(id);
        schemaElement.setName(name);
        schemaElement.setType(SchemaElementType.DIMENSION);
        return schemaElement;
    }
}
