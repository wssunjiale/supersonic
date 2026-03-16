package com.tencent.supersonic.headless.server.sync.superset.semantic;

import com.tencent.supersonic.common.pojo.User;
import lombok.Getter;

@Getter
public class SupersetSemanticDatasetChangedEvent {

    private final Long dataSetId;
    private final User user;

    public SupersetSemanticDatasetChangedEvent(Long dataSetId, User user) {
        this.dataSetId = dataSetId;
        this.user = user;
    }
}
