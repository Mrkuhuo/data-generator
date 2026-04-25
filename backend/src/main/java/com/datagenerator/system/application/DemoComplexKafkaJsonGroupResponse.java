package com.datagenerator.system.application;

public record DemoComplexKafkaJsonGroupResponse(
        Long connectionId,
        String connectionName,
        Long groupId,
        String groupName,
        Long seed,
        String parentTopic,
        String childTopic,
        int taskCount,
        int relationCount
) {
}
