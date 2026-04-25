package com.datagenerator.system.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.system.application.DemoComplexKafkaJsonGroupResponse;
import com.datagenerator.system.application.DemoDataRebuildResponse;
import com.datagenerator.system.application.DemoDataRebuildService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/system/demo")
public class DemoDataController {

    private static final String REBUILD_SUCCESS_MESSAGE = "\u6f14\u793a\u6570\u636e\u5df2\u91cd\u5efa";
    private static final String COMPLEX_KAFKA_GROUP_SUCCESS_MESSAGE =
            "\u590d\u6742\u7236\u5b50 JSON \u5173\u7cfb\u4efb\u52a1\u5df2\u521b\u5efa";
    private static final String PAYLOAD_KAFKA_GROUP_SUCCESS_MESSAGE =
            "\u8d1f\u8f7d\u7236\u5b50 JSON \u5173\u7cfb\u4efb\u52a1\u5df2\u521b\u5efa";

    private final DemoDataRebuildService demoDataRebuildService;

    public DemoDataController(DemoDataRebuildService demoDataRebuildService) {
        this.demoDataRebuildService = demoDataRebuildService;
    }

    @PostMapping("/rebuild")
    public ApiResponse<DemoDataRebuildResponse> rebuild(
            @RequestParam(required = false) Long sourceConnectionId
    ) {
        return ApiResponse.success(
                demoDataRebuildService.rebuild(sourceConnectionId),
                REBUILD_SUCCESS_MESSAGE
        );
    }

    @PostMapping("/kafka-complex-group")
    public ApiResponse<DemoComplexKafkaJsonGroupResponse> createComplexKafkaJsonGroup(
            @RequestParam(required = false) Long connectionId
    ) {
        return ApiResponse.success(
                demoDataRebuildService.createComplexKafkaJsonGroup(connectionId),
                COMPLEX_KAFKA_GROUP_SUCCESS_MESSAGE
        );
    }

    @PostMapping("/kafka-payload-group")
    public ApiResponse<DemoComplexKafkaJsonGroupResponse> createPayloadKafkaJsonGroup(
            @RequestParam(required = false) Long connectionId
    ) {
        return ApiResponse.success(
                demoDataRebuildService.createPayloadKafkaJsonGroup(connectionId),
                PAYLOAD_KAFKA_GROUP_SUCCESS_MESSAGE
        );
    }
}
