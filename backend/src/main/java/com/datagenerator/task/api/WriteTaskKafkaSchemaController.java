package com.datagenerator.task.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.task.application.KafkaSchemaImportService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/write-tasks/kafka/schema")
public class WriteTaskKafkaSchemaController {

    private final KafkaSchemaImportService importService;

    public WriteTaskKafkaSchemaController(KafkaSchemaImportService importService) {
        this.importService = importService;
    }

    @PostMapping("/example")
    public ApiResponse<KafkaSchemaImportResponse> importExample(@Valid @RequestBody KafkaSchemaImportRequest request) {
        return ApiResponse.success(importService.importExampleJson(request.content()), "已根据示例 JSON 生成消息结构");
    }

    @PostMapping("/json-schema")
    public ApiResponse<KafkaSchemaImportResponse> importJsonSchema(@Valid @RequestBody KafkaSchemaImportRequest request) {
        return ApiResponse.success(importService.importJsonSchema(request.content()), "已根据 JSON Schema 生成消息结构");
    }
}
