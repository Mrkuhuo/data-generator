package com.datagenerator.connection.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.connection.application.TargetConnectionService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/connections")
public class TargetConnectionController {

    private final TargetConnectionService service;

    public TargetConnectionController(TargetConnectionService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<TargetConnectionResponse>> list() {
        return ApiResponse.success(service.findAll().stream().map(TargetConnectionResponse::from).toList());
    }

    @GetMapping("/{id}")
    public ApiResponse<TargetConnectionResponse> detail(@PathVariable Long id) {
        return ApiResponse.success(TargetConnectionResponse.from(service.findById(id)));
    }

    @PostMapping
    public ApiResponse<TargetConnectionResponse> create(@Valid @RequestBody TargetConnectionUpsertRequest request) {
        return ApiResponse.success(TargetConnectionResponse.from(service.create(request)), "目标连接已创建");
    }

    @PutMapping("/{id}")
    public ApiResponse<TargetConnectionResponse> update(@PathVariable Long id, @Valid @RequestBody TargetConnectionUpsertRequest request) {
        return ApiResponse.success(TargetConnectionResponse.from(service.update(id, request)), "目标连接已更新");
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        service.delete(id);
        return ApiResponse.success(null, "目标连接已删除");
    }

    @PostMapping("/{id}/test")
    public ApiResponse<ConnectionTestResponse> test(@PathVariable Long id) {
        return ApiResponse.success(service.test(id), "目标连接测试已执行");
    }

    @PostMapping("/test")
    public ApiResponse<ConnectionTestResponse> testDraft(@Valid @RequestBody TargetConnectionDraftTestRequest request) {
        return ApiResponse.success(service.testDraft(request), "当前配置连接测试已完成");
    }

    @GetMapping("/{id}/tables")
    public ApiResponse<List<DatabaseTableResponse>> listTables(@PathVariable Long id) {
        return ApiResponse.success(service.listTables(id));
    }

    @GetMapping("/{id}/table-columns")
    public ApiResponse<List<DatabaseColumnResponse>> listColumns(
            @PathVariable Long id,
            @RequestParam String tableName
    ) {
        return ApiResponse.success(service.listColumns(id, tableName));
    }

    @GetMapping("/{id}/table-schema")
    public ApiResponse<DatabaseTableSchemaResponse> describeTable(
            @PathVariable Long id,
            @RequestParam String tableName
    ) {
        return ApiResponse.success(service.describeTable(id, tableName));
    }

    @GetMapping("/{id}/schema-model")
    public ApiResponse<DatabaseModelResponse> describeModel(
            @PathVariable Long id,
            @RequestParam List<String> tableNames
    ) {
        return ApiResponse.success(service.describeModel(id, tableNames));
    }
}
