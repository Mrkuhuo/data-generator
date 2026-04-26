package com.datagenerator.connector.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.connector.application.ConnectorService;
import com.datagenerator.connector.domain.ConnectorInstance;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/connectors")
@ConditionalOnProperty(name = "mdg.legacy.enabled", havingValue = "true")
public class ConnectorController {

    private final ConnectorService connectorService;

    public ConnectorController(ConnectorService connectorService) {
        this.connectorService = connectorService;
    }

    @GetMapping
    public ApiResponse<List<ConnectorInstance>> list() {
        return ApiResponse.success(connectorService.findAll());
    }

    @GetMapping("/{id}")
    public ApiResponse<ConnectorInstance> detail(@PathVariable Long id) {
        return ApiResponse.success(connectorService.findById(id));
    }

    @PostMapping
    public ApiResponse<ConnectorInstance> create(@Valid @RequestBody ConnectorUpsertRequest request) {
        return ApiResponse.success(connectorService.create(request), "连接器已创建");
    }

    @PostMapping("/quickstart/file")
    public ApiResponse<ConnectorInstance> createExampleFileConnector() {
        return ApiResponse.success(connectorService.createExampleFileConnector(), "示例文件连接器已创建");
    }

    @PostMapping("/quickstart/http")
    public ApiResponse<ConnectorInstance> createExampleHttpConnector() {
        return ApiResponse.success(connectorService.createExampleHttpConnector(), "示例 HTTP 连接器已创建");
    }

    @PostMapping("/quickstart/mysql")
    public ApiResponse<ConnectorInstance> createExampleMysqlConnector() {
        return ApiResponse.success(connectorService.createExampleMysqlConnector(), "示例 MySQL 连接器已创建");
    }

    @PostMapping("/quickstart/postgresql")
    public ApiResponse<ConnectorInstance> createExamplePostgresqlConnector() {
        return ApiResponse.success(connectorService.createExamplePostgresqlConnector(), "示例 PostgreSQL 连接器已创建");
    }

    @PostMapping("/quickstart/kafka")
    public ApiResponse<ConnectorInstance> createExampleKafkaConnector() {
        return ApiResponse.success(connectorService.createExampleKafkaConnector(), "示例 Kafka 连接器已创建");
    }

    @PutMapping("/{id}")
    public ApiResponse<ConnectorInstance> update(@PathVariable Long id, @Valid @RequestBody ConnectorUpsertRequest request) {
        return ApiResponse.success(connectorService.update(id, request), "连接器已更新");
    }

    @PostMapping("/{id}/test")
    public ApiResponse<ConnectorTestResponse> test(@PathVariable Long id) {
        return ApiResponse.success(connectorService.markTested(id), "连接器测试已执行");
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        connectorService.delete(id);
        return ApiResponse.success(null, "连接器已删除");
    }
}
