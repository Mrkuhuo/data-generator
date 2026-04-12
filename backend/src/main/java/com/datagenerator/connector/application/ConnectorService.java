package com.datagenerator.connector.application;

import com.datagenerator.connector.api.ConnectorUpsertRequest;
import com.datagenerator.connector.api.ConnectorTestResponse;
import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorRole;
import com.datagenerator.connector.domain.ConnectorStatus;
import com.datagenerator.connector.domain.ConnectorType;
import com.datagenerator.connector.repository.ConnectorInstanceRepository;
import com.datagenerator.connector.spi.ConnectorRegistry;
import com.datagenerator.connector.spi.ConnectorTestResult;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Sort;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ConnectorService {

    private final ConnectorInstanceRepository connectorRepository;
    private final ConnectorRegistry connectorRegistry;
    private final String quickstartHttpBaseUrl;
    private final String quickstartMysqlJdbcUrl;
    private final String quickstartPostgresqlJdbcUrl;
    private final String quickstartKafkaBootstrapServers;

    public ConnectorService(
            ConnectorInstanceRepository connectorRepository,
            ConnectorRegistry connectorRegistry,
            @Value("${mdg.quickstart.http-base-url:http://localhost:9000/mock/intake}") String quickstartHttpBaseUrl,
            @Value("${mdg.quickstart.mysql-jdbc-url:jdbc:mysql://localhost:3306/demo_sink?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai}") String quickstartMysqlJdbcUrl,
            @Value("${mdg.quickstart.postgresql-jdbc-url:jdbc:postgresql://localhost:5432/demo_sink}") String quickstartPostgresqlJdbcUrl,
            @Value("${mdg.quickstart.kafka-bootstrap-servers:localhost:9092}") String quickstartKafkaBootstrapServers
    ) {
        this.connectorRepository = connectorRepository;
        this.connectorRegistry = connectorRegistry;
        this.quickstartHttpBaseUrl = quickstartHttpBaseUrl;
        this.quickstartMysqlJdbcUrl = quickstartMysqlJdbcUrl;
        this.quickstartPostgresqlJdbcUrl = quickstartPostgresqlJdbcUrl;
        this.quickstartKafkaBootstrapServers = quickstartKafkaBootstrapServers;
    }

    public List<ConnectorInstance> findAll() {
        return connectorRepository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt"));
    }

    public ConnectorInstance findById(Long id) {
        return connectorRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("未找到连接器：" + id));
    }

    @Transactional
    public ConnectorInstance create(ConnectorUpsertRequest request) {
        ConnectorInstance connector = new ConnectorInstance();
        apply(connector, request);
        return connectorRepository.save(connector);
    }

    @Transactional
    public ConnectorInstance update(Long id, ConnectorUpsertRequest request) {
        ConnectorInstance connector = findById(id);
        apply(connector, request);
        return connectorRepository.save(connector);
    }

    @Transactional
    public ConnectorTestResponse markTested(Long id) {
        ConnectorInstance connector = findById(id);
        ConnectorTestResult result = connectorRegistry.test(connector);
        connector.setLastTestStatus(result.status());
        connector.setLastTestMessage(result.message());
        connector.setLastTestDetailsJson(result.detailsJson());
        connector.setLastTestedAt(Instant.now());
        ConnectorInstance saved = connectorRepository.save(connector);
        return new ConnectorTestResponse(
                saved.getId(),
                result.success(),
                result.status(),
                result.message(),
                result.detailsJson()
        );
    }

    @Transactional
    public void delete(Long id) {
        connectorRepository.deleteById(id);
    }

    public long count() {
        return connectorRepository.count();
    }

    @Transactional
    public ConnectorInstance createExampleFileConnector() {
        ConnectorInstance connector = new ConnectorInstance();
        connector.setName("示例文件输出");
        connector.setConnectorType(ConnectorType.FILE);
        connector.setConnectorRole(ConnectorRole.TARGET);
        connector.setStatus(ConnectorStatus.READY);
        connector.setDescription("把预览生成的数据写入本地 JSONL 文件的快速示例连接器。");
        connector.setConfigJson("""
                {
                  "path": "./output/example-preview.jsonl",
                  "format": "jsonl"
                }
                """);
        return connectorRepository.save(connector);
    }

    @Transactional
    public ConnectorInstance createExampleHttpConnector() {
        ConnectorInstance connector = new ConnectorInstance();
        connector.setName("示例 HTTP 输出");
        connector.setConnectorType(ConnectorType.HTTP);
        connector.setConnectorRole(ConnectorRole.TARGET);
        connector.setStatus(ConnectorStatus.READY);
        connector.setDescription("把生成数据通过 HTTP 请求投递到目标接口的快速示例连接器。");
        connector.setConfigJson("""
                {
                  "url": "%s",
                  "method": "POST",
                  "batch": false,
                  "timeoutMs": 5000
                }
                """.formatted(quickstartHttpBaseUrl));
        return connectorRepository.save(connector);
    }

    @Transactional
    public ConnectorInstance createExampleMysqlConnector() {
        ConnectorInstance connector = new ConnectorInstance();
        connector.setName("示例 MySQL 输出");
        connector.setConnectorType(ConnectorType.MYSQL);
        connector.setConnectorRole(ConnectorRole.TARGET);
        connector.setStatus(ConnectorStatus.READY);
        connector.setDescription("把生成数据写入 MySQL 表的快速示例连接器，目标表由任务运行时配置 target.table 指定。");
        connector.setConfigJson("""
                {
                  "jdbcUrl": "%s",
                  "username": "root",
                  "password": "root"
                }
                """.formatted(quickstartMysqlJdbcUrl));
        return connectorRepository.save(connector);
    }

    @Transactional
    public ConnectorInstance createExamplePostgresqlConnector() {
        ConnectorInstance connector = new ConnectorInstance();
        connector.setName("示例 PostgreSQL 输出");
        connector.setConnectorType(ConnectorType.POSTGRESQL);
        connector.setConnectorRole(ConnectorRole.TARGET);
        connector.setStatus(ConnectorStatus.READY);
        connector.setDescription("把生成数据写入 PostgreSQL 表的快速示例连接器，目标表由任务运行时配置 target.table 指定。");
        connector.setConfigJson("""
                {
                  "jdbcUrl": "%s",
                  "username": "postgres",
                  "password": "postgres"
                }
                """.formatted(quickstartPostgresqlJdbcUrl));
        return connectorRepository.save(connector);
    }

    @Transactional
    public ConnectorInstance createExampleKafkaConnector() {
        ConnectorInstance connector = new ConnectorInstance();
        connector.setName("示例 Kafka 输出");
        connector.setConnectorType(ConnectorType.KAFKA);
        connector.setConnectorRole(ConnectorRole.TARGET);
        connector.setStatus(ConnectorStatus.READY);
        connector.setDescription("把每条生成数据作为一条 JSON 消息发送到 Kafka Topic 的快速示例连接器。");
        connector.setConfigJson("""
                {
                  "bootstrapServers": "%s",
                  "acks": "all",
                  "clientId": "mdg-local"
                }
                """.formatted(quickstartKafkaBootstrapServers));
        return connectorRepository.save(connector);
    }

    private void apply(ConnectorInstance connector, ConnectorUpsertRequest request) {
        connector.setName(request.name());
        connector.setConnectorType(request.connectorType());
        connector.setConnectorRole(request.connectorRole());
        connector.setStatus(request.status());
        connector.setDescription(request.description());
        connector.setConfigJson(request.configJson());
    }
}
