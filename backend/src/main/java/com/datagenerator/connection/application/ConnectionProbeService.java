package com.datagenerator.connection.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.api.ConnectionTestResponse;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.connection.repository.TargetConnectionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ConnectionProbeService {

    private final ConnectionJdbcSupport jdbcSupport;
    private final KafkaConnectionSupport kafkaConnectionSupport;
    private final TargetConnectionRepository repository;
    private final ObjectMapper objectMapper;

    public ConnectionProbeService(
            ConnectionJdbcSupport jdbcSupport,
            KafkaConnectionSupport kafkaConnectionSupport,
            TargetConnectionRepository repository,
            ObjectMapper objectMapper
    ) {
        this.jdbcSupport = jdbcSupport;
        this.kafkaConnectionSupport = kafkaConnectionSupport;
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ConnectionTestResponse test(TargetConnection connection) {
        return execute(connection, true);
    }

    public ConnectionTestResponse probe(TargetConnection connection) {
        return execute(connection, false);
    }

    private ConnectionTestResponse execute(TargetConnection connection, boolean persistResult) {
        if (connection.getDbType() == DatabaseType.KAFKA) {
            return executeKafka(connection, persistResult);
        }

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("jdbcUrl", jdbcSupport.buildJdbcUrl(connection));
        details.put("dbType", connection.getDbType().name());

        try (Connection jdbcConnection = jdbcSupport.open(connection)) {
            DatabaseMetaData metadata = jdbcConnection.getMetaData();
            details.put("productName", metadata.getDatabaseProductName());
            details.put("productVersion", metadata.getDatabaseProductVersion());
            details.put("catalog", jdbcConnection.getCatalog());
            details.put("schema", jdbcSupport.defaultSchema(connection));

            String detailsJson = writeJson(details);
            persist(connection, persistResult, "READY", "连接成功", detailsJson);
            return new ConnectionTestResponse(connection.getId(), true, "READY", "连接成功", detailsJson);
        } catch (Exception exception) {
            details.put("error", exception.getMessage());
            String detailsJson = writeJson(details);
            persist(connection, persistResult, "FAILED", exception.getMessage(), detailsJson);
            return new ConnectionTestResponse(connection.getId(), false, "FAILED", exception.getMessage(), detailsJson);
        }
    }

    private ConnectionTestResponse executeKafka(TargetConnection connection, boolean persistResult) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("dbType", connection.getDbType().name());
        details.put("bootstrapServers", kafkaConnectionSupport.bootstrapServers(connection));

        try (AdminClient adminClient = AdminClient.create(kafkaConnectionSupport.buildAdminProperties(connection))) {
            details.put("clusterId", adminClient.describeCluster().clusterId().get());
            details.put("nodeCount", adminClient.describeCluster().nodes().get().size());
            details.put("topicCount", adminClient.listTopics().names().get().size());

            String detailsJson = writeJson(details);
            persist(connection, persistResult, "READY", "Kafka 连接成功", detailsJson);
            return new ConnectionTestResponse(connection.getId(), true, "READY", "Kafka 连接成功", detailsJson);
        } catch (Exception exception) {
            details.put("error", exception.getMessage());
            String detailsJson = writeJson(details);
            persist(connection, persistResult, "FAILED", exception.getMessage(), detailsJson);
            return new ConnectionTestResponse(connection.getId(), false, "FAILED", exception.getMessage(), detailsJson);
        }
    }

    private void persist(
            TargetConnection connection,
            boolean persistResult,
            String status,
            String message,
            String detailsJson
    ) {
        if (!persistResult) {
            return;
        }
        connection.setLastTestStatus(status);
        connection.setLastTestMessage(message);
        connection.setLastTestDetailsJson(detailsJson);
        connection.setLastTestedAt(Instant.now());
        repository.save(connection);
    }

    private String writeJson(Map<String, Object> details) {
        try {
            return objectMapper.writeValueAsString(details);
        } catch (Exception exception) {
            return JsonConfigSupport.writeJson(Map.of("error", "连接测试详情序列化失败"), "连接测试详情序列化失败");
        }
    }
}
