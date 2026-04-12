package com.datagenerator.connector.spi.impl;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorType;
import com.datagenerator.connector.spi.ConnectorAdapter;
import com.datagenerator.connector.spi.ConnectorConfigSupport;
import com.datagenerator.connector.spi.ConnectorDeliveryRequest;
import com.datagenerator.connector.spi.ConnectorDeliveryResult;
import com.datagenerator.connector.spi.ConnectorTestResult;
import com.datagenerator.connector.spi.DeliveryStatus;
import com.datagenerator.job.domain.JobWriteStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

@Component
public class KafkaConnectorAdapter implements ConnectorAdapter {

    private final ObjectMapper objectMapper;

    public KafkaConnectorAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public ConnectorType supports() {
        return ConnectorType.KAFKA;
    }

    @Override
    public ConnectorTestResult test(ConnectorInstance connector) {
        Map<String, Object> config = ConnectorConfigSupport.readConfig(connector);
        String bootstrapServers = ConnectorConfigSupport.requireString(config, "bootstrapServers");

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("bootstrapServers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(buildCommonProperties(config))) {
            int topicCount = adminClient.listTopics(new ListTopicsOptions().timeoutMs(5000)).names().get().size();
            details.put("topicCount", topicCount);
            return new ConnectorTestResult(true, "READY", "Kafka connection test succeeded", ConnectorConfigSupport.writeDetails(details));
        } catch (Exception exception) {
            details.put("error", exception.getMessage());
            return new ConnectorTestResult(false, "UNREACHABLE", "Kafka connection test failed", ConnectorConfigSupport.writeDetails(details));
        }
    }

    @Override
    public ConnectorDeliveryResult deliver(ConnectorDeliveryRequest request) {
        if (request.job().getWriteStrategy() != JobWriteStrategy.APPEND
                && request.job().getWriteStrategy() != JobWriteStrategy.STREAM) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.FAILED,
                    0,
                    request.rows().size(),
                    "Kafka delivery supports APPEND and STREAM only",
                    ConnectorConfigSupport.writeDetails(Map.of("writeStrategy", request.job().getWriteStrategy()))
            );
        }

        Map<String, Object> connectorConfig = ConnectorConfigSupport.readConfig(request.connector());
        Map<String, Object> runtimeConfig = ConnectorConfigSupport.readConfig(
                request.job().getRuntimeConfigJson(),
                "Job runtime config"
        );
        String bootstrapServers = ConnectorConfigSupport.requireString(connectorConfig, "bootstrapServers");
        String topic = ConnectorConfigSupport.requireString(runtimeConfig, "target.topic", "topic", "targetTopic");
        String keyField = ConnectorConfigSupport.optionalString(runtimeConfig, "target.keyField", "keyField");
        Integer partition = ConnectorConfigSupport.optionalInteger(runtimeConfig, "target.partition", "partition");
        Map<String, Object> headers = readHeaders(runtimeConfig);

        long successCount = 0;
        List<String> errors = new ArrayList<>();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("bootstrapServers", bootstrapServers);
        details.put("topic", topic);
        details.put("writeStrategy", request.job().getWriteStrategy());
        if (keyField != null) {
            details.put("keyField", keyField);
        }
        if (partition != null) {
            details.put("partition", partition);
        }
        if (!headers.isEmpty()) {
            details.put("headers", headers);
        }

        Properties producerProperties = buildProducerProperties(connectorConfig, request.execution().getId());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            for (Map<String, Object> row : request.rows()) {
                try {
                    ProducerRecord<String, String> record = buildRecord(topic, partition, keyField, headers, row);
                    producer.send(record).get();
                    successCount++;
                } catch (Exception exception) {
                    if (errors.size() < 5) {
                        errors.add(exception.getMessage());
                    }
                }
            }
            producer.flush();
        } catch (Exception exception) {
            if (errors.isEmpty()) {
                errors.add(exception.getMessage());
            }
        }

        long errorCount = request.rows().size() - successCount;
        details.put("successCount", successCount);
        details.put("errorCount", errorCount);
        if (!errors.isEmpty()) {
            details.put("errors", errors);
        }

        if (successCount == request.rows().size()) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.SUCCESS,
                    successCount,
                    0,
                    "Delivered " + successCount + " rows to Kafka topic " + topic,
                    ConnectorConfigSupport.writeDetails(details)
            );
        }

        if (successCount > 0) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.PARTIAL_SUCCESS,
                    successCount,
                    errorCount,
                    "Delivered " + successCount + " rows with " + errorCount + " Kafka failures",
                    ConnectorConfigSupport.writeDetails(details)
            );
        }

        return new ConnectorDeliveryResult(
                DeliveryStatus.FAILED,
                0,
                errorCount,
                "Kafka delivery failed",
                ConnectorConfigSupport.writeDetails(details)
        );
    }

    private Properties buildCommonProperties(Map<String, Object> config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConnectorConfigSupport.requireString(config, "bootstrapServers"));
        properties.put("request.timeout.ms", String.valueOf(
                ConnectorConfigSupport.optionalInteger(config, "requestTimeoutMs") == null
                        ? 5000
                        : ConnectorConfigSupport.optionalInteger(config, "requestTimeoutMs")
        ));
        properties.put("connections.max.idle.ms", "5000");
        applyPassthroughProperties(properties, config);
        return properties;
    }

    private Properties buildProducerProperties(Map<String, Object> config, Long executionId) {
        Properties properties = buildCommonProperties(config);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, ConnectorConfigSupport.optionalString(config, "acks") == null
                ? "all"
                : ConnectorConfigSupport.optionalString(config, "acks"));
        properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(
                ConnectorConfigSupport.optionalInteger(config, "retries") == null
                        ? 3
                        : ConnectorConfigSupport.optionalInteger(config, "retries")
        ));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(
                ConnectorConfigSupport.optionalInteger(config, "lingerMs") == null
                        ? 0
                        : ConnectorConfigSupport.optionalInteger(config, "lingerMs")
        ));
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(
                ConnectorConfigSupport.optionalInteger(config, "deliveryTimeoutMs") == null
                        ? 15000
                        : ConnectorConfigSupport.optionalInteger(config, "deliveryTimeoutMs")
        ));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, ConnectorConfigSupport.optionalString(config, "clientId") == null
                ? "mdg-execution-" + executionId
                : ConnectorConfigSupport.optionalString(config, "clientId"));

        String compressionType = ConnectorConfigSupport.optionalString(config, "compressionType");
        if (compressionType != null) {
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }
        return properties;
    }

    private void applyPassthroughProperties(Properties properties, Map<String, Object> config) {
        Object customProperties = ConnectorConfigSupport.findValue(config, "properties");
        if (!(customProperties instanceof Map<?, ?> rawProperties)) {
            return;
        }
        for (Map.Entry<?, ?> entry : rawProperties.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                properties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }
    }

    private ProducerRecord<String, String> buildRecord(
            String topic,
            Integer partition,
            String keyField,
            Map<String, Object> headers,
            Map<String, Object> row
    ) throws Exception {
        String key = resolveKey(keyField, row);
        String payload = objectMapper.writeValueAsString(row);
        ProducerRecord<String, String> record = partition == null
                ? new ProducerRecord<>(topic, key, payload)
                : new ProducerRecord<>(topic, partition, key, payload);

        for (Map.Entry<String, Object> header : headers.entrySet()) {
            record.headers().add(new RecordHeader(
                    header.getKey(),
                    String.valueOf(header.getValue()).getBytes(StandardCharsets.UTF_8)
            ));
        }
        return record;
    }

    private String resolveKey(String keyField, Map<String, Object> row) throws Exception {
        if (keyField == null || keyField.isBlank()) {
            return null;
        }
        Object keyValue = row.get(keyField);
        if (keyValue == null) {
            return null;
        }
        if (keyValue instanceof Map<?, ?> || keyValue instanceof List<?>) {
            return objectMapper.writeValueAsString(keyValue);
        }
        return String.valueOf(keyValue);
    }

    private Map<String, Object> readHeaders(Map<String, Object> runtimeConfig) {
        Object rawHeaders = ConnectorConfigSupport.findValue(runtimeConfig, "target.headers");
        if (rawHeaders == null) {
            rawHeaders = ConnectorConfigSupport.findValue(runtimeConfig, "headers");
        }
        if (!(rawHeaders instanceof Map<?, ?> headersMap)) {
            return Map.of();
        }

        Map<String, Object> headers = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : headersMap.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                headers.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return headers;
    }
}
