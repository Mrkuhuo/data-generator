package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.datagenerator.connection.application.KafkaConnectionSupport;
import com.datagenerator.connection.application.TargetConnectionSecretCodec;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

class WriteTaskKafkaWriterIntegrationTest {

    private static final EmbeddedKafkaKraftBroker BROKER = new EmbeddedKafkaKraftBroker(1, 1);

    @BeforeAll
    static void startBroker() {
        BROKER.afterPropertiesSet();
    }

    @AfterAll
    static void stopBroker() {
        BROKER.destroy();
    }

    @Test
    void write_shouldPublishRowsToKafkaTopicWithKeyHeadersAndPartition() throws Exception {
        String topic = "mdg.kafka.writer." + UUID.randomUUID();
        createTopic(topic, 3);

        ObjectMapper objectMapper = new ObjectMapper();
        WriteTaskKafkaWriter writer = new WriteTaskKafkaWriter(
                new KafkaConnectionSupport(new TargetConnectionSecretCodec("test-secret-key")),
                objectMapper
        );

        TargetConnection connection = new TargetConnection();
        connection.setName("Embedded Kafka");
        connection.setDbType(DatabaseType.KAFKA);
        connection.setHost("127.0.0.1");
        connection.setPort(9092);
        connection.setDatabaseName("kafka");
        connection.setSchemaName(null);
        connection.setUsername("");
        connection.setPasswordValue("");
        connection.setConfigJson("""
                {
                  "bootstrapServers": "%s",
                  "clientId": "mdg-integration-test",
                  "acks": "all"
                }
                """.formatted(BROKER.getBrokersAsString()).trim());

        WriteTask task = new WriteTask();
        task.setConnectionId(1L);
        task.setName("Kafka Integration");
        task.setTableName(topic);
        task.setTableMode(TableMode.CREATE_IF_MISSING);
        task.setWriteMode(WriteMode.APPEND);
        task.setBatchSize(10);
        task.setRowCount(2);
        task.setTargetConfigJson("""
                {
                  "payloadFormat": "JSON",
                  "keyMode": "FIELD",
                  "keyField": "event_id",
                  "partition": 1,
                  "headers": {
                    "source": "mdg",
                    "env": "test"
                  }
                }
                """);

        List<Map<String, Object>> rows = List.of(
                Map.of("event_id", "evt-1", "action", "login"),
                Map.of("event_id", "evt-2", "action", "logout")
        );

        WriteTaskDeliveryResult result = writer.write(task, connection, rows, 99L);

        assertThat(result.successCount()).isEqualTo(2L);
        assertThat(result.errorCount()).isEqualTo(0L);
        assertThat(result.details())
                .containsEntry("deliveryType", "KAFKA")
                .containsEntry("topic", topic)
                .containsEntry("payloadFormat", "JSON")
                .containsEntry("keyMode", "FIELD")
                .containsEntry("keyField", "event_id")
                .containsEntry("partition", 1)
                .containsEntry("headerCount", 2)
                .containsEntry("writtenRowCount", 2L);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(topic));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

            assertThat(records.count()).isEqualTo(2);

            List<ConsumerRecord<String, String>> recordList = StreamSupport.stream(records.spliterator(), false).toList();
            assertThat(recordList).allSatisfy(record -> {
                assertThat(record.partition()).isEqualTo(1);
                assertThat(record.key()).startsWith("evt-");
                assertThat(headerValue(record, "source")).isEqualTo("mdg");
                assertThat(headerValue(record, "env")).isEqualTo("test");
            });

            List<Map<String, Object>> payloads = recordList.stream()
                    .map(record -> readPayload(record.value(), objectMapper))
                    .toList();
            assertThat(payloads)
                    .extracting(payload -> payload.get("action"))
                    .containsExactlyInAnyOrder("login", "logout");
        }
    }

    @Test
    void write_shouldResolveNestedKeyPathForComplexPayload() throws Exception {
        String topic = "mdg.kafka.writer.nested." + UUID.randomUUID();
        createTopic(topic, 1);

        ObjectMapper objectMapper = new ObjectMapper();
        WriteTaskKafkaWriter writer = new WriteTaskKafkaWriter(
                new KafkaConnectionSupport(new TargetConnectionSecretCodec("test-secret-key")),
                objectMapper
        );

        TargetConnection connection = new TargetConnection();
        connection.setName("Embedded Kafka");
        connection.setDbType(DatabaseType.KAFKA);
        connection.setHost("127.0.0.1");
        connection.setPort(9092);
        connection.setDatabaseName("kafka");
        connection.setSchemaName(null);
        connection.setUsername("");
        connection.setPasswordValue("");
        connection.setConfigJson("""
                {
                  "bootstrapServers": "%s",
                  "clientId": "mdg-nested-key-test",
                  "acks": "all"
                }
                """.formatted(BROKER.getBrokersAsString()).trim());

        WriteTask task = new WriteTask();
        task.setConnectionId(1L);
        task.setName("Kafka Nested Key");
        task.setTableName(topic);
        task.setTableMode(TableMode.CREATE_IF_MISSING);
        task.setWriteMode(WriteMode.APPEND);
        task.setBatchSize(10);
        task.setRowCount(1);
        task.setTargetConfigJson("""
                {
                  "payloadFormat": "JSON",
                  "keyMode": "FIELD",
                  "keyPath": "order.id"
                }
                """);

        List<Map<String, Object>> rows = List.of(
                Map.of(
                        "order", Map.of("id", "evt-nested-1", "action", "pay"),
                        "items", List.of(Map.of("sku", "sku-1"))
                )
        );

        WriteTaskDeliveryResult result = writer.write(task, connection, rows, 100L);

        assertThat(result.successCount()).isEqualTo(1L);
        assertThat(result.details()).containsEntry("keyPath", "order.id");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(topic));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

            assertThat(records.count()).isEqualTo(1);
            ConsumerRecord<String, String> record = records.iterator().next();
            assertThat(record.key()).isEqualTo("evt-nested-1");

            Map<String, Object> payload = readPayload(record.value(), objectMapper);
            assertThat(((Map<?, ?>) payload.get("order")).get("action")).isEqualTo("pay");
        }
    }

    @Test
    void write_shouldResolveFieldBackedKafkaHeaders() throws Exception {
        String topic = "mdg.kafka.writer.headers." + UUID.randomUUID();
        createTopic(topic, 1);

        ObjectMapper objectMapper = new ObjectMapper();
        WriteTaskKafkaWriter writer = new WriteTaskKafkaWriter(
                new KafkaConnectionSupport(new TargetConnectionSecretCodec("test-secret-key")),
                objectMapper
        );

        TargetConnection connection = new TargetConnection();
        connection.setName("Embedded Kafka");
        connection.setDbType(DatabaseType.KAFKA);
        connection.setHost("127.0.0.1");
        connection.setPort(9092);
        connection.setDatabaseName("kafka");
        connection.setSchemaName(null);
        connection.setUsername("");
        connection.setPasswordValue("");
        connection.setConfigJson("""
                {
                  "bootstrapServers": "%s",
                  "clientId": "mdg-header-definition-test",
                  "acks": "all"
                }
                """.formatted(BROKER.getBrokersAsString()).trim());

        WriteTask task = new WriteTask();
        task.setConnectionId(1L);
        task.setName("Kafka Header Definitions");
        task.setTableName(topic);
        task.setTableMode(TableMode.CREATE_IF_MISSING);
        task.setWriteMode(WriteMode.APPEND);
        task.setBatchSize(10);
        task.setRowCount(1);
        task.setTargetConfigJson("""
                {
                  "payloadFormat": "JSON",
                  "keyMode": "FIELD",
                  "keyPath": "order.id",
                  "headerDefinitions": [
                    {
                      "name": "source",
                      "mode": "FIXED",
                      "value": "mdg"
                    },
                    {
                      "name": "amount",
                      "mode": "FIELD",
                      "path": "order.amount"
                    }
                  ]
                }
                """);

        List<Map<String, Object>> rows = List.of(
                Map.of(
                        "order", Map.of("id", "evt-nested-2", "amount", 125.5, "action", "ship"),
                        "items", List.of(Map.of("sku", "sku-2"))
                )
        );

        WriteTaskDeliveryResult result = writer.write(task, connection, rows, 101L);

        assertThat(result.successCount()).isEqualTo(1L);
        assertThat(result.details())
                .containsEntry("keyPath", "order.id")
                .containsEntry("headerCount", 2);
        assertThat(result.details().get("headerDefinitions")).isInstanceOf(List.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(topic));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

            assertThat(records.count()).isEqualTo(1);
            ConsumerRecord<String, String> record = records.iterator().next();
            assertThat(record.key()).isEqualTo("evt-nested-2");
            assertThat(headerValue(record, "source")).isEqualTo("mdg");
            assertThat(headerValue(record, "amount")).isEqualTo("125.5");
        }
    }

    private static void createTopic(String topic, int partitions) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKER.getBrokersAsString());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private static Map<String, Object> readPayload(String payload, ObjectMapper objectMapper) {
        try {
            return objectMapper.readValue(payload, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new AssertionError("Unable to parse Kafka payload", exception);
        }
    }

    private static String headerValue(ConsumerRecord<String, String> record, String headerName) {
        if (record.headers().lastHeader(headerName) == null) {
            return null;
        }
        return new String(record.headers().lastHeader(headerName).value(), StandardCharsets.UTF_8);
    }

    private static Map<String, Object> consumerProperties() {
        Map<String, Object> properties = KafkaTestUtils.consumerProps("mdg-kafka-writer-test", "true", BROKER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }
}
