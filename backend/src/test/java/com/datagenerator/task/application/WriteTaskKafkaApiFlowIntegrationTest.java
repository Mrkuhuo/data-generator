package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:mdg-kafka-api-flow;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.flyway.enabled=false",
        "spring.quartz.auto-startup=false"
})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class WriteTaskKafkaApiFlowIntegrationTest {

    private static final EmbeddedKafkaKraftBroker BROKER = new EmbeddedKafkaKraftBroker(1, 1);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeAll
    static void startBroker() {
        BROKER.afterPropertiesSet();
    }

    @AfterAll
    static void stopBroker() {
        BROKER.destroy();
    }

    @Test
    void apiFlow_shouldCreatePreviewRunAndConsumeComplexKafkaMessages() throws Exception {
        String topic = "mdg.kafka.api.flow." + UUID.randomUUID();
        createTopic(topic, 2);

        Long connectionId = createKafkaConnection();

        JsonNode connectionTest = postWithoutBody("/api/connections/%d/test".formatted(connectionId));
        assertThat(connectionTest.path("success").asBoolean()).isTrue();
        assertThat(connectionTest.path("message").asText()).isEqualTo("目标连接测试已执行");
        assertThat(connectionTest.at("/data/success").asBoolean()).isTrue();
        assertThat(connectionTest.at("/data/status").asText()).isEqualTo("READY");
        assertThat(connectionTest.at("/data/message").asText()).isEqualTo("Kafka 连接成功");

        Map<String, Object> taskRequest = kafkaTaskRequest(connectionId, topic);

        JsonNode preview = postJson(
                "/api/write-tasks/preview",
                Map.of(
                        "task", taskRequest,
                        "count", 2,
                        "seed", 20260417L
                )
        );
        assertThat(preview.path("success").asBoolean()).isTrue();
        assertThat(preview.at("/data/count").asInt()).isEqualTo(2);
        assertThat(preview.at("/data/rows").isArray()).isTrue();
        assertThat(preview.at("/data/rows").size()).isEqualTo(2);
        assertThat(preview.at("/data/rows/0/order/id").asLong()).isEqualTo(1001L);
        assertThat(preview.at("/data/rows/1/order/id").asLong()).isEqualTo(1002L);
        assertThat(preview.at("/data/rows/0/order/paid").asBoolean()).isTrue();
        assertThat(preview.at("/data/rows/0/items").size()).isEqualTo(2);
        assertThat(preview.at("/data/rows/0/tags").size()).isEqualTo(2);

        JsonNode createdTask = postJson("/api/write-tasks", taskRequest);
        assertThat(createdTask.path("success").asBoolean()).isTrue();
        assertThat(createdTask.path("message").asText()).isEqualTo("写入任务已创建");
        Long taskId = createdTask.at("/data/id").asLong();
        assertThat(taskId).isPositive();
        assertThat(createdTask.at("/data/columns").size()).isZero();
        assertThat(createdTask.at("/data/payloadSchemaJson").asText()).isNotBlank();

        JsonNode runResponse = postWithoutBody("/api/write-tasks/%d/run".formatted(taskId));
        assertThat(runResponse.path("success").asBoolean()).isTrue();
        assertThat(runResponse.path("message").asText()).isEqualTo("写入任务已开始执行");
        Long executionId = runResponse.at("/data/id").asLong();
        assertThat(runResponse.at("/data/status").asText()).isEqualTo("SUCCESS");

        Map<String, Object> deliveryDetails = objectMapper.readValue(
                runResponse.at("/data/deliveryDetailsJson").asText(),
                new TypeReference<>() {
                }
        );
        assertThat(deliveryDetails)
                .containsEntry("targetType", "KAFKA")
                .containsEntry("deliveryType", "KAFKA")
                .containsEntry("topic", topic)
                .containsEntry("keyMode", "FIELD")
                .containsEntry("keyPath", "order.id")
                .containsEntry("partition", 1)
                .containsEntry("writtenRowCount", 2)
                .containsEntry("generatedCount", 2)
                .containsEntry("plannedRowCount", 2);
        assertThat(deliveryDetails.get("headers"))
                .isEqualTo(Map.of("source", "api-flow", "env", "test"));

        JsonNode executionDetail = getJson("/api/write-tasks/executions/%d".formatted(executionId));
        assertThat(executionDetail.path("success").asBoolean()).isTrue();
        assertThat(executionDetail.at("/data/id").asLong()).isEqualTo(executionId);
        assertThat(executionDetail.at("/data/status").asText()).isEqualTo("SUCCESS");

        JsonNode executionLogs = getJson("/api/write-tasks/executions/%d/logs".formatted(executionId));
        assertThat(executionLogs.path("success").asBoolean()).isTrue();
        List<String> messages = StreamSupport.stream(executionLogs.at("/data").spliterator(), false)
                .map(node -> node.path("message").asText())
                .toList();
        assertThat(messages)
                .contains("开始执行写入任务")
                .contains("已生成模拟数据")
                .contains("Kafka Topic 写入完成");

        List<ConsumerRecord<String, String>> recordList = consumeRecords(topic, 2);
        assertThat(recordList).hasSize(2);
        assertThat(recordList)
                .extracting(ConsumerRecord::partition)
                .containsOnly(1);
        assertThat(recordList)
                .extracting(ConsumerRecord::key)
                .containsExactly("1001", "1002");
        assertThat(recordList).allSatisfy(record -> {
            assertThat(headerValue(record, "source")).isEqualTo("api-flow");
            assertThat(headerValue(record, "env")).isEqualTo("test");
        });

        Map<String, Object> firstPayload = readPayload(recordList.getFirst().value());
        Map<String, Object> secondPayload = readPayload(recordList.get(1).value());
        assertThat(((Number) ((Map<?, ?>) firstPayload.get("order")).get("id")).longValue()).isEqualTo(1001L);
        assertThat(((Number) ((Map<?, ?>) secondPayload.get("order")).get("id")).longValue()).isEqualTo(1002L);
        assertThat(((Map<?, ?>) firstPayload.get("order")).get("paid")).isEqualTo(true);
        assertThat((List<?>) firstPayload.get("items")).hasSize(2);
        assertThat((List<?>) firstPayload.get("tags")).hasSize(2);
    }

    private Long createKafkaConnection() throws Exception {
        BootstrapEndpoint endpoint = firstEndpoint(BROKER.getBrokersAsString());
        LinkedHashMap<String, Object> request = new LinkedHashMap<>();
        request.put("name", "Kafka API 验收连接");
        request.put("dbType", "KAFKA");
        request.put("host", endpoint.host());
        request.put("port", endpoint.port());
        request.put("databaseName", "kafka");
        request.put("schemaName", null);
        request.put("username", "");
        request.put("password", "");
        request.put("jdbcParams", null);
        request.put("configJson", objectMapper.writeValueAsString(Map.of(
                "bootstrapServers", BROKER.getBrokersAsString(),
                "clientId", "mdg-kafka-api-flow-test",
                "acks", "all",
                "requestTimeoutMs", 5000,
                "defaultApiTimeoutMs", 5000
        )));
        request.put("status", "READY");
        request.put("description", "用于 Kafka API 级联调验收");

        JsonNode response = postJson("/api/connections", request);
        assertThat(response.path("success").asBoolean()).isTrue();
        assertThat(response.path("message").asText()).isEqualTo("目标连接已创建");
        return response.at("/data/id").asLong();
    }

    private Map<String, Object> kafkaTaskRequest(Long connectionId, String topic) throws Exception {
        LinkedHashMap<String, Object> request = new LinkedHashMap<>();
        request.put("name", "Kafka 复杂消息 API 验收");
        request.put("connectionId", connectionId);
        request.put("tableName", topic);
        request.put("tableMode", "CREATE_IF_MISSING");
        request.put("writeMode", "APPEND");
        request.put("rowCount", 2);
        request.put("batchSize", 10);
        request.put("seed", 20260417L);
        request.put("status", "READY");
        request.put("scheduleType", "MANUAL");
        request.put("cronExpression", null);
        request.put("triggerAt", null);
        request.put("intervalSeconds", null);
        request.put("maxRuns", null);
        request.put("maxRowsTotal", null);
        request.put("description", "覆盖连接、预览、创建、执行与消费校验");
        request.put("targetConfigJson", objectMapper.writeValueAsString(Map.of(
                "payloadFormat", "JSON",
                "keyMode", "FIELD",
                "keyPath", "order.id",
                "partition", 1,
                "headers", Map.of(
                        "source", "api-flow",
                        "env", "test"
                )
        )));
        request.put("payloadSchemaJson", payloadSchemaJson());
        request.put("columns", List.of());
        return request;
    }

    private String payloadSchemaJson() {
        return """
                {
                  "type": "OBJECT",
                  "children": [
                    {
                      "name": "order",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "id",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "SEQUENCE",
                          "generatorConfig": { "start": 1001, "step": 1 },
                          "nullable": false
                        },
                        {
                          "name": "amount",
                          "type": "SCALAR",
                          "valueType": "DECIMAL",
                          "generatorType": "RANDOM_DECIMAL",
                          "generatorConfig": { "min": 10, "max": 20, "scale": 2 },
                          "nullable": false
                        },
                        {
                          "name": "paid",
                          "type": "SCALAR",
                          "valueType": "BOOLEAN",
                          "generatorType": "BOOLEAN",
                          "generatorConfig": { "trueRate": 1.0 },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "items",
                      "type": "ARRAY",
                      "minItems": 2,
                      "maxItems": 2,
                      "itemSchema": {
                        "type": "OBJECT",
                        "children": [
                          {
                            "name": "sku",
                            "type": "SCALAR",
                            "valueType": "STRING",
                            "generatorType": "STRING",
                            "generatorConfig": { "prefix": "sku-", "length": 4 },
                            "nullable": false
                          },
                          {
                            "name": "quantity",
                            "type": "SCALAR",
                            "valueType": "INT",
                            "generatorType": "RANDOM_INT",
                            "generatorConfig": { "min": 1, "max": 3 },
                            "nullable": false
                          }
                        ]
                      }
                    },
                    {
                      "name": "tags",
                      "type": "ARRAY",
                      "minItems": 2,
                      "maxItems": 2,
                      "itemSchema": {
                        "type": "SCALAR",
                        "valueType": "STRING",
                        "generatorType": "ENUM",
                        "generatorConfig": { "values": ["new", "vip"] },
                        "nullable": false
                      }
                    }
                  ]
                }
                """;
    }

    private JsonNode postJson(String path, Object body) throws Exception {
        MvcResult result = mockMvc.perform(post(path)
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(body)))
                .andExpect(status().isOk())
                .andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsByteArray());
    }

    private JsonNode postWithoutBody(String path) throws Exception {
        MvcResult result = mockMvc.perform(post(path))
                .andExpect(status().isOk())
                .andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsByteArray());
    }

    private JsonNode getJson(String path) throws Exception {
        MvcResult result = mockMvc.perform(get(path))
                .andExpect(status().isOk())
                .andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsByteArray());
    }

    private void createTopic(String topic, int partitions) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKER.getBrokersAsString());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private Map<String, Object> readPayload(String payload) {
        try {
            return objectMapper.readValue(payload, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new AssertionError("Unable to parse Kafka payload", exception);
        }
    }

    private String headerValue(ConsumerRecord<String, String> record, String headerName) {
        if (record.headers().lastHeader(headerName) == null) {
            return null;
        }
        return new String(record.headers().lastHeader(headerName).value(), StandardCharsets.UTF_8);
    }

    private List<ConsumerRecord<String, String>> consumeRecords(String topic, int expectedCount) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(topic));
            ArrayList<ConsumerRecord<String, String>> records = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline && records.size() < expectedCount) {
                ConsumerRecords<String, String> polled = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
                polled.forEach(records::add);
            }
            return records;
        }
    }

    private Map<String, Object> consumerProperties() {
        Map<String, Object> properties = KafkaTestUtils.consumerProps("mdg-kafka-api-flow-" + UUID.randomUUID(), "true", BROKER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private BootstrapEndpoint firstEndpoint(String bootstrapServers) {
        String first = bootstrapServers.split(",")[0].trim();
        int colonIndex = first.lastIndexOf(':');
        if (colonIndex < 0) {
            return new BootstrapEndpoint(first, 9092);
        }
        return new BootstrapEndpoint(
                first.substring(0, colonIndex).trim(),
                Integer.parseInt(first.substring(colonIndex + 1).trim())
        );
    }

    private record BootstrapEndpoint(String host, int port) {
    }
}
