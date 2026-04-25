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
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
        "spring.datasource.url=jdbc:h2:mem:mdg-write-group-kafka-api-flow;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
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
class WriteTaskGroupKafkaApiFlowIntegrationTest {

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
    void apiFlow_shouldPreviewCreateRunAndConsumeKafkaRelationshipMessages() throws Exception {
        String orderTopic = "mdg.kafka.group.orders." + UUID.randomUUID();
        String itemTopic = "mdg.kafka.group.items." + UUID.randomUUID();
        createTopic(orderTopic, 2);
        createTopic(itemTopic, 2);

        Long connectionId = createKafkaConnection();
        Map<String, Object> groupRequest = kafkaGroupRequest(connectionId, orderTopic, itemTopic);

        JsonNode preview = postJson(
                "/api/write-task-groups/preview",
                Map.of(
                        "group", groupRequest,
                        "previewCount", 5,
                        "seed", 20260424L
                )
        );
        assertThat(preview.path("success").asBoolean()).isTrue();
        assertThat(preview.at("/data/tables").size()).isEqualTo(2);
        assertThat(preview.at("/data/tables/0/generatedRowCount").asInt()).isEqualTo(2);
        assertThat(preview.at("/data/tables/1/generatedRowCount").asInt()).isEqualTo(4);

        JsonNode createdGroup = postJson("/api/write-task-groups", groupRequest);
        assertThat(createdGroup.path("success").asBoolean()).isTrue();
        Long groupId = createdGroup.at("/data/id").asLong();
        assertThat(groupId).isPositive();
        assertThat(createdGroup.at("/data/tasks/1/payloadSchemaJson").asText()).isNotBlank();
        assertThat(createdGroup.at("/data/relations/0/relationMode").asText()).isEqualTo("KAFKA_EVENT");

        JsonNode runResponse = postWithoutBody("/api/write-task-groups/%d/run".formatted(groupId));
        assertThat(runResponse.path("success").asBoolean()).isTrue();
        assertThat(runResponse.at("/data/status").asText()).isEqualTo("SUCCESS");
        assertThat(runResponse.at("/data/insertedRowCount").asLong()).isEqualTo(6L);
        assertThat(runResponse.at("/data/tables").size()).isEqualTo(2);
        assertThat(runResponse.at("/data/tables/0/summary/topic").asText()).isEqualTo(orderTopic);
        assertThat(runResponse.at("/data/tables/1/summary/topic").asText()).isEqualTo(itemTopic);

        JsonNode executionList = getJson("/api/write-task-groups/%d/executions".formatted(groupId));
        assertThat(executionList.path("success").asBoolean()).isTrue();
        assertThat(executionList.at("/data/0/insertedRowCount").asLong()).isEqualTo(6L);

        List<ConsumerRecord<String, String>> orderRecords = consumeRecords(orderTopic, 2);
        List<ConsumerRecord<String, String>> itemRecords = consumeRecords(itemTopic, 4);
        assertThat(orderRecords).hasSize(2);
        assertThat(itemRecords).hasSize(4);
        assertThat(orderRecords)
                .extracting(ConsumerRecord::key)
                .containsExactlyInAnyOrder("1", "2");
        assertThat(itemRecords)
                .extracting(ConsumerRecord::key)
                .containsOnly("1", "2");

        Map<String, Object> firstOrderPayload = readPayload(orderRecords.getFirst().value());
        Map<String, Object> firstItemPayload = readPayload(itemRecords.getFirst().value());
        assertThat(firstOrderPayload).containsKeys("orderId", "userId");
        assertThat(firstItemPayload).containsKeys("orderId", "buyer", "itemName");
        @SuppressWarnings("unchecked")
        Map<String, Object> buyer = (Map<String, Object>) firstItemPayload.get("buyer");
        assertThat(buyer).containsKey("userId");

        List<Long> orderIds = itemRecords.stream()
                .map(record -> ((Number) readPayload(record.value()).get("orderId")).longValue())
                .toList();
        List<Long> buyerIds = itemRecords.stream()
                .map(record -> ((Number) ((Map<?, ?>) readPayload(record.value()).get("buyer")).get("userId")).longValue())
                .toList();
        assertThat(orderIds).containsOnly(1L, 2L);
        assertThat(buyerIds).containsOnly(101L, 102L);
    }

    private Long createKafkaConnection() throws Exception {
        BootstrapEndpoint endpoint = firstEndpoint(BROKER.getBrokersAsString());
        LinkedHashMap<String, Object> request = new LinkedHashMap<>();
        request.put("name", "Kafka Relation Test");
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
                "clientId", "mdg-write-group-kafka-flow-test",
                "acks", "all",
                "requestTimeoutMs", 5000,
                "defaultApiTimeoutMs", 5000
        )));
        request.put("status", "READY");
        request.put("description", "Kafka relationship task test");

        JsonNode response = postJson("/api/connections", request);
        assertThat(response.path("success").asBoolean()).isTrue();
        return response.at("/data/id").asLong();
    }

    private Map<String, Object> kafkaGroupRequest(Long connectionId, String orderTopic, String itemTopic) {
        LinkedHashMap<String, Object> request = new LinkedHashMap<>();
        LinkedHashMap<String, Object> parentTask = new LinkedHashMap<>();
        LinkedHashMap<String, Object> childTask = new LinkedHashMap<>();
        LinkedHashMap<String, Object> relation = new LinkedHashMap<>();
        request.put("name", "Kafka relationship flow");
        request.put("connectionId", connectionId);
        request.put("description", "Parent and child event generation");
        request.put("seed", 20260424L);
        request.put("status", "READY");
        request.put("scheduleType", "MANUAL");
        request.put("cronExpression", null);
        request.put("triggerAt", null);
        request.put("intervalSeconds", null);
        request.put("maxRuns", null);
        request.put("maxRowsTotal", null);

        parentTask.put("id", null);
        parentTask.put("taskKey", "orders");
        parentTask.put("name", "Orders Topic");
        parentTask.put("tableName", orderTopic);
        parentTask.put("tableMode", "CREATE_IF_MISSING");
        parentTask.put("writeMode", "APPEND");
        parentTask.put("batchSize", 10);
        parentTask.put("seed", 20260424L);
        parentTask.put("description", "parent topic");
        parentTask.put("status", "READY");
        parentTask.put("rowPlan", new LinkedHashMap<>(Map.of("mode", "FIXED", "rowCount", 2)));
        ((Map<String, Object>) parentTask.get("rowPlan")).put("driverTaskKey", null);
        ((Map<String, Object>) parentTask.get("rowPlan")).put("minChildrenPerParent", null);
        ((Map<String, Object>) parentTask.get("rowPlan")).put("maxChildrenPerParent", null);
        parentTask.put("targetConfigJson", "{\"payloadFormat\":\"JSON\",\"keyMode\":\"FIELD\",\"keyPath\":\"orderId\"}");
        parentTask.put("payloadSchemaJson", null);
        parentTask.put("columns", List.of(
                columnRequest("orderId", true, false, Map.of("start", 1, "step", 1), 0),
                columnRequest("userId", false, false, Map.of("start", 101, "step", 1), 1)
        ));

        childTask.put("id", null);
        childTask.put("taskKey", "items");
        childTask.put("name", "Order Item Topic");
        childTask.put("tableName", itemTopic);
        childTask.put("tableMode", "CREATE_IF_MISSING");
        childTask.put("writeMode", "APPEND");
        childTask.put("batchSize", 10);
        childTask.put("seed", 20260424L);
        childTask.put("description", "child topic");
        childTask.put("status", "READY");
        childTask.put("rowPlan", new LinkedHashMap<>(Map.of(
                "mode", "CHILD_PER_PARENT",
                "driverTaskKey", "orders",
                "minChildrenPerParent", 2,
                "maxChildrenPerParent", 2
        )));
        ((Map<String, Object>) childTask.get("rowPlan")).put("rowCount", null);
        childTask.put("targetConfigJson", "{\"payloadFormat\":\"JSON\",\"keyMode\":\"FIELD\",\"keyPath\":\"orderId\"}");
        childTask.put("payloadSchemaJson", """
                {
                  "type": "OBJECT",
                  "children": [
                    {
                      "name": "orderId",
                      "type": "SCALAR",
                      "valueType": "LONG",
                      "generatorType": "SEQUENCE",
                      "generatorConfig": { "start": 500, "step": 1 },
                      "nullable": false
                    },
                    {
                      "name": "buyer",
                      "type": "OBJECT",
                      "children": [
                        {
                          "name": "userId",
                          "type": "SCALAR",
                          "valueType": "LONG",
                          "generatorType": "SEQUENCE",
                          "generatorConfig": { "start": 900, "step": 1 },
                          "nullable": false
                        }
                      ]
                    },
                    {
                      "name": "itemName",
                      "type": "SCALAR",
                      "valueType": "STRING",
                      "generatorType": "STRING",
                      "generatorConfig": { "prefix": "item-", "length": 4 },
                      "nullable": false
                    }
                  ]
                }
                """);
        childTask.put("columns", List.of());

        relation.put("id", null);
        relation.put("relationName", "order_to_item");
        relation.put("parentTaskKey", "orders");
        relation.put("childTaskKey", "items");
        relation.put("relationMode", "KAFKA_EVENT");
        relation.put("relationType", "ONE_TO_MANY");
        relation.put("sourceMode", "CURRENT_BATCH");
        relation.put("selectionStrategy", "PARENT_DRIVEN");
        relation.put("reusePolicy", "ALLOW_REPEAT");
        relation.put("parentColumns", List.of());
        relation.put("childColumns", List.of());
        relation.put("nullRate", 0.0);
        relation.put("mixedExistingRatio", null);
        relation.put("minChildrenPerParent", 2);
        relation.put("maxChildrenPerParent", 2);
        relation.put("mappingConfigJson", """
                {
                  "fieldMappings": [
                    { "from": "orderId", "to": "orderId", "required": true },
                    { "from": "userId", "to": "buyer.userId", "required": true }
                  ]
                }
                """);
        relation.put("sortOrder", 0);

        request.put("tasks", List.of(parentTask, childTask));
        request.put("relations", List.of(relation));
        return request;
    }

    private Map<String, Object> columnRequest(
            String columnName,
            boolean primaryKeyFlag,
            boolean foreignKeyFlag,
            Map<String, Object> generatorConfig,
            int sortOrder
    ) {
        LinkedHashMap<String, Object> column = new LinkedHashMap<>();
        column.put("columnName", columnName);
        column.put("dbType", "BIGINT");
        column.put("lengthValue", null);
        column.put("precisionValue", null);
        column.put("scaleValue", null);
        column.put("nullableFlag", false);
        column.put("primaryKeyFlag", primaryKeyFlag);
        column.put("foreignKeyFlag", foreignKeyFlag);
        column.put("generatorType", "SEQUENCE");
        column.put("generatorConfig", generatorConfig);
        column.put("sortOrder", sortOrder);
        return column;
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
        Map<String, Object> properties = KafkaTestUtils.consumerProps("mdg-write-group-kafka-" + UUID.randomUUID(), "true", BROKER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private Map<String, Object> readPayload(String payload) {
        try {
            return objectMapper.readValue(payload, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new AssertionError("Unable to parse Kafka payload", exception);
        }
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
