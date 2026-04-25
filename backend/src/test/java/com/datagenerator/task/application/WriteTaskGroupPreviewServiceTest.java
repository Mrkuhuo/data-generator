package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskGroupPreviewResponse;
import com.datagenerator.task.api.WriteTaskGroupRelationUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupRowPlanRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupTaskUpsertRequest;
import com.datagenerator.task.api.WriteTaskGroupUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.ReferenceSourceMode;
import com.datagenerator.task.domain.RelationReusePolicy;
import com.datagenerator.task.domain.RelationSelectionStrategy;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTaskRelationMode;
import com.datagenerator.task.domain.WriteTaskRelationType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WriteTaskGroupPreviewServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WriteTaskGroupPreviewService service = new WriteTaskGroupPreviewService(
            new WriteTaskValueGenerator(),
            mock(ConnectionJdbcSupport.class),
            new KafkaPayloadSchemaService(objectMapper),
            objectMapper
    );

    @Test
    void preview_shouldGenerateParentAndChildRowsFromCurrentBatch() {
        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.MYSQL);

        WriteTaskGroupPreviewResponse preview = service.preview(
                sampleRequest(),
                null,
                connection,
                3,
                20260422L
        );

        assertThat(preview.tables()).hasSize(2);
        assertThat(preview.tables().get(0).tableName()).isEqualTo("customer");
        assertThat(preview.tables().get(0).generatedRowCount()).isEqualTo(2);
        assertThat(preview.tables().get(1).tableName()).isEqualTo("orders");
        assertThat(preview.tables().get(1).generatedRowCount()).isEqualTo(4);
        assertThat(preview.tables().get(1).foreignKeyMissCount()).isZero();

        List<Map<String, Object>> customerRows = preview.tables().get(0).rows();
        List<Map<String, Object>> orderRows = preview.tables().get(1).rows();
        assertThat(customerRows.get(0).get("id")).isEqualTo(1L);
        assertThat(customerRows.get(1).get("id")).isEqualTo(2L);
        assertThat(orderRows).allSatisfy(row ->
                assertThat(List.of(1L, 2L)).contains(((Number) row.get("customer_id")).longValue())
        );
    }

    @Test
    void preview_shouldGenerateKafkaParentAndChildMessages() {
        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.KAFKA);

        WriteTaskGroupPreviewResponse preview = service.preview(
                kafkaRequest(),
                null,
                connection,
                5,
                20260424L
        );

        assertThat(preview.tables()).hasSize(2);
        assertThat(preview.tables().get(0).tableName()).isEqualTo("orders-topic");
        assertThat(preview.tables().get(0).generatedRowCount()).isEqualTo(2);
        assertThat(preview.tables().get(1).tableName()).isEqualTo("order-item-topic");
        assertThat(preview.tables().get(1).generatedRowCount()).isEqualTo(4);
        assertThat(preview.tables().get(1).foreignKeyMissCount()).isZero();

        List<Map<String, Object>> orderRows = preview.tables().get(0).rows();
        List<Map<String, Object>> itemRows = preview.tables().get(1).rows();
        assertThat(((Number) orderRows.get(0).get("orderId")).longValue()).isEqualTo(1L);
        assertThat(((Number) orderRows.get(1).get("orderId")).longValue()).isEqualTo(2L);
        assertThat(itemRows.stream()
                .map(row -> ((Number) row.get("orderId")).longValue())
                .collect(java.util.stream.Collectors.toSet())).isEqualTo(java.util.Set.of(1L, 2L));
        assertThat(itemRows.stream()
                .map(row -> ((Map<?, ?>) row.get("buyer")).get("userId"))
                .collect(java.util.stream.Collectors.toSet())).isEqualTo(java.util.Set.of(101L, 102L));
    }

    private WriteTaskGroupUpsertRequest sampleRequest() {
        return new WriteTaskGroupUpsertRequest(
                "db-flow",
                9L,
                null,
                20260422L,
                WriteTaskStatus.READY,
                com.datagenerator.task.domain.WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                List.of(
                        new WriteTaskGroupTaskUpsertRequest(
                                null,
                                "customer",
                                "customer",
                                "customer",
                                TableMode.USE_EXISTING,
                                WriteMode.APPEND,
                                200,
                                null,
                                null,
                                WriteTaskStatus.READY,
                                new WriteTaskGroupRowPlanRequest(
                                        com.datagenerator.task.domain.WriteTaskGroupRowPlanMode.FIXED,
                                        2,
                                        null,
                                        null,
                                        null
                                ),
                                null,
                                null,
                                List.of(
                                        new WriteTaskGroupTaskColumnUpsertRequest(
                                                "id",
                                                "BIGINT",
                                                null,
                                                null,
                                                null,
                                                false,
                                                true,
                                                false,
                                                ColumnGeneratorType.SEQUENCE,
                                                Map.of("start", 1, "step", 1),
                                                0
                                        ),
                                        new WriteTaskGroupTaskColumnUpsertRequest(
                                                "name",
                                                "VARCHAR",
                                                100,
                                                null,
                                                null,
                                                false,
                                                false,
                                                false,
                                                ColumnGeneratorType.STRING,
                                                Map.of("prefix", "customer_", "length", 4),
                                                1
                                        )
                                )
                        ),
                        new WriteTaskGroupTaskUpsertRequest(
                                null,
                                "orders",
                                "orders",
                                "orders",
                                TableMode.USE_EXISTING,
                                WriteMode.APPEND,
                                200,
                                null,
                                null,
                                WriteTaskStatus.READY,
                                new WriteTaskGroupRowPlanRequest(
                                        com.datagenerator.task.domain.WriteTaskGroupRowPlanMode.CHILD_PER_PARENT,
                                        null,
                                        "customer",
                                        2,
                                        2
                                ),
                                null,
                                null,
                                List.of(
                                        new WriteTaskGroupTaskColumnUpsertRequest(
                                                "id",
                                                "BIGINT",
                                                null,
                                                null,
                                                null,
                                                false,
                                                true,
                                                false,
                                                ColumnGeneratorType.SEQUENCE,
                                                Map.of("start", 100, "step", 1),
                                                0
                                        ),
                                        new WriteTaskGroupTaskColumnUpsertRequest(
                                                "customer_id",
                                                "BIGINT",
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                true,
                                                ColumnGeneratorType.SEQUENCE,
                                                Map.of(),
                                                1
                                        )
                                )
                        )
                ),
                List.of(
                        new WriteTaskGroupRelationUpsertRequest(
                                null,
                                "fk_orders_customer",
                                "customer",
                                "orders",
                                WriteTaskRelationMode.DATABASE_COLUMNS,
                                WriteTaskRelationType.ONE_TO_MANY,
                                ReferenceSourceMode.CURRENT_BATCH,
                                RelationSelectionStrategy.PARENT_DRIVEN,
                                RelationReusePolicy.ALLOW_REPEAT,
                                List.of("id"),
                                List.of("customer_id"),
                                0D,
                                null,
                                2,
                                2,
                                null,
                                0
                        )
                )
        );
    }

    private WriteTaskGroupUpsertRequest kafkaRequest() {
        return new WriteTaskGroupUpsertRequest(
                "kafka-flow",
                99L,
                null,
                20260424L,
                WriteTaskStatus.READY,
                com.datagenerator.task.domain.WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                List.of(
                        new WriteTaskGroupTaskUpsertRequest(
                                null,
                                "orders",
                                "orders-topic",
                                "orders-topic",
                                TableMode.CREATE_IF_MISSING,
                                WriteMode.APPEND,
                                100,
                                null,
                                null,
                                WriteTaskStatus.READY,
                                new WriteTaskGroupRowPlanRequest(
                                        com.datagenerator.task.domain.WriteTaskGroupRowPlanMode.FIXED,
                                        2,
                                        null,
                                        null,
                                        null
                                ),
                                "{\"payloadFormat\":\"JSON\",\"keyMode\":\"FIELD\",\"keyPath\":\"orderId\"}",
                                null,
                                List.of(
                                        new WriteTaskGroupTaskColumnUpsertRequest(
                                                "orderId",
                                                "BIGINT",
                                                null,
                                                null,
                                                null,
                                                false,
                                                true,
                                                false,
                                                ColumnGeneratorType.SEQUENCE,
                                                Map.of("start", 1, "step", 1),
                                                0
                                        ),
                                        new WriteTaskGroupTaskColumnUpsertRequest(
                                                "userId",
                                                "BIGINT",
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                false,
                                                ColumnGeneratorType.SEQUENCE,
                                                Map.of("start", 101, "step", 1),
                                                1
                                        )
                                )
                        ),
                        new WriteTaskGroupTaskUpsertRequest(
                                null,
                                "items",
                                "order-item-topic",
                                "order-item-topic",
                                TableMode.CREATE_IF_MISSING,
                                WriteMode.APPEND,
                                100,
                                null,
                                null,
                                WriteTaskStatus.READY,
                                new WriteTaskGroupRowPlanRequest(
                                        com.datagenerator.task.domain.WriteTaskGroupRowPlanMode.CHILD_PER_PARENT,
                                        null,
                                        "orders",
                                        2,
                                        2
                                ),
                                "{\"payloadFormat\":\"JSON\",\"keyMode\":\"FIELD\",\"keyPath\":\"orderId\"}",
                                """
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
                                """,
                                List.of()
                        )
                ),
                List.of(
                        new WriteTaskGroupRelationUpsertRequest(
                                null,
                                "order_to_item",
                                "orders",
                                "items",
                                WriteTaskRelationMode.KAFKA_EVENT,
                                WriteTaskRelationType.ONE_TO_MANY,
                                ReferenceSourceMode.CURRENT_BATCH,
                                RelationSelectionStrategy.PARENT_DRIVEN,
                                RelationReusePolicy.ALLOW_REPEAT,
                                List.of(),
                                List.of(),
                                0D,
                                null,
                                2,
                                2,
                                """
                                {
                                  "fieldMappings": [
                                    { "from": "orderId", "to": "orderId", "required": true },
                                    { "from": "userId", "to": "buyer.userId", "required": true }
                                  ]
                                }
                                """,
                                0
                        )
                )
        );
    }
}
