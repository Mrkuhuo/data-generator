package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskPreviewResponse;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WriteTaskPreviewServiceTest {

    private final WriteTaskPreviewService service = new WriteTaskPreviewService(
            new KafkaPayloadSchemaService(new ObjectMapper()),
            new WriteTaskValueGenerator()
    );

    @Test
    void preview_shouldGenerateDeterministicRows() {
        WriteTaskUpsertRequest task = sampleTask();

        WriteTaskPreviewResponse first = service.preview(task, 3, 20260413L);
        WriteTaskPreviewResponse second = service.preview(task, 3, 20260413L);

        assertThat(first.rows()).isEqualTo(second.rows());
        assertThat(first.rows()).hasSize(3);
        assertThat(first.rows().get(0).get("id")).isEqualTo(1L);
        assertThat(first.rows().get(0).get("email")).isEqualTo("user1@demo.local");
        assertThat(first.rows().get(0).get("enabled")).isInstanceOf(Boolean.class);
    }

    @Test
    void preview_shouldRejectInvalidIntegerRange() {
        WriteTaskUpsertRequest task = new WriteTaskUpsertRequest(
                "bad-task",
                1L,
                "demo_table",
                TableMode.USE_EXISTING,
                WriteMode.APPEND,
                10,
                100,
                1L,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                List.of(new WriteTaskColumnUpsertRequest(
                        "age",
                        "INT",
                        null,
                        null,
                        null,
                        true,
                        false,
                        ColumnGeneratorType.RANDOM_INT,
                        Map.of("min", 10, "max", 1),
                        0
                ))
        );

        assertThatThrownBy(() -> service.preview(task, 1, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max");
    }

    @Test
    void preview_shouldGenerateNestedKafkaPayloadRows() {
        WriteTaskUpsertRequest task = new WriteTaskUpsertRequest(
                "kafka-complex",
                1L,
                "demo.topic",
                TableMode.CREATE_IF_MISSING,
                WriteMode.APPEND,
                10,
                100,
                20260417L,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                null,
                "{\"payloadFormat\":\"JSON\",\"keyMode\":\"FIELD\",\"keyPath\":\"order.id\"}",
                """
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
                                  "generatorConfig": { "start": 1, "step": 1 },
                                  "nullable": false
                                },
                                {
                                  "name": "amount",
                                  "type": "SCALAR",
                                  "valueType": "DECIMAL",
                                  "generatorType": "RANDOM_DECIMAL",
                                  "generatorConfig": { "min": 10, "max": 20, "scale": 2 },
                                  "nullable": false
                                }
                              ]
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
                        """,
                List.of()
        );

        WriteTaskPreviewResponse preview = service.preview(task, 2, 20260417L);

        assertThat(preview.rows()).hasSize(2);
        assertThat(preview.rows().get(0)).containsKeys("order", "tags");
        assertThat(((Map<?, ?>) preview.rows().get(0).get("order")).get("id")).isEqualTo(1L);
        assertThat(preview.rows().get(0).get("tags")).isInstanceOf(List.class);
        assertThat((List<?>) preview.rows().get(0).get("tags")).hasSize(2);
    }

    private WriteTaskUpsertRequest sampleTask() {
        return new WriteTaskUpsertRequest(
                "demo-task",
                1L,
                "demo_table",
                TableMode.USE_EXISTING,
                WriteMode.APPEND,
                10,
                100,
                20260413L,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                "test",
                null,
                null,
                List.of(
                        new WriteTaskColumnUpsertRequest(
                                "id",
                                "BIGINT",
                                null,
                                null,
                                null,
                                false,
                                true,
                                ColumnGeneratorType.SEQUENCE,
                                Map.of("start", 1, "step", 1),
                                0
                        ),
                        new WriteTaskColumnUpsertRequest(
                                "email",
                                "VARCHAR",
                                120,
                                null,
                                null,
                                false,
                                false,
                                ColumnGeneratorType.STRING,
                                Map.of("mode", "email", "domain", "demo.local"),
                                1
                        ),
                        new WriteTaskColumnUpsertRequest(
                                "enabled",
                                "BOOLEAN",
                                null,
                                null,
                                null,
                                false,
                                false,
                                ColumnGeneratorType.BOOLEAN,
                                Map.of("trueRate", 0.75),
                                2
                        )
                )
        );
    }
}
