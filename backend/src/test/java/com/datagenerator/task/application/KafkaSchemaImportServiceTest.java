package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class KafkaSchemaImportServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaPayloadSchemaService payloadSchemaService = new KafkaPayloadSchemaService(objectMapper);
    private final KafkaSchemaImportService service = new KafkaSchemaImportService(objectMapper, payloadSchemaService);

    @Test
    void importExampleJson_shouldInferNestedPayloadSchema() throws Exception {
        var response = service.importExampleJson("""
                {
                  "order": {
                    "id": 1001,
                    "createdAt": "2026-04-20T10:00:00Z"
                  },
                  "items": [
                    {
                      "sku": "A001",
                      "qty": 2
                    }
                  ],
                  "traceId": "550e8400-e29b-41d4-a716-446655440000"
                }
                """);

        assertThat(response.schemaSource()).isEqualTo("EXAMPLE_JSON");
        assertThat(response.scalarPaths())
                .contains("order.id", "order.createdAt", "items[].sku", "items[].qty", "traceId");

        JsonNode schema = objectMapper.readTree(response.payloadSchemaJson());
        assertThat(schema.path("type").asText()).isEqualTo("OBJECT");
        assertThat(schema.path("children")).hasSize(3);
    }

    @Test
    void importJsonSchema_shouldInferEnumNullableAndArrayStructure() throws Exception {
        var response = service.importJsonSchema("""
                {
                  "type": "object",
                  "required": ["orderId"],
                  "properties": {
                    "orderId": {
                      "type": "integer",
                      "format": "int64"
                    },
                    "status": {
                      "type": ["string", "null"],
                      "enum": ["NEW", "PAID"]
                    },
                    "items": {
                      "type": "array",
                      "minItems": 1,
                      "items": {
                        "type": "object",
                        "required": ["sku"],
                        "properties": {
                          "sku": {
                            "type": "string"
                          }
                        }
                      }
                    }
                  }
                }
                """);

        assertThat(response.schemaSource()).isEqualTo("JSON_SCHEMA");
        assertThat(response.scalarPaths()).contains("orderId", "status", "items[].sku");
        assertThat(response.warnings()).isEmpty();

        JsonNode schema = objectMapper.readTree(response.payloadSchemaJson());
        JsonNode children = schema.path("children");
        JsonNode statusNode = children.get(1);
        assertThat(statusNode.path("generatorType").asText()).isEqualTo("ENUM");
        assertThat(statusNode.path("nullable").asBoolean()).isTrue();
    }
}
