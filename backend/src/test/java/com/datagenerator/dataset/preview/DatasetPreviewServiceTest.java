package com.datagenerator.dataset.preview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.datagenerator.dataset.domain.DatasetDefinition;
import com.datagenerator.dataset.domain.DatasetStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DatasetPreviewServiceTest {

    private final DatasetPreviewService service = new DatasetPreviewService(mock(), new ObjectMapper());

    @Test
    void generate_shouldProduceStructuredRowsAcrossSupportedRules() {
        DatasetDefinition dataset = dataset("""
                {
                  "type": "object",
                  "fields": {
                    "id": { "rule": "sequence", "start": 1, "step": 1 },
                    "code": { "rule": "fixed", "value": "ALPHA" },
                    "amount": { "rule": "random_decimal", "min": 10, "max": 20, "scale": 2 },
                    "status": { "rule": "enum", "values": ["NEW", "DONE"] },
                    "active": { "rule": "boolean", "trueRate": 0.9 },
                    "createdAt": { "rule": "datetime", "from": "2025-01-01T00:00:00Z", "to": "2025-01-02T00:00:00Z" },
                    "profile": {
                      "rule": "object",
                      "fields": {
                        "city": {
                          "rule": "weighted_enum",
                          "options": [
                            { "value": "Shanghai", "weight": 3 },
                            { "value": "Beijing", "weight": 1 }
                          ]
                        }
                      }
                    },
                    "tags": {
                      "rule": "array",
                      "size": 2,
                      "item": { "rule": "enum", "values": ["vip", "trial"] }
                    },
                    "mirrorId": { "rule": "reference", "path": "id" },
                    "email": { "rule": "template", "template": "user-${id}@demo.local" }
                  }
                }
                """);

        GeneratedDatasetBatch batch = service.generate(dataset, 3, 42L);

        assertThat(batch.count()).isEqualTo(3);
        assertThat(batch.seed()).isEqualTo(42L);
        assertThat(batch.rows()).hasSize(3);

        Map<String, Object> firstRow = batch.rows().get(0);
        Map<String, Object> secondRow = batch.rows().get(1);

        assertThat(firstRow.get("id")).isEqualTo(1L);
        assertThat(secondRow.get("id")).isEqualTo(2L);
        assertThat(firstRow.get("code")).isEqualTo("ALPHA");
        assertThat(firstRow.get("mirrorId")).isEqualTo(firstRow.get("id"));
        assertThat(firstRow.get("email")).isEqualTo("user-1@demo.local");
        assertThat(firstRow.get("amount")).isInstanceOf(BigDecimal.class);
        assertThat(((BigDecimal) firstRow.get("amount")).scale()).isEqualTo(2);
        assertThat((BigDecimal) firstRow.get("amount")).isBetween(new BigDecimal("10.00"), new BigDecimal("20.00"));
        assertThat(firstRow.get("status")).isIn("NEW", "DONE");
        assertThat(firstRow.get("active")).isInstanceOf(Boolean.class);
        assertThat(Instant.parse(firstRow.get("createdAt").toString()))
                .isBetween(Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2025-01-02T00:00:00Z"));

        @SuppressWarnings("unchecked")
        Map<String, Object> profile = (Map<String, Object>) firstRow.get("profile");
        assertThat(profile.get("city")).isIn("Shanghai", "Beijing");

        @SuppressWarnings("unchecked")
        List<String> tags = (List<String>) firstRow.get("tags");
        assertThat(tags).hasSize(2).allMatch(tag -> Set.of("vip", "trial").contains(tag));
    }

    @Test
    void generate_shouldBeDeterministicForSameSeed() {
        DatasetDefinition dataset = dataset("""
                {
                  "type": "object",
                  "fields": {
                    "id": { "rule": "sequence", "start": 100, "step": 1 },
                    "score": { "rule": "random_int", "min": 10, "max": 99 },
                    "city": { "rule": "enum", "values": ["A", "B", "C"] }
                  }
                }
                """);

        GeneratedDatasetBatch first = service.generate(dataset, 5, 20260412L);
        GeneratedDatasetBatch second = service.generate(dataset, 5, 20260412L);
        GeneratedDatasetBatch third = service.generate(dataset, 5, 20260413L);

        assertThat(first.rows()).isEqualTo(second.rows());
        assertThat(third.rows()).isNotEqualTo(first.rows());
    }

    @Test
    void generate_shouldClampRequestedCountToSupportedRange() {
        DatasetDefinition dataset = dataset("""
                {
                  "type": "object",
                  "fields": {
                    "id": { "rule": "sequence", "start": 1, "step": 1 }
                  }
                }
                """);

        GeneratedDatasetBatch huge = service.generate(dataset, 999, 1L);
        GeneratedDatasetBatch tiny = service.generate(dataset, 0, 1L);

        assertThat(huge.count()).isEqualTo(100);
        assertThat(huge.rows()).hasSize(100);
        assertThat(tiny.count()).isEqualTo(1);
        assertThat(tiny.rows()).hasSize(1);
    }

    @Test
    void generate_shouldRejectArchivedDatasets() {
        DatasetDefinition dataset = dataset("""
                {
                  "type": "object",
                  "fields": {
                    "id": { "rule": "sequence", "start": 1, "step": 1 }
                  }
                }
                """);
        dataset.setStatus(DatasetStatus.ARCHIVED);

        assertThatThrownBy(() -> service.generate(dataset, 5, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Archived datasets cannot be previewed");
    }

    private DatasetDefinition dataset(String schemaJson) {
        DatasetDefinition dataset = new DatasetDefinition();
        dataset.setId(1L);
        dataset.setName("test");
        dataset.setStatus(DatasetStatus.READY);
        dataset.setSchemaJson(schemaJson);
        dataset.setSampleConfigJson("""
                {
                  "count": 5,
                  "seed": 20260412
                }
                """);
        return dataset;
    }
}
