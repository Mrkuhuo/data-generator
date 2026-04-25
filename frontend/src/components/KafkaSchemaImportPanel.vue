<template>
  <section class="kafka-import-panel">
    <div class="panel__row panel__row--divider">
      <div>
        <h4>导入消息结构</h4>
        <p class="muted">直接粘贴示例 JSON 或 JSON Schema，系统会自动生成 Kafka 消息结构。</p>
      </div>
    </div>

    <div class="mode-switch">
      <button
        class="mode-switch__item"
        :class="{ 'mode-switch__item--active': mode === 'EXAMPLE_JSON' }"
        type="button"
        @click="mode = 'EXAMPLE_JSON'"
      >
        示例 JSON
      </button>
      <button
        class="mode-switch__item"
        :class="{ 'mode-switch__item--active': mode === 'JSON_SCHEMA' }"
        type="button"
        @click="mode = 'JSON_SCHEMA'"
      >
        JSON Schema
      </button>
    </div>

    <div class="field">
      <label>
        <span>{{ mode === "EXAMPLE_JSON" ? "示例 JSON" : "JSON Schema" }}</span>
        <textarea
          v-model.trim="content"
          name="kafkaSchemaImportContent"
          class="code-input kafka-import-panel__input"
          rows="8"
          :placeholder="mode === 'EXAMPLE_JSON' ? examplePlaceholder : schemaPlaceholder"
        ></textarea>
      </label>
    </div>

    <div class="panel__actions">
      <button class="button button--ghost" type="button" :disabled="isImporting" @click="fillTemplate">
        填入示例
      </button>
      <button class="button" type="button" :disabled="isImporting" @click="importSchema">
        {{ isImporting ? "解析中..." : (mode === "EXAMPLE_JSON" ? "解析示例 JSON" : "解析 JSON Schema") }}
      </button>
    </div>

    <div
      v-if="feedback.message"
      class="status-banner"
      :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
    >
      {{ feedback.message }}
    </div>

    <div v-if="warnings.length" class="kafka-import-panel__warnings">
      <div v-for="warning in warnings" :key="`${warning.path}-${warning.code}`" class="kafka-import-panel__warning">
        <strong>{{ warning.path || "$" }}</strong>
        <span>{{ warning.message }}</span>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import type { KafkaSchemaImportResult, KafkaSchemaImportWarning, KafkaSchemaSource } from "../utils/kafka";

type FeedbackKind = "success" | "error";

const emit = defineEmits<{
  (event: "imported", value: KafkaSchemaImportResult): void;
}>();

const mode = ref<KafkaSchemaSource>("EXAMPLE_JSON");
const content = ref("");
const isImporting = ref(false);
const warnings = ref<KafkaSchemaImportWarning[]>([]);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});

const examplePlaceholder = `{
  "order": {
    "id": 1001,
    "createdAt": "2026-04-20T10:00:00Z"
  },
  "items": [
    {
      "sku": "A001",
      "qty": 2
    }
  ]
}`;

const schemaPlaceholder = `{
  "type": "object",
  "properties": {
    "orderId": {
      "type": "integer"
    },
    "createdAt": {
      "type": "string",
      "format": "date-time"
    }
  }
}`;

function setFeedback(kind: FeedbackKind, message: string) {
  feedback.kind = kind;
  feedback.message = message;
}

function fillTemplate() {
  content.value = mode.value === "EXAMPLE_JSON" ? examplePlaceholder : schemaPlaceholder;
  feedback.message = "";
  warnings.value = [];
}

async function importSchema() {
  if (!content.value.trim()) {
    setFeedback("error", mode.value === "EXAMPLE_JSON" ? "请先输入示例 JSON" : "请先输入 JSON Schema");
    return;
  }

  isImporting.value = true;
  warnings.value = [];
  try {
    const endpoint = mode.value === "EXAMPLE_JSON"
      ? "/write-tasks/kafka/schema/example"
      : "/write-tasks/kafka/schema/json-schema";
    const response = await apiClient.post<ApiResponse<KafkaSchemaImportResult>>(endpoint, {
      content: content.value
    });
    const result = response.data.data;
    warnings.value = result.warnings ?? [];
    emit("imported", result);
    setFeedback("success", warnings.value.length ? `已生成结构，包含 ${warnings.value.length} 条提示` : "已生成结构");
  } catch (error) {
    warnings.value = [];
    setFeedback("error", readApiError(error, "消息结构解析失败"));
  } finally {
    isImporting.value = false;
  }
}
</script>
