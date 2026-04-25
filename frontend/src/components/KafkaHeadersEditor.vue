<template>
  <section class="kafka-headers-editor">
    <div class="panel__row panel__row--divider">
      <div>
        <h4>Headers</h4>
        <p class="muted">优先通过选择配置 Header，避免手工写 JSON。</p>
      </div>
      <div class="panel__actions panel__actions--tight">
        <button class="button button--ghost" data-action="add-kafka-header" type="button" @click="addEntry">
          新增 Header
        </button>
      </div>
    </div>

    <div v-if="modelValue.length" class="stack">
      <article
        v-for="(entry, index) in modelValue"
        :key="`${index}-${entry.name}-${entry.mode}`"
        class="panel panel--subtle kafka-header-row"
        :data-header-index="index"
      >
        <div class="field field--half">
          <label>
            <span>Header 名称</span>
            <input
              :value="entry.name"
              data-header-field="name"
              type="text"
              placeholder="例如：source"
              @input="updateText(index, 'name', $event)"
            />
          </label>

          <label>
            <span>来源</span>
            <select :value="entry.mode" data-header-field="mode" @change="updateMode(index, $event)">
              <option value="FIXED">固定值</option>
              <option value="FIELD" :disabled="!availablePaths.length">来自字段</option>
            </select>
          </label>
        </div>

        <div class="field field--half">
          <label v-if="entry.mode === 'FIXED'">
            <span>Header 值</span>
            <input
              :value="entry.value"
              data-header-field="value"
              type="text"
              placeholder="例如：mdg"
              @input="updateText(index, 'value', $event)"
            />
          </label>

          <label v-else>
            <span>字段路径</span>
            <select :value="entry.path" data-header-field="path" @change="updateText(index, 'path', $event)">
              <option value="">请选择字段</option>
              <option v-for="path in availablePaths" :key="path" :value="path">{{ path }}</option>
            </select>
          </label>
        </div>

        <div class="panel__actions panel__actions--tight">
          <button class="button button--danger" type="button" @click="removeEntry(index)">
            删除
          </button>
        </div>
      </article>
    </div>

    <div v-else class="connection-table-panel__empty">
      暂未配置 Header
    </div>
  </section>
</template>

<script setup lang="ts">
import type { KafkaHeaderEntryDraft, KafkaHeaderMode } from "../utils/kafka";

const props = defineProps<{
  modelValue: KafkaHeaderEntryDraft[];
  availablePaths: string[];
}>();

const emit = defineEmits<{
  (event: "update:modelValue", value: KafkaHeaderEntryDraft[]): void;
}>();

function nextEntries() {
  return props.modelValue.map((entry) => ({ ...entry }));
}

function addEntry() {
  emit("update:modelValue", [
    ...nextEntries(),
    {
      name: "",
      mode: "FIXED",
      value: "",
      path: ""
    }
  ]);
}

function removeEntry(index: number) {
  emit("update:modelValue", props.modelValue.filter((_, current) => current !== index));
}

function updateMode(index: number, event: Event) {
  const next = nextEntries();
  next[index].mode = (event.target as HTMLSelectElement).value as KafkaHeaderMode;
  if (next[index].mode === "FIXED") {
    next[index].path = "";
  } else {
    next[index].value = "";
  }
  emit("update:modelValue", next);
}

function updateText(index: number, field: "name" | "value" | "path", event: Event) {
  const next = nextEntries();
  next[index][field] = (event.target as HTMLInputElement | HTMLSelectElement).value;
  emit("update:modelValue", next);
}
</script>
