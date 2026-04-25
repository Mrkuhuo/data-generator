<template>
  <section class="kafka-relation-mapping">
    <div class="panel__row panel__row--divider">
      <div>
        <h4>事件字段映射</h4>
        <p class="muted">选择父任务字段写入到子任务消息中的哪个路径，用来维持主外键或业务关联。</p>
      </div>
      <div class="panel__actions panel__actions--tight">
        <button class="button button--ghost" data-action="add-kafka-mapping" type="button" @click="addEntry">
          新增映射
        </button>
      </div>
    </div>

    <div v-if="modelValue.length" class="stack">
      <article
        v-for="(entry, index) in modelValue"
        :key="entry.localId || `${index}-${entry.from}-${entry.to}`"
        class="panel panel--subtle kafka-relation-mapping__row"
      >
        <div class="field field--half">
          <label>
            <span>来源字段</span>
            <select :value="entry.from" data-mapping-field="from" @change="updateText(index, 'from', $event)">
              <option value="">请选择来源字段</option>
              <option v-for="path in sourcePaths" :key="`source-${path}`" :value="path">{{ path }}</option>
            </select>
          </label>

          <label>
            <span>写入路径</span>
            <select :value="entry.to" data-mapping-field="to" @change="updateText(index, 'to', $event)">
              <option value="">请选择写入路径</option>
              <option v-for="path in targetPaths" :key="`target-${path}`" :value="path">{{ path }}</option>
            </select>
          </label>
        </div>

        <div class="kafka-relation-mapping__actions">
          <label class="checkbox-chip">
            <input :checked="entry.required" type="checkbox" @change="updateChecked(index, $event)" />
            <span>必填映射</span>
          </label>
          <button class="button button--danger" type="button" @click="removeEntry(index)">删除</button>
        </div>
      </article>
    </div>

    <div v-else class="connection-table-panel__empty">
      还没有字段映射
    </div>
  </section>
</template>

<script setup lang="ts">
interface MappingEntry {
  localId: string;
  from: string;
  to: string;
  required: boolean;
}

const props = defineProps<{
  modelValue: MappingEntry[];
  sourcePaths: string[];
  targetPaths: string[];
}>();

const emit = defineEmits<{
  (event: "update:modelValue", value: MappingEntry[]): void;
}>();

function cloneEntries() {
  return props.modelValue.map((entry) => ({ ...entry }));
}

function createEntry(): MappingEntry {
  return {
    localId: `mapping-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    from: props.sourcePaths[0] ?? "",
    to: props.targetPaths[0] ?? "",
    required: true
  };
}

function addEntry() {
  emit("update:modelValue", [...cloneEntries(), createEntry()]);
}

function removeEntry(index: number) {
  emit("update:modelValue", props.modelValue.filter((_, current) => current !== index));
}

function updateText(index: number, field: "from" | "to", event: Event) {
  const next = cloneEntries();
  next[index][field] = (event.target as HTMLSelectElement).value;
  emit("update:modelValue", next);
}

function updateChecked(index: number, event: Event) {
  const next = cloneEntries();
  next[index].required = (event.target as HTMLInputElement).checked;
  emit("update:modelValue", next);
}
</script>

<style scoped>
.kafka-relation-mapping {
  display: grid;
  gap: 14px;
}

.kafka-relation-mapping__row {
  display: grid;
  gap: 12px;
}

.kafka-relation-mapping__actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
</style>
