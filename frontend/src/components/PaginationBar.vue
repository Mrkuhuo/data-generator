<template>
  <div v-if="shouldRender" class="list-pagination">
    <div class="list-pagination__summary">
      {{ labels.showing }} {{ startItem }} - {{ endItem }} {{ noun }}，{{ labels.total }} {{ total }} {{ noun }}
    </div>

    <div class="list-pagination__controls">
      <label class="list-pagination__size">
        <span>{{ labels.perPage }}</span>
        <select :value="pageSize" @change="handlePageSizeChange">
          <option v-for="option in pageSizeOptions" :key="option" :value="option">{{ option }}</option>
        </select>
        <span>{{ noun }}</span>
      </label>

      <button class="button button--ghost" type="button" :disabled="safePage <= 1" @click="emitPage(safePage - 1)">
        {{ labels.previous }}
      </button>
      <span class="pill pill--soft">{{ labels.page }} {{ safePage }} / {{ totalPages }}</span>
      <button class="button button--ghost" type="button" :disabled="safePage >= totalPages" @click="emitPage(safePage + 1)">
        {{ labels.next }}
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { clampPage, pageCount } from "../utils/pagination";

const labels = {
  showing: "\u663e\u793a",
  total: "\u5171",
  perPage: "\u6bcf\u9875",
  previous: "\u4e0a\u4e00\u9875",
  next: "\u4e0b\u4e00\u9875",
  page: "\u7b2c"
} as const;

const props = withDefaults(defineProps<{
  page: number;
  pageSize: number;
  total: number;
  noun?: string;
  pageSizeOptions?: number[];
}>(), {
  noun: "\u6761",
  pageSizeOptions: () => [8, 10, 20, 50]
});

const emit = defineEmits<{
  (event: "update:page", value: number): void;
  (event: "update:pageSize", value: number): void;
}>();

const totalPages = computed(() => pageCount(props.total, props.pageSize));
const shouldRender = computed(() => totalPages.value > 1);
const safePage = computed(() => clampPage(props.page, props.total, props.pageSize));
const startItem = computed(() => (props.total === 0 ? 0 : (safePage.value - 1) * props.pageSize + 1));
const endItem = computed(() => Math.min(safePage.value * props.pageSize, props.total));

function emitPage(value: number) {
  emit("update:page", clampPage(value, props.total, props.pageSize));
}

function handlePageSizeChange(event: Event) {
  const nextValue = Number((event.target as HTMLSelectElement).value);
  const safeValue = Number.isFinite(nextValue) && nextValue > 0 ? nextValue : props.pageSize;
  emit("update:pageSize", safeValue);
  emit("update:page", clampPage(1, props.total, safeValue));
}
</script>
