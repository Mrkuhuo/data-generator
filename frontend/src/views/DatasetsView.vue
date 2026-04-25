<template>
  <section class="page">
    <header class="page-header">
      <div>
        <h2>数据集</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="createExampleDataset">快速示例数据集</button>
        <button class="button button--ghost" type="button" @click="loadDatasets">刷新</button>
      </div>
    </header>

    <section class="workspace-grid">
      <article class="panel form-panel">
        <div>
          <h3>{{ selectedDatasetId ? "编辑数据集" : "新建数据集" }}</h3>
        </div>

        <div
          v-if="feedback.message"
          class="status-banner"
          :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
        >
          {{ feedback.message }}
        </div>

        <form class="form-grid" @submit.prevent="saveDataset">
          <div class="field">
            <label>
              <span>名称</span>
              <input v-model.trim="form.name" type="text" placeholder="用户行为数据 v1" />
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>分类</span>
              <input v-model.trim="form.category" type="text" placeholder="电商" />
            </label>

            <label>
              <span>版本</span>
              <input v-model.trim="form.version" type="text" placeholder="v1" />
            </label>
          </div>

          <div class="field">
            <label>
              <span>状态</span>
              <select v-model="form.status">
                <option v-for="status in datasetStatuses" :key="status" :value="status">{{ labelDatasetStatus(status) }}</option>
              </select>
            </label>
          </div>

          <div class="field">
            <label>
              <span>说明</span>
              <textarea v-model.trim="form.description" rows="3" placeholder="描述这个模拟数据模型的用途。" />
            </label>
          </div>

          <div class="field">
            <label>
              <span>Schema JSON</span>
              <textarea
                v-model="form.schemaJson"
                class="code-input"
                placeholder='{"type":"object","fields":{"id":{"rule":"sequence","start":1}}}'
              />
            </label>
          </div>

          <div class="field">
            <label>
              <span>示例配置 JSON</span>
              <textarea
                v-model="form.sampleConfigJson"
                class="code-input"
                placeholder='{"count":5,"seed":20260412}'
              />
            </label>
          </div>

          <div class="button-row">
            <button class="button" type="submit">{{ selectedDatasetId ? "保存数据集" : "创建数据集" }}</button>
            <button class="button button--ghost" type="button" @click="resetForm">重置</button>
            <button
              v-if="selectedDatasetId"
              class="button button--ghost"
              type="button"
              @click="previewDataset(selectedDatasetId)"
            >
              预览当前数据集
            </button>
            <button
              v-if="selectedDatasetId"
              class="button button--danger"
              type="button"
              @click="removeDataset(selectedDatasetId)"
            >
              删除
            </button>
          </div>
        </form>
      </article>

      <div class="stack">
        <article class="panel form-panel">
          <div>
            <h3>预览</h3>
          </div>

          <div class="field field--half">
            <label>
              <span>生成条数</span>
              <input v-model.number="previewForm.count" type="number" min="1" step="1" />
            </label>

            <label>
              <span>随机种子</span>
              <input v-model="previewForm.seed" type="text" placeholder="20260412" />
            </label>
          </div>

          <div class="button-row">
            <button
              class="button"
              type="button"
              :disabled="selectedDatasetId == null"
              @click="selectedDatasetId && previewDataset(selectedDatasetId)"
            >
              生成预览
            </button>
            <span class="muted">{{ selectedDatasetId ? `数据集 #${selectedDatasetId}` : "未选择数据集" }}</span>
          </div>

          <div v-if="previewRows.length" class="preview-block">
            <h3>结果</h3>
            <pre class="code-block">{{ formatJson(previewRows) }}</pre>
          </div>
        </article>

        <template v-if="datasets.length">
          <section class="panel-grid">
          <article
            v-for="dataset in paginatedDatasets"
            :key="dataset.id"
            class="panel"
            :class="{ 'panel--selected': selectedDatasetId === dataset.id }"
          >
            <div class="panel__row">
              <h3>{{ dataset.name }}</h3>
              <span class="pill">{{ labelDatasetStatus(dataset.status) }}</span>
            </div>

            <div class="meta-grid">
              <p class="muted">{{ dataset.category || "未分类" }} / {{ dataset.version }}</p>
              <p>{{ dataset.description || "暂无数据集说明。" }}</p>
              <pre class="code-block">{{ dataset.schemaJson }}</pre>
            </div>

            <div class="panel__actions">
              <button class="button" type="button" @click="selectDataset(dataset)">编辑</button>
              <button class="button button--ghost" type="button" @click="previewDataset(dataset.id)">预览</button>
              <button class="button button--danger" type="button" @click="removeDataset(dataset.id)">删除</button>
            </div>
          </article>
        </section>

        <PaginationBar
          :page="datasetPage"
          :page-size="datasetPageSize"
          :total="datasets.length"
          noun="个数据集"
          @update:page="datasetPage = $event"
          @update:page-size="datasetPageSize = $event"
        />
        </template>

        <section v-else class="empty-state">
          <h3>暂无数据集</h3>
        </section>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import PaginationBar from "../components/PaginationBar.vue";
import { clampPage, paginateItems } from "../utils/pagination";
import { labelDatasetStatus } from "../utils/display";

type FeedbackKind = "success" | "error";
type DatasetStatus = "DRAFT" | "READY" | "ARCHIVED";

interface Dataset {
  id: number;
  name: string;
  category: string | null;
  version: string;
  status: DatasetStatus;
  description: string | null;
  schemaJson: string;
  sampleConfigJson: string;
}

interface DatasetPreviewResponse {
  count: number;
  seed: number;
  rows: Array<Record<string, unknown>>;
}

const datasetStatuses: DatasetStatus[] = ["READY", "DRAFT", "ARCHIVED"];

const defaultSchema = JSON.stringify(
  {
    type: "object",
    fields: {
      id: { rule: "sequence", start: 1, step: 1 },
      email: { rule: "template", template: "user-${id}@demo.local" },
      score: { rule: "random_decimal", min: 80, max: 99.9, scale: 1 }
    }
  },
  null,
  2
);

const defaultSampleConfig = JSON.stringify(
  {
    count: 5,
    seed: 20260412
  },
  null,
  2
);

const datasets = ref<Dataset[]>([]);
const datasetPage = ref(1);
const datasetPageSize = ref(8);
const selectedDatasetId = ref<number | null>(null);
const previewRows = ref<Array<Record<string, unknown>>>([]);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});
const paginatedDatasets = computed(() => paginateItems(datasets.value, datasetPage.value, datasetPageSize.value));

const form = reactive({
  name: "",
  category: "",
  version: "v1",
  status: "READY" as DatasetStatus,
  description: "",
  schemaJson: defaultSchema,
  sampleConfigJson: defaultSampleConfig
});

const previewForm = reactive({
  count: 5,
  seed: "20260412"
});

function setFeedback(kind: FeedbackKind, message: string) {
  feedback.kind = kind;
  feedback.message = message;
}

function clearFeedback() {
  feedback.message = "";
}

function normalizeJson(value: string, label: string) {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch (error) {
    throw new Error(`${label} 必须是合法的 JSON`);
  }
}

function syncPreviewDefaults(sampleConfigJson: string) {
  try {
    const parsed = JSON.parse(sampleConfigJson) as { count?: number; seed?: number | string };
    previewForm.count = parsed.count ?? 5;
    previewForm.seed = parsed.seed == null ? "" : String(parsed.seed);
  } catch (error) {
    previewForm.count = 5;
    previewForm.seed = "";
  }
}

function resetForm() {
  selectedDatasetId.value = null;
  form.name = "";
  form.category = "";
  form.version = "v1";
  form.status = "READY";
  form.description = "";
  form.schemaJson = defaultSchema;
  form.sampleConfigJson = defaultSampleConfig;
  previewRows.value = [];
  syncPreviewDefaults(defaultSampleConfig);
  clearFeedback();
}

function focusDatasetPage(datasetId: number) {
  const datasetIndex = datasets.value.findIndex((dataset) => dataset.id === datasetId);
  if (datasetIndex < 0) {
    return;
  }
  datasetPage.value = Math.floor(datasetIndex / datasetPageSize.value) + 1;
}

function selectDataset(dataset: Dataset) {
  selectedDatasetId.value = dataset.id;
  focusDatasetPage(dataset.id);
  form.name = dataset.name;
  form.category = dataset.category ?? "";
  form.version = dataset.version;
  form.status = dataset.status;
  form.description = dataset.description ?? "";
  form.schemaJson = normalizeJson(dataset.schemaJson, "Schema JSON");
  form.sampleConfigJson = normalizeJson(dataset.sampleConfigJson, "示例配置 JSON");
  syncPreviewDefaults(dataset.sampleConfigJson);
  clearFeedback();
}

async function loadDatasets() {
  try {
    const response = await apiClient.get<ApiResponse<Dataset[]>>("/datasets");
    datasets.value = response.data.success ? response.data.data : [];
    if (feedback.kind === "error" && feedback.message) {
      clearFeedback();
    }

    if (selectedDatasetId.value != null) {
      const refreshed = datasets.value.find((dataset) => dataset.id === selectedDatasetId.value);
      if (refreshed) {
        selectDataset(refreshed);
      } else {
        resetForm();
      }
    }
  } catch (error) {
    datasets.value = [];
    setFeedback("error", readApiError(error, "加载数据集失败"));
  }
}

async function createExampleDataset() {
  try {
    await apiClient.post("/datasets/quickstart");
    await loadDatasets();
    setFeedback("success", "快速示例数据集已创建");
  } catch (error) {
    setFeedback("error", readApiError(error, "创建快速示例数据集失败"));
  }
}

async function saveDataset() {
  try {
    const payload = {
      name: form.name,
      category: form.category || null,
      version: form.version,
      status: form.status,
      description: form.description || null,
      schemaJson: normalizeJson(form.schemaJson, "Schema JSON"),
      sampleConfigJson: normalizeJson(form.sampleConfigJson, "示例配置 JSON")
    };

    if (selectedDatasetId.value) {
      await apiClient.put(`/datasets/${selectedDatasetId.value}`, payload);
      setFeedback("success", "数据集已更新");
    } else {
      await apiClient.post("/datasets", payload);
      setFeedback("success", "数据集已创建");
    }

    await loadDatasets();
  } catch (error) {
    setFeedback("error", readApiError(error, "保存数据集失败"));
  }
}

async function previewDataset(datasetId: number) {
  try {
    const dataset = datasets.value.find((item) => item.id === datasetId);
    if (dataset) {
      selectDataset(dataset);
    }

    const response = await apiClient.post<ApiResponse<DatasetPreviewResponse>>(`/datasets/${datasetId}/preview`, {
      count: previewForm.count || 5,
      seed: previewForm.seed ? Number(previewForm.seed) : null
    });

    if (response.data.success) {
      previewRows.value = response.data.data.rows;
      setFeedback("success", `预览已生成，共 ${response.data.data.count} 条记录`);
    }
  } catch (error) {
    previewRows.value = [];
    setFeedback("error", readApiError(error, "生成预览失败"));
  }
}

async function removeDataset(datasetId: number) {
  try {
    await apiClient.delete(`/datasets/${datasetId}`);
    if (selectedDatasetId.value === datasetId) {
      resetForm();
    }
    await loadDatasets();
    setFeedback("success", "数据集已删除");
  } catch (error) {
    setFeedback("error", readApiError(error, "删除数据集失败"));
  }
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

watch(
  () => datasets.value.length,
  () => {
    datasetPage.value = clampPage(datasetPage.value, datasets.value.length, datasetPageSize.value);
  }
);

watch(datasetPageSize, () => {
  if (selectedDatasetId.value !== null) {
    focusDatasetPage(selectedDatasetId.value);
    return;
  }
  datasetPage.value = 1;
});

onMounted(async () => {
  resetForm();
  await loadDatasets();
});
</script>
