<template>
  <section class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">Dataset Studio</p>
        <h2>Define reusable generation rules and preview them instantly.</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="createExampleDataset">Quickstart Dataset</button>
        <button class="button button--ghost" type="button" @click="loadDatasets">Refresh</button>
      </div>
    </header>

    <section class="workspace-grid">
      <article class="panel form-panel">
        <div>
          <p class="eyebrow">{{ selectedDatasetId ? "Edit Dataset" : "Create Dataset" }}</p>
          <h3>{{ selectedDatasetId ? "Update dataset definition" : "New dataset definition" }}</h3>
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
              <span>Name</span>
              <input v-model.trim="form.name" type="text" placeholder="User Activity v1" />
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>Category</span>
              <input v-model.trim="form.category" type="text" placeholder="commerce" />
            </label>

            <label>
              <span>Version</span>
              <input v-model.trim="form.version" type="text" placeholder="v1" />
            </label>
          </div>

          <div class="field">
            <label>
              <span>Status</span>
              <select v-model="form.status">
                <option v-for="status in datasetStatuses" :key="status" :value="status">{{ status }}</option>
              </select>
            </label>
          </div>

          <div class="field">
            <label>
              <span>Description</span>
              <textarea v-model.trim="form.description" rows="3" placeholder="Describe this synthetic model." />
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
              <span>Sample Config JSON</span>
              <textarea
                v-model="form.sampleConfigJson"
                class="code-input"
                placeholder='{"count":5,"seed":20260412}'
              />
            </label>
          </div>

          <div class="button-row">
            <button class="button" type="submit">{{ selectedDatasetId ? "Save Dataset" : "Create Dataset" }}</button>
            <button class="button button--ghost" type="button" @click="resetForm">Reset</button>
            <button
              v-if="selectedDatasetId"
              class="button button--ghost"
              type="button"
              @click="previewDataset(selectedDatasetId)"
            >
              Preview Selected
            </button>
            <button
              v-if="selectedDatasetId"
              class="button button--danger"
              type="button"
              @click="removeDataset(selectedDatasetId)"
            >
              Delete
            </button>
          </div>
        </form>
      </article>

      <div class="stack">
        <article class="panel form-panel">
          <div>
            <p class="eyebrow">Preview Console</p>
            <h3>Preview a saved dataset with custom count and seed.</h3>
          </div>

          <div class="field field--half">
            <label>
              <span>Count</span>
              <input v-model.number="previewForm.count" type="number" min="1" step="1" />
            </label>

            <label>
              <span>Seed</span>
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
              Generate Preview
            </button>
            <span class="muted">{{ selectedDatasetId ? `Current dataset #${selectedDatasetId}` : "Select a saved dataset first" }}</span>
          </div>

          <div v-if="previewRows.length" class="preview-block">
            <p class="eyebrow">Preview Rows</p>
            <pre class="code-block">{{ formatJson(previewRows) }}</pre>
          </div>
        </article>

        <section v-if="datasets.length" class="panel-grid">
          <article
            v-for="dataset in datasets"
            :key="dataset.id"
            class="panel"
            :class="{ 'panel--selected': selectedDatasetId === dataset.id }"
          >
            <div class="panel__row">
              <h3>{{ dataset.name }}</h3>
              <span class="pill">{{ dataset.status }}</span>
            </div>

            <div class="meta-grid">
              <p class="muted">{{ dataset.category || "Uncategorized" }} / {{ dataset.version }}</p>
              <p>{{ dataset.description || "Dataset description is still empty." }}</p>
              <pre class="code-block">{{ dataset.schemaJson }}</pre>
            </div>

            <div class="panel__actions">
              <button class="button" type="button" @click="selectDataset(dataset)">Edit</button>
              <button class="button button--ghost" type="button" @click="previewDataset(dataset.id)">Preview</button>
              <button class="button button--danger" type="button" @click="removeDataset(dataset.id)">Delete</button>
            </div>
          </article>
        </section>

        <section v-else class="empty-state">
          <h3>No dataset definitions yet</h3>
          <p>Create one from the form or load the quickstart dataset to start validating generation rules.</p>
        </section>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";

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
const selectedDatasetId = ref<number | null>(null);
const previewRows = ref<Array<Record<string, unknown>>>([]);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});

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
    throw new Error(`${label} must be valid JSON`);
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

function selectDataset(dataset: Dataset) {
  selectedDatasetId.value = dataset.id;
  form.name = dataset.name;
  form.category = dataset.category ?? "";
  form.version = dataset.version;
  form.status = dataset.status;
  form.description = dataset.description ?? "";
  form.schemaJson = normalizeJson(dataset.schemaJson, "Schema JSON");
  form.sampleConfigJson = normalizeJson(dataset.sampleConfigJson, "Sample config JSON");
  syncPreviewDefaults(dataset.sampleConfigJson);
  clearFeedback();
}

async function loadDatasets() {
  try {
    const response = await apiClient.get<ApiResponse<Dataset[]>>("/datasets");
    datasets.value = response.data.success ? response.data.data : [];

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
    setFeedback("error", readApiError(error, "Failed to load datasets"));
  }
}

async function createExampleDataset() {
  try {
    await apiClient.post("/datasets/quickstart");
    await loadDatasets();
    setFeedback("success", "Quickstart dataset created");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to create quickstart dataset"));
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
      sampleConfigJson: normalizeJson(form.sampleConfigJson, "Sample config JSON")
    };

    if (selectedDatasetId.value) {
      await apiClient.put(`/datasets/${selectedDatasetId.value}`, payload);
      setFeedback("success", "Dataset updated");
    } else {
      await apiClient.post("/datasets", payload);
      setFeedback("success", "Dataset created");
    }

    await loadDatasets();
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to save dataset"));
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
      setFeedback("success", `Preview generated with ${response.data.data.count} rows`);
    }
  } catch (error) {
    previewRows.value = [];
    setFeedback("error", readApiError(error, "Failed to preview dataset"));
  }
}

async function removeDataset(datasetId: number) {
  try {
    await apiClient.delete(`/datasets/${datasetId}`);
    if (selectedDatasetId.value === datasetId) {
      resetForm();
    }
    await loadDatasets();
    setFeedback("success", "Dataset deleted");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to delete dataset"));
  }
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

onMounted(async () => {
  resetForm();
  await loadDatasets();
});
</script>
