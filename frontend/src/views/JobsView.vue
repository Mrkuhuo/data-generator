<template>
  <section class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">Job Control</p>
        <h2>Bind datasets to connectors and define delivery runtime.</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="createExampleJob">Quickstart Job</button>
        <button class="button button--ghost" type="button" @click="loadPageData">Refresh</button>
      </div>
    </header>

    <section class="workspace-grid">
      <article class="panel form-panel">
        <div>
          <p class="eyebrow">{{ selectedJobId ? "Edit Job" : "Create Job" }}</p>
          <h3>{{ selectedJobId ? "Update job definition" : "New job definition" }}</h3>
        </div>

        <div
          v-if="feedback.message"
          class="status-banner"
          :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
        >
          {{ feedback.message }}
        </div>

        <form class="form-grid" @submit.prevent="saveJob">
          <div class="field">
            <label>
              <span>Name</span>
              <input v-model.trim="form.name" type="text" placeholder="Deliver users to MySQL" />
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>Dataset</span>
              <select v-model.number="form.datasetDefinitionId">
                <option :value="null">Select dataset</option>
                <option v-for="dataset in datasets" :key="dataset.id" :value="dataset.id">{{ dataset.name }}</option>
              </select>
            </label>

            <label>
              <span>Connector</span>
              <select v-model.number="form.targetConnectorId">
                <option :value="null">Select connector</option>
                <option v-for="connector in connectors" :key="connector.id" :value="connector.id">
                  {{ connector.name }} ({{ connector.connectorType }})
                </option>
              </select>
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>Write Strategy</span>
              <select v-model="form.writeStrategy">
                <option v-for="strategy in writeStrategies" :key="strategy" :value="strategy">{{ strategy }}</option>
              </select>
            </label>

            <label>
              <span>Status</span>
              <select v-model="form.status">
                <option v-for="status in jobStatuses" :key="status" :value="status">{{ status }}</option>
              </select>
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>Schedule Type</span>
              <select v-model="form.scheduleType">
                <option v-for="type in scheduleTypes" :key="type" :value="type">{{ type }}</option>
              </select>
            </label>

            <label>
              <span>Cron Expression</span>
              <input
                v-model.trim="form.cronExpression"
                type="text"
                :placeholder="form.scheduleType === 'CRON' ? '0 */5 * * * ?' : form.scheduleType === 'ONCE' ? 'Use runtimeConfig schedule.triggerAt' : 'Only required for CRON'"
              />
            </label>
          </div>

          <div class="field">
            <label>
              <span>Runtime Config JSON</span>
              <textarea
                v-model="form.runtimeConfigJson"
                class="code-input"
                placeholder='{"count":100,"target":{"table":"synthetic_user_activity"}}'
              />
            </label>
          </div>

          <div class="button-row">
            <button class="button" type="submit">{{ selectedJobId ? "Save Job" : "Create Job" }}</button>
            <button class="button button--ghost" type="button" @click="resetForm">Reset</button>
            <button class="button button--ghost" type="button" @click="loadRuntimeTemplate">Load Runtime Template</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="runJob(selectedJobId)">Run Selected</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="pauseJob(selectedJobId)">Pause</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="resumeJob(selectedJobId)">Resume</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="disableJob(selectedJobId)">Disable</button>
            <button v-if="selectedJobId" class="button button--danger" type="button" @click="removeJob(selectedJobId)">Delete</button>
          </div>

          <p class="muted" v-if="!datasets.length || !connectors.length">
            Create at least one dataset and one connector first, otherwise job creation will fail.
          </p>
        </form>
      </article>

      <div class="stack">
        <section v-if="jobs.length" class="panel-grid">
          <article
            v-for="job in jobs"
            :key="job.id"
            class="panel"
            :class="{ 'panel--selected': selectedJobId === job.id }"
          >
            <div class="panel__row">
              <h3>{{ job.name }}</h3>
              <span class="pill">{{ job.status }}</span>
            </div>

            <div class="meta-grid">
              <p class="muted">
                {{ resolveDatasetName(job.datasetDefinitionId) }} -> {{ resolveConnectorName(job.targetConnectorId) }}
              </p>
              <p>Write strategy: {{ job.writeStrategy }} / Schedule: {{ job.scheduleType }}</p>
              <p class="muted">
                Scheduler: {{ job.schedulerState || "N/A" }}
                <span v-if="job.nextFireAt"> / Next {{ formatDate(job.nextFireAt) }}</span>
                <span v-if="job.previousFireAt"> / Previous {{ formatDate(job.previousFireAt) }}</span>
              </p>
              <pre class="code-block">{{ job.runtimeConfigJson }}</pre>
            </div>

            <div class="panel__actions">
              <button class="button" type="button" @click="selectJob(job)">Edit</button>
              <button class="button button--ghost" type="button" @click="runJob(job.id)">Run</button>
              <button class="button button--ghost" type="button" @click="pauseJob(job.id)">Pause</button>
              <button class="button button--ghost" type="button" @click="resumeJob(job.id)">Resume</button>
              <button class="button button--ghost" type="button" @click="disableJob(job.id)">Disable</button>
              <button class="button button--danger" type="button" @click="removeJob(job.id)">Delete</button>
            </div>
          </article>
        </section>

        <section v-else class="empty-state">
          <h3>No jobs yet</h3>
          <p>Create one from the form to connect a dataset to any configured file, HTTP, MySQL, PostgreSQL, or Kafka target.</p>
        </section>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";

type FeedbackKind = "success" | "error";
type JobStatus = "DRAFT" | "READY" | "RUNNING" | "PAUSED" | "DISABLED";
type JobScheduleType = "MANUAL" | "ONCE" | "CRON";
type JobWriteStrategy = "APPEND" | "OVERWRITE" | "UPSERT" | "STREAM";

interface Job {
  id: number;
  name: string;
  datasetDefinitionId: number;
  targetConnectorId: number;
  writeStrategy: JobWriteStrategy;
  scheduleType: JobScheduleType;
  cronExpression: string | null;
  status: JobStatus;
  runtimeConfigJson: string;
  schedulerState: string | null;
  nextFireAt: string | null;
  previousFireAt: string | null;
}

interface DatasetOption {
  id: number;
  name: string;
}

interface ConnectorOption {
  id: number;
  name: string;
  connectorType: string;
}

const writeStrategies: JobWriteStrategy[] = ["APPEND", "OVERWRITE", "STREAM", "UPSERT"];
const scheduleTypes: JobScheduleType[] = ["MANUAL", "ONCE", "CRON"];
const jobStatuses: JobStatus[] = ["READY", "DRAFT", "PAUSED", "DISABLED", "RUNNING"];

const runtimeTemplate = JSON.stringify(
  {
    count: 100,
    seed: 20260412,
    batchSize: 500,
    target: {
      table: "synthetic_user_activity",
      topic: "synthetic.user.activity",
      keyField: "userId"
    },
    retry: {
      maxAttempts: 2,
      backoffMs: 1000
    },
    schedule: {
      triggerAt: "2026-04-12T14:00:00Z"
    }
  },
  null,
  2
);

const jobs = ref<Job[]>([]);
const datasets = ref<DatasetOption[]>([]);
const connectors = ref<ConnectorOption[]>([]);
const selectedJobId = ref<number | null>(null);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});

const form = reactive({
  name: "",
  datasetDefinitionId: null as number | null,
  targetConnectorId: null as number | null,
  writeStrategy: "APPEND" as JobWriteStrategy,
  scheduleType: "MANUAL" as JobScheduleType,
  cronExpression: "",
  status: "READY" as JobStatus,
  runtimeConfigJson: runtimeTemplate
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

function resetForm() {
  selectedJobId.value = null;
  form.name = "";
  form.datasetDefinitionId = null;
  form.targetConnectorId = null;
  form.writeStrategy = "APPEND";
  form.scheduleType = "MANUAL";
  form.cronExpression = "";
  form.status = "READY";
  form.runtimeConfigJson = runtimeTemplate;
  clearFeedback();
}

function loadRuntimeTemplate() {
  form.runtimeConfigJson = runtimeTemplate;
}

function selectJob(job: Job) {
  selectedJobId.value = job.id;
  form.name = job.name;
  form.datasetDefinitionId = job.datasetDefinitionId;
  form.targetConnectorId = job.targetConnectorId;
  form.writeStrategy = job.writeStrategy;
  form.scheduleType = job.scheduleType;
  form.cronExpression = job.cronExpression ?? "";
  form.status = job.status;
  form.runtimeConfigJson = normalizeJson(job.runtimeConfigJson, "Runtime config");
  clearFeedback();
}

async function loadPageData() {
  try {
    const [jobsResponse, datasetsResponse, connectorsResponse] = await Promise.all([
      apiClient.get<ApiResponse<Job[]>>("/jobs"),
      apiClient.get<ApiResponse<DatasetOption[]>>("/datasets"),
      apiClient.get<ApiResponse<ConnectorOption[]>>("/connectors")
    ]);

    jobs.value = jobsResponse.data.success ? jobsResponse.data.data : [];
    datasets.value = datasetsResponse.data.success ? datasetsResponse.data.data : [];
    connectors.value = connectorsResponse.data.success ? connectorsResponse.data.data : [];

    if (selectedJobId.value != null) {
      const refreshed = jobs.value.find((job) => job.id === selectedJobId.value);
      if (refreshed) {
        selectJob(refreshed);
      } else {
        resetForm();
      }
    }
  } catch (error) {
    jobs.value = [];
    datasets.value = [];
    connectors.value = [];
    setFeedback("error", readApiError(error, "Failed to load job workspace"));
  }
}

async function createExampleJob() {
  try {
    await apiClient.post("/jobs/quickstart");
    await loadPageData();
    setFeedback("success", "Quickstart job created");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to create quickstart job"));
  }
}

async function saveJob() {
  try {
    const payload = {
      name: form.name,
      datasetDefinitionId: form.datasetDefinitionId,
      targetConnectorId: form.targetConnectorId,
      writeStrategy: form.writeStrategy,
      scheduleType: form.scheduleType,
      cronExpression: form.scheduleType === "CRON" ? form.cronExpression || null : null,
      status: form.status,
      runtimeConfigJson: normalizeJson(form.runtimeConfigJson, "Runtime config")
    };

    if (selectedJobId.value) {
      await apiClient.put(`/jobs/${selectedJobId.value}`, payload);
      setFeedback("success", "Job updated");
    } else {
      await apiClient.post("/jobs", payload);
      setFeedback("success", "Job created");
    }

    await loadPageData();
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to save job"));
  }
}

async function runJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/run`);
    await loadPageData();
    setFeedback("success", "Job execution started");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to run job"));
  }
}

async function pauseJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/pause`);
    await loadPageData();
    setFeedback("success", "Job paused");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to pause job"));
  }
}

async function resumeJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/resume`);
    await loadPageData();
    setFeedback("success", "Job resumed");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to resume job"));
  }
}

async function disableJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/disable`);
    await loadPageData();
    setFeedback("success", "Job disabled");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to disable job"));
  }
}

async function removeJob(jobId: number) {
  try {
    await apiClient.delete(`/jobs/${jobId}`);
    if (selectedJobId.value === jobId) {
      resetForm();
    }
    await loadPageData();
    setFeedback("success", "Job deleted");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to delete job"));
  }
}

function resolveDatasetName(datasetId: number) {
  return datasets.value.find((dataset) => dataset.id === datasetId)?.name ?? `dataset #${datasetId}`;
}

function resolveConnectorName(connectorId: number) {
  const connector = connectors.value.find((item) => item.id === connectorId);
  if (!connector) {
    return `connector #${connectorId}`;
  }
  return `${connector.name} (${connector.connectorType})`;
}

function formatDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

onMounted(async () => {
  resetForm();
  await loadPageData();
});
</script>
