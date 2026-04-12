<template>
  <section class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">Execution Ledger</p>
        <h2>Inspect run outcomes, delivery snapshots, and execution logs.</h2>
      </div>
      <button class="button button--ghost" type="button" @click="loadPageData">Refresh</button>
    </header>

    <div
      v-if="feedback.message"
      class="status-banner"
      :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
    >
      {{ feedback.message }}
    </div>

    <section v-if="executions.length" class="stack">
      <article
        v-for="execution in executions"
        :key="execution.id"
        class="panel"
        :class="{ 'panel--selected': selectedExecutionId === execution.id }"
      >
        <div class="panel__row">
          <h3>Execution #{{ execution.id }}</h3>
          <span class="pill">{{ execution.status }}</span>
        </div>

        <div class="meta-grid">
          <p class="muted">
            {{ resolveJobName(execution.jobDefinitionId) }} / Trigger {{ execution.triggerType }} / Started {{ formatDate(execution.startedAt) }}
          </p>
          <p class="muted">
            Finished {{ execution.finishedAt ? formatDate(execution.finishedAt) : "-" }} / Generated {{ execution.generatedCount }} / Success {{ execution.successCount }} / Error {{ execution.errorCount }}
          </p>
          <p>{{ execution.errorSummary || "No execution summary yet." }}</p>
        </div>

        <div class="panel__actions">
          <button class="button button--ghost" type="button" @click="loadLogs(execution.id)">
            {{ selectedExecutionId === execution.id ? "Refresh Logs" : "Inspect Logs" }}
          </button>
        </div>

        <div v-if="selectedExecutionId === execution.id" class="preview-block">
          <div v-if="deliverySnapshot" class="preview-block">
            <p class="eyebrow">Delivery Snapshot</p>
            <pre class="code-block">{{ formatJson(deliverySnapshot) }}</pre>
          </div>

          <div v-if="logs.length" class="log-list">
            <div v-for="log in parsedLogs" :key="log.id" class="log-entry">
              <span class="pill pill--soft">{{ log.logLevel }}</span>
              <div>
                <strong>{{ log.message }}</strong>
                <pre class="code-block">{{ formatJson(log.detail) }}</pre>
              </div>
            </div>
          </div>

          <p v-else class="muted">No logs loaded for this execution yet.</p>
        </div>
      </article>
    </section>

    <section v-else class="empty-state">
      <h3>No executions yet</h3>
      <p>Use the job page to create the first execution. All current connectors now flow through the same execution ledger.</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";

type FeedbackKind = "success" | "error";

interface Execution {
  id: number;
  jobDefinitionId: number;
  triggerType: string;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  generatedCount: number;
  successCount: number;
  errorCount: number;
  errorSummary: string | null;
  deliveryDetailsJson: string | null;
}

interface ExecutionLog {
  id: number;
  logLevel: string;
  message: string;
  detailJson: string | null;
}

interface JobOption {
  id: number;
  name: string;
}

const executions = ref<Execution[]>([]);
const jobs = ref<JobOption[]>([]);
const logs = ref<ExecutionLog[]>([]);
const selectedExecutionId = ref<number | null>(null);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});

const parsedLogs = computed(() =>
  logs.value.map((log) => ({
    ...log,
    detail: parseJson(log.detailJson)
  }))
);

const deliverySnapshot = computed(() => {
  const selectedExecution = executions.value.find((execution) => execution.id === selectedExecutionId.value);
  if (selectedExecution?.deliveryDetailsJson) {
    return parseJson(selectedExecution.deliveryDetailsJson);
  }

  const deliveryLog = parsedLogs.value.find((log) => log.message === "Connector delivery finished");
  if (!deliveryLog || typeof deliveryLog.detail !== "object" || deliveryLog.detail == null) {
    return null;
  }

  const detail = deliveryLog.detail as Record<string, unknown>;
  return {
    connectorId: detail.connectorId,
    deliveryStatus: detail.deliveryStatus,
    deliveredCount: detail.deliveredCount,
    errorCount: detail.errorCount,
    details: parseNestedJson(detail.details)
  };
});

function setFeedback(kind: FeedbackKind, message: string) {
  feedback.kind = kind;
  feedback.message = message;
}

function parseJson(value: string | null) {
  if (!value) {
    return {};
  }

  try {
    return JSON.parse(value);
  } catch (error) {
    return { raw: value };
  }
}

function parseNestedJson(value: unknown) {
  if (typeof value !== "string") {
    return value;
  }

  try {
    return JSON.parse(value);
  } catch (error) {
    return value;
  }
}

async function loadPageData() {
  try {
    const [executionResponse, jobsResponse] = await Promise.all([
      apiClient.get<ApiResponse<Execution[]>>("/executions"),
      apiClient.get<ApiResponse<JobOption[]>>("/jobs")
    ]);

    executions.value = executionResponse.data.success ? executionResponse.data.data : [];
    jobs.value = jobsResponse.data.success ? jobsResponse.data.data : [];

    if (selectedExecutionId.value != null && !executions.value.some((execution) => execution.id === selectedExecutionId.value)) {
      selectedExecutionId.value = null;
      logs.value = [];
    }
  } catch (error) {
    executions.value = [];
    jobs.value = [];
    setFeedback("error", readApiError(error, "Failed to load executions"));
  }
}

async function loadLogs(executionId: number) {
  try {
    const response = await apiClient.get<ApiResponse<ExecutionLog[]>>(`/executions/${executionId}/logs`);
    selectedExecutionId.value = executionId;
    logs.value = response.data.success ? response.data.data : [];
    setFeedback("success", `Loaded ${logs.value.length} logs for execution #${executionId}`);
  } catch (error) {
    selectedExecutionId.value = executionId;
    logs.value = [];
    setFeedback("error", readApiError(error, "Failed to load execution logs"));
  }
}

function resolveJobName(jobId: number) {
  return jobs.value.find((job) => job.id === jobId)?.name ?? `Job #${jobId}`;
}

function formatDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

onMounted(loadPageData);
</script>
