<template>
  <section class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">执行账本</p>
        <h2>查看运行结果、投递快照和执行日志。</h2>
      </div>
      <button class="button button--ghost" type="button" @click="loadPageData">刷新</button>
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
          <h3>执行记录 #{{ execution.id }}</h3>
          <span class="pill">{{ labelExecutionStatus(execution.status) }}</span>
        </div>

        <div class="meta-grid">
          <p class="muted">
            {{ resolveJobName(execution.jobDefinitionId) }} / 触发方式 {{ labelTriggerType(execution.triggerType) }} / 开始于 {{ formatDate(execution.startedAt) }}
          </p>
          <p class="muted">
            结束于 {{ execution.finishedAt ? formatDate(execution.finishedAt) : "-" }} / 生成 {{ execution.generatedCount }} / 成功 {{ execution.successCount }} / 失败 {{ execution.errorCount }}
          </p>
          <p>{{ execution.errorSummary || "暂时没有执行摘要。" }}</p>
        </div>

        <div class="panel__actions">
          <button class="button button--ghost" type="button" @click="loadLogs(execution.id)">
            {{ selectedExecutionId === execution.id ? "刷新日志" : "查看日志" }}
          </button>
        </div>

        <div v-if="selectedExecutionId === execution.id" class="preview-block">
          <div v-if="deliverySnapshot" class="preview-block">
            <p class="eyebrow">投递快照</p>
            <pre class="code-block">{{ formatJson(deliverySnapshot) }}</pre>
          </div>

          <div v-if="logs.length" class="log-list">
            <div v-for="log in parsedLogs" :key="log.id" class="log-entry">
              <span class="pill pill--soft">{{ labelLogLevel(log.logLevel) }}</span>
              <div>
                <strong>{{ log.message }}</strong>
                <pre class="code-block">{{ formatJson(log.detail) }}</pre>
              </div>
            </div>
          </div>

          <p v-else class="muted">当前执行还没有加载到日志。</p>
        </div>
      </article>
    </section>

    <section v-else class="empty-state">
      <h3>还没有执行记录</h3>
      <p>请先在任务页执行一次任务，所有连接器的投递结果都会在这里统一展示。</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import { formatDisplayDate, labelExecutionStatus, labelLogLevel, labelTriggerType } from "../utils/display";

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

  const deliveryLog = parsedLogs.value.find((log) =>
    ["Connector delivery finished", "连接器投递完成"].includes(log.message)
  );
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
    setFeedback("error", readApiError(error, "加载执行记录失败"));
  }
}

async function loadLogs(executionId: number) {
  try {
    const response = await apiClient.get<ApiResponse<ExecutionLog[]>>(`/executions/${executionId}/logs`);
    selectedExecutionId.value = executionId;
    logs.value = response.data.success ? response.data.data : [];
    setFeedback("success", `已加载执行记录 #${executionId} 的 ${logs.value.length} 条日志`);
  } catch (error) {
    selectedExecutionId.value = executionId;
    logs.value = [];
    setFeedback("error", readApiError(error, "加载执行日志失败"));
  }
}

function resolveJobName(jobId: number) {
  return jobs.value.find((job) => job.id === jobId)?.name ?? `任务 #${jobId}`;
}

function formatDate(value: string) {
  return formatDisplayDate(value);
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

onMounted(loadPageData);
</script>
