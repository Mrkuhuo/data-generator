<template>
  <section class="page">
    <header class="page-header">
      <div class="page-header__content">
        <h2>任务实例</h2>
        <p class="muted">
          <span v-if="task">
            {{ task.name }} / {{ task.tableName }}
          </span>
          <span v-else>按任务查看每一次写入实例、写入结果和日志。</span>
        </p>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="goToTask">返回任务</button>
        <button class="button button--ghost" type="button" @click="loadPageData">刷新</button>
      </div>
    </header>

    <div
      v-if="feedback.message"
      class="status-banner"
      :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
    >
      {{ feedback.message }}
    </div>

    <section class="stats-grid stats-grid--compact">
      <article class="stat-card">
        <span>实例总数</span>
        <strong>{{ executions.length }}</strong>
      </article>
      <article class="stat-card">
        <span>成功 / 部分成功</span>
        <strong>{{ successExecutionCount }}</strong>
      </article>
      <article class="stat-card">
        <span>失败</span>
        <strong>{{ failedExecutionCount }}</strong>
      </article>
      <article class="stat-card">
        <span>执行中</span>
        <strong>{{ runningExecutionCount }}</strong>
      </article>
    </section>

    <section
      v-if="executions.length"
      class="workspace-grid executions-workspace"
      :class="{
        'executions-workspace--single': !showExecutionDetail,
        'executions-workspace--focused': showExecutionDetail
      }"
    >
      <section v-if="!isListCollapsed" class="panel executions-list">
        <div class="panel__row panel__row--divider">
          <div>
            <p class="list-caption">实例较多时按页查看，详情区会保持稳定。</p>
            <h3>实例列表</h3>
          </div>
        </div>

        <div class="task-list">
          <article
            v-for="execution in paginatedExecutions"
            :key="execution.id"
            class="task-list__item"
            :class="{ 'panel--selected': selectedExecutionId === execution.id }"
          >
            <div class="task-list__main">
              <div class="panel__row">
                <h3>实例 #{{ execution.id }}</h3>
                <span class="pill">{{ labelExecutionStatus(execution.status) }}</span>
              </div>

              <div class="task-list__meta">
                <span>{{ labelTriggerType(execution.triggerType) }}</span>
                <span>开始于：{{ formatDate(execution.startedAt) }}</span>
                <span>结束于：{{ execution.finishedAt ? formatDate(execution.finishedAt) : "-" }}</span>
                <span>生成 {{ execution.generatedCount }}</span>
                <span>成功 {{ execution.successCount }}</span>
                <span>失败 {{ execution.errorCount }}</span>
              </div>

              <div v-if="readExecutionSummary(execution)" class="meta-grid">
                <p>
                  <template v-if="readExecutionSummary(execution)?.deliveryType === 'KAFKA'">
                    <strong>本次写入：</strong>{{ readExecutionSummary(execution)?.writtenRowCount }} 条消息 /
                    <strong>Topic：</strong>{{ readExecutionSummary(execution)?.topic || "-" }} /
                    <strong>Payload：</strong>{{ readExecutionSummary(execution)?.payloadFormat || "JSON" }}
                  </template>
                  <template v-else>
                    <strong>本次写入：</strong>{{ readExecutionSummary(execution)?.writtenRowCount }} 条 /
                    <strong>写入前：</strong>{{ readExecutionSummary(execution)?.beforeWriteRowCount }} 条 /
                    <strong>写入后：</strong>{{ readExecutionSummary(execution)?.afterWriteRowCount }} 条 /
                    <strong>净变化：</strong>{{ readExecutionSummary(execution)?.rowDelta }} 条
                  </template>
                </p>
                <p v-if="readExecutionSummary(execution)?.deliveryType === 'KAFKA'">
                  <strong>Key 模式：</strong>{{ readExecutionSummary(execution)?.keyMode || "NONE" }} /
                  <strong>Key 路径：</strong>{{ readExecutionSummary(execution)?.keyPath || readExecutionSummary(execution)?.keyField || "-" }} /
                  <strong>固定 Key：</strong>{{ readExecutionSummary(execution)?.fixedKey || "-" }} /
                  <strong>Headers：</strong>{{ readExecutionSummary(execution)?.headerCount }}
                </p>
                <p v-else-if="readExecutionSummary(execution)?.hasValidation">
                  <strong>非空校验：</strong>{{ readExecutionSummary(execution)?.validationPassed ? "通过" : "未通过" }} /
                  <strong>空值：</strong>{{ readExecutionSummary(execution)?.nullValueCount }} /
                  <strong>空字符串：</strong>{{ readExecutionSummary(execution)?.blankStringCount }}
                </p>
              </div>

              <p v-if="execution.errorSummary" class="muted">{{ execution.errorSummary }}</p>
            </div>

            <div class="task-list__actions">
              <button class="button button--ghost" type="button" @click="openExecutionDetail(execution.id)">
                {{ selectedExecutionId === execution.id ? "刷新详情与日志" : "查看实例详情" }}
              </button>
            </div>
          </article>
        </div>

        <PaginationBar
          :page="executionPage"
          :page-size="executionPageSize"
          :total="executions.length"
          noun="条实例"
          @update:page="executionPage = $event"
          @update:page-size="executionPageSize = $event"
        />
      </section>

      <aside v-if="selectedExecutionId !== null || isExecutionDetailPage" ref="detailPanelRef" class="stack executions-detail">
        <section class="panel executions-detail__panel">
          <div class="panel__row panel__row--divider">
            <div>
              <h3>{{ selectedExecution ? `实例 #${selectedExecution.id}` : "请选择一条实例" }}</h3>
            </div>
            <div v-if="selectedExecution" class="panel__actions panel__actions--tight">
              <button
                v-if="!isListCollapsed"
                class="button button--ghost"
                type="button"
                @click="collapseExecutionList"
              >
                收起列表
              </button>
              <button
                v-else
                class="button button--ghost"
                type="button"
                @click="expandExecutionList"
              >
                展开列表
              </button>
            </div>
          </div>

          <template v-if="selectedExecution">
            <div class="meta-grid">
              <p>
                <strong>任务：</strong>{{ task?.name || `任务 #${routedTaskId}` }} /
                <strong>状态：</strong>{{ labelExecutionStatus(selectedExecution.status) }} /
                <strong>触发方式：</strong>{{ labelTriggerType(selectedExecution.triggerType) }}
              </p>
              <p>
                <strong>开始：</strong>{{ formatDate(selectedExecution.startedAt) }} /
                <strong>结束：</strong>{{ selectedExecution.finishedAt ? formatDate(selectedExecution.finishedAt) : "-" }}
              </p>
            </div>

            <div class="data-list">
              <div class="data-list__row">
                <strong>生成条数</strong>
                <span>{{ selectedExecution.generatedCount }}</span>
              </div>
              <div class="data-list__row">
                <strong>成功条数</strong>
                <span>{{ selectedExecution.successCount }}</span>
              </div>
              <div class="data-list__row">
                <strong>失败条数</strong>
                <span>{{ selectedExecution.errorCount }}</span>
              </div>
              <div v-if="selectedWriteSummary?.hasBeforeAfterMetrics" class="data-list__row">
                <strong>写入前 / 写入后</strong>
                <span>{{ selectedWriteSummary.beforeWriteRowCount }} / {{ selectedWriteSummary.afterWriteRowCount }}</span>
              </div>
              <div v-if="selectedWriteSummary?.hasBeforeAfterMetrics" class="data-list__row">
                <strong>净变化</strong>
                <span>{{ selectedWriteSummary.rowDelta }}</span>
              </div>
              <div v-if="selectedWriteSummary?.deliveryType === 'KAFKA'" class="data-list__row">
                <strong>Topic</strong>
                <span>{{ selectedWriteSummary.topic || "-" }}</span>
              </div>
              <div v-if="selectedWriteSummary?.deliveryType === 'KAFKA'" class="data-list__row">
                <strong>Key / Headers</strong>
                <span>{{ selectedWriteSummary.keyMode || "NONE" }} / {{ selectedWriteSummary.headerCount }}</span>
              </div>
            </div>

            <div v-if="selectedExecution.errorSummary" class="status-banner status-banner--error">
              {{ selectedExecution.errorSummary }}
            </div>

            <section v-if="selectedWriteSummary?.hasValidation" class="record-detail__section">
              <h3>写入校验</h3>
              <div class="meta-grid">
                <p>
                  <strong>非空校验：</strong>{{ selectedWriteSummary.validationPassed ? "通过" : "未通过" }} /
                  <strong>空值：</strong>{{ selectedWriteSummary.nullValueCount }} /
                  <strong>空字符串：</strong>{{ selectedWriteSummary.blankStringCount }}
                </p>
              </div>
              <PaginationBar
                :page="validationIssuePage"
                :page-size="validationIssuePageSize"
                :total="selectedValidationIssues.length"
                noun="个问题"
                @update:page="validationIssuePage = $event"
                @update:page-size="validationIssuePageSize = $event"
              />
            </section>

            <section v-if="selectedWriteSummary?.deliveryType === 'KAFKA'" class="record-detail__section">
              <h3>Kafka 摘要</h3>
              <div class="meta-grid">
                <p>
                  <strong>Topic：</strong>{{ selectedWriteSummary.topic || "-" }} /
                  <strong>Payload：</strong>{{ selectedWriteSummary.payloadFormat || "JSON" }}
                </p>
                <p>
                  <strong>Key 模式：</strong>{{ selectedWriteSummary.keyMode || "NONE" }} /
                  <strong>Key 字段：</strong>{{ selectedWriteSummary.keyPath || selectedWriteSummary.keyField || "-" }} /
                  <strong>固定 Key：</strong>{{ selectedWriteSummary.fixedKey || "-" }}
                </p>
                <p>
                  <strong>分区：</strong>{{ selectedWriteSummary.partition ?? "-" }} /
                  <strong>Headers：</strong>{{ selectedWriteSummary.headerCount }}
                </p>
              </div>
            </section>

            <section v-if="selectedValidationIssues.length" class="record-detail__section">
              <h3>校验问题</h3>
              <div class="log-list">
                <div v-for="issue in paginatedValidationIssues" :key="`${issue.columnName}-${issue.issueType}`" class="log-entry">
                  <span class="pill pill--soft">{{ issue.issueType }}</span>
                  <div>
                    <strong>{{ issue.columnName }}</strong>
                    <p class="muted">{{ issue.message }}，影响 {{ issue.affectedRowCount }} 行</p>
                  </div>
                </div>
              </div>
              <PaginationBar
                :page="validationIssuePage"
                :page-size="validationIssuePageSize"
                :total="selectedValidationIssues.length"
                noun="个问题"
                @update:page="validationIssuePage = $event"
                @update:page-size="validationIssuePageSize = $event"
              />
            </section>

            <section v-if="selectedDeliverySnapshot" class="record-detail__section">
              <h3>投递详情</h3>
              <pre class="code-block">{{ formatJson(selectedDeliverySnapshot) }}</pre>
            </section>

            <section class="record-detail__section">
              <h3>运行日志</h3>
              <div v-if="logs.length" class="log-list">
                <div v-for="log in paginatedLogs" :key="log.id" class="log-entry">
                  <span class="pill pill--soft">{{ labelLogLevel(log.logLevel) }}</span>
                  <div>
                    <strong>{{ log.message }}</strong>
                    <pre class="code-block">{{ formatJson(log.detail) }}</pre>
                  </div>
                </div>
              </div>
              <PaginationBar
                v-if="logs.length"
                :page="logPage"
                :page-size="logPageSize"
                :total="logs.length"
                noun="条日志"
                @update:page="logPage = $event"
                @update:page-size="logPageSize = $event"
              />
              <p v-else class="muted">暂无日志</p>
            </section>
          </template>

          <section v-else class="empty-state">
            <h3>未选择实例</h3>
          </section>
        </section>
      </aside>
    </section>

    <section v-else class="empty-state">
      <h3>暂无执行实例</h3>
      <p>先回到任务页执行一次，这里就会留下实例记录。</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import PaginationBar from "../components/PaginationBar.vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import { clampPage, paginateItems } from "../utils/pagination";
import {
  formatDisplayDate,
  labelExecutionStatus,
  labelLogLevel,
  labelTriggerType
} from "../utils/display";

type FeedbackKind = "success" | "error";

interface Execution {
  id: number;
  writeTaskId: number;
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

interface TaskDetail {
  id: number;
  name: string;
  tableName: string;
}

interface ValidationIssue {
  columnName: string;
  issueType: string;
  affectedRowCount: number;
  message: string;
}

interface ExecutionWriteSummary {
  deliveryType: string;
  writtenRowCount: number;
  beforeWriteRowCount: number;
  afterWriteRowCount: number;
  rowDelta: number;
  hasBeforeAfterMetrics: boolean;
  hasValidation: boolean;
  validationPassed: boolean;
  nullValueCount: number;
  blankStringCount: number;
  topic: string | null;
  payloadFormat: string | null;
  keyMode: string | null;
  keyField: string | null;
  keyPath: string | null;
  fixedKey: string | null;
  partition: number | null;
  headerCount: number;
}

const executions = ref<Execution[]>([]);
const logs = ref<ExecutionLog[]>([]);
const task = ref<TaskDetail | null>(null);
const executionPage = ref(1);
const executionPageSize = ref(8);
const validationIssuePage = ref(1);
const validationIssuePageSize = ref(10);
const logPage = ref(1);
const logPageSize = ref(10);
const selectedExecutionId = ref<number | null>(null);
const isListCollapsed = ref(false);
const detailPanelRef = ref<HTMLElement | null>(null);
const lastListScrollTop = ref(0);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});
const route = useRoute();
const router = useRouter();

const routedTaskId = computed(() => {
  const raw = Number(route.params.taskId);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const routedExecutionId = computed(() => {
  const raw = Number(route.params.executionId);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const isExecutionDetailPage = computed(() => route.name === "write-task-execution-detail" || routedExecutionId.value !== null);
const parsedLogs = computed(() =>
  logs.value.map((log) => ({
    ...log,
    detail: parseJson(log.detailJson)
  }))
);
const paginatedExecutions = computed(() =>
  paginateItems(executions.value, executionPage.value, executionPageSize.value)
);
const selectedExecution = computed(() =>
  selectedExecutionId.value === null
    ? null
    : executions.value.find((execution) => execution.id === selectedExecutionId.value) ?? null
);
const showExecutionDetail = computed(() => selectedExecutionId.value !== null || isExecutionDetailPage.value);
const selectedDeliverySnapshot = computed(() =>
  selectedExecution.value?.deliveryDetailsJson ? parseJson(selectedExecution.value.deliveryDetailsJson) : null
);
const selectedWriteSummary = computed(() =>
  selectedExecution.value ? readExecutionSummary(selectedExecution.value) : null
);
const selectedValidationIssues = computed<ValidationIssue[]>(() => {
  const snapshot = selectedDeliverySnapshot.value;
  if (!snapshot || typeof snapshot !== "object") {
    return [];
  }
  const validation = snapshot.nonNullValidation;
  if (!validation || typeof validation !== "object" || !Array.isArray(validation.issues)) {
    return [];
  }
  return validation.issues as ValidationIssue[];
});
const paginatedValidationIssues = computed(() =>
  paginateItems(selectedValidationIssues.value, validationIssuePage.value, validationIssuePageSize.value)
);
const paginatedLogs = computed(() =>
  paginateItems(parsedLogs.value, logPage.value, logPageSize.value)
);
const successExecutionCount = computed(() =>
  executions.value.filter((execution) => execution.status === "SUCCESS" || execution.status === "PARTIAL_SUCCESS").length
);
const failedExecutionCount = computed(() =>
  executions.value.filter((execution) => execution.status === "FAILED").length
);
const runningExecutionCount = computed(() =>
  executions.value.filter((execution) => execution.status === "RUNNING").length
);

function setFeedback(kind: FeedbackKind, message: string) {
  feedback.kind = kind;
  feedback.message = message;
}

function clearFeedback() {
  feedback.message = "";
}

function scrollDetailIntoView() {
  detailPanelRef.value?.scrollIntoView?.({
    behavior: "smooth",
    block: "start"
  });
}

function rememberListScrollPosition() {
  if (typeof window === "undefined") {
    return;
  }
  lastListScrollTop.value = window.scrollY || window.pageYOffset || 0;
}

function restoreListScrollPosition() {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.scrollTo({
      top: lastListScrollTop.value,
      behavior: "smooth"
    });
  } catch {
    document.documentElement.scrollTop = lastListScrollTop.value;
    document.body.scrollTop = lastListScrollTop.value;
  }
}

async function goToTask() {
  if (!routedTaskId.value) {
    return;
  }
  await router.push({ name: "write-task-edit", params: { id: routedTaskId.value } });
}

async function collapseExecutionList() {
  if (!selectedExecution.value || routedTaskId.value === null) {
    return;
  }
  await router.push({
    name: "write-task-execution-detail",
    params: {
      taskId: routedTaskId.value,
      executionId: selectedExecution.value.id
    }
  });
}

async function expandExecutionList() {
  if (routedTaskId.value === null) {
    return;
  }
  await router.push({
    name: "write-task-executions",
    params: { taskId: routedTaskId.value }
  });
}

function parseJson(value: string | null) {
  if (!value) {
    return {};
  }
  try {
    return JSON.parse(value) as Record<string, any>;
  } catch {
    return { raw: value };
  }
}

async function syncExecutionRouteState() {
  if (routedTaskId.value === null) {
    setFeedback("error", "未指定任务");
    return;
  }

  if (!isExecutionDetailPage.value) {
    selectedExecutionId.value = null;
    logs.value = [];
    isListCollapsed.value = false;
    validationIssuePage.value = 1;
    logPage.value = 1;
    await nextTick();
    restoreListScrollPosition();
    return;
  }

  if (routedExecutionId.value === null) {
    return;
  }

  let execution = executions.value.find((item) => item.id === routedExecutionId.value) ?? null;
  if (!execution) {
    try {
      const response = await apiClient.get<ApiResponse<Execution>>(`/write-tasks/executions/${routedExecutionId.value}`);
      const remoteExecution = response.data.success ? response.data.data : null;
      if (!remoteExecution || remoteExecution.writeTaskId !== routedTaskId.value) {
        setFeedback("error", "未找到对应的任务实例");
        await router.replace({
          name: "write-task-executions",
          params: { taskId: routedTaskId.value }
        });
        return;
      }
      execution = remoteExecution;
      executions.value = [remoteExecution, ...executions.value.filter((item) => item.id !== remoteExecution.id)];
    } catch (error) {
      setFeedback("error", readApiError(error, "加载实例详情失败"));
      await router.replace({
        name: "write-task-executions",
        params: { taskId: routedTaskId.value }
      });
      return;
    }
  }

  isListCollapsed.value = true;
  await loadLogs(execution.id, { stayOnRoute: true });
}

function readExecutionSummary(execution: Execution): ExecutionWriteSummary | null {
  const snapshot = parseJson(execution.deliveryDetailsJson);
  if (!snapshot || typeof snapshot !== "object") {
    return null;
  }

  const validation = typeof snapshot.nonNullValidation === "object" && snapshot.nonNullValidation
    ? snapshot.nonNullValidation
    : null;
  const headers = snapshot.headers && typeof snapshot.headers === "object"
    ? snapshot.headers as Record<string, unknown>
    : null;
  const headerDefinitions = Array.isArray(snapshot.headerDefinitions)
    ? snapshot.headerDefinitions
    : [];
  const deliveryType = String(snapshot.deliveryType ?? snapshot.targetType ?? "JDBC").toUpperCase();
  const hasBeforeAfterMetrics = deliveryType !== "KAFKA"
    && (
      snapshot.beforeWriteRowCount !== undefined
      || snapshot.afterWriteRowCount !== undefined
      || snapshot.rowDelta !== undefined
    );
  const hasMetrics = snapshot.writtenRowCount !== undefined
    || hasBeforeAfterMetrics
    || validation !== null
    || deliveryType === "KAFKA";

  if (!hasMetrics) {
    return null;
  }

  return {
    deliveryType,
    writtenRowCount: Number(snapshot.writtenRowCount ?? execution.successCount ?? 0),
    beforeWriteRowCount: hasBeforeAfterMetrics ? Number(snapshot.beforeWriteRowCount ?? 0) : 0,
    afterWriteRowCount: hasBeforeAfterMetrics ? Number(snapshot.afterWriteRowCount ?? 0) : 0,
    rowDelta: hasBeforeAfterMetrics ? Number(snapshot.rowDelta ?? 0) : 0,
    hasBeforeAfterMetrics,
    hasValidation: validation !== null,
    validationPassed: Boolean(validation?.passed ?? false),
    nullValueCount: Number(validation?.nullValueCount ?? 0),
    blankStringCount: Number(validation?.blankStringCount ?? 0),
    topic: typeof snapshot.topic === "string" ? snapshot.topic : null,
    payloadFormat: typeof snapshot.payloadFormat === "string" ? snapshot.payloadFormat : null,
    keyMode: typeof snapshot.keyMode === "string" ? snapshot.keyMode : null,
    keyField: typeof snapshot.keyField === "string" ? snapshot.keyField : null,
    keyPath: typeof snapshot.keyPath === "string" ? snapshot.keyPath : null,
    fixedKey: typeof snapshot.fixedKey === "string" ? snapshot.fixedKey : null,
    partition: typeof snapshot.partition === "number" && Number.isFinite(snapshot.partition) ? snapshot.partition : null,
    headerCount: typeof snapshot.headerCount === "number" && Number.isFinite(snapshot.headerCount)
      ? snapshot.headerCount
      : (headerDefinitions.length ? headerDefinitions.length : (headers ? Object.keys(headers).length : 0))
  };
}

async function loadPageData() {
  if (routedTaskId.value === null) {
    setFeedback("error", "未指定任务");
    return;
  }

  try {
    const [executionResponse, taskResponse] = await Promise.all([
      apiClient.get<ApiResponse<Execution[]>>(`/write-tasks/${routedTaskId.value}/executions`),
      apiClient.get<ApiResponse<TaskDetail>>(`/write-tasks/${routedTaskId.value}`)
    ]);

    executions.value = executionResponse.data.success ? executionResponse.data.data : [];
    task.value = taskResponse.data.success ? taskResponse.data.data : null;
    executionPage.value = clampPage(executionPage.value, executions.value.length, executionPageSize.value);

    await syncExecutionRouteState();

    if (feedback.kind === "error" && feedback.message) {
      clearFeedback();
    }
  } catch (error) {
    executions.value = [];
    logs.value = [];
    task.value = null;
    selectedExecutionId.value = null;
    isListCollapsed.value = false;
    setFeedback("error", readApiError(error, "加载任务实例失败"));
  }
}

async function openExecutionDetail(executionId: number) {
  if (routedTaskId.value === null) {
    return;
  }
  await loadLogs(executionId);
}

async function loadLogs(executionId: number, options?: { stayOnRoute?: boolean }) {
  if (routedTaskId.value === null) {
    return;
  }
  selectedExecutionId.value = executionId;
  validationIssuePage.value = 1;
  logPage.value = 1;
  if (!options?.stayOnRoute && (!isExecutionDetailPage.value || routedExecutionId.value !== executionId)) {
    await router.push({
      name: "write-task-execution-detail",
      params: {
        taskId: routedTaskId.value,
        executionId
      }
    });
    await syncExecutionRouteState();
    return;
  }
  rememberListScrollPosition();
  try {
    const response = await apiClient.get<ApiResponse<ExecutionLog[]>>(`/write-tasks/executions/${executionId}/logs`);
    selectedExecutionId.value = executionId;
    logs.value = response.data.success ? response.data.data : [];
    isListCollapsed.value = true;
    setFeedback("success", `已加载实例 #${executionId} 的 ${logs.value.length} 条日志`);
    await nextTick();
    scrollDetailIntoView();
  } catch (error) {
    selectedExecutionId.value = executionId;
    logs.value = [];
    isListCollapsed.value = true;
    setFeedback("error", readApiError(error, "加载实例日志失败"));
    await nextTick();
    scrollDetailIntoView();
  }
}

function formatDate(value: string) {
  return formatDisplayDate(value);
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

onMounted(loadPageData);

watch(
  () => [route.name, route.params.taskId, route.params.executionId],
  () => {
    void syncExecutionRouteState();
  }
);

watch(
  () => routedTaskId.value,
  () => {
    void loadPageData();
  }
);

watch(
  () => executions.value.length,
  () => {
    executionPage.value = clampPage(executionPage.value, executions.value.length, executionPageSize.value);
  }
);

watch(executionPageSize, () => {
  executionPage.value = 1;
});

watch(
  () => selectedValidationIssues.value.length,
  () => {
    validationIssuePage.value = clampPage(
      validationIssuePage.value,
      selectedValidationIssues.value.length,
      validationIssuePageSize.value
    );
  }
);

watch(validationIssuePageSize, () => {
  validationIssuePage.value = 1;
});

watch(
  () => logs.value.length,
  () => {
    logPage.value = clampPage(logPage.value, logs.value.length, logPageSize.value);
  }
);

watch(logPageSize, () => {
  logPage.value = 1;
});
</script>
