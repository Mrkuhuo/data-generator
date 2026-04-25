<template>
  <section class="page">
    <header class="page-header">
      <div class="page-header__content">
        <h2>关系任务实例</h2>
        <p class="muted">
          <span v-if="group">
            {{ group.name }} / {{ group.tasks.length }} 张表任务 / {{ group.relations.length }} 条关系
          </span>
          <span v-else>按关系任务查看每一次执行实例和每张表的写入结果。</span>
        </p>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="goToGroup">返回任务</button>
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
            <p class="list-caption">每一次实例都会记录整体状态和每张表的写入结果。</p>
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
                <span>写入 {{ execution.insertedRowCount ?? 0 }} 条</span>
                <span>成功表 {{ execution.successTableCount }} / {{ execution.plannedTableCount }}</span>
                <span>失败表 {{ execution.failureTableCount }}</span>
              </div>

              <p v-if="execution.errorSummary" class="muted">{{ execution.errorSummary }}</p>
            </div>

            <div class="task-list__actions">
              <button class="button button--ghost" type="button" @click="openExecutionDetail(execution.id)">
                {{ selectedExecutionId === execution.id ? "刷新实例详情" : "查看实例详情" }}
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
                <strong>关系任务：</strong>{{ group?.name || `任务组 #${routedGroupId}` }} /
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
                <strong>计划表数</strong>
                <span>{{ selectedExecution.plannedTableCount }}</span>
              </div>
              <div class="data-list__row">
                <strong>已完成表数</strong>
                <span>{{ selectedExecution.completedTableCount ?? 0 }}</span>
              </div>
              <div class="data-list__row">
                <strong>成功表数</strong>
                <span>{{ selectedExecution.successTableCount }}</span>
              </div>
              <div class="data-list__row">
                <strong>失败表数</strong>
                <span>{{ selectedExecution.failureTableCount }}</span>
              </div>
              <div class="data-list__row">
                <strong>写入行数</strong>
                <span>{{ selectedExecution.insertedRowCount ?? 0 }}</span>
              </div>
            </div>

            <div v-if="selectedExecution.errorSummary" class="status-banner status-banner--error">
              {{ selectedExecution.errorSummary }}
            </div>

            <section class="record-detail__section">
              <h3>表级结果</h3>
              <div class="execution-result-table">
                <div class="execution-result-table__head">
                  <span>表 / Topic</span>
                  <span>状态</span>
                  <span>写入前</span>
                  <span>写入后</span>
                  <span>插入</span>
                  <span>校验结果</span>
                </div>
                <div v-for="table in selectedExecution.tables" :key="table.id" class="execution-result-table__row">
                  <strong class="execution-result-table__table-name">{{ table.tableName }}</strong>
                  <span>{{ labelExecutionStatus(table.status) }}</span>
                  <span>{{ table.beforeWriteRowCount ?? "-" }}</span>
                  <span>{{ table.afterWriteRowCount ?? "-" }}</span>
                  <span>{{ table.insertedCount }}</span>
                  <div class="execution-result-table__validation">
                    <span>
                      空值 {{ table.nullViolationCount }} /
                      空串 {{ table.blankStringCount }} /
                      外键 {{ table.fkMissCount }} /
                      主键 {{ table.pkDuplicateCount }}
                    </span>
                    <div v-if="table.errorSummary" class="execution-result-table__errors">
                      <strong>错误：</strong>
                      <span>{{ table.errorSummary }}</span>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            <section v-if="selectedExecution.summary && Object.keys(selectedExecution.summary).length" class="record-detail__section">
              <h3>实例摘要</h3>
              <pre class="code-block">{{ formatJson(selectedExecution.summary) }}</pre>
            </section>

            <section
              v-for="table in selectedExecution.tables.filter((item) => item.summary && Object.keys(item.summary).length)"
              :key="`summary-${table.id}`"
              class="record-detail__section"
            >
              <h3>{{ table.tableName }} 详情</h3>
              <pre class="code-block">{{ formatJson(table.summary) }}</pre>
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
      <p>先回到关系任务页执行一次，这里就会留下实例记录。</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import PaginationBar from "../components/PaginationBar.vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import { clampPage, paginateItems } from "../utils/pagination";
import { formatDisplayDate, labelExecutionStatus, labelTriggerType } from "../utils/display";

type FeedbackKind = "success" | "error";

interface GroupDetail {
  id: number;
  name: string;
  tasks: Array<{ id: number | null }>;
  relations: Array<{ id: number | null }>;
}

interface TableExecution {
  id: number;
  writeTaskId: number;
  tableName: string;
  status: string;
  beforeWriteRowCount: number | null;
  afterWriteRowCount: number | null;
  insertedCount: number;
  nullViolationCount: number;
  blankStringCount: number;
  fkMissCount: number;
  pkDuplicateCount: number;
  errorSummary: string | null;
  summary: Record<string, unknown>;
}

interface GroupExecution {
  id: number;
  writeTaskGroupId: number;
  triggerType: string;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  plannedTableCount: number;
  completedTableCount: number | null;
  successTableCount: number;
  failureTableCount: number;
  insertedRowCount: number | null;
  errorSummary: string | null;
  summary: Record<string, unknown>;
  tables: TableExecution[];
}

const route = useRoute();
const router = useRouter();
const group = ref<GroupDetail | null>(null);
const executions = ref<GroupExecution[]>([]);
const selectedExecutionId = ref<number | null>(null);
const executionPage = ref(1);
const executionPageSize = ref(8);
const isListCollapsed = ref(false);
const detailPanelRef = ref<HTMLElement | null>(null);
const lastListScrollTop = ref(0);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});

const routedGroupId = computed(() => {
  const raw = Number(route.params.id);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const routedExecutionId = computed(() => {
  const raw = Number(route.params.executionId);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const isExecutionDetailPage = computed(() => route.name === "relational-write-task-execution-detail" || routedExecutionId.value !== null);
const paginatedExecutions = computed(() =>
  paginateItems(executions.value, executionPage.value, executionPageSize.value)
);
const selectedExecution = computed(() =>
  selectedExecutionId.value === null
    ? null
    : executions.value.find((execution) => execution.id === selectedExecutionId.value) ?? null
);
const showExecutionDetail = computed(() => selectedExecutionId.value !== null || isExecutionDetailPage.value);
const successExecutionCount = computed(() =>
  executions.value.filter((execution) => execution.status === "SUCCESS" || execution.status === "PARTIAL_SUCCESS").length
);
const failedExecutionCount = computed(() =>
  executions.value.filter((execution) => execution.status === "FAILED").length
);
const runningExecutionCount = computed(() =>
  executions.value.filter((execution) => execution.status === "RUNNING" || execution.status === "PENDING").length
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

async function goToGroup() {
  if (routedGroupId.value === null) {
    return;
  }
  await router.push({ name: "relational-write-task-edit", params: { id: routedGroupId.value } });
}

async function collapseExecutionList() {
  if (!selectedExecution.value || routedGroupId.value === null) {
    return;
  }
  await router.push({
    name: "relational-write-task-execution-detail",
    params: {
      id: routedGroupId.value,
      executionId: selectedExecution.value.id
    }
  });
}

async function expandExecutionList() {
  if (routedGroupId.value === null) {
    return;
  }
  await router.push({
    name: "relational-write-task-executions",
    params: { id: routedGroupId.value }
  });
}

async function syncExecutionRouteState() {
  if (routedGroupId.value === null) {
    setFeedback("error", "未指定关系任务");
    return;
  }

  if (!isExecutionDetailPage.value) {
    selectedExecutionId.value = null;
    isListCollapsed.value = false;
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
      const response = await apiClient.get<ApiResponse<GroupExecution>>(`/write-task-groups/executions/${routedExecutionId.value}`);
      const remoteExecution = response.data.success ? response.data.data : null;
      if (!remoteExecution || remoteExecution.writeTaskGroupId !== routedGroupId.value) {
        setFeedback("error", "未找到对应的关系任务实例");
        await router.replace({
          name: "relational-write-task-executions",
          params: { id: routedGroupId.value }
        });
        return;
      }
      execution = remoteExecution;
      executions.value = [remoteExecution, ...executions.value.filter((item) => item.id !== remoteExecution.id)];
    } catch (error) {
      setFeedback("error", readApiError(error, "加载关系任务实例失败"));
      await router.replace({
        name: "relational-write-task-executions",
        params: { id: routedGroupId.value }
      });
      return;
    }
  }

  selectedExecutionId.value = execution.id;
  isListCollapsed.value = true;
  await nextTick();
  scrollDetailIntoView();
}

async function loadPageData() {
  if (routedGroupId.value === null) {
    setFeedback("error", "未指定关系任务");
    return;
  }

  try {
    const [groupResponse, executionResponse] = await Promise.all([
      apiClient.get<ApiResponse<GroupDetail>>(`/write-task-groups/${routedGroupId.value}`),
      apiClient.get<ApiResponse<GroupExecution[]>>(`/write-task-groups/${routedGroupId.value}/executions`)
    ]);

    group.value = groupResponse.data.success ? groupResponse.data.data : null;
    executions.value = executionResponse.data.success ? executionResponse.data.data : [];
    executionPage.value = clampPage(executionPage.value, executions.value.length, executionPageSize.value);

    await syncExecutionRouteState();

    if (feedback.kind === "error" && feedback.message) {
      clearFeedback();
    }
  } catch (error) {
    group.value = null;
    executions.value = [];
    selectedExecutionId.value = null;
    isListCollapsed.value = false;
    setFeedback("error", readApiError(error, "加载关系任务实例失败"));
  }
}

async function openExecutionDetail(executionId: number) {
  if (routedGroupId.value === null) {
    return;
  }
  rememberListScrollPosition();
  await router.push({
    name: "relational-write-task-execution-detail",
    params: {
      id: routedGroupId.value,
      executionId
    }
  });
  await syncExecutionRouteState();
}

function formatDate(value: string) {
  return formatDisplayDate(value);
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

onMounted(loadPageData);

watch(
  () => [route.name, route.params.id, route.params.executionId],
  () => {
    void syncExecutionRouteState();
  }
);

watch(
  () => routedGroupId.value,
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
</script>
