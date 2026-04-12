<template>
  <section class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">任务控制台</p>
        <h2>把数据集绑定到连接器，并定义投递运行时配置。</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="createExampleJob">快速示例任务</button>
        <button class="button button--ghost" type="button" @click="loadPageData">刷新</button>
      </div>
    </header>

    <section class="workspace-grid">
      <article class="panel form-panel">
        <div>
          <p class="eyebrow">{{ selectedJobId ? "编辑任务" : "新建任务" }}</p>
          <h3>{{ selectedJobId ? "更新任务定义" : "创建新的任务定义" }}</h3>
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
              <span>名称</span>
              <input v-model.trim="form.name" type="text" placeholder="用户行为数据投递到 MySQL" />
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>数据集</span>
              <select v-model.number="form.datasetDefinitionId">
                <option :value="null">请选择数据集</option>
                <option v-for="dataset in datasets" :key="dataset.id" :value="dataset.id">{{ dataset.name }}</option>
              </select>
            </label>

            <label>
              <span>连接器</span>
              <select v-model.number="form.targetConnectorId">
                <option :value="null">请选择连接器</option>
                <option v-for="connector in connectors" :key="connector.id" :value="connector.id">
                  {{ connector.name }}（{{ labelConnectorType(connector.connectorType) }}）
                </option>
              </select>
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>写入策略</span>
              <select v-model="form.writeStrategy">
                <option v-for="strategy in writeStrategies" :key="strategy" :value="strategy">{{ labelJobWriteStrategy(strategy) }}</option>
              </select>
            </label>

            <label>
              <span>状态</span>
              <select v-model="form.status">
                <option v-for="status in jobStatuses" :key="status" :value="status">{{ labelJobStatus(status) }}</option>
              </select>
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>调度类型</span>
              <select v-model="form.scheduleType">
                <option v-for="type in scheduleTypes" :key="type" :value="type">{{ labelJobScheduleType(type) }}</option>
              </select>
            </label>

            <label>
              <span>Cron 表达式</span>
              <input
                v-model.trim="form.cronExpression"
                type="text"
                :placeholder="form.scheduleType === 'CRON' ? '0 */5 * * * ?' : form.scheduleType === 'ONCE' ? '请在 runtimeConfig.schedule.triggerAt 中配置触发时间' : '仅定时任务需要填写'"
              />
            </label>
          </div>

          <div class="field">
            <label>
              <span>运行时配置 JSON</span>
              <textarea
                v-model="form.runtimeConfigJson"
                class="code-input"
                placeholder='{"count":100,"target":{"table":"synthetic_user_activity"}}'
              />
            </label>
          </div>

          <div class="button-row">
            <button class="button" type="submit">{{ selectedJobId ? "保存任务" : "创建任务" }}</button>
            <button class="button button--ghost" type="button" @click="resetForm">重置</button>
            <button class="button button--ghost" type="button" @click="loadRuntimeTemplate">加载运行模板</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="runJob(selectedJobId)">立即执行</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="pauseJob(selectedJobId)">暂停</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="resumeJob(selectedJobId)">恢复</button>
            <button v-if="selectedJobId" class="button button--ghost" type="button" @click="disableJob(selectedJobId)">禁用</button>
            <button v-if="selectedJobId" class="button button--danger" type="button" @click="removeJob(selectedJobId)">删除</button>
          </div>

          <p class="muted" v-if="!datasets.length || !connectors.length">
            请至少先创建一个数据集和一个连接器，否则任务创建会失败。
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
              <span class="pill">{{ labelJobStatus(job.status) }}</span>
            </div>

            <div class="meta-grid">
              <p class="muted">
                数据集：{{ resolveDatasetName(job.datasetDefinitionId) }} / 连接器：{{ resolveConnectorName(job.targetConnectorId) }}
              </p>
              <p>写入策略：{{ labelJobWriteStrategy(job.writeStrategy) }} / 调度方式：{{ labelJobScheduleType(job.scheduleType) }}</p>
              <p class="muted">
                调度状态：{{ labelSchedulerState(job.schedulerState) }}
                <span v-if="job.nextFireAt"> / 下次触发：{{ formatDate(job.nextFireAt) }}</span>
                <span v-if="job.previousFireAt"> / 上次触发：{{ formatDate(job.previousFireAt) }}</span>
              </p>
              <pre class="code-block">{{ job.runtimeConfigJson }}</pre>
            </div>

            <div class="panel__actions">
              <button class="button" type="button" @click="selectJob(job)">编辑</button>
              <button class="button button--ghost" type="button" @click="runJob(job.id)">执行</button>
              <button class="button button--ghost" type="button" @click="pauseJob(job.id)">暂停</button>
              <button class="button button--ghost" type="button" @click="resumeJob(job.id)">恢复</button>
              <button class="button button--ghost" type="button" @click="disableJob(job.id)">禁用</button>
              <button class="button button--danger" type="button" @click="removeJob(job.id)">删除</button>
            </div>
          </article>
        </section>

        <section v-else class="empty-state">
          <h3>还没有任务</h3>
          <p>请从左侧表单创建任务，把数据集连接到文件、HTTP、MySQL、PostgreSQL 或 Kafka 目标。</p>
        </section>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import {
  formatDisplayDate,
  labelConnectorType,
  labelJobScheduleType,
  labelJobStatus,
  labelJobWriteStrategy,
  labelSchedulerState
} from "../utils/display";

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
    },
    note: "target.table 供 MySQL/PostgreSQL 使用，target.topic 与 target.keyField 供 Kafka 使用"
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
    throw new Error(`${label} 必须是合法的 JSON`);
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
  form.runtimeConfigJson = normalizeJson(job.runtimeConfigJson, "运行时配置");
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
    setFeedback("error", readApiError(error, "加载任务工作台失败"));
  }
}

async function createExampleJob() {
  try {
    await apiClient.post("/jobs/quickstart");
    await loadPageData();
    setFeedback("success", "快速示例任务已创建");
  } catch (error) {
    setFeedback("error", readApiError(error, "创建快速示例任务失败"));
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
      runtimeConfigJson: normalizeJson(form.runtimeConfigJson, "运行时配置")
    };

    if (selectedJobId.value) {
      await apiClient.put(`/jobs/${selectedJobId.value}`, payload);
      setFeedback("success", "任务已更新");
    } else {
      await apiClient.post("/jobs", payload);
      setFeedback("success", "任务已创建");
    }

    await loadPageData();
  } catch (error) {
    setFeedback("error", readApiError(error, "保存任务失败"));
  }
}

async function runJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/run`);
    await loadPageData();
    setFeedback("success", "任务已开始执行");
  } catch (error) {
    setFeedback("error", readApiError(error, "执行任务失败"));
  }
}

async function pauseJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/pause`);
    await loadPageData();
    setFeedback("success", "任务已暂停");
  } catch (error) {
    setFeedback("error", readApiError(error, "暂停任务失败"));
  }
}

async function resumeJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/resume`);
    await loadPageData();
    setFeedback("success", "任务已恢复");
  } catch (error) {
    setFeedback("error", readApiError(error, "恢复任务失败"));
  }
}

async function disableJob(jobId: number) {
  try {
    await apiClient.post(`/jobs/${jobId}/disable`);
    await loadPageData();
    setFeedback("success", "任务已禁用");
  } catch (error) {
    setFeedback("error", readApiError(error, "禁用任务失败"));
  }
}

async function removeJob(jobId: number) {
  try {
    await apiClient.delete(`/jobs/${jobId}`);
    if (selectedJobId.value === jobId) {
      resetForm();
    }
    await loadPageData();
    setFeedback("success", "任务已删除");
  } catch (error) {
    setFeedback("error", readApiError(error, "删除任务失败"));
  }
}

function resolveDatasetName(datasetId: number) {
  return datasets.value.find((dataset) => dataset.id === datasetId)?.name ?? `数据集 #${datasetId}`;
}

function resolveConnectorName(connectorId: number) {
  const connector = connectors.value.find((item) => item.id === connectorId);
  if (!connector) {
    return `连接器 #${connectorId}`;
  }
  return `${connector.name}（${labelConnectorType(connector.connectorType)}）`;
}

function formatDate(value: string) {
  return formatDisplayDate(value);
}

onMounted(async () => {
  resetForm();
  await loadPageData();
});
</script>
