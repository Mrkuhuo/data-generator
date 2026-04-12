<template>
  <section class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">连接器中心</p>
        <h2>配置模拟数据的测试通道与投递目标。</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('file')">文件</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('http')">HTTP</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('mysql')">MySQL</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('postgresql')">PostgreSQL</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('kafka')">Kafka</button>
        <button class="button button--ghost" type="button" @click="loadConnectors">刷新</button>
      </div>
    </header>

    <section class="workspace-grid">
      <article class="panel form-panel">
        <div>
          <p class="eyebrow">{{ selectedConnectorId ? "编辑连接器" : "新建连接器" }}</p>
          <h3>{{ selectedConnectorId ? "更新连接器定义" : "创建新的连接器定义" }}</h3>
        </div>

        <div
          v-if="feedback.message"
          class="status-banner"
          :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
        >
          {{ feedback.message }}
        </div>

        <form class="form-grid" @submit.prevent="saveConnector">
          <div class="field">
            <label>
              <span>名称</span>
              <input v-model.trim="form.name" type="text" placeholder="示例 MySQL 落库连接器" />
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>类型</span>
              <select v-model="form.connectorType">
                <option v-for="type in connectorTypes" :key="type" :value="type">{{ labelConnectorType(type) }}</option>
              </select>
            </label>

            <label>
              <span>角色</span>
              <select v-model="form.connectorRole">
                <option v-for="role in connectorRoles" :key="role" :value="role">{{ labelConnectorRole(role) }}</option>
              </select>
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>状态</span>
              <select v-model="form.status">
                <option v-for="status in connectorStatuses" :key="status" :value="status">{{ labelConnectorStatus(status) }}</option>
              </select>
            </label>

            <label>
              <span>模板</span>
              <button class="button button--ghost" type="button" @click="applyTemplate(form.connectorType)">
                加载 {{ labelConnectorType(form.connectorType) }} 模板
              </button>
            </label>
          </div>

          <div class="field">
            <label>
              <span>说明</span>
              <textarea v-model.trim="form.description" rows="3" placeholder="说明这个连接器的用途和目标环境。" />
            </label>
          </div>

          <div class="field">
            <label>
              <span>配置 JSON</span>
              <textarea
                v-model="form.configJson"
                class="code-input"
                placeholder='{"jdbcUrl":"jdbc:mysql://localhost:3306/demo","username":"root","password":"123456"}'
              />
            </label>
          </div>

          <div class="button-row">
            <button class="button" type="submit">{{ selectedConnectorId ? "保存连接器" : "创建连接器" }}</button>
            <button class="button button--ghost" type="button" @click="resetForm">重置</button>
            <button
              v-if="selectedConnectorId"
              class="button button--ghost"
              type="button"
              @click="runTest(selectedConnectorId)"
            >
              测试当前连接器
            </button>
            <button
              v-if="selectedConnectorId"
              class="button button--danger"
              type="button"
              @click="removeConnector(selectedConnectorId)"
            >
              删除
            </button>
          </div>
        </form>
      </article>

      <div class="stack">
        <section v-if="connectors.length" class="panel-grid">
          <article
            v-for="connector in connectors"
            :key="connector.id"
            class="panel"
            :class="{ 'panel--selected': selectedConnectorId === connector.id }"
          >
            <div class="panel__row">
              <h3>{{ connector.name }}</h3>
              <span class="pill">{{ labelConnectorType(connector.connectorType) }}</span>
            </div>

            <div class="meta-grid">
              <p class="muted">{{ labelConnectorRole(connector.connectorRole) }} / {{ labelConnectorStatus(connector.status) }}</p>
              <p>{{ connector.description || "暂无说明。" }}</p>
              <pre class="code-block">{{ connector.configJson }}</pre>
            </div>

            <div class="panel__actions">
              <button class="button" type="button" @click="selectConnector(connector)">编辑</button>
              <button class="button button--ghost" type="button" @click="runTest(connector.id)">测试</button>
              <button class="button button--danger" type="button" @click="removeConnector(connector.id)">删除</button>
            </div>

            <p class="muted">最近测试：{{ labelConnectorProbeStatus(connector.lastTestStatus) }}</p>
            <p v-if="connector.lastTestMessage" class="muted">{{ connector.lastTestMessage }}</p>
            <pre v-if="connector.lastTestDetailsJson" class="code-block">{{ connector.lastTestDetailsJson }}</pre>
          </article>
        </section>

        <section v-else class="empty-state">
          <h3>还没有连接器</h3>
          <p>可以使用上方快速创建按钮，也可以在左侧表单中手动创建连接器。</p>
        </section>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import { labelConnectorProbeStatus, labelConnectorRole, labelConnectorStatus, labelConnectorType } from "../utils/display";

type FeedbackKind = "success" | "error";

interface Connector {
  id: number;
  name: string;
  connectorType: ConnectorType;
  connectorRole: ConnectorRole;
  status: ConnectorStatus;
  description: string | null;
  configJson: string;
  lastTestStatus: string | null;
  lastTestMessage: string | null;
  lastTestDetailsJson: string | null;
}

type ConnectorType = "FILE" | "HTTP" | "MYSQL" | "POSTGRESQL" | "KAFKA";
type ConnectorRole = "SOURCE" | "TARGET" | "BOTH";
type ConnectorStatus = "DRAFT" | "READY" | "DISABLED";

const connectorTypes: ConnectorType[] = ["FILE", "HTTP", "MYSQL", "POSTGRESQL", "KAFKA"];
const connectorRoles: ConnectorRole[] = ["TARGET", "SOURCE", "BOTH"];
const connectorStatuses: ConnectorStatus[] = ["READY", "DRAFT", "DISABLED"];

const connectorTemplates: Record<ConnectorType, string> = {
  FILE: JSON.stringify(
    {
      path: "./output/generated-users.jsonl",
      format: "jsonl"
    },
    null,
    2
  ),
  HTTP: JSON.stringify(
    {
      url: "http://localhost:9000/mock/intake",
      method: "POST",
      batch: false,
      timeoutMs: 5000,
      headers: {
        "X-Source": "multisource-data-generator"
      }
    },
    null,
    2
  ),
  MYSQL: JSON.stringify(
    {
      jdbcUrl: "jdbc:mysql://localhost:3306/demo_sink?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
      username: "root",
      password: "123456"
    },
    null,
    2
  ),
  POSTGRESQL: JSON.stringify(
    {
      jdbcUrl: "jdbc:postgresql://localhost:5432/demo_sink",
      username: "postgres",
      password: "postgres"
    },
    null,
    2
  ),
  KAFKA: JSON.stringify(
    {
      bootstrapServers: "localhost:9092",
      acks: "all",
      clientId: "mdg-local",
      properties: {
        "security.protocol": "PLAINTEXT"
      }
    },
    null,
    2
  )
};

const connectors = ref<Connector[]>([]);
const selectedConnectorId = ref<number | null>(null);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});

const form = reactive({
  name: "",
  connectorType: "FILE" as ConnectorType,
  connectorRole: "TARGET" as ConnectorRole,
  status: "READY" as ConnectorStatus,
  description: "",
  configJson: connectorTemplates.FILE
});

const selectedConnector = computed(() =>
  connectors.value.find((connector) => connector.id === selectedConnectorId.value) ?? null
);

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
  selectedConnectorId.value = null;
  form.name = "";
  form.connectorType = "FILE";
  form.connectorRole = "TARGET";
  form.status = "READY";
  form.description = "";
  form.configJson = connectorTemplates.FILE;
  clearFeedback();
}

function applyTemplate(type: ConnectorType) {
  form.configJson = connectorTemplates[type];
  if (!form.name) {
    form.name = `新建${labelConnectorType(type)}连接器`;
  }
}

function selectConnector(connector: Connector) {
  selectedConnectorId.value = connector.id;
  form.name = connector.name;
  form.connectorType = connector.connectorType;
  form.connectorRole = connector.connectorRole;
  form.status = connector.status;
  form.description = connector.description ?? "";
  form.configJson = normalizeJson(connector.configJson, "连接器配置");
  clearFeedback();
}

async function loadConnectors() {
  try {
    const response = await apiClient.get<ApiResponse<Connector[]>>("/connectors");
    connectors.value = response.data.success ? response.data.data : [];

    if (selectedConnectorId.value != null) {
      const refreshed = connectors.value.find((connector) => connector.id === selectedConnectorId.value);
      if (refreshed) {
        selectConnector(refreshed);
      } else {
        resetForm();
      }
    }
  } catch (error) {
    connectors.value = [];
    setFeedback("error", readApiError(error, "加载连接器列表失败"));
  }
}

async function createQuickstartConnector(type: string) {
  try {
    await apiClient.post(`/connectors/quickstart/${type}`);
    await loadConnectors();
    setFeedback("success", `${labelConnectorType(type.toUpperCase())} 快速连接器已创建`);
  } catch (error) {
    setFeedback("error", readApiError(error, `创建 ${labelConnectorType(type.toUpperCase())} 连接器失败`));
  }
}

async function saveConnector() {
  try {
    const payload = {
      name: form.name,
      connectorType: form.connectorType,
      connectorRole: form.connectorRole,
      status: form.status,
      description: form.description || null,
      configJson: normalizeJson(form.configJson, "连接器配置")
    };

    if (selectedConnectorId.value) {
      await apiClient.put(`/connectors/${selectedConnectorId.value}`, payload);
      setFeedback("success", "连接器已更新");
    } else {
      await apiClient.post("/connectors", payload);
      setFeedback("success", "连接器已创建");
    }

    await loadConnectors();
  } catch (error) {
    setFeedback("error", readApiError(error, "保存连接器失败"));
  }
}

async function runTest(connectorId: number) {
  try {
    await apiClient.post(`/connectors/${connectorId}/test`);
    await loadConnectors();

    const connectorName = connectors.value.find((connector) => connector.id === connectorId)?.name ?? "连接器";
    setFeedback("success", `${connectorName} 测试完成`);
  } catch (error) {
    setFeedback("error", readApiError(error, "连接器测试失败"));
  }
}

async function removeConnector(connectorId: number) {
  try {
    await apiClient.delete(`/connectors/${connectorId}`);
    if (selectedConnectorId.value === connectorId) {
      resetForm();
    }
    await loadConnectors();
    setFeedback("success", "连接器已删除");
  } catch (error) {
    setFeedback("error", readApiError(error, "删除连接器失败"));
  }
}

onMounted(async () => {
  await loadConnectors();

  if (selectedConnector.value == null) {
    applyTemplate(form.connectorType);
  }
});
</script>
