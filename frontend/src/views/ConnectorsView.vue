<template>
  <section class="page">
    <header class="page-header">
      <div>
        <p class="eyebrow">Connector Center</p>
        <h2>Configure where synthetic data is tested and delivered.</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('file')">File</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('http')">HTTP</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('mysql')">MySQL</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('postgresql')">PostgreSQL</button>
        <button class="button button--ghost" type="button" @click="createQuickstartConnector('kafka')">Kafka</button>
        <button class="button button--ghost" type="button" @click="loadConnectors">Refresh</button>
      </div>
    </header>

    <section class="workspace-grid">
      <article class="panel form-panel">
        <div>
          <p class="eyebrow">{{ selectedConnectorId ? "Edit Connector" : "Create Connector" }}</p>
          <h3>{{ selectedConnectorId ? "Update connector definition" : "New connector definition" }}</h3>
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
              <span>Name</span>
              <input v-model.trim="form.name" type="text" placeholder="Example MySQL Sink" />
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>Type</span>
              <select v-model="form.connectorType">
                <option v-for="type in connectorTypes" :key="type" :value="type">{{ type }}</option>
              </select>
            </label>

            <label>
              <span>Role</span>
              <select v-model="form.connectorRole">
                <option v-for="role in connectorRoles" :key="role" :value="role">{{ role }}</option>
              </select>
            </label>
          </div>

          <div class="field field--half">
            <label>
              <span>Status</span>
              <select v-model="form.status">
                <option v-for="status in connectorStatuses" :key="status" :value="status">{{ status }}</option>
              </select>
            </label>

            <label>
              <span>Template</span>
              <button class="button button--ghost" type="button" @click="applyTemplate(form.connectorType)">Load {{ form.connectorType }} Template</button>
            </label>
          </div>

          <div class="field">
            <label>
              <span>Description</span>
              <textarea v-model.trim="form.description" rows="3" placeholder="Describe how this connector will be used." />
            </label>
          </div>

          <div class="field">
            <label>
              <span>Config JSON</span>
              <textarea
                v-model="form.configJson"
                class="code-input"
                placeholder='{"jdbcUrl":"jdbc:mysql://localhost:3306/demo","username":"root","password":"root"}'
              />
            </label>
          </div>

          <div class="button-row">
            <button class="button" type="submit">{{ selectedConnectorId ? "Save Connector" : "Create Connector" }}</button>
            <button class="button button--ghost" type="button" @click="resetForm">Reset</button>
            <button
              v-if="selectedConnectorId"
              class="button button--ghost"
              type="button"
              @click="runTest(selectedConnectorId)"
            >
              Test Selected
            </button>
            <button
              v-if="selectedConnectorId"
              class="button button--danger"
              type="button"
              @click="removeConnector(selectedConnectorId)"
            >
              Delete
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
              <span class="pill">{{ connector.connectorType }}</span>
            </div>

            <div class="meta-grid">
              <p class="muted">{{ connector.connectorRole }} / {{ connector.status }}</p>
              <p>{{ connector.description || "No description yet." }}</p>
              <pre class="code-block">{{ connector.configJson }}</pre>
            </div>

            <div class="panel__actions">
              <button class="button" type="button" @click="selectConnector(connector)">Edit</button>
              <button class="button button--ghost" type="button" @click="runTest(connector.id)">Test</button>
              <button class="button button--danger" type="button" @click="removeConnector(connector.id)">Delete</button>
            </div>

            <p class="muted">Last test: {{ connector.lastTestStatus || "Not tested" }}</p>
            <p v-if="connector.lastTestMessage" class="muted">{{ connector.lastTestMessage }}</p>
            <pre v-if="connector.lastTestDetailsJson" class="code-block">{{ connector.lastTestDetailsJson }}</pre>
          </article>
        </section>

        <section v-else class="empty-state">
          <h3>No connectors yet</h3>
          <p>Use the quickstart buttons above or create one manually from the form on the left.</p>
        </section>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";

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
      password: "root"
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
    throw new Error(`${label} must be valid JSON`);
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
    form.name = `New ${type} Connector`;
  }
}

function selectConnector(connector: Connector) {
  selectedConnectorId.value = connector.id;
  form.name = connector.name;
  form.connectorType = connector.connectorType;
  form.connectorRole = connector.connectorRole;
  form.status = connector.status;
  form.description = connector.description ?? "";
  form.configJson = normalizeJson(connector.configJson, "Connector config");
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
    setFeedback("error", readApiError(error, "Failed to load connectors"));
  }
}

async function createQuickstartConnector(type: string) {
  try {
    await apiClient.post(`/connectors/quickstart/${type}`);
    await loadConnectors();
    setFeedback("success", `${type.toUpperCase()} connector quickstart created`);
  } catch (error) {
    setFeedback("error", readApiError(error, `Failed to create ${type} connector`));
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
      configJson: normalizeJson(form.configJson, "Connector config")
    };

    if (selectedConnectorId.value) {
      await apiClient.put(`/connectors/${selectedConnectorId.value}`, payload);
      setFeedback("success", "Connector updated");
    } else {
      await apiClient.post("/connectors", payload);
      setFeedback("success", "Connector created");
    }

    await loadConnectors();
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to save connector"));
  }
}

async function runTest(connectorId: number) {
  try {
    await apiClient.post(`/connectors/${connectorId}/test`);
    await loadConnectors();

    const connectorName = connectors.value.find((connector) => connector.id === connectorId)?.name ?? "Connector";
    setFeedback("success", `${connectorName} test finished`);
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to test connector"));
  }
}

async function removeConnector(connectorId: number) {
  try {
    await apiClient.delete(`/connectors/${connectorId}`);
    if (selectedConnectorId.value === connectorId) {
      resetForm();
    }
    await loadConnectors();
    setFeedback("success", "Connector deleted");
  } catch (error) {
    setFeedback("error", readApiError(error, "Failed to delete connector"));
  }
}

onMounted(async () => {
  await loadConnectors();

  if (selectedConnector.value == null) {
    applyTemplate(form.connectorType);
  }
});
</script>
