<template>
  <section class="page">
    <header class="page-header">
      <div class="page-header__content">
        <h2>数据源连接</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="loadConnections">刷新</button>
        <button
          v-if="isConnectionListPage"
          class="button"
          type="button"
          :disabled="isConnectionBusy"
          @click="startNewConnection"
        >
          新建连接
        </button>
        <button
          v-else
          class="button button--ghost"
          type="button"
          :disabled="isConnectionBusy"
          @click="goToConnectionList"
        >
          返回连接列表
        </button>
      </div>
    </header>

    <section v-if="isConnectionListPage" class="stats-grid stats-grid--compact">
      <article class="stat-card">
        <span>已配置连接</span>
        <strong>{{ connections.length }}</strong>
      </article>
      <article class="stat-card">
        <span>就绪连接</span>
        <strong>{{ readyConnectionCount }}</strong>
      </article>
      <article class="stat-card">
        <span>已测试连接</span>
        <strong>{{ testedConnectionCount }}</strong>
      </article>
      <article class="stat-card">
        <span>已读取表</span>
        <strong>{{ tables.length }}</strong>
      </article>
    </section>

    <div
      v-if="feedback.message && isConnectionListPage && (feedback.kind === 'error' || activeTablesConnectionId === null)"
      class="status-banner"
      :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
    >
      {{ feedback.message }}
    </div>

    <section class="workspace-grid connections-workspace connections-workspace--single">
      <article v-if="isConnectionEditorPage || selectedConnectionId !== null" ref="formPanelRef" class="panel builder-layout">
        <div class="panel__actions panel__actions--tight">
          <button class="button button--ghost" type="button" :disabled="isConnectionBusy" @click="goToConnectionList">
            返回连接列表
          </button>
        </div>
        <div>
          <h3>{{ selectedConnectionId ? "编辑连接" : "新建连接" }}</h3>
        </div>

        <div
          v-if="feedback.message"
          class="status-banner"
          :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
        >
          {{ feedback.message }}
        </div>

        <section v-if="selectedConnection" class="panel panel--embedded">
          <h3>当前连接</h3>
          <div class="meta-grid">
            <p>
              <strong>连接：</strong>{{ selectedConnection.name }} /
              <strong>类型：</strong>{{ labelDatabaseType(selectedConnection.dbType) }} /
              <strong>状态：</strong>{{ labelWriteTaskStatus(selectedConnection.status) }}
            </p>
            <p>
              <strong>目标：</strong>{{ describeConnectionEndpoint(selectedConnection) }}
            </p>
            <p>
              <strong>最近测试：</strong>{{ labelConnectorProbeStatus(selectedConnection.lastTestStatus) }} /
              <strong>测试说明：</strong>{{ selectedConnection.lastTestMessage || "尚未测试" }}
            </p>
            <p v-if="selectedConnection.dbType === 'KAFKA' && selectedKafkaConfig">
              <strong>Kafka 配置：</strong>
              {{ selectedKafkaConfig.bootstrapServers || "-" }}
              <span v-if="selectedKafkaConfig.securityProtocol"> / {{ selectedKafkaConfig.securityProtocol }}</span>
              <span v-if="selectedKafkaConfig.acks"> / acks={{ selectedKafkaConfig.acks }}</span>
            </p>
          </div>
        </section>

        <form class="form-grid" @submit.prevent="saveConnection">
          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">01</span>
              <div>
                <h3>基础信息</h3>
              </div>
            </div>

            <div class="field">
              <label>
                <span>连接名称</span>
                <input v-model.trim="form.name" name="connectionName" type="text" placeholder="示例：订单库 MySQL" />
              </label>
            </div>

            <template v-if="isKafkaForm">
              <div class="field field--half">
                <label>
                  <span>Bootstrap Servers</span>
                  <input
                    v-model.trim="form.bootstrapServers"
                    name="bootstrapServers"
                    type="text"
                    placeholder="示例：127.0.0.1:9092,127.0.0.1:9093"
                  />
                </label>

                <label>
                  <span>Client ID</span>
                  <input v-model.trim="form.clientId" name="clientId" type="text" placeholder="可选，例如 mdg-producer" />
                </label>
              </div>

              <div class="field field--half">
                <label>
                  <span>Security Protocol</span>
                  <select v-model="form.securityProtocol" name="securityProtocol">
                    <option value="">默认</option>
                    <option value="PLAINTEXT">PLAINTEXT</option>
                    <option value="SSL">SSL</option>
                    <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
                    <option value="SASL_SSL">SASL_SSL</option>
                  </select>
                </label>

                <label>
                  <span>SASL Mechanism</span>
                  <select v-model="form.saslMechanism" name="saslMechanism">
                    <option value="">默认</option>
                    <option value="PLAIN">PLAIN</option>
                    <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                    <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                  </select>
                </label>
              </div>

              <div class="field field--half">
                <label>
                  <span>Acks</span>
                  <select v-model="form.acks" name="acks">
                    <option value="">默认</option>
                    <option value="all">all</option>
                    <option value="1">1</option>
                    <option value="0">0</option>
                  </select>
                </label>

                <label>
                  <span>Compression</span>
                  <select v-model="form.compressionType" name="compressionType">
                    <option value="">默认</option>
                    <option value="none">none</option>
                    <option value="gzip">gzip</option>
                    <option value="snappy">snappy</option>
                    <option value="lz4">lz4</option>
                    <option value="zstd">zstd</option>
                  </select>
                </label>
              </div>

              <div class="field">
                <label>
                  <span>额外 Producer Properties（JSON）</span>
                  <textarea
                    v-model.trim="form.propertiesText"
                    name="propertiesJson"
                    rows="4"
                    placeholder='例如：{"linger.ms":"5","batch.size":"32768"}'
                  ></textarea>
                </label>
              </div>
            </template>

            <div v-else class="field field--half">
              <label>
                <span>数据库类型</span>
                <select v-model="form.dbType" name="dbType">
                  <option v-for="type in databaseTypes" :key="type" :value="type">{{ labelDatabaseType(type) }}</option>
                </select>
              </label>

              <label>
                <span>连接状态</span>
                <select v-model="form.status" name="status">
                  <option v-for="status in statuses" :key="status" :value="status">{{ labelWriteTaskStatus(status) }}</option>
                </select>
              </label>
            </div>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">02</span>
              <div>
                <h3>地址与库</h3>
              </div>
            </div>

            <div v-if="!isKafkaForm" class="field field--half">
              <label>
                <span>主机地址</span>
                <input v-model.trim="form.host" name="host" type="text" placeholder="127.0.0.1" />
              </label>

              <label>
                <span>端口</span>
                <input v-model.number="form.port" name="port" type="number" min="1" max="65535" />
              </label>
            </div>

            <div class="field field--half">
              <label>
                <span>数据库名</span>
                <input v-model.trim="form.databaseName" name="databaseName" type="text" placeholder="demo_db" />
              </label>

              <label>
                <span>Schema</span>
                <input v-model.trim="form.schemaName" name="schemaName" type="text" :placeholder="schemaPlaceholder" />
              </label>
            </div>

            <div v-if="!isKafkaForm" class="field">
              <label>
                <span>JDBC 参数</span>
                <input v-model.trim="form.jdbcParams" name="jdbcParams" type="text" :placeholder="jdbcParamsPlaceholder" />
              </label>
            </div>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">03</span>
              <div>
                <h3>账号与说明</h3>
              </div>
            </div>

            <div class="field field--half">
              <label>
                <span>用户名</span>
                <input v-model.trim="form.username" name="username" type="text" placeholder="root" />
              </label>

              <label>
                <span>密码</span>
                <input
                  v-model="form.password"
                  name="password"
                  type="password"
                  :placeholder="passwordPlaceholder"
                />
              </label>
            </div>

            <details class="details-panel">
              <summary>补充说明</summary>
              <div class="details-panel__content">
                <div class="field">
                  <label>
                    <span>说明</span>
                    <textarea
                      v-model.trim="form.description"
                      name="description"
                      rows="3"
                      placeholder="例如：生成测试订单并写入验收库。"
                    ></textarea>
                  </label>
                </div>
              </div>
            </details>
          </section>

          <div class="button-row">
            <button class="button button--ghost" type="button" :disabled="isConnectionBusy || !canTestCurrentConfig" @click="testCurrentConnection">
              {{ isTestingCurrent ? "测试中..." : "测试当前配置" }}
            </button>
            <button class="button" type="submit" :disabled="isConnectionBusy || !canSaveConnection">
              {{ isSaving ? "保存中..." : (selectedConnectionId ? "保存连接" : "创建连接") }}
            </button>
            <button class="button button--ghost" type="button" :disabled="isConnectionBusy" @click="resetForm">重置</button>
            <button
              v-if="selectedConnectionId"
              class="button button--danger"
              type="button"
              :disabled="isConnectionBusy"
              @click="removeConnection(selectedConnectionId)"
            >
              删除连接
            </button>
          </div>
        </form>
      </article>

      <aside v-if="isConnectionListPage && selectedConnectionId === null" class="stack jobs-workspace__sidebar">
        <section class="panel connector-list-panel">
          <div class="panel__row panel__row--divider">
            <div>
              <p class="list-caption">列表较长时按页查看，避免页面内容堆叠过多。</p>
              <h3>连接列表</h3>
            </div>
            <div class="panel__actions panel__actions--tight">
              <button class="button button--ghost" type="button" :disabled="isConnectionBusy" @click="startNewConnection">新建连接</button>
            </div>
          </div>

          <div v-if="connections.length" class="task-list">
            <article
              v-for="connection in paginatedConnections"
              :key="connection.id"
              class="task-list__item"
              :class="{ 'panel--selected': selectedConnectionId === connection.id }"
            >
              <div class="task-list__main">
                <div class="panel__row">
                  <h3>{{ connection.name }}</h3>
                  <span class="pill">{{ labelDatabaseType(connection.dbType) }}</span>
                </div>

                <div class="task-list__meta">
                  <span>{{ describeConnectionEndpoint(connection) }}</span>
                  <span>连接状态：{{ labelWriteTaskStatus(connection.status) }}</span>
                  <span>最近测试：{{ labelConnectorProbeStatus(connection.lastTestStatus) }}</span>
                  <span v-if="connection.lastTestMessage">{{ connection.lastTestMessage }}</span>
                </div>

              </div>

              <div class="task-list__actions">
                <button class="button" type="button" :disabled="isConnectionBusy" @click="selectConnection(connection)">编辑</button>
                <button class="button button--ghost" type="button" :disabled="isConnectionBusy" @click="testConnection(connection.id)">测试</button>
                <button
                  v-if="canBrowseTables(connection)"
                  class="button button--ghost"
                  type="button"
                  :disabled="isConnectionBusy || isLoadingTablesConnectionId === connection.id"
                  @click="toggleTables(connection.id)"
                >
                  {{
                    isLoadingTablesConnectionId === connection.id
                      ? "读取中..."
                      : isTablesExpanded(connection.id)
                        ? "收起表"
                        : "查看表"
                  }}
                </button>
                <button class="button button--danger" type="button" :disabled="isConnectionBusy" @click="removeConnection(connection.id)">删除</button>
              </div>

              <section
                v-if="canBrowseTables(connection) && isTablesExpanded(connection.id)"
                class="connection-table-panel"
              >
                <div class="connection-table-panel__header">
                  <div>
                    <h4>表列表</h4>
                  </div>
                  <div class="connection-table-panel__actions">
                    <button
                      class="button button--ghost"
                      type="button"
                      :disabled="isLoadingTablesConnectionId === connection.id"
                      @click="loadTables(connection.id, { preserveSearch: true })"
                    >
                      刷新表
                    </button>
                    <button class="button button--ghost" type="button" @click="collapseTables">
                      收起
                    </button>
                  </div>
                </div>

                <div class="connection-table-panel__toolbar">
                  <input
                    v-model.trim="tableSearchKeyword"
                    class="connection-table-panel__search"
                    type="text"
                    placeholder="搜索表名"
                  />
                  <span class="pill pill--soft">共 {{ tables.length }} 张表 / 筛选后 {{ filteredTables.length }} 张</span>
                </div>

                <div v-if="isLoadingTablesConnectionId === connection.id" class="connection-table-panel__empty">
                  正在读取表列表...
                </div>

                <div v-else-if="filteredTables.length" class="connection-table-panel__body">
                  <div class="data-list data-list--table-browser">
                    <div v-for="table in paginatedTables" :key="`${table.schemaName}.${table.tableName}`" class="data-list__row">
                      <div class="connection-table-panel__table">
                        <strong>{{ table.tableName }}</strong>
                        <span class="muted">{{ table.schemaName || "默认 schema" }}</span>
                      </div>
                      <button class="button button--ghost" type="button" @click="startTaskFromTable(connection.id, table)">
                        用于新建任务
                      </button>
                    </div>
                  </div>
                </div>

                <div v-else class="connection-table-panel__empty">
                  {{ tableSearchKeyword ? "未找到匹配的表" : "暂无可用表" }}
                </div>
                <PaginationBar
                  v-if="filteredTables.length"
                  :page="tablePage"
                  :page-size="tablePageSize"
                  :total="filteredTables.length"
                  noun="张表"
                  @update:page="tablePage = $event"
                  @update:page-size="tablePageSize = $event"
                />
              </section>
            </article>
          </div>

          <PaginationBar
            v-if="connections.length"
            :page="connectionPage"
            :page-size="connectionPageSize"
            :total="connections.length"
            noun="个连接"
            @update:page="connectionPage = $event"
            @update:page-size="connectionPageSize = $event"
          />

          <section v-else class="empty-state">
            <h3>暂无连接</h3>
          </section>
        </section>
      </aside>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import PaginationBar from "../components/PaginationBar.vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import { clampPage, paginateItems } from "../utils/pagination";
import { labelConnectorProbeStatus, labelDatabaseType, labelWriteTaskStatus } from "../utils/display";

type FeedbackKind = "success" | "error";
type DatabaseType = "MYSQL" | "POSTGRESQL" | "SQLSERVER" | "ORACLE" | "KAFKA";
type ConnectionStatus = "READY" | "DRAFT" | "DISABLED";

interface TargetConnection {
  id: number;
  name: string;
  dbType: DatabaseType;
  host: string;
  port: number;
  databaseName: string;
  schemaName: string | null;
  username: string;
  jdbcParams: string | null;
  configJson: string | null;
  status: ConnectionStatus;
  description: string | null;
  hasPassword: boolean;
  lastTestStatus: string | null;
  lastTestMessage: string | null;
  lastTestDetailsJson: string | null;
}

interface DatabaseTable {
  schemaName: string;
  tableName: string;
}

interface ConnectionUpsertPayload {
  name: string;
  dbType: DatabaseType;
  host: string;
  port: number;
  databaseName: string;
  schemaName: string | null;
  username: string;
  password: string | null;
  jdbcParams: string | null;
  configJson: string | null;
  status: ConnectionStatus;
  description: string | null;
}

interface ConnectionTestResult {
  connectionId: number | null;
  success: boolean;
  status: string;
  message: string;
  detailsJson: string | null;
}

interface KafkaConfig {
  bootstrapServers?: string;
  securityProtocol?: string;
  saslMechanism?: string;
  clientId?: string;
  acks?: string;
  compressionType?: string;
  properties?: Record<string, string>;
}

const databaseTypes: DatabaseType[] = ["MYSQL", "POSTGRESQL", "SQLSERVER", "ORACLE", "KAFKA"];
const statuses: ConnectionStatus[] = ["READY", "DRAFT", "DISABLED"];
const DEFAULT_MYSQL_JDBC_PARAMS = "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
const DEFAULT_SQLSERVER_JDBC_PARAMS = "encrypt=true;trustServerCertificate=true";

const connections = ref<TargetConnection[]>([]);
const tables = ref<DatabaseTable[]>([]);
const selectedConnectionId = ref<number | null>(null);
const activeTablesConnectionId = ref<number | null>(null);
const tableSearchKeyword = ref("");
const connectionPage = ref(1);
const connectionPageSize = ref(8);
const tablePage = ref(1);
const tablePageSize = ref(10);
const isLoadingTablesConnectionId = ref<number | null>(null);
const formPanelRef = ref<HTMLElement | null>(null);
const isSaving = ref(false);
const isTestingCurrent = ref(false);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});
const route = useRoute();
const router = useRouter();

const form = reactive({
  name: "",
  dbType: "MYSQL" as DatabaseType,
  host: "127.0.0.1",
  port: 3306,
  databaseName: "",
  schemaName: "",
  username: "root",
  password: "",
  jdbcParams: DEFAULT_MYSQL_JDBC_PARAMS,
  status: "READY" as ConnectionStatus,
  description: "",
  bootstrapServers: "",
  securityProtocol: "",
  saslMechanism: "",
  clientId: "",
  acks: "",
  compressionType: "",
  propertiesText: ""
});

const selectedConnection = computed(() =>
  selectedConnectionId.value === null
    ? null
    : connections.value.find((connection) => connection.id === selectedConnectionId.value) ?? null
);
const selectedKafkaConfig = computed(() =>
  selectedConnection.value?.dbType === "KAFKA" ? parseKafkaConfig(selectedConnection.value.configJson) : null
);
const filteredTables = computed(() => {
  const keyword = tableSearchKeyword.value.trim().toLowerCase();
  if (!keyword) {
    return tables.value;
  }
  return tables.value.filter((table) => {
    const fullName = `${table.schemaName ?? ""}.${table.tableName}`.toLowerCase();
    return fullName.includes(keyword) || table.tableName.toLowerCase().includes(keyword);
  });
});
const paginatedConnections = computed(() =>
  paginateItems(connections.value, connectionPage.value, connectionPageSize.value)
);
const paginatedTables = computed(() =>
  paginateItems(filteredTables.value, tablePage.value, tablePageSize.value)
);
const isKafkaForm = computed(() => form.dbType === "KAFKA");
const readyConnectionCount = computed(() => connections.value.filter((connection) => connection.status === "READY").length);
const testedConnectionCount = computed(() => connections.value.filter((connection) => connection.lastTestStatus !== null).length);
const canTestCurrentConfig = computed(() => isConnectionFormReady("test"));
const canSaveConnection = computed(() => isConnectionFormReady("save"));
const isConnectionBusy = computed(() => isSaving.value || isTestingCurrent.value);
const isConnectionListPage = computed(() => route.name === "connections");
const isConnectionCreatePage = computed(() => route.name === "connection-create");
const routedConnectionId = computed(() => {
  const raw = Number(route.params.id);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const isConnectionEditPage = computed(() => route.name === "connection-edit" || routedConnectionId.value !== null);
const isConnectionEditorPage = computed(() => isConnectionCreatePage.value || isConnectionEditPage.value);
const schemaPlaceholder = computed(() => {
  switch (form.dbType) {
    case "POSTGRESQL":
      return "可选，默认 public";
    case "SQLSERVER":
      return "可选，默认 dbo";
    case "ORACLE":
      return "可选，默认当前用户名";
    case "KAFKA":
      return "Kafka 不使用 Schema";
    default:
      return "可选，MySQL 一般无需填写";
  }
});
const jdbcParamsPlaceholder = computed(() => defaultJdbcParamsFor(form.dbType));
const passwordPlaceholder = computed(() => {
  if (isKafkaForm.value) {
    return selectedConnectionId.value ? "留空表示保留原密码，可选" : "可选，开启 SASL 时再填写";
  }
  return selectedConnectionId.value ? "留空表示保留原密码" : "请输入数据库密码";
});

function setFeedback(kind: FeedbackKind, message: string) {
  feedback.kind = kind;
  feedback.message = message;
}

function clearFeedback() {
  feedback.message = "";
}

function defaultPortFor(type: DatabaseType) {
  switch (type) {
    case "MYSQL":
      return 3306;
    case "POSTGRESQL":
      return 5432;
    case "SQLSERVER":
      return 1433;
    case "ORACLE":
      return 1521;
    case "KAFKA":
      return 9092;
  }
}

function defaultDatabaseNameFor(type: DatabaseType) {
  return type === "KAFKA" ? "kafka" : "";
}

function defaultJdbcParamsFor(type: DatabaseType) {
  switch (type) {
    case "MYSQL":
      return DEFAULT_MYSQL_JDBC_PARAMS;
    case "SQLSERVER":
      return DEFAULT_SQLSERVER_JDBC_PARAMS;
    case "POSTGRESQL":
    case "ORACLE":
    case "KAFKA":
      return "";
  }
}

function parseJsonObject(value: string | null | undefined) {
  if (!value) {
    return {};
  }
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function parseKafkaConfig(value: string | null | undefined): KafkaConfig {
  const parsed = parseJsonObject(value);
  const properties = parsed.properties && typeof parsed.properties === "object"
    ? Object.fromEntries(
        Object.entries(parsed.properties as Record<string, unknown>).map(([key, item]) => [key, String(item)])
      )
    : undefined;

  return {
    bootstrapServers: typeof parsed.bootstrapServers === "string" ? parsed.bootstrapServers : undefined,
    securityProtocol: typeof parsed.securityProtocol === "string" ? parsed.securityProtocol : undefined,
    saslMechanism: typeof parsed.saslMechanism === "string" ? parsed.saslMechanism : undefined,
    clientId: typeof parsed.clientId === "string" ? parsed.clientId : undefined,
    acks: typeof parsed.acks === "string" ? parsed.acks : undefined,
    compressionType: typeof parsed.compressionType === "string" ? parsed.compressionType : undefined,
    properties
  };
}

function splitHostAndPort(addresses: string) {
  const first = addresses
    .split(",")
    .map((item) => item.trim())
    .find(Boolean);

  if (!first) {
    return {
      host: "127.0.0.1",
      port: defaultPortFor("KAFKA")
    };
  }

  if (first.startsWith("[")) {
    const closingIndex = first.indexOf("]");
    const host = closingIndex >= 0 ? first.slice(0, closingIndex + 1) : first;
    const portText = closingIndex >= 0 && first.slice(closingIndex + 1).startsWith(":")
      ? first.slice(closingIndex + 2)
      : "";
    const port = Number(portText);

    return {
      host,
      port: Number.isInteger(port) && port > 0 ? port : defaultPortFor("KAFKA")
    };
  }

  const separatorIndex = first.lastIndexOf(":");
  if (separatorIndex <= 0) {
    return {
      host: first,
      port: defaultPortFor("KAFKA")
    };
  }

  const host = first.slice(0, separatorIndex);
  const port = Number(first.slice(separatorIndex + 1));
  return {
    host: host || "127.0.0.1",
    port: Number.isInteger(port) && port > 0 ? port : defaultPortFor("KAFKA")
  };
}

function buildKafkaConfigObject(): KafkaConfig {
  const bootstrapServers = form.bootstrapServers.trim();
  if (!bootstrapServers) {
    throw new Error("请输入 Kafka bootstrap servers");
  }

  let properties: Record<string, string> | undefined;
  if (form.propertiesText.trim()) {
    try {
      const parsed = JSON.parse(form.propertiesText);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("Kafka 额外属性必须是 JSON 对象");
      }

      properties = Object.fromEntries(
        Object.entries(parsed as Record<string, unknown>).map(([key, value]) => [key, String(value)])
      );
    } catch (error) {
      if (error instanceof Error && error.message === "Kafka 额外属性必须是 JSON 对象") {
        throw error;
      }
      throw new Error("Kafka 额外属性必须是合法的 JSON 对象");
    }
  }

  return {
    bootstrapServers,
    securityProtocol: form.securityProtocol.trim() || undefined,
    saslMechanism: form.saslMechanism.trim() || undefined,
    clientId: form.clientId.trim() || undefined,
    acks: form.acks.trim() || undefined,
    compressionType: form.compressionType.trim() || undefined,
    properties
  };
}

function buildConnectionPayload(): ConnectionUpsertPayload {
  if (isKafkaForm.value) {
    const kafkaConfig = buildKafkaConfigObject();
    const endpoint = splitHostAndPort(kafkaConfig.bootstrapServers ?? "");

    return {
      name: form.name.trim(),
      dbType: form.dbType,
      host: endpoint.host,
      port: endpoint.port,
      databaseName: form.databaseName.trim() || defaultDatabaseNameFor("KAFKA"),
      schemaName: null,
      username: form.username.trim(),
      password: form.password.trim() || null,
      jdbcParams: null,
      configJson: JSON.stringify(kafkaConfig),
      status: form.status,
      description: form.description.trim() || null
    };
  }

  return {
    name: form.name.trim(),
    dbType: form.dbType,
    host: form.host.trim(),
    port: Number(form.port),
    databaseName: form.databaseName.trim(),
    schemaName: form.schemaName.trim() || null,
    username: form.username.trim(),
    password: form.password || null,
    jdbcParams: form.jdbcParams.trim() || null,
    configJson: null,
    status: form.status,
    description: form.description.trim() || null
  };
}

function isConnectionFormReady(mode: "test" | "save") {
  if (!form.name.trim()) {
    return false;
  }

  if (isKafkaForm.value) {
    if (!form.bootstrapServers.trim()) {
      return false;
    }

    if (form.propertiesText.trim()) {
      try {
        const parsed = JSON.parse(form.propertiesText);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          return false;
        }
      } catch {
        return false;
      }
    }

    return true;
  }

  const hasRequiredText = [
    form.host.trim(),
    form.databaseName.trim(),
    form.username.trim()
  ].every(Boolean);
  const hasValidPort = Number.isInteger(form.port) && form.port >= 1 && form.port <= 65535;
  const hasPassword = Boolean(form.password.trim()) || selectedConnectionId.value !== null;

  if (!hasRequiredText || !hasValidPort) {
    return false;
  }

  if (mode === "save" || mode === "test") {
    return hasPassword;
  }

  return true;
}

async function scrollFormIntoView() {
  await nextTick();
  formPanelRef.value?.scrollIntoView({ behavior: "smooth", block: "start" });
  formPanelRef.value?.querySelector<HTMLInputElement>("input[name='connectionName']")?.focus();
}

function applyKafkaConfigToForm(configJson: string | null | undefined) {
  const config = parseKafkaConfig(configJson);
  form.bootstrapServers = config.bootstrapServers ?? "";
  form.securityProtocol = config.securityProtocol ?? "";
  form.saslMechanism = config.saslMechanism ?? "";
  form.clientId = config.clientId ?? "";
  form.acks = config.acks ?? "";
  form.compressionType = config.compressionType ?? "";
  form.propertiesText = config.properties ? JSON.stringify(config.properties, null, 2) : "";
}

function resetKafkaFields() {
  form.bootstrapServers = "";
  form.securityProtocol = "";
  form.saslMechanism = "";
  form.clientId = "";
  form.acks = "";
  form.compressionType = "";
  form.propertiesText = "";
}

function applyConnectionToForm(connection: TargetConnection) {
  selectedConnectionId.value = connection.id;
  form.name = connection.name;
  form.dbType = connection.dbType;
  form.host = connection.host;
  form.port = connection.port;
  form.databaseName = connection.databaseName;
  form.schemaName = connection.schemaName ?? "";
  form.username = connection.username;
  form.password = "";
  form.jdbcParams = connection.jdbcParams ?? defaultJdbcParamsFor(connection.dbType);
  form.status = connection.status;
  form.description = connection.description ?? "";

  if (connection.dbType === "KAFKA") {
    applyKafkaConfigToForm(connection.configJson);
  } else {
    resetKafkaFields();
  }
}

function syncSelectedConnectionFromList() {
  if (selectedConnectionId.value === null) {
    return;
  }
  const connection = connections.value.find((item) => item.id === selectedConnectionId.value);
  if (connection) {
    applyConnectionToForm(connection);
  }
}

function resetForm() {
  selectedConnectionId.value = null;
  form.name = "";
  form.dbType = "MYSQL";
  form.host = "127.0.0.1";
  form.port = defaultPortFor("MYSQL");
  form.databaseName = "";
  form.schemaName = "";
  form.username = "root";
  form.password = "";
  form.jdbcParams = defaultJdbcParamsFor("MYSQL");
  form.status = "READY";
  form.description = "";
  resetKafkaFields();
  clearFeedback();
}

function startNewConnection() {
  resetForm();
  void router.push({ name: "connection-create" });
}

function goToConnectionList() {
  clearFeedback();
  void router.push({ name: "connections" });
}

function applyDefaultPort(type: DatabaseType) {
  form.port = defaultPortFor(type);
}

function canBrowseTables(connection: TargetConnection) {
  return connection.dbType !== "KAFKA";
}

function isTablesExpanded(connectionId: number) {
  return activeTablesConnectionId.value === connectionId;
}

function formatTableRouteValue(table: DatabaseTable) {
  return table.schemaName ? `${table.schemaName}.${table.tableName}` : table.tableName;
}

function describeConnectionEndpoint(connection: TargetConnection) {
  if (connection.dbType === "KAFKA") {
    const config = parseKafkaConfig(connection.configJson);
    return config.bootstrapServers || `${connection.host}:${connection.port}`;
  }

  return `${connection.host}:${connection.port} / ${connection.databaseName}${connection.schemaName ? ` / ${connection.schemaName}` : ""}`;
}

function selectConnection(connection: TargetConnection) {
  applyConnectionToForm(connection);
  clearFeedback();
  void router.push({ name: "connection-edit", params: { id: connection.id } });
}

function collapseTables() {
  activeTablesConnectionId.value = null;
  tableSearchKeyword.value = "";
}

async function toggleTables(connectionId: number) {
  if (activeTablesConnectionId.value === connectionId) {
    collapseTables();
    return;
  }
  await loadTables(connectionId);
}

function startTaskFromTable(connectionId: number, table: DatabaseTable) {
  clearFeedback();
  void router.push({
    name: "write-task-create",
    query: {
      connectionId: String(connectionId),
      tableName: formatTableRouteValue(table)
    }
  });
}

function syncConnectionRouteState() {
  if (isConnectionListPage.value) {
    selectedConnectionId.value = null;
    return;
  }

  if (isConnectionCreatePage.value) {
    resetForm();
    return;
  }

  if (!isConnectionEditPage.value || routedConnectionId.value === null) {
    return;
  }

  const connection = connections.value.find((item) => item.id === routedConnectionId.value);
  if (!connection) {
    setFeedback("error", "未找到数据源连接");
    void router.replace({ name: "connections" });
    return;
  }

  applyConnectionToForm(connection);
  void scrollFormIntoView();
}

async function loadConnections() {
  try {
    const response = await apiClient.get<ApiResponse<TargetConnection[]>>("/connections");
    connections.value = response.data.success ? response.data.data : [];

    syncConnectionRouteState();

    if (activeTablesConnectionId.value && !connections.value.some((connection) => connection.id === activeTablesConnectionId.value)) {
      activeTablesConnectionId.value = null;
      tables.value = [];
      tableSearchKeyword.value = "";
    }

    if (feedback.kind === "error" && feedback.message) {
      clearFeedback();
    }
  } catch (error) {
    connections.value = [];
    setFeedback("error", readApiError(error, "加载数据源连接失败"));
  }
}

async function saveConnection() {
  if (!canSaveConnection.value) {
    setFeedback("error", isKafkaForm.value ? "请填写连接名称和 Bootstrap Servers" : "请填写完整连接信息");
    return;
  }

  const isEditing = selectedConnectionId.value !== null;
  isSaving.value = true;

  try {
    const payload = buildConnectionPayload();
    const response = selectedConnectionId.value
      ? await apiClient.put<ApiResponse<TargetConnection>>(`/connections/${selectedConnectionId.value}`, payload)
      : await apiClient.post<ApiResponse<TargetConnection>>("/connections", payload);
    const savedConnection = response.data.success ? response.data.data : null;
    if (savedConnection) {
      selectedConnectionId.value = savedConnection.id;
      if (route.name !== "connection-edit" || routedConnectionId.value !== savedConnection.id) {
        await router.replace({ name: "connection-edit", params: { id: savedConnection.id } });
      }
    }

    form.password = "";
    await loadConnections();
    syncSelectedConnectionFromList();
    setFeedback("success", response.data.message ?? (isEditing ? "数据源连接已更新" : "数据源连接已创建"));
    await scrollFormIntoView();
  } catch (error) {
    const message = error instanceof Error ? error.message : "保存数据源连接失败";
    setFeedback("error", readApiError(error, message));
  } finally {
    isSaving.value = false;
  }
}

async function testConnection(connectionId: number) {
  try {
    const response = await apiClient.post<ApiResponse<ConnectionTestResult>>(`/connections/${connectionId}/test`);
    await loadConnections();
    syncSelectedConnectionFromList();
    const result = response.data.data;
    setFeedback(result.success ? "success" : "error", result.message || response.data.message || "连接测试已完成");
  } catch (error) {
    setFeedback("error", readApiError(error, "连接测试失败"));
  }
}

async function testCurrentConnection() {
  if (!canTestCurrentConfig.value) {
    setFeedback("error", isKafkaForm.value ? "请填写当前 Kafka 连接配置" : "请填写当前连接配置");
    return;
  }

  isTestingCurrent.value = true;

  try {
    const response = await apiClient.post<ApiResponse<ConnectionTestResult>>("/connections/test", {
      connectionId: selectedConnectionId.value,
      connection: buildConnectionPayload()
    });
    const result = response.data.data;
    setFeedback(result.success ? "success" : "error", result.message || response.data.message || "当前配置测试已完成");
  } catch (error) {
    const message = error instanceof Error ? error.message : "当前配置测试失败";
    setFeedback("error", readApiError(error, message));
  } finally {
    isTestingCurrent.value = false;
  }
}

async function removeConnection(connectionId: number) {
  try {
    await apiClient.delete(`/connections/${connectionId}`);
    const removingSelectedConnection = selectedConnectionId.value === connectionId;
    if (selectedConnectionId.value === connectionId) {
      resetForm();
    }
    if (activeTablesConnectionId.value === connectionId) {
      activeTablesConnectionId.value = null;
      tables.value = [];
      tableSearchKeyword.value = "";
    }
    if (removingSelectedConnection && isConnectionEditorPage.value) {
      await router.replace({ name: "connections" });
    }
    await loadConnections();
    setFeedback("success", "数据源连接已删除");
  } catch (error) {
    setFeedback("error", readApiError(error, "删除数据源连接失败"));
  }
}

async function loadTables(connectionId: number, options?: { preserveSearch?: boolean }) {
  const connection = connections.value.find((item) => item.id === connectionId);
  if (connection?.dbType === "KAFKA") {
    setFeedback("error", "Kafka 不支持读取表结构");
    return;
  }

  activeTablesConnectionId.value = connectionId;
  if (!options?.preserveSearch) {
    tableSearchKeyword.value = "";
  }
  isLoadingTablesConnectionId.value = connectionId;

  try {
    const response = await apiClient.get<ApiResponse<DatabaseTable[]>>(`/connections/${connectionId}/tables`);
    tables.value = response.data.success ? response.data.data : [];
    tablePage.value = 1;
    clearFeedback();
  } catch (error) {
    tables.value = [];
    activeTablesConnectionId.value = connectionId;
    setFeedback("error", readApiError(error, "读取数据库表失败"));
  }
  isLoadingTablesConnectionId.value = null;
}

onMounted(async () => {
  await loadConnections();
  applyDefaultPort(form.dbType);
});

watch(
  () => [route.name, route.params.id],
  () => {
    syncConnectionRouteState();
  }
);

watch(
  () => connections.value.length,
  () => {
    connectionPage.value = clampPage(connectionPage.value, connections.value.length, connectionPageSize.value);
  }
);

watch(connectionPageSize, () => {
  connectionPage.value = 1;
});

watch(
  () => filteredTables.value.length,
  () => {
    tablePage.value = clampPage(tablePage.value, filteredTables.value.length, tablePageSize.value);
  }
);

watch(tablePageSize, () => {
  tablePage.value = 1;
});

watch(tableSearchKeyword, () => {
  tablePage.value = 1;
});

watch(activeTablesConnectionId, () => {
  tablePage.value = 1;
});

watch(
  () => form.dbType,
  (currentType, previousType) => {
    if (!previousType || currentType === previousType) {
      return;
    }

    const previousDefaultPort = defaultPortFor(previousType);
    if (selectedConnectionId.value === null || form.port === previousDefaultPort) {
      applyDefaultPort(currentType);
    }

    const previousDefaultParams = defaultJdbcParamsFor(previousType);
    if (!form.jdbcParams.trim() || form.jdbcParams === previousDefaultParams) {
      form.jdbcParams = defaultJdbcParamsFor(currentType);
    }

    if (currentType === "KAFKA") {
      form.schemaName = "";
      if (!form.databaseName.trim() || form.databaseName === defaultDatabaseNameFor(previousType)) {
        form.databaseName = defaultDatabaseNameFor("KAFKA");
      }
      if (previousType !== "KAFKA") {
        form.username = "";
        form.password = "";
      }
    } else if (previousType === "KAFKA") {
      if (form.databaseName === defaultDatabaseNameFor("KAFKA")) {
        form.databaseName = "";
      }
      resetKafkaFields();
      if (!form.username.trim()) {
        form.username = "root";
      }
    }
  }
);
</script>
