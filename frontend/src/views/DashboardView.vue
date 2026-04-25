<template>
  <section class="page">
    <header class="page-header">
      <div>
        <h2>平台总览</h2>
      </div>
    </header>

    <section class="stats-grid">
      <article class="stat-card">
        <span>数据源连接</span>
        <strong>{{ overview.connectionCount }}</strong>
      </article>
      <article class="stat-card">
        <span>写入任务</span>
        <strong>{{ overview.writeTaskCount }}</strong>
      </article>
      <article class="stat-card">
        <span>执行记录</span>
        <strong>{{ overview.writeExecutionCount }}</strong>
      </article>
      <article class="stat-card">
        <span>支持目标库</span>
        <strong>{{ supportedDatabaseTypes.length }}</strong>
      </article>
    </section>

    <section class="panel">
      <div class="panel__row">
        <div>
          <h3>快捷入口</h3>
        </div>
        <div class="panel__actions">
          <RouterLink class="button" to="/connections">连接管理</RouterLink>
          <RouterLink class="button button--ghost" to="/write-tasks">任务管理</RouterLink>
          <RouterLink class="button button--ghost" to="/executions">执行记录</RouterLink>
        </div>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive } from "vue";
import { apiClient, type ApiResponse } from "../api/client";
import { supportedDatabaseTypes } from "../utils/display";

interface Overview {
  connectorCount: number;
  datasetCount: number;
  jobCount: number;
  executionCount: number;
  connectionCount: number;
  writeTaskCount: number;
  writeExecutionCount: number;
}

const overview = reactive<Overview>({
  connectorCount: 0,
  datasetCount: 0,
  jobCount: 0,
  executionCount: 0,
  connectionCount: 0,
  writeTaskCount: 0,
  writeExecutionCount: 0
});

async function loadOverview() {
  try {
    const response = await apiClient.get<ApiResponse<Overview>>("/overview");
    if (response.data.success) {
      Object.assign(overview, response.data.data);
    }
  } catch (error) {
    console.error("加载平台总览失败", error);
  }
}

onMounted(loadOverview);
</script>
