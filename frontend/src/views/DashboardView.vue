<template>
  <section class="page">
    <header class="hero">
      <div>
        <p class="eyebrow">平台总览</p>
        <h2>面向多数据源的模拟数据平台，现已进入可用重构版本。</h2>
      </div>
      <p class="hero__copy">
        旧版面向 MySQL/Kafka 的实现已经退出主流程。现在看到的是新的平台底座：
        连接器中心、数据集工作台、任务控制台、执行账本，以及面向多目标的真实投递能力。
      </p>
    </header>

    <section class="stats-grid">
      <article class="stat-card">
        <span>连接器</span>
        <strong>{{ overview.connectorCount }}</strong>
      </article>
      <article class="stat-card">
        <span>数据集</span>
        <strong>{{ overview.datasetCount }}</strong>
      </article>
      <article class="stat-card">
        <span>任务</span>
        <strong>{{ overview.jobCount }}</strong>
      </article>
      <article class="stat-card">
        <span>执行记录</span>
        <strong>{{ overview.executionCount }}</strong>
      </article>
    </section>

    <section class="panel-grid">
      <article class="panel">
        <p class="eyebrow">当前能力</p>
        <h3>已可运行的平台切片</h3>
        <ul class="bullet-list">
          <li>新的平台数据模型已经覆盖连接器、数据集、任务、执行记录与日志实体。</li>
          <li>文件、HTTP、MySQL、PostgreSQL、Kafka 都具备真实投递适配器。</li>
          <li>前端已经提供连接器、数据集、任务配置与手动执行的可编辑工作台。</li>
        </ul>
      </article>

      <article class="panel">
        <p class="eyebrow">下一阶段</p>
        <h3>运营与治理层</h3>
        <ul class="bullet-list">
          <li>在 JSON DSL 之上补充可视化建模，让非开发人员也能快速定义数据集。</li>
          <li>继续强化定时调度、重试策略和更细粒度的运行控制。</li>
          <li>增强执行详情视图和本地基础设施预设，支撑端到端连接器验证。</li>
        </ul>
      </article>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive } from "vue";
import { apiClient, type ApiResponse } from "../api/client";

interface Overview {
  connectorCount: number;
  datasetCount: number;
  jobCount: number;
  executionCount: number;
}

const overview = reactive<Overview>({
  connectorCount: 0,
  datasetCount: 0,
  jobCount: 0,
  executionCount: 0
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
