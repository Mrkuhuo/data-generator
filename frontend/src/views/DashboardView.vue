<template>
  <section class="page">
    <header class="hero">
      <div>
        <p class="eyebrow">Platform Blueprint</p>
        <h2>Direct rewrite into a multisource synthetic data platform.</h2>
      </div>
      <p class="hero__copy">
        The old MySQL/Kafka-specific implementation is archived. What you see here is the new foundation layer:
        connector registry, dataset studio, job control, execution ledger, and real multi-target delivery.
      </p>
    </header>

    <section class="stats-grid">
      <article class="stat-card">
        <span>Connectors</span>
        <strong>{{ overview.connectorCount }}</strong>
      </article>
      <article class="stat-card">
        <span>Datasets</span>
        <strong>{{ overview.datasetCount }}</strong>
      </article>
      <article class="stat-card">
        <span>Jobs</span>
        <strong>{{ overview.jobCount }}</strong>
      </article>
      <article class="stat-card">
        <span>Executions</span>
        <strong>{{ overview.executionCount }}</strong>
      </article>
    </section>

    <section class="panel-grid">
      <article class="panel">
        <p class="eyebrow">Now in repo</p>
        <h3>Working platform slice</h3>
        <ul class="bullet-list">
          <li>New MySQL-backed platform schema with connector, dataset, job, execution, and log entities.</li>
          <li>Real delivery adapters for file, HTTP, MySQL, PostgreSQL, and Kafka targets.</li>
          <li>Editable Vue workspaces for connectors, datasets, job runtime config, and manual execution.</li>
        </ul>
      </article>

      <article class="panel">
        <p class="eyebrow">Next build step</p>
        <h3>Operator layer</h3>
        <ul class="bullet-list">
          <li>Guided schema builders on top of the JSON DSL so non-developers can define datasets faster.</li>
          <li>Scheduled execution, retries, and runtime controls beyond manual runs.</li>
          <li>Richer execution detail views and local infra presets for end-to-end connector testing.</li>
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
    console.error("Failed to load overview", error);
  }
}

onMounted(loadOverview);
</script>
