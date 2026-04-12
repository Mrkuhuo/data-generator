<template>
  <div class="shell">
    <aside class="shell__rail">
      <div class="brand">
        <span class="brand__mark">MDG</span>
        <div>
          <p class="eyebrow">中文系统</p>
          <h1>多数据源模拟数据生成器</h1>
        </div>
      </div>

      <nav class="nav">
        <RouterLink v-for="item in navItems" :key="item.to" :to="item.to" class="nav__item">
          <span class="nav__label">{{ item.label }}</span>
          <span class="nav__hint">{{ item.hint }}</span>
        </RouterLink>
      </nav>

      <section class="rail-note">
        <p class="eyebrow">当前阶段</p>
        <p>系统已经支持文件、HTTP、MySQL、PostgreSQL 与 Kafka 的真实投递，并提供可编辑的连接器、数据集、任务与执行工作台。</p>
      </section>
    </aside>

    <main class="shell__content">
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [
  { to: "/", label: "总览", hint: "平台状态" },
  { to: "/connectors", label: "连接器", hint: "目标与来源" },
  { to: "/datasets", label: "数据集", hint: "模拟模型" },
  { to: "/jobs", label: "任务", hint: "运行定义" },
  { to: "/executions", label: "执行记录", hint: "运行观测" }
];
</script>
