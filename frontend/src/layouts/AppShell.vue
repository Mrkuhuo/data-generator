<template>
  <div class="shell">
    <aside class="shell__rail">
      <div class="brand">
        <span class="brand__mark">MDG</span>
        <div>
          <p class="eyebrow">Rewrite Track</p>
          <h1>Multisource Generator</h1>
        </div>
      </div>

      <nav class="nav">
        <RouterLink v-for="item in navItems" :key="item.to" :to="item.to" class="nav__item">
          <span class="nav__label">{{ item.label }}</span>
          <span class="nav__hint">{{ item.hint }}</span>
        </RouterLink>
      </nav>

      <section class="rail-note">
        <p class="eyebrow">Current Phase</p>
        <p>Runtime delivery now works for file, HTTP, MySQL, PostgreSQL, and Kafka, with editable connector, dataset, and job workspaces.</p>
      </section>
    </aside>

    <main class="shell__content">
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [
  { to: "/", label: "Overview", hint: "Product status" },
  { to: "/connectors", label: "Connectors", hint: "Targets and sources" },
  { to: "/datasets", label: "Datasets", hint: "Synthetic models" },
  { to: "/jobs", label: "Jobs", hint: "Runtime definitions" },
  { to: "/executions", label: "Executions", hint: "Observed runs" }
];
</script>
