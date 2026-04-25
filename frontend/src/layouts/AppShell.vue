<template>
  <div class="shell" :class="{ 'shell--wide': isRelationalRoute }">
    <header class="shell__topbar">
      <div class="shell__topbar-inner">
        <div class="brand">
          <span class="brand__mark">MDG</span>
          <h1>{{ currentNav.label }}</h1>
        </div>

        <nav class="nav nav--tabs">
          <RouterLink v-for="item in navItems" :key="item.to" :to="item.to" class="nav__item nav__item--tab">
            <span class="nav__label">{{ item.label }}</span>
          </RouterLink>
        </nav>
      </div>
    </header>

    <main class="shell__content shell__content--centered">
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useRoute } from "vue-router";

const navItems = [
  { to: "/connections", label: "数据源连接" },
  { to: "/write-tasks", label: "写入任务" },
  { to: "/relational-write-tasks", label: "关系任务" },
  { to: "/executions", label: "执行记录" },
  { to: "/overview", label: "平台总览" }
];

const route = useRoute();

const currentNav = computed(() =>
  navItems.find((item) => route.path.startsWith(item.to)) ?? navItems[0]
);

const isRelationalRoute = computed(() => route.path.startsWith("/relational-write-tasks"));
</script>
