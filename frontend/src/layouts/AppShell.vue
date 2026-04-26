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

        <form v-if="!authState" class="auth-form" @submit.prevent="saveAuth">
          <input v-model.trim="authUsername" autocomplete="username" placeholder="用户名" />
          <input v-model="authPassword" autocomplete="current-password" placeholder="密码" type="password" />
          <button class="button" type="submit">登录</button>
        </form>
        <div v-else class="auth-state">
          <span>{{ authState.username }}</span>
          <button class="button button--ghost" type="button" @click="logout">退出</button>
        </div>
      </div>
    </header>

    <main class="shell__content shell__content--centered">
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useRoute } from "vue-router";
import { clearApiCredentials, getApiCredentials, setApiCredentials, type ApiAuthState } from "../api/client";

const navItems = [
  { to: "/connections", label: "数据源连接" },
  { to: "/write-tasks", label: "写入任务" },
  { to: "/relational-write-tasks", label: "关系任务" },
  { to: "/executions", label: "执行记录" },
  { to: "/overview", label: "平台总览" }
];

const route = useRoute();
const authState = ref<ApiAuthState | null>(getApiCredentials());
const authUsername = ref(authState.value?.username ?? "admin");
const authPassword = ref("");

const currentNav = computed(() =>
  navItems.find((item) => route.path.startsWith(item.to)) ?? navItems[0]
);

const isRelationalRoute = computed(() => route.path.startsWith("/relational-write-tasks"));

function saveAuth() {
  setApiCredentials(authUsername.value, authPassword.value);
  authState.value = getApiCredentials();
  authPassword.value = "";
}

function logout() {
  clearApiCredentials();
  authState.value = null;
  authPassword.value = "";
}
</script>
