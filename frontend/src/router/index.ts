import { createRouter, createWebHistory } from "vue-router";
import AppShell from "../layouts/AppShell.vue";
import DashboardView from "../views/DashboardView.vue";
import ConnectorsView from "../views/ConnectorsView.vue";
import JobsView from "../views/JobsView.vue";
import ExecutionsView from "../views/ExecutionsView.vue";
import RelationalTasksView from "../views/RelationalTasksView.vue";
import RelationalTaskExecutionsView from "../views/RelationalTaskExecutionsView.vue";
import WriteTaskExecutionsView from "../views/WriteTaskExecutionsView.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/",
      component: AppShell,
      children: [
        { path: "", redirect: "/connections" },
        { path: "connections", name: "connections", component: ConnectorsView, meta: { title: "\u6570\u636e\u6e90\u8fde\u63a5" } },
        { path: "connections/new", name: "connection-create", component: ConnectorsView, meta: { title: "\u65b0\u5efa\u6570\u636e\u6e90\u8fde\u63a5" } },
        { path: "connections/:id/edit", name: "connection-edit", component: ConnectorsView, meta: { title: "\u7f16\u8f91\u6570\u636e\u6e90\u8fde\u63a5" } },
        { path: "write-tasks", name: "write-tasks", component: JobsView, meta: { title: "\u5199\u5165\u4efb\u52a1" } },
        { path: "write-tasks/new", name: "write-task-create", component: JobsView, meta: { title: "\u65b0\u5efa\u5199\u5165\u4efb\u52a1" } },
        { path: "write-tasks/:id/edit", name: "write-task-edit", component: JobsView, meta: { title: "\u7f16\u8f91\u5199\u5165\u4efb\u52a1" } },
        { path: "write-tasks/:taskId/executions", name: "write-task-executions", component: WriteTaskExecutionsView, meta: { title: "\u5199\u5165\u4efb\u52a1\u5b9e\u4f8b" } },
        { path: "write-tasks/:taskId/executions/:executionId", name: "write-task-execution-detail", component: WriteTaskExecutionsView, meta: { title: "\u5199\u5165\u4efb\u52a1\u5b9e\u4f8b\u8be6\u60c5" } },
        { path: "relational-write-tasks", name: "relational-write-tasks", component: RelationalTasksView, meta: { title: "\u5173\u7cfb\u4efb\u52a1" } },
        { path: "relational-write-tasks/new", name: "relational-write-task-create", component: RelationalTasksView, meta: { title: "\u65b0\u5efa\u5173\u7cfb\u4efb\u52a1" } },
        { path: "relational-write-tasks/:id/edit", name: "relational-write-task-edit", component: RelationalTasksView, meta: { title: "\u7f16\u8f91\u5173\u7cfb\u4efb\u52a1" } },
        { path: "relational-write-tasks/:id/executions", name: "relational-write-task-executions", component: RelationalTaskExecutionsView, meta: { title: "\u5173\u7cfb\u4efb\u52a1\u5b9e\u4f8b" } },
        { path: "relational-write-tasks/:id/executions/:executionId", name: "relational-write-task-execution-detail", component: RelationalTaskExecutionsView, meta: { title: "\u5173\u7cfb\u4efb\u52a1\u5b9e\u4f8b\u8be6\u60c5" } },
        { path: "executions", name: "executions", component: ExecutionsView, meta: { title: "\u6267\u884c\u8bb0\u5f55" } },
        { path: "executions/:id", name: "execution-detail", component: ExecutionsView, meta: { title: "\u6267\u884c\u8be6\u60c5" } },
        { path: "overview", name: "overview", component: DashboardView, meta: { title: "\u5e73\u53f0\u603b\u89c8" } },
        { path: "connectors", redirect: "/connections" },
        { path: "jobs", redirect: "/write-tasks" },
        { path: "datasets", redirect: "/write-tasks" },
        { path: "dashboard", redirect: "/overview" }
      ]
    }
  ]
});

router.afterEach((to) => {
  if (to.meta.title) {
    document.title = `${to.meta.title} | \u591a\u6570\u636e\u6e90\u6a21\u62df\u6570\u636e\u751f\u6210\u5668`;
  }
});

export default router;
