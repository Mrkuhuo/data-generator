import { createRouter, createWebHistory } from "vue-router";
import AppShell from "../layouts/AppShell.vue";
import DashboardView from "../views/DashboardView.vue";
import ConnectorsView from "../views/ConnectorsView.vue";
import DatasetsView from "../views/DatasetsView.vue";
import JobsView from "../views/JobsView.vue";
import ExecutionsView from "../views/ExecutionsView.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/",
      component: AppShell,
      children: [
        { path: "", name: "dashboard", component: DashboardView, meta: { title: "平台总览" } },
        { path: "connectors", name: "connectors", component: ConnectorsView, meta: { title: "连接器中心" } },
        { path: "datasets", name: "datasets", component: DatasetsView, meta: { title: "数据集工作台" } },
        { path: "jobs", name: "jobs", component: JobsView, meta: { title: "任务控制台" } },
        { path: "executions", name: "executions", component: ExecutionsView, meta: { title: "执行账本" } }
      ]
    }
  ]
});

router.afterEach((to) => {
  if (to.meta.title) {
    document.title = `${to.meta.title} | 多数据源模拟数据生成器`;
  }
});

export default router;
