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
        { path: "", name: "dashboard", component: DashboardView, meta: { title: "Platform Overview" } },
        { path: "connectors", name: "connectors", component: ConnectorsView, meta: { title: "Connector Center" } },
        { path: "datasets", name: "datasets", component: DatasetsView, meta: { title: "Dataset Studio" } },
        { path: "jobs", name: "jobs", component: JobsView, meta: { title: "Job Control" } },
        { path: "executions", name: "executions", component: ExecutionsView, meta: { title: "Execution Ledger" } }
      ]
    }
  ]
});

router.afterEach((to) => {
  if (to.meta.title) {
    document.title = `${to.meta.title} | Multisource Data Generator`;
  }
});

export default router;

