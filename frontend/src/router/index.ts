import { createRouter, createWebHistory } from 'vue-router'
import DefaultLayout from '@/layouts/DefaultLayout.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      component: DefaultLayout,
      children: [
        {
          path: '',
          name: 'Home',
          component: () => import('@/views/Home.vue'),
          meta: { title: '首页' }
        },
        {
          path: 'data-sources',
          name: 'DataSources',
          component: () => import('@/views/DataSources.vue'),
          meta: { title: '数据源管理' }
        },
        {
          path: 'tasks',
          name: 'Tasks',
          component: () => import('@/views/Tasks.vue'),
          meta: { title: '任务管理' }
        },
        {
          path: 'executions',
          name: 'Executions',
          component: () => import('@/views/Executions.vue'),
          meta: { title: '执行记录' }
        }
      ]
    }
  ]
})

export default router 