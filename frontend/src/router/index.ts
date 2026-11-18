import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import TaskQueueView from '@/views/TaskQueueView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 任务队列使用专用页面：排队水位、稳定顺序与版本升级
    { path: '/task_queue', name: 'task_queue', component: TaskQueueView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
