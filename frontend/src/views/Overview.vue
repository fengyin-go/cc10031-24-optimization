<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'

import { fetchJson } from '@/api/client'
import { QUEUE_UPDATED_EVENT } from '@/api/backend-service'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

async function reload() {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "流程定义", "created": 0, "pending": 0, "abnormal": 0}, {"name": "步骤编排", "created": 0, "pending": 0, "abnormal": 0}, {"name": "元素定位", "created": 0, "pending": 0, "abnormal": 0}, {"name": "触发配置", "created": 0, "pending": 0, "abnormal": 0}, {"name": "执行记录", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据提取", "created": 0, "pending": 0, "abnormal": 0}, {"name": "凭据管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "环境配置", "created": 0, "pending": 0, "abnormal": 0}, {"name": "反封锁", "created": 0, "pending": 0, "abnormal": 0}, {"name": "结果校验", "created": 0, "pending": 0, "abnormal": 0}, {"name": "批次追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "页面建模", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据补全", "created": 0, "pending": 0, "abnormal": 0}, {"name": "代理路由", "created": 0, "pending": 0, "abnormal": 0}, {"name": "频控策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "输出格式", "created": 0, "pending": 0, "abnormal": 0}, {"name": "异常日志", "created": 0, "pending": 0, "abnormal": 0}, {"name": "调度计划", "created": 0, "pending": 0, "abnormal": 0}, {"name": "运行概览", "created": 0, "pending": 0, "abnormal": 0}, {"name": "任务队列", "created": 0, "pending": 0, "abnormal": 0}]
  }
}

// 任务队列操作后广播 queue-updated：概览随结果联动刷新
function onQueueUpdated() {
  void reload()
}
function onStorage(event: StorageEvent) {
  if (event.key === QUEUE_UPDATED_EVENT) onQueueUpdated()
}

onMounted(() => {
  void reload()
  window.addEventListener(QUEUE_UPDATED_EVENT, onQueueUpdated)
  window.addEventListener('storage', onStorage)
})

onUnmounted(() => {
  window.removeEventListener(QUEUE_UPDATED_EVENT, onQueueUpdated)
  window.removeEventListener('storage', onStorage)
})
</script>
