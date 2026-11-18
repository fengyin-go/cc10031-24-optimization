<template>
  <section class="page" data-module="task_queue">
    <header class="page-head">
      <div>
        <h2>任务队列</h2>
        <p class="page-desc">
          管理RPA采集任务的优先级队列：优先级 + 提交时间共同形成稳定队列位置，
          升级调度流程后仍能按稳定顺序续做任务。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="dispatch" type="button" @click="onDispatch">
          展开到期任务
        </button>
        <button class="btn primary" data-testid="schedule" type="button" @click="onSchedule">
          消费当前窗口
        </button>
        <button class="btn" data-testid="snapshot" type="button" @click="onSnapshot">
          生成排队快照
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">队列版本</span>
        <strong class="stat-value" data-testid="queue-version">{{ status?.queue_version ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已取走边界</span>
        <strong class="stat-value" data-testid="taken-boundary">
          {{ status?.water_level.taken_boundary ?? 0 }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">窗口边界</span>
        <strong class="stat-value" data-testid="window-boundary">
          {{ status ? `${status.water_level.window_start} ~ ${status.water_level.window_end}` : '—' }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">水位处理终态</span>
        <strong class="stat-value" data-testid="water-terminal">
          {{ status?.water_level.terminal_state ?? '—' }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">排队中</span>
        <strong class="stat-value">{{ status?.water_level.queued_total ?? 0 }}</strong>
      </article>
    </div>

    <div class="queue-ops">
      <span class="ops-label">升级调度流程：</span>
      <button class="btn" data-testid="upgrade-continue" type="button" @click="onUpgrade('continue')">
        升级·沿用旧快照完成存量
      </button>
      <button class="btn" data-testid="upgrade-rebuild" type="button" @click="onUpgrade('rebuild')">
        升级·按新规则重建队列
      </button>
      <button class="btn ghost" data-testid="verify" type="button" @click="onVerify">
        核对稳定顺序
      </button>
      <span v-if="verifyResult" class="ops-result" :class="{ bad: !verifyResult.ok }" data-testid="verify-result">
        {{ verifyResult.message }}（终态 {{ verifyResult.terminal_count }} 个保持不可重入）
      </span>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>优先级</span>
        <select v-model="filters.优先级" data-testid="filter-优先级">
          <option value="">全部</option>
          <option value="高">高</option>
          <option value="中">中</option>
          <option value="低">低</option>
        </select>
      </label>
      <label class="filter-item">
        <span>提交时间</span>
        <input v-model="filters.提交时间" data-testid="filter-提交时间" placeholder="如 2026-09-01 或 08:05" />
      </label>
      <label class="filter-item">
        <span>队列版本</span>
        <select v-model="filters.queue_version" data-testid="filter-queue-version">
          <option value="">全部</option>
          <option value="v1">v1（既有版本）</option>
          <option value="v2">v2（稳定排序）</option>
        </select>
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <h3 class="section-title">预计执行顺序（按稳定队列位置）</h3>
    <table class="data-table" data-testid="queue-table">
      <thead>
        <tr>
          <th>位置</th><th>任务编号</th><th>流程名称</th><th>优先级</th>
          <th>提交时间</th><th>队列序号</th><th>队列版本</th><th>当前状态</th><th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in queuedRows" :key="String(row.id)" :data-testid="`row-${row.id}`">
          <td>{{ row.预计执行顺序 ?? '—' }}</td>
          <td>{{ row.任务编号 }}</td>
          <td>{{ row.流程名称 }}</td>
          <td>{{ row.优先级 }}</td>
          <td>{{ row.提交时间 }}</td>
          <td>{{ row.queue_seq }}</td>
          <td>{{ row.队列版本 }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`action-${row.id}-取消任务`" @click="onAction(row.id, '取消任务')">取消任务</button>
            <button class="link" type="button" :data-testid="`action-${row.id}-提升优先级`" @click="onAction(row.id, '提升优先级')">提升优先级</button>
          </td>
        </tr>
        <tr v-if="!queuedRows.length">
          <td colspan="9" class="empty-state">当前筛选条件下没有排队中的任务</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">处理清单（执行中）</h3>
    <table class="data-table" data-testid="running-table">
      <thead>
        <tr><th>任务编号</th><th>流程名称</th><th>优先级</th><th>提交时间</th><th>当前状态</th><th>完成</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in runningRows" :key="String(row.id)" :data-testid="`running-${row.id}`">
          <td>{{ row.任务编号 }}</td>
          <td>{{ row.流程名称 }}</td>
          <td>{{ row.优先级 }}</td>
          <td>{{ row.提交时间 }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`complete-${row.id}`" @click="onComplete(row.id)">
              标记完成
            </button>
            <button class="link" type="button" :data-testid="`complete-race-${row.id}`" @click="onCompleteRace(row.id)">
              并发完成
            </button>
          </td>
        </tr>
        <tr v-if="!runningRows.length">
          <td colspan="6" class="empty-state">暂无执行中任务，可点击「消费当前窗口」取走任务</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">结果列表（终态不可重入）</h3>
    <table class="data-table" data-testid="result-table">
      <thead>
        <tr><th>任务编号</th><th>流程名称</th><th>优先级</th><th>提交时间</th><th>完成时间</th><th>当前状态</th><th>重入</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in terminalRows" :key="String(row.id)" :data-testid="`terminal-${row.id}`">
          <td>{{ row.任务编号 }}</td>
          <td>{{ row.流程名称 }}</td>
          <td>{{ row.优先级 }}</td>
          <td>{{ row.提交时间 }}</td>
          <td>{{ row.完成时间 ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`recomplete-${row.id}`" @click="onComplete(row.id)">
              再次完成
            </button>
          </td>
        </tr>
        <tr v-if="!terminalRows.length">
          <td colspan="7" class="empty-state">暂无终态任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条排队任务记录</span>
      <span v-if="message" class="ops-message" data-testid="queue-message">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  broadcastQueueUpdated,
  completeQueueTask,
  dispatchDueTasks,
  listEntries,
  loadQueueStatus,
  loadQueueSnapshot,
  runAction,
  scheduleQueueBatch,
  upgradeQueue,
  verifyQueueOrder,
  type QueueStatus,
  type VerifyResult,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const rows = ref<EntryRow[]>([])
const total = ref(0)
const status = ref<QueueStatus | null>(null)
const verifyResult = ref<VerifyResult | null>(null)
const message = ref('')
const errorMessage = ref('')
const filters = ref<Record<string, string>>({ 优先级: '', 提交时间: '', queue_version: '' })

const queuedRows = computed(() => rows.value.filter((row) => row.status === '排队中'))
const runningRows = computed(() => rows.value.filter((row) => row.status === '执行中'))
const terminalRows = computed(() =>
  rows.value.filter((row) => ['已完成', '已取消', '执行失败'].includes(String(row.status))),
)

function activeFilters(): Record<string, string> {
  return Object.fromEntries(
    Object.entries(filters.value).filter(([, value]) => String(value).trim() !== ''),
  )
}

async function reload() {
  errorMessage.value = ''
  try {
    const [payload, queueStatus] = await Promise.all([
      listEntries('task_queue', { ...activeFilters(), size: '200' }),
      loadQueueStatus(),
    ])
    rows.value = payload.items
    total.value = payload.total
    status.value = queueStatus
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务队列读取失败'
  }
}

function resetFilters() {
  filters.value = { 优先级: '', 提交时间: '', queue_version: '' }
  void reload()
}

/** 操作后广播队列变更：其余业务页面随结果联动刷新 */
async function afterMutation(text: string) {
  message.value = text
  broadcastQueueUpdated()
  await reload()
}

async function onDispatch() {
  const result = (await dispatchDueTasks()) as {
    expanded: { expanded: number }
    queued: { generated: number }
  }
  await afterMutation(
    `已展开到期采集任务 ${result.expanded.expanded} 个处理项，入队 ${result.queued.generated} 条`,
  )
}

async function onSchedule() {
  const result = (await scheduleQueueBatch(5)) as {
    scheduled: number
    taken_boundary: number
  }
  await afterMutation(
    `已按稳定位置消费当前窗口 ${result.scheduled} 个任务，已取走边界推进到 ${result.taken_boundary}`,
  )
}

async function onSnapshot() {
  const result = (await loadQueueSnapshot(5)) as {
    snapshot: { snapshot_id: number; queue_version: string; window: { task_ids: number[] } }
  }
  const snap = result.snapshot
  await afterMutation(
    `已生成任务排队数据快照 #${snap.snapshot_id}（${snap.queue_version}），窗口任务边界：${snap.window.task_ids.join(', ') || '空'}`,
  )
}

async function onUpgrade(strategy: 'continue' | 'rebuild') {
  const result = (await upgradeQueue(strategy)) as { ok: boolean; message: string }
  if (!result.ok) {
    errorMessage.value = result.message
    return
  }
  await afterMutation(result.message)
}

async function onVerify() {
  verifyResult.value = await verifyQueueOrder()
  await reload()
}

async function onComplete(taskId: number) {
  const result = await completeQueueTask(taskId, { source: 'manual' })
  if (result.conflict) {
    // 并发/重入冲突：只接受首个成功结果，水位处理终态随之改变
    errorMessage.value = result.message
    await reload()
    return
  }
  await afterMutation(result.message)
}

/** 并发完成演示：同时发起多个完成请求，后端只接受首个成功结果 */
async function onCompleteRace(taskId: number) {
  const results = await Promise.all([
    completeQueueTask(taskId, { source: 'race-1' }),
    completeQueueTask(taskId, { source: 'race-2' }),
    completeQueueTask(taskId, { source: 'race-3' }),
  ])
  const okCount = results.filter((r) => r.ok).length
  const conflictCount = results.filter((r) => r.conflict).length
  await afterMutation(
    `并发完成：${okCount} 个成功结果被接受，${conflictCount} 个冲突结果被丢弃`,
  )
}

async function onAction(taskId: number, action: string) {
  const result = await runAction('task_queue', taskId, action)
  if (!result.ok) {
    errorMessage.value = result.message
    return
  }
  await afterMutation(result.message)
}

onMounted(reload)
</script>

<style scoped>
.section-title {
  font-size: 14px;
  margin: 16px 0 8px;
}
.queue-ops {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 12px;
  padding: 10px 12px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
}
.ops-label {
  font-size: 13px;
  color: var(--muted);
}
.ops-result {
  font-size: 13px;
  color: #15803d;
}
.ops-result.bad {
  color: #b42318;
}
.ops-message {
  color: #15803d;
}
.filter-item select,
.filter-item input {
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 13px;
}
</style>
