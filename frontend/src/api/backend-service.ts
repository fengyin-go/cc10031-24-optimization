import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 任务队列调度：排队水位、任务排队数据快照与版本升级
// ---------------------------------------------------------------------------

/** 队列变更事件名：任务队列操作后广播，其余业务页面随结果联动刷新 */
export const QUEUE_UPDATED_EVENT = 'queue-updated'

export function broadcastQueueUpdated(): void {
  window.dispatchEvent(new CustomEvent(QUEUE_UPDATED_EVENT))
  // 跨标签页联动：写入时间戳触发其他页面的 storage 监听
  try {
    localStorage.setItem(QUEUE_UPDATED_EVENT, String(Date.now()))
  } catch {
    /* 隐私模式下忽略 */
  }
}

export type QueueWaterLevelView = {
  taken_boundary: number
  window_start: number
  window_end: number
  terminal_state: string
  queued_total: number
}

export type QueuePosition = {
  task_id: number
  任务编号: string
  流程名称: string
  优先级: string
  提交时间: string
  queue_seq: number
  queue_version: string
  position: number
  status: string
}

export type QueueStatus = {
  ok: boolean
  queue_version: string
  water_level: QueueWaterLevelView
  expected_order: QueuePosition[]
  runtime: Record<string, unknown>
}

export async function loadQueueStatus(): Promise<QueueStatus> {
  const response = await request('/api/queue/status')
  if (!response.ok) throw new Error('队列水位读取失败')
  return await response.json()
}

export async function loadQueueSnapshot(size = 5): Promise<Record<string, unknown>> {
  const response = await request(`/api/queue/snapshot?size=${size}`)
  if (!response.ok) throw new Error('队列快照生成失败')
  return await response.json()
}

export async function dispatchDueTasks(): Promise<Record<string, unknown>> {
  const response = await request('/api/queue/dispatch', { method: 'POST' })
  if (!response.ok) throw new Error('到期任务展开失败')
  return await response.json()
}

export async function scheduleQueueBatch(size = 5): Promise<Record<string, unknown>> {
  const response = await request(`/api/queue/schedule?size=${size}`, { method: 'POST' })
  if (!response.ok) throw new Error('窗口任务消费失败')
  return await response.json()
}

export async function upgradeQueue(strategy: 'continue' | 'rebuild'): Promise<Record<string, unknown>> {
  const response = await request('/api/queue/upgrade', {
    method: 'POST',
    body: JSON.stringify({ strategy }),
  })
  if (!response.ok) throw new Error('调度流程升级失败')
  return await response.json()
}

export type CompleteResult = {
  ok: boolean
  conflict: boolean
  message: string
  status?: string
}

export async function completeQueueTask(
  taskId: number,
  result: Record<string, unknown> = {},
): Promise<CompleteResult> {
  const response = await request(`/api/queue/${taskId}/complete`, {
    method: 'POST',
    body: JSON.stringify({ result, success: true }),
  })
  return await response.json()
}

export type VerifyResult = {
  ok: boolean
  queue_version: string
  expected_order: number[]
  stable_sorted: boolean
  taken_boundary: number
  boundary_overlap: string[]
  terminal_count: number
  terminal_reentrant: string[]
  message: string
}

export async function verifyQueueOrder(): Promise<VerifyResult> {
  const response = await request('/api/queue/verify')
  if (!response.ok) throw new Error('稳定顺序核对失败')
  return await response.json()
}
