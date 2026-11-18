"""任务队列调度管理接口：排队水位、任务排队数据快照与版本升级。

与通用资源路由（/api/{module}）分离，路径前缀 /api/queue，
在 main.py 中先于资源路由注册，避免被动态路径抢占。

覆盖能力：
- 查看当前水位和预计执行顺序（GET /status, GET /snapshot）
- 调度计划展开到期采集任务清单并入队（POST /dispatch）
- 按稳定位置消费当前窗口任务并记录已取走边界（POST /schedule）
- 升级调度流程：continue 沿用旧快照完成存量 / rebuild 重建全部队列（POST /upgrade）
- 完成任务：并发执行只接受首个成功结果，终态不可重入（POST /{task_id}/complete）
- 核对升级后仍能按稳定顺序续做任务（GET /verify）
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.scheduler import SchedulerService
from app.services.task_queue import TaskQueueService
from app.store import store

router = APIRouter(prefix="/api/queue", tags=["任务队列调度"])

task_queue = TaskQueueService()
scheduler = SchedulerService()


class CompletePayload(BaseModel):
    result: dict[str, Any] = Field(default_factory=dict)
    success: bool = True


class UpgradePayload(BaseModel):
    strategy: str = "continue"  # continue=沿用旧快照完成存量, rebuild=按新规则重建


@router.get("/status")
def queue_status() -> dict[str, Any]:
    """当前排队水位、队列版本与预计执行顺序。"""
    water = task_queue.compute_water_level()
    runtime_status = store.queue_runtime.status()
    return {
        "ok": True,
        "queue_version": water["queue_version"],
        "water_level": {
            "taken_boundary": water["taken_boundary"],
            "window_start": water["window_start"],
            "window_end": water["window_end"],
            "terminal_state": water["terminal_state"],
            "queued_total": water["queued_total"],
            "conflicts": runtime_status["water_level"]["conflicts"],
        },
        "expected_order": task_queue.expected_order(),
        "runtime": runtime_status,
    }


@router.get("/snapshot")
def queue_snapshot(size: int = Query(default=5, ge=1, le=100)) -> dict[str, Any]:
    """生成任务排队数据快照：解析同一时间窗口可执行的任务边界。"""
    snapshot = task_queue.build_queue_snapshot(window_size=size)
    return {"ok": True, "snapshot": snapshot}


@router.post("/dispatch")
def dispatch_due_tasks() -> dict[str, Any]:
    """调度计划接收并展开处理项承载的到期采集任务清单，交由任务队列入队。"""
    expanded = scheduler.expand_due_tasks()
    queued = task_queue.consume_scheduled_to_queue()
    return {"ok": True, "expanded": expanded, "queued": queued}


@router.post("/schedule")
def schedule_batch(size: int = Query(default=5, ge=1, le=100)) -> dict[str, Any]:
    """按稳定队列位置消费当前窗口任务，并记录已取走边界。"""
    result = task_queue.schedule_next_batch(max_tasks=size)
    return {"ok": True, **result}


@router.post("/upgrade")
def upgrade_schedule(payload: UpgradePayload) -> dict[str, Any]:
    """升级调度流程：两种存量处理选择都不重复执行已完成任务。"""
    return task_queue.upgrade_schedule(strategy=payload.strategy)


@router.post("/{task_id}/complete")
def complete_task(task_id: int, payload: CompletePayload) -> dict[str, Any]:
    """完成任务：并发执行只接受首个成功结果，已完成任务保持不可重入。"""
    return task_queue.complete_task(task_id, result=payload.result,
                                    success=payload.success)


@router.get("/verify")
def verify_stable_resume() -> dict[str, Any]:
    """核对升级调度流程后仍能按稳定顺序续做任务。"""
    return task_queue.verify_stable_resume()
