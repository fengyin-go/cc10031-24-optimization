"""任务队列领域服务 — 消费编排结果生成执行任务。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    channel("rpa.steps_submitted") ← step_editor
    → task_queue 消费 → 生成排队任务 + execution_log 记录

调度排队水位与稳定顺序：
    既有版本（v1）只按优先级排序，同优先级任务的顺序随列表位置变化，
    中断恢复后很难判断下一项应从哪里继续。升级版本（v2）让
    优先级 + 提交时间 + queue_seq 共同形成稳定队列位置：

    - compute_water_level: 读取排队任务、优先级、提交时间，重算窗口内调度排队水位
    - build_queue_snapshot: 解析同一时间窗口可执行的任务边界并生成任务排队数据快照
    - schedule_next_batch: 按稳定位置消费当前窗口任务并记录已取走边界
    - upgrade_schedule: 升级调度流程（continue 沿用旧快照完成存量 /
      rebuild 按新排序规则重建全部队列），两种选择都不重复执行已完成任务
    - complete_task: 并发执行只接受首个成功结果，已完成任务保持不可重入；
      消费冲突会改变调度排队水位的处理终态
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from app.queue_runtime import (
    QUEUE_VERSION_LEGACY,
    QUEUE_VERSION_STABLE,
    TERMINAL_STATUSES,
)
from app.services.base import BaseModuleService
from app.store import store

# 优先级权重：数值越小越优先
PRIORITY_WEIGHT = {"高": 0, "中": 1, "低": 2}

# 升级策略
STRATEGY_CONTINUE = "continue"  # 沿用旧快照顺序完成存量
STRATEGY_REBUILD = "rebuild"    # 按新排序规则重建全部队列


def _parse_submit_time(value: Any) -> tuple[int, str]:
    """把提交时间规范化为可比较的时间键。

    既有数据里提交时间可能是日期（2026-09-01）或日期时间
    （2026-09-01 08:30:00 / ISO 格式），统一折叠成可比较元组，
    无法解析时排到同优先级最后。
    """
    text = str(value or "").strip()
    if not text:
        return (1, "9999-12-31 23:59:59")
    normalized = text.replace("T", " ").replace("Z", "")[:19]
    fmt = "%Y-%m-%d %H:%M:%S" if " " in normalized else "%Y-%m-%d"
    try:
        dt = datetime.strptime(normalized, fmt)
        return (0, dt.strftime("%Y-%m-%d %H:%M:%S"))
    except ValueError:
        return (1, "9999-12-31 23:59:59")


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "流程名称", "提交时间", "优先级", "预计执行", "排队时长", "当前状态"]
    REQUIRED_FIELDS = ["任务编号", "流程名称", "提交时间"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已取消", "执行失败"]
    ACTION_RULES = {"取消任务": "已取消", "提升优先级": "排队中"}
    NEGATIVE_ACTIONS = []
    # 列表接口在 LIST_FIELDS 之外额外开放的筛选字段（队列版本筛选）
    EXTRA_FILTER_FIELDS = ["queue_version"]

    # 完成任务的互斥锁：保证「检查终态 → 置为终态」原子执行，
    # 并发执行同一任务时只接受首个成功结果
    _complete_lock = threading.Lock()
    # 取任务的互斥锁：保证并发调度不会重复取走同一任务
    _schedule_lock = threading.Lock()

    # ---- 稳定队列位置 ----

    def _ensure_queue_meta(self, entry: dict[str, Any]) -> dict[str, Any]:
        """为既有版本任务补齐队列元数据（兼容既有版本边界）。

        v1 遗留数据没有 queue_seq / queue_version：补发单调序列号并
        标记为 v1，保证升级前旧数据也能参与稳定排序与水位计算。
        """
        runtime = store.queue_runtime
        if not entry.get("queue_seq"):
            entry["queue_seq"] = runtime.next_seq()
        else:
            runtime.align_seq(int(entry["queue_seq"]))
        if not entry.get("queue_version"):
            entry["queue_version"] = QUEUE_VERSION_LEGACY
        return entry

    def _order_key(self, entry: dict[str, Any]) -> tuple[Any, ...]:
        """统一队列位置排序键。

        - 冻结存量（升级 continue 策略）：(0, 优先级, 占位, queue_seq)，
          整体排在最前，沿用旧快照顺序完成存量；
        - v1 既有版本：(0, 优先级, 占位, queue_seq)，仅按优先级分组，
          同优先级保持旧列表顺序；
        - v2 稳定版本：(1, 优先级, 提交时间, queue_seq)，优先级与提交
          时间共同形成稳定队列位置，并发提交不再随列表位置变化。
        """
        self._ensure_queue_meta(entry)
        prio = PRIORITY_WEIGHT.get(str(entry.get("优先级", "低")), 99)
        seq = int(entry.get("queue_seq", 0))
        frozen = bool(entry.get("order_frozen"))
        if frozen or store.queue_runtime.queue_version == QUEUE_VERSION_LEGACY:
            return (0, prio, (0, ""), seq)
        return (1, prio, _parse_submit_time(entry.get("提交时间")), seq)

    def _ordered_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按当前队列版本的稳定规则排序（排序键见 _order_key）。"""
        return sorted(entries, key=self._order_key)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is not None:
            runtime = store.queue_runtime
            entry["queue_seq"] = runtime.next_seq()
            entry["queue_version"] = runtime.queue_version
            entry["当前状态"] = entry["status"]
        return entry, missing

    # ---- 排队水位与任务排队数据快照 ----

    def compute_water_level(self, window_size: int = 5) -> dict[str, Any]:
        """读取排队任务、优先级、提交时间，重算窗口内调度排队水位。

        同一时间窗口内可执行的任务边界 = 已取走边界（稳定排序键）之后
        的前 window_size 个排队中任务；窗口边界以其 queue_seq 区间表示。

        Returns:
            当前水位视图：已取走边界、窗口边界、处理终态、窗口内任务。
        """
        runtime = store.queue_runtime
        rows = [r for r in store.rows(self.MODULE) if r.get("status") == "排队中"]
        ordered = self._ordered_entries(rows)

        taken_key = runtime.water_level.taken_key
        # 水位之后仍未取走的任务才进入当前窗口（续做不重复）
        window_tasks = [
            r for r in ordered
            if taken_key is None or self._order_key(r) > taken_key
        ]
        window = window_tasks[:window_size]
        if window:
            seqs = [int(r["queue_seq"]) for r in window]
            runtime.set_window(min(seqs), max(seqs))
        else:
            runtime.set_window(0, 0)
        if not rows:
            runtime.mark_exhausted()

        wl = runtime.water_level
        return {
            "queue_version": runtime.queue_version,
            "taken_boundary": wl.taken_boundary,
            "window_start": wl.window_start,
            "window_end": wl.window_end,
            "terminal_state": wl.terminal_state,
            "queued_total": len(rows),
            "window_tasks": [self._position_view(r, i + 1) for i, r in enumerate(window)],
        }

    def build_queue_snapshot(self, window_size: int = 5) -> dict[str, Any]:
        """解析同一时间窗口可执行的任务边界并生成任务排队数据快照。

        快照固化：全部排队任务的稳定位置、当前水位、窗口边界。
        升级调度流程后的恢复以快照为准，按稳定顺序续做任务。
        """
        rows = [r for r in store.rows(self.MODULE) if r.get("status") == "排队中"]
        ordered = self._ordered_entries(rows)
        positions = [self._position_view(r, i + 1) for i, r in enumerate(ordered)]

        water = self.compute_water_level(window_size=window_size)
        window = {
            "window_start": water["window_start"],
            "window_end": water["window_end"],
            "task_ids": [t["task_id"] for t in water["window_tasks"]],
            "size": window_size,
        }
        snap = store.queue_runtime.build_snapshot(positions, window)
        return snap.to_dict()

    def _position_view(self, entry: dict[str, Any], position: int) -> dict[str, Any]:
        self._ensure_queue_meta(entry)
        return {
            "task_id": entry.get("id"),
            "任务编号": entry.get("任务编号"),
            "流程名称": entry.get("流程名称"),
            "优先级": entry.get("优先级"),
            "提交时间": entry.get("提交时间"),
            "queue_seq": entry.get("queue_seq"),
            "queue_version": entry.get("queue_version"),
            "position": position,
            "status": entry.get("status"),
        }

    def expected_order(self) -> list[dict[str, Any]]:
        """预计执行顺序：全部排队中任务按稳定队列位置展开。"""
        rows = [r for r in store.rows(self.MODULE) if r.get("status") == "排队中"]
        return [
            self._position_view(r, i + 1)
            for i, r in enumerate(self._ordered_entries(rows))
        ]

    # ---- 调度消费：按稳定位置取走当前窗口任务 ----

    def schedule_next_batch(self, max_tasks: int = 5) -> dict[str, Any]:
        """按稳定队列位置消费当前窗口任务，并记录已取走边界。

        取任务顺序由 优先级 + 提交时间 + queue_seq 共同决定；
        取走后以稳定排序键推进水位，恢复续做时只消费水位之后的任务，
        不重复执行已完成任务。取任务在类级锁内完成，并发调度不重复取。
        """
        with self._schedule_lock:
            return self._take_batch(max_tasks)

    def _take_batch(self, max_tasks: int) -> dict[str, Any]:
        rows = [
            r for r in store.rows(self.MODULE)
            if r.get("status") == "排队中" and not r.get("abnormal")
        ]
        ordered = self._ordered_entries(rows)
        runtime = store.queue_runtime
        taken_key = runtime.water_level.taken_key
        # 只取已取走边界之后的任务，保证续做不重复
        takeable = [
            r for r in ordered
            if taken_key is None or self._order_key(r) > taken_key
        ]

        scheduled: list[int] = []
        last_key: tuple[Any, ...] | None = None
        last_seq = 0
        for task in takeable[:max_tasks]:
            task["status"] = "执行中"
            task["当前状态"] = "执行中"
            task["pending"] = True
            task["开始执行时间"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S")
            scheduled.append(task["id"])
            last_key = self._order_key(task)
            last_seq = int(task["queue_seq"])
            store.channel.publish(
                topic="rpa.task_started",
                source=self.MODULE,
                payload={
                    "task_id": task["id"],
                    "task_number": task.get("任务编号"),
                    "queue_seq": task["queue_seq"],
                    "queue_version": task.get("queue_version"),
                    "start_time": task["开始执行时间"],
                },
            )

        if last_key is not None:
            remaining = [int(r["queue_seq"]) for r in takeable[max_tasks:]]
            window_end = max(remaining) if remaining else last_seq
            runtime.record_taken_boundary(last_key, last_seq, window_end)
        self.compute_water_level(window_size=max_tasks)

        return {
            "scheduled": len(scheduled),
            "task_ids": scheduled,
            "taken_boundary": runtime.water_level.taken_boundary,
            "queue_version": runtime.queue_version,
        }

    # ---- 完成：并发执行只接受首个成功结果 ----

    def complete_task(self, task_id: int, result: dict[str, Any] | None = None,
                      success: bool = True) -> dict[str, Any]:
        """把任务置为终态；已完成任务保持不可重入。

        并发执行同一任务时只接受首个成功结果：任务已进入终态后，
        后续完成请求一律判定为冲突，并改变调度排队水位的处理终态。
        检查与置态在类级锁内原子完成。
        """
        with self._complete_lock:
            entry = store.find(self.MODULE, task_id)
            if entry is None:
                return {"ok": False, "conflict": False,
                        "message": f"排队任务 {task_id} 不存在或已归档"}

            current = entry.get("status")
            if current in TERMINAL_STATUSES:
                # 不可重入冲突：记录并改变水位处理终态
                store.queue_runtime.mark_conflict({
                    "type": "重复完成",
                    "task_id": task_id,
                    "task_number": entry.get("任务编号"),
                    "held_status": current,
                })
                return {
                    "ok": False,
                    "conflict": True,
                    "message": f"任务 {entry.get('任务编号')} 已处于终态「{current}」，"
                               "只接受首个成功结果，本次结果已丢弃",
                    "held_status": current,
                }

            final_status = "已完成" if success else "执行失败"
            entry["status"] = final_status
            entry["当前状态"] = final_status
            entry["pending"] = False
            entry["abnormal"] = not success
            entry["完成时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            if result:
                entry["result"] = result

        store.channel.publish(
            topic="rpa.task_completed",
            source=self.MODULE,
            payload={
                "task_id": task_id,
                "task_number": entry.get("任务编号"),
                "queue_seq": entry.get("queue_seq"),
                "final_status": final_status,
                "processed_by": self.MODULE,
                "result": result or {},
            },
        )
        return {
            "ok": True,
            "conflict": False,
            "message": f"任务 {entry.get('任务编号')} 已置为「{final_status}」",
            "task_id": task_id,
            "status": final_status,
        }

    # ---- 版本升级：存量两种处理选择 ----

    def upgrade_schedule(self, strategy: str = STRATEGY_CONTINUE) -> dict[str, Any]:
        """升级调度流程到稳定排序版本。

        两种存量处理选择，都不重复执行已完成任务：

        - continue: 沿用旧快照顺序完成存量 —— 升级前已排队任务冻结在
          旧快照顺序中（queue_seq 不变），新任务按 v2 稳定规则排队；
        - rebuild: 按新排序规则重建全部队列 —— 所有非终态任务按
          优先级 + 提交时间重发 queue_seq，终态任务位置冻结、保持不可重入。

        Returns:
            升级结果：策略、重排任务数、冻结的已完成任务数、新快照。
        """
        if strategy not in (STRATEGY_CONTINUE, STRATEGY_REBUILD):
            return {"ok": False, "message": f"未知升级策略：{strategy}"}

        runtime = store.queue_runtime
        rows = store.rows(self.MODULE)
        for row in rows:
            self._ensure_queue_meta(row)

        terminal = [r for r in rows if r.get("status") in TERMINAL_STATUSES]
        queued = [r for r in rows if r.get("status") == "排队中"]

        # 升级前先生成旧版本快照：恢复时按它续做，已完成任务不重排
        pre_snapshot = self.build_queue_snapshot()

        reassigned = 0
        if strategy == STRATEGY_REBUILD:
            # 按新排序规则重建全部队列：排队中任务按稳定键重发位置
            for row in queued:
                row.pop("order_frozen", None)
            for row in self._ordered_entries(queued):
                row["queue_seq"] = runtime.next_seq()
                reassigned += 1
            # 重建后水位边界失效，重置后由下次消费重新推进；
            # 终态任务不在排队集合中，不会被重复执行
            runtime.reset_water_level()
        else:
            # 沿用旧快照顺序完成存量：存量任务冻结在当前顺序中，
            # 新任务按 v2 稳定规则排在存量之后
            for row in queued:
                row["order_frozen"] = True

        # 全部任务升级到 v2 版本标记；终态任务位置与状态冻结，保持不可重入
        for row in rows:
            row["queue_version"] = QUEUE_VERSION_STABLE

        runtime.upgrade(strategy)
        post_snapshot = self.build_queue_snapshot()

        return {
            "ok": True,
            "strategy": strategy,
            "queue_version": runtime.queue_version,
            "reassigned": reassigned,
            "terminal_frozen": len(terminal),
            "pre_snapshot_id": pre_snapshot["snapshot_id"],
            "post_snapshot_id": post_snapshot["snapshot_id"],
            "message": "调度流程已升级："
                       + ("沿用旧快照顺序完成存量" if strategy == STRATEGY_CONTINUE
                          else "已按新排序规则重建全部队列")
                       + "，已完成任务保持终态不可重入",
        }

    # ---- 升级核对 ----

    def verify_stable_resume(self) -> dict[str, Any]:
        """核对升级调度流程后仍能按稳定顺序续做任务。

        校验点：
        1. 预计执行顺序是确定的（连续两次重算结果一致）
        2. 已取走边界不与排队任务重叠（续做不重复）
        3. 已完成任务全部处于终态且保持不可重入
        """
        rows = store.rows(self.MODULE)
        queued = [r for r in rows if r.get("status") == "排队中"]
        order_first = [p["task_id"] for p in self.expected_order()]
        order_second = [p["task_id"] for p in self.expected_order()]

        taken_key = store.queue_runtime.water_level.taken_key
        overlap = [
            r.get("任务编号") for r in queued
            if taken_key is not None and self._order_key(r) <= taken_key
        ]
        terminal = [r for r in rows if r.get("status") in TERMINAL_STATUSES]
        terminal_reentrant = [r.get("任务编号") for r in terminal
                              if r.get("pending")]

        stable_sorted = order_first == order_second
        ok = stable_sorted and not overlap and not terminal_reentrant
        return {
            "ok": ok,
            "queue_version": store.queue_runtime.queue_version,
            "expected_order": order_first,
            "stable_sorted": stable_sorted,
            "taken_boundary": store.queue_runtime.water_level.taken_boundary,
            "boundary_overlap": overlap,
            "terminal_count": len(terminal),
            "terminal_reentrant": terminal_reentrant,
            "message": "升级后可按稳定顺序续做任务" if ok
                       else "队列存在乱序/重复消费/终态可重入风险",
        }

    # ---- 列表：优先级 / 提交时间 / 队列版本筛选 + 预计执行顺序 ----

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        filters = dict(filters or {})
        version_filter = filters.pop("queue_version", "")
        items, total = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
        if version_filter:
            items = [
                row for row in items
                if str(self._ensure_queue_meta(row).get("queue_version"))
                == version_filter
            ]
        # 排队中任务按稳定队列位置展示，其余保持登记顺序
        queued = [r for r in items if r.get("status") == "排队中"]
        ordered_queued = self._ordered_entries(queued)
        position_of = {id(r): i + 1 for i, r in enumerate(ordered_queued)}
        for row in items:
            self._ensure_queue_meta(row)
            row["队列版本"] = row["queue_version"]
            row["预计执行顺序"] = position_of.get(id(row))
        items.sort(key=lambda r: (
            0 if r.get("status") == "排队中" else 1,
            position_of.get(id(r), 1_000_000),
            int(r.get("id", 0)),
        ))
        total = len(items)
        start = (page - 1) * size
        return items[start:start + size], total

    # ---- 跨模块业务方法 ----

    def consume_steps_to_queue(self) -> dict[str, Any]:
        """消费 step_editor 提交的编排结果，生成任务队列条目。

        从 channel("rpa.steps_submitted") 读取编排计划，
        为每个 bot 的每一步创建一个排队任务。

        数据流终点：这里是编排链路的末端 — 任务进入队列等待调度执行。

        Returns:
            {"generated": int, "plans_consumed": int, "task_ids": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.steps_submitted",
        )
        if not msgs:
            return {"generated": 0, "plans_consumed": 0, "task_ids": []}

        task_ids = []
        for msg in msgs:
            plan = msg.payload
            bot_id = plan.get("bot_id", 0)
            steps = plan.get("steps", [])
            if not steps:
                # 确认空计划
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            for i, step in enumerate(steps):
                seq = i + 1
                now = datetime.now(timezone.utc)
                task_number = f"TASK-{bot_id}-{seq:03d}"

                values = {
                    "任务编号": task_number,
                    "流程名称": f"Bot-{bot_id}",
                    "提交时间": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "优先级": "高" if seq <= 3 else "中",
                    "预计执行": step.get("name", f"Step-{seq}"),
                    "排队时长": "0s",
                    "当前状态": "排队中",
                }
                entry, missing = self.create_entry(values)
                if entry:
                    entry["step_id"] = step.get("step_id")
                    entry["bot_id"] = bot_id
                    entry["step_type"] = step.get("type", "")
                    task_ids.append(entry["id"])

            store.channel.confirm(msg.msg_id, self.MODULE)

        return {
            "generated": len(task_ids),
            "plans_consumed": len(msgs),
            "task_ids": task_ids,
        }

    def consume_scheduled_to_queue(self) -> dict[str, Any]:
        """消费调度计划展开的到期采集任务清单，按处理项约定入队。

        调度计划（scheduler）把到期采集任务清单发布到
        channel("rpa.crawl_scheduled")，这里按清单逐条生成排队任务，
        提交时间取自清单项（缺省取当前时间），并分配稳定队列位置。
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.crawl_scheduled",
        )
        if not msgs:
            return {"generated": 0, "plans_consumed": 0, "task_ids": []}

        task_ids: list[int] = []
        for msg in msgs:
            payload = msg.payload
            plan_id = payload.get("plan_id", 0)
            due_tasks = payload.get("due_tasks") or []
            if not due_tasks:
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue
            for item in due_tasks:
                values = {
                    "任务编号": item.get("任务编号")
                                or f"TASK-S{plan_id}-{item.get('seq', 0):03d}",
                    "流程名称": item.get("流程名称") or payload.get("bot_ref", ""),
                    "提交时间": item.get("提交时间")
                                or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "优先级": item.get("优先级") or payload.get("priority") or "中",
                    "预计执行": item.get("预计执行") or payload.get("plan_name", ""),
                    "排队时长": "0s",
                    "当前状态": "排队中",
                }
                entry, _ = self.create_entry(values)
                if entry:
                    entry["plan_id"] = plan_id
                    task_ids.append(entry["id"])
            store.channel.confirm(msg.msg_id, self.MODULE)

        return {
            "generated": len(task_ids),
            "plans_consumed": len(msgs),
            "task_ids": task_ids,
        }

    def update_queue_status(self, task_id: int,
                             status: str, result: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """更新任务状态并在执行完成时写 execution_log。

        Args:
            task_id: 任务 ID
            status: 目标状态（"执行中", "已完成", "执行失败"）
            result: 执行结果数据（可选）

        Returns:
            更新后的任务信息或 None
        """
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return None

        valid_statuses = {"执行中", "已完成", "已取消", "执行失败"}
        if status not in valid_statuses:
            return None

        entry["status"] = status
        entry["当前状态"] = status
        entry["pending"] = status not in ("已完成", "已取消")
        entry["abnormal"] = status == "执行失败"

        if result:
            entry["result"] = result

        # 完成后写入执行日志通道
        if status in ("已完成", "执行失败"):
            exec_payload = {
                "task_id": task_id,
                "task_number": entry.get("任务编号"),
                "final_status": status,
                "processed_by": self.MODULE,
                "result": result or {},
            }
            store.channel.publish(
                topic="rpa.task_completed",
                source=self.MODULE,
                payload=exec_payload,
            )

        return {
            "task_id": task_id,
            "status": status,
            "task_number": entry.get("任务编号"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 通道消费统计
        consumed = store.channel.snapshot(topic="rpa.steps_submitted")
        completed = store.channel.snapshot(topic="rpa.task_completed")
        runtime_status = store.queue_runtime.status()
        return {
            **base,
            "steps_plans_consumed": len(consumed),
            "tasks_completed": len(completed),
            "queue_version": runtime_status["queue_version"],
            "water_level": runtime_status["water_level"],
        }
