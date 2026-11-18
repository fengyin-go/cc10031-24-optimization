"""任务队列调度升级测试：稳定队列位置、排队水位、快照与版本升级。

覆盖需求：
1. 同优先级任务的稳定第二排序键（提交时间 + queue_seq）
2. 读取排队任务/优先级/提交时间，重算窗口内调度排队水位
3. 解析同一时间窗口可执行的任务边界并生成任务排队数据快照
4. 按稳定位置消费当前窗口任务并记录已取走边界
5. 升级调度流程：continue 沿用旧快照完成存量 / rebuild 重建全部队列，
   两种选择都不重复执行已完成任务
6. 并发执行只接受首个成功结果，已完成任务保持不可重入
7. 冲突会改变调度排队水位的处理终态
8. 按优先级、提交时间和队列版本筛选，查看当前水位和预计执行顺序
9. 调度计划展开处理项承载的到期采集任务清单并入队
10. 最终核对升级调度流程后仍能按稳定顺序续做任务
"""
from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from app.queue_runtime import (
    QUEUE_VERSION_LEGACY,
    QUEUE_VERSION_STABLE,
    WATER_LEVEL_CONFLICT,
    QueueRuntime,
)
from app.services.scheduler import SchedulerService
from app.services.task_queue import TaskQueueService
from app.store import store

# 测试专用队列数据：覆盖同优先级不同提交时间、不同优先级、终态任务
QUEUE_ROWS = [
    {"id": 1, "status": "已完成", "pending": False, "abnormal": False,
     "任务编号": "TASK-T01", "流程名称": "电商价格采集",
     "提交时间": "2026-09-01 08:10:00", "优先级": "高",
     "预计执行": "打开列表页", "排队时长": "12m", "当前状态": "已完成"},
    {"id": 2, "status": "排队中", "pending": True, "abnormal": False,
     "任务编号": "TASK-T02", "流程名称": "电商价格采集",
     "提交时间": "2026-09-01 09:20:00", "优先级": "高",
     "预计执行": "提取价格表格", "排队时长": "58m", "当前状态": "排队中"},
    {"id": 3, "status": "排队中", "pending": True, "abnormal": False,
     "任务编号": "TASK-T03", "流程名称": "新闻资讯抓取",
     "提交时间": "2026-09-01 08:05:00", "优先级": "高",
     "预计执行": "翻页采集", "排队时长": "1h13m", "当前状态": "排队中"},
    {"id": 4, "status": "排队中", "pending": True, "abnormal": False,
     "任务编号": "TASK-T04", "流程名称": "新闻资讯抓取",
     "提交时间": "2026-09-01 10:40:00", "优先级": "中",
     "预计执行": "正文抽取", "排队时长": "32m", "当前状态": "排队中"},
    {"id": 5, "status": "排队中", "pending": True, "abnormal": False,
     "任务编号": "TASK-T05", "流程名称": "招投标公告采集",
     "提交时间": "2026-09-01 07:55:00", "优先级": "中",
     "预计执行": "附件下载", "排队时长": "1h20m", "当前状态": "排队中"},
    {"id": 6, "status": "执行失败", "pending": False, "abnormal": True,
     "任务编号": "TASK-T06", "流程名称": "招投标公告采集",
     "提交时间": "2026-09-01 06:30:00", "优先级": "低",
     "预计执行": "登录目标站", "排队时长": "—", "当前状态": "执行失败"},
]


@pytest.fixture()
def queue_env():
    """每个用例独立的队列环境：测试数据 + 全新队列运行时。"""
    saved_rows = store._tables.get("task_queue", [])
    saved_runtime = store.queue_runtime
    store._tables["task_queue"] = [dict(r) for r in QUEUE_ROWS]
    store.queue_runtime = QueueRuntime()
    svc = TaskQueueService()
    yield svc
    store._tables["task_queue"] = saved_rows
    store.queue_runtime = saved_runtime


def _queued_ids(svc: TaskQueueService) -> list[int]:
    return [p["task_id"] for p in svc.expected_order()]


class TestStableQueuePosition:
    """同优先级任务的稳定第二排序键。"""

    def test_v2_same_priority_sorted_by_submit_time(self, queue_env):
        """v2：同优先级按提交时间先后排，不再随列表位置变化。"""
        store.queue_runtime.queue_version = QUEUE_VERSION_STABLE
        order = _queued_ids(queue_env)
        # 高优先级：T03(08:05) 先于 T02(09:20)；中优先级：T05(07:55) 先于 T04(10:40)
        assert order == [3, 2, 5, 4]

    def test_same_submit_time_falls_back_to_queue_seq(self, queue_env):
        """提交时间相同的并发提交，按单调序列号保持稳定顺序。"""
        store.queue_runtime.queue_version = QUEUE_VERSION_STABLE
        for i, tid in enumerate((7, 8, 9)):
            entry, _ = queue_env.create_entry({
                "任务编号": f"TASK-C{tid}", "流程名称": "并发采集",
                "提交时间": "2026-09-01 12:00:00", "优先级": "高",
                "预计执行": "并发步骤", "排队时长": "0s", "当前状态": "排队中",
            })
            assert entry is not None
        order = queue_env.expected_order()
        concurrent = [p["任务编号"] for p in order if p["流程名称"] == "并发采集"]
        assert concurrent == ["TASK-C7", "TASK-C8", "TASK-C9"]

    def test_legacy_rows_get_queue_meta(self, queue_env):
        """兼容既有版本边界：v1 遗留数据自动补发 queue_seq 并标记 v1。"""
        rows = store.rows("task_queue")
        queue_env._ensure_queue_meta(rows[1])
        assert rows[1]["queue_seq"] >= 1
        assert rows[1]["queue_version"] == QUEUE_VERSION_LEGACY

    def test_v1_keeps_legacy_list_order_within_priority(self, queue_env):
        """v1 存量：同优先级沿用旧列表顺序（queue_seq），不被打断。"""
        order = _queued_ids(queue_env)
        # v1 只按优先级分组，同优先级保持 queue_seq（旧列表位置）顺序
        assert order == [2, 3, 4, 5]


class TestWaterLevelAndSnapshot:
    """排队水位重算与任务排队数据快照。"""

    def test_compute_water_level_reads_priority_and_submit_time(self, queue_env):
        """读取排队任务、优先级、提交时间，重算窗口内调度排队水位。"""
        store.queue_runtime.queue_version = QUEUE_VERSION_STABLE
        water = queue_env.compute_water_level(window_size=2)
        assert water["queued_total"] == 4
        assert water["taken_boundary"] == 0
        # 窗口内是稳定顺序的前 2 个任务
        window_numbers = [t["任务编号"] for t in water["window_tasks"]]
        assert window_numbers == ["TASK-T03", "TASK-T02"]
        assert water["window_start"] <= water["window_end"]

    def test_snapshot_freezes_positions_and_window(self, queue_env):
        """生成任务排队数据快照：同一时间窗口可执行的任务边界被固化。"""
        store.queue_runtime.queue_version = QUEUE_VERSION_STABLE
        snap = queue_env.build_queue_snapshot(window_size=3)
        assert snap["queue_version"] == QUEUE_VERSION_STABLE
        assert len(snap["positions"]) == 4
        assert snap["window"]["size"] == 3
        assert len(snap["window"]["task_ids"]) == 3
        # 快照是固化视图：后续消费不改变已生成快照的内容
        queue_env.schedule_next_batch(max_tasks=2)
        assert len(snap["positions"]) == 4

    def test_schedule_records_taken_boundary(self, queue_env):
        """按稳定位置消费当前窗口任务并记录已取走边界。"""
        store.queue_runtime.queue_version = QUEUE_VERSION_STABLE
        first = queue_env.schedule_next_batch(max_tasks=2)
        assert first["scheduled"] == 2
        boundary = first["taken_boundary"]
        assert boundary > 0
        # 再次消费从已取走边界之后继续，不重复取走
        second = queue_env.schedule_next_batch(max_tasks=2)
        assert second["scheduled"] == 2
        assert set(second["task_ids"]).isdisjoint(set(first["task_ids"]))
        # 队列耗尽后水位处理终态
        third = queue_env.schedule_next_batch(max_tasks=2)
        assert third["scheduled"] == 0


class TestUpgradeStrategies:
    """升级调度流程的两种存量处理选择。"""

    def test_continue_keeps_legacy_order_for_existing(self, queue_env):
        """continue：沿用旧快照顺序完成存量，新任务按 v2 排在存量之后。"""
        legacy_order = _queued_ids(queue_env)
        result = queue_env.upgrade_schedule(strategy="continue")
        assert result["ok"] is True
        assert result["queue_version"] == QUEUE_VERSION_STABLE
        assert result["reassigned"] == 0
        # 存量任务 queue_seq 不变：相对顺序与升级前一致
        after = [tid for tid in _queued_ids(queue_env) if tid in legacy_order]
        assert after == legacy_order
        # 升级后新任务按 v2 稳定规则排队，位于冻结存量之后
        entry, _ = queue_env.create_entry({
            "任务编号": "TASK-NEW", "流程名称": "新采集",
            "提交时间": "2026-09-01 07:00:00", "优先级": "高",
            "预计执行": "新步骤", "排队时长": "0s", "当前状态": "排队中",
        })
        assert entry["queue_version"] == QUEUE_VERSION_STABLE
        order = _queued_ids(queue_env)
        assert order[: len(legacy_order)] == legacy_order
        assert order[-1] == entry["id"]

    def test_rebuild_reorders_active_and_freezes_terminal(self, queue_env):
        """rebuild：按新排序规则重建全部队列，已完成任务位置冻结。"""
        terminal_before = {
            r["任务编号"]: (r["status"], r.get("queue_seq"))
            for r in store.rows("task_queue") if r["status"] in ("已完成", "执行失败")
        }
        result = queue_env.upgrade_schedule(strategy="rebuild")
        assert result["ok"] is True
        assert result["reassigned"] == 4  # 仅非终态任务重发位置
        assert result["terminal_frozen"] == 2
        # 重建后排队任务按 v2 稳定顺序
        assert _queued_ids(queue_env) == [3, 2, 5, 4]
        # 终态任务状态不变、保持不可重入
        for row in store.rows("task_queue"):
            if row["任务编号"] in terminal_before:
                assert row["status"] == terminal_before[row["任务编号"]][0]
                assert row["pending"] is False

    @pytest.mark.parametrize("strategy", ["continue", "rebuild"])
    def test_both_strategies_never_rerun_completed(self, queue_env, strategy):
        """两种处理选择都不重复执行已完成任务。"""
        queue_env.upgrade_schedule(strategy=strategy)
        completed = [r for r in store.rows("task_queue") if r["status"] == "已完成"]
        assert len(completed) == 1
        # 调度消费不会取到已完成任务
        taken: list[int] = []
        for _ in range(5):
            batch = queue_env.schedule_next_batch(max_tasks=2)
            taken.extend(batch["task_ids"])
            if batch["scheduled"] == 0:
                break
        assert completed[0]["id"] not in taken
        assert len(taken) == len(set(taken))  # 无重复取走

    def test_upgrade_unknown_strategy_rejected(self, queue_env):
        result = queue_env.upgrade_schedule(strategy="unknown")
        assert result["ok"] is False


class TestFirstSuccessWins:
    """并发执行只接受首个成功结果；已完成任务保持不可重入。"""

    def test_complete_marks_terminal(self, queue_env):
        result = queue_env.complete_task(2, result={"rows": 10})
        assert result["ok"] is True
        entry = store.find("task_queue", 2)
        assert entry["status"] == "已完成"
        assert entry["pending"] is False

    def test_terminal_task_not_reentrant(self, queue_env):
        """已完成任务再次完成：拒绝并判定冲突。"""
        first = queue_env.complete_task(2, result={"rows": 10})
        assert first["ok"] is True
        second = queue_env.complete_task(2, result={"rows": 999})
        assert second["ok"] is False
        assert second["conflict"] is True
        # 首个成功结果保留，不被后续覆盖
        entry = store.find("task_queue", 2)
        assert entry["result"] == {"rows": 10}

    def test_concurrent_complete_accepts_first_only(self, queue_env):
        """并发完成同一任务：只接受首个成功结果。"""
        results = list(ThreadPoolExecutor(max_workers=4).map(
            lambda i: queue_env.complete_task(3, result={"attempt": i}),
            range(4),
        ))
        ok_count = sum(1 for r in results if r["ok"])
        conflict_count = sum(1 for r in results if r.get("conflict"))
        assert ok_count == 1
        assert conflict_count == 3

    def test_concurrent_schedule_never_double_takes(self, queue_env):
        """并发消费当前窗口：同一任务不会被重复取走。"""
        batches = list(ThreadPoolExecutor(max_workers=4).map(
            lambda _: queue_env.schedule_next_batch(max_tasks=2),
            range(4),
        ))
        taken = [tid for b in batches for tid in b["task_ids"]]
        assert len(taken) == len(set(taken))
        # 4 个排队任务全部被且只被取走一次
        assert sorted(taken) == [2, 3, 4, 5]

    def test_conflict_changes_water_level_terminal_state(self, queue_env):
        """冲突会改变调度排队水位的处理终态。"""
        queue_env.complete_task(2)
        before = store.queue_runtime.water_level.terminal_state
        queue_env.complete_task(2)  # 重复完成 → 冲突
        after = store.queue_runtime.water_level.terminal_state
        assert before != WATER_LEVEL_CONFLICT
        assert after == WATER_LEVEL_CONFLICT
        conflicts = store.queue_runtime.water_level.conflicts
        assert any(c["type"] == "重复完成" and c["task_id"] == 2 for c in conflicts)

    def test_conflict_state_not_overwritten_by_exhaustion(self, queue_env):
        """冲突终态不被队列耗尽覆盖：冲突是更严重的处理终态。"""
        queue_env.complete_task(2)
        queue_env.complete_task(2)  # 冲突 → 有冲突
        # 取空队列触发「已耗尽」
        for _ in range(3):
            queue_env.schedule_next_batch(max_tasks=5)
        assert store.queue_runtime.water_level.terminal_state == WATER_LEVEL_CONFLICT


class TestFilterAndOrderView:
    """按优先级、提交时间和队列版本筛选，查看水位和预计执行顺序。"""

    def test_filter_by_priority(self, queue_env):
        items, total = queue_env.list_entries(filters={"优先级": "高"})
        # T01(已完成) + T02 + T03 均为高优先级
        assert total == 3
        assert all(i["优先级"] == "高" for i in items)

    def test_filter_by_submit_time(self, queue_env):
        items, total = queue_env.list_entries(filters={"提交时间": "08:05"})
        assert total == 1
        assert items[0]["任务编号"] == "TASK-T03"

    def test_filter_by_queue_version(self, queue_env):
        queue_env.upgrade_schedule(strategy="continue")
        items, total = queue_env.list_entries(filters={"queue_version": "v2"})
        assert total == len(QUEUE_ROWS)
        items_v1, total_v1 = queue_env.list_entries(filters={"queue_version": "v1"})
        assert total_v1 == 0

    def test_list_exposes_expected_position(self, queue_env):
        store.queue_runtime.queue_version = QUEUE_VERSION_STABLE
        items, _ = queue_env.list_entries()
        queued = [i for i in items if i["status"] == "排队中"]
        positions = [i["预计执行顺序"] for i in queued]
        assert positions == sorted(positions)
        assert all(p is not None for p in positions)


class TestSchedulerDispatchFlow:
    """调度计划展开处理项承载的到期采集任务清单，队列按快照消费。"""

    @pytest.fixture()
    def scheduler_env(self, queue_env):
        saved = store._tables.get("scheduler", [])
        store._tables["scheduler"] = [
            {"id": 1, "status": "已激活", "pending": True, "abnormal": False,
             "计划名称": "新闻资讯整点采集", "绑定流程": "新闻资讯抓取",
             "cron表达式": "0 * * * *", "执行窗口": "整点-整点后10分",
             "并发数": "2", "优先级": "中", "通知渠道": "Webhook"},
        ]
        yield SchedulerService()
        store._tables["scheduler"] = saved

    def test_expand_due_tasks_and_enqueue(self, queue_env, scheduler_env):
        """展开到期清单 → 入队（携带优先级/提交时间/稳定位置）。"""
        expanded = scheduler_env.expand_due_tasks()
        assert expanded["expanded"] == 2  # 并发数=2 → 2 个处理项
        queued = queue_env.consume_scheduled_to_queue()
        assert queued["generated"] == 2
        for tid in queued["task_ids"]:
            entry = store.find("task_queue", tid)
            assert entry["优先级"] == "中"
            assert entry["queue_version"] == QUEUE_VERSION_LEGACY
            assert entry["queue_seq"] >= 1
            assert entry["提交时间"]

    def test_dispatched_tasks_consumed_by_stable_position(self, queue_env, scheduler_env):
        """入队后按任务排队快照的稳定位置消费当前窗口。"""
        scheduler_env.expand_due_tasks()
        queue_env.consume_scheduled_to_queue()
        store.queue_runtime.queue_version = QUEUE_VERSION_STABLE
        snap = queue_env.build_queue_snapshot(window_size=10)
        window_ids = snap["window"]["task_ids"]
        batch = queue_env.schedule_next_batch(max_tasks=10)
        # 消费顺序与快照窗口内的稳定位置一致
        assert batch["task_ids"] == window_ids[: batch["scheduled"]]


class TestVerifyStableResume:
    """最终核对：升级调度流程后仍能按稳定顺序续做任务。"""

    @pytest.mark.parametrize("strategy", ["continue", "rebuild"])
    def test_verify_after_upgrade(self, queue_env, strategy):
        # 先消费一部分，制造已取走边界
        queue_env.schedule_next_batch(max_tasks=1)
        queue_env.upgrade_schedule(strategy=strategy)
        result = queue_env.verify_stable_resume()
        assert result["ok"] is True
        assert result["stable_sorted"] is True
        assert result["boundary_overlap"] == []
        assert result["terminal_reentrant"] == []
        assert result["queue_version"] == QUEUE_VERSION_STABLE

    def test_verify_detects_boundary_overlap(self, queue_env):
        """水位与排队任务重叠时核出不通过。"""
        queue_env.schedule_next_batch(max_tasks=1)
        # 人为制造冲突：把执行中任务放回排队但保留旧 queue_seq（水位已越过）
        row = store.find("task_queue", 2)
        row["status"] = "排队中"
        result = queue_env.verify_stable_resume()
        assert result["ok"] is False
        assert result["boundary_overlap"]
