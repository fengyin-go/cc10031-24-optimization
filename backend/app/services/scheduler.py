"""调度计划领域服务 — 管理定时采集计划，输出 domain_snapshot。

数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "采集计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划名称", "绑定流程", "cron表达式", "执行窗口", "并发数", "优先级", "通知渠道"]
    REQUIRED_FIELDS = ["计划名称", "绑定流程", "cron表达式"]
    STATUS_ORDER = ["草稿", "已激活", "已暂停", "已归档"]
    ACTION_RULES = {"激活计划": "草稿", "暂停计划": "已激活", "归档计划": "已暂停"}
    NEGATIVE_ACTIONS = []

    def schedule_crawl_jobs(self) -> dict[str, Any]:
        """模拟调度触发：将已激活的计划对应的采集任务加入队列。

        遍历已激活的调度计划，模拟根据 cron 表达式判断是否在当前窗口，
        将到期任务发布到跨模块通道。

        Returns:
            {"triggered": int, "plans": [...]}
        """
        rows = store.rows(self.MODULE)
        active = [
            r for r in rows if r.get("status") == "已激活"
        ]

        triggered = []
        for plan in active:
            msg = store.channel.publish(
                topic="rpa.crawl_scheduled",
                source=self.MODULE,
                payload={
                    "plan_id": plan.get("id"),
                    "plan_name": plan.get("计划名称"),
                    "bot_ref": plan.get("绑定流程"),
                    "cron": plan.get("cron表达式"),
                    "priority": plan.get("优先级"),
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            triggered.append({
                "plan_id": plan.get("id"),
                "plan_name": plan.get("计划名称"),
                "msg_id": msg.msg_id,
            })

        return {"triggered": len(triggered), "plans": triggered}

    def expand_due_tasks(self) -> dict[str, Any]:
        """接收并展开处理项承载的到期采集任务清单。

        对每个已激活的调度计划，按其并发数把当前执行窗口内到期的
        采集任务展开成处理项清单（每条处理项携带任务编号、流程、
        提交时间、优先级），发布到 channel("rpa.crawl_scheduled")，
        由任务队列按任务排队快照的稳定位置消费。

        Returns:
            {"expanded": int, "plans": [...]}
        """
        rows = store.rows(self.MODULE)
        active = [r for r in rows if r.get("status") == "已激活"]

        now = datetime.now(timezone.utc)
        submitted_at = now.strftime("%Y-%m-%d %H:%M:%S")
        expanded: list[dict[str, Any]] = []
        for plan in active:
            plan_id = int(plan.get("id", 0))
            # 并发数解析失败时按 1 个处理项展开
            try:
                concurrency = max(1, int(str(plan.get("并发数", "1")).strip()
                                         .replace("个", "").replace("路", "")))
            except ValueError:
                concurrency = 1
            due_tasks = []
            for seq in range(1, concurrency + 1):
                due_tasks.append({
                    "seq": seq,
                    "任务编号": f"TASK-S{plan_id}-{now.strftime('%H%M%S')}-{seq:03d}",
                    "流程名称": plan.get("绑定流程", ""),
                    "提交时间": submitted_at,
                    "优先级": plan.get("优先级") or "中",
                    "预计执行": plan.get("执行窗口", ""),
                })
            msg = store.channel.publish(
                topic="rpa.crawl_scheduled",
                source=self.MODULE,
                payload={
                    "plan_id": plan_id,
                    "plan_name": plan.get("计划名称"),
                    "bot_ref": plan.get("绑定流程"),
                    "cron": plan.get("cron表达式"),
                    "priority": plan.get("优先级"),
                    "window": plan.get("执行窗口"),
                    "due_tasks": due_tasks,
                    "triggered_at": now.isoformat(),
                },
            )
            expanded.append({
                "plan_id": plan_id,
                "plan_name": plan.get("计划名称"),
                "due_count": len(due_tasks),
                "msg_id": msg.msg_id,
            })

        return {"expanded": sum(p["due_count"] for p in expanded), "plans": expanded}

    def list_active_plans(self) -> list[dict[str, Any]]:
        """列出已激活的调度计划。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("计划名称"),
                "cron": r.get("cron表达式"), "bot": r.get("绑定流程"),
                "window": r.get("执行窗口"), "concurrency": r.get("并发数"),
                "priority": r.get("优先级"),
            }
            for r in rows if r.get("status") == "已激活"
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_plans": sum(1 for r in rows if r.get("status") == "已激活"),
            "paused_plans": sum(1 for r in rows if r.get("status") == "已暂停"),
            "archived_plans": sum(1 for r in rows if r.get("status") == "已归档"),
        }