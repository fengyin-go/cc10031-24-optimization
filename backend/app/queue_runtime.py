"""队列运行时基础设施：排队水位、任务排队数据快照与稳定队列位置。

解决的核心问题：任务队列只按优先级排序时，同优先级任务的顺序随列表
位置变化，中断恢复后无法判断下一项应从哪里继续。这里引入：

- queue_seq: 单调递增的队列序列号，与优先级、提交时间共同形成稳定队列位置
- QueueWaterLevel: 调度排队水位，记录已取走边界与当前窗口边界，
  冲突会改变水位的处理终态
- QueueSnapshot: 任务排队数据快照，解析同一时间窗口可执行的任务边界，
  版本升级后的恢复按快照稳定顺序续做，不重排已完成任务

队列版本：
- v1 (legacy): 既有版本，仅按优先级排序，同优先级顺序不稳定
- v2 (stable): 优先级 + 提交时间 + queue_seq 共同形成稳定队列位置
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# 队列版本
QUEUE_VERSION_LEGACY = "v1"
QUEUE_VERSION_STABLE = "v2"

# 终态：进入终态的任务不可重入，版本升级后的恢复不能重排已完成任务
TERMINAL_STATUSES = ("已完成", "已取消", "执行失败")

# 水位处理终态
WATER_LEVEL_OK = "正常"
WATER_LEVEL_CONFLICT = "有冲突"
WATER_LEVEL_EXHAUSTED = "已耗尽"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class QueueWaterLevel:
    """调度排队水位：记录已取走边界与当前窗口边界。

    - taken_key: 已取走边界（最后被调度取走任务的稳定排序键），
      恢复续做时只消费排序键大于它的任务，不重复执行已完成任务
    - taken_boundary: 最后取走任务的 queue_seq（展示用）
    - window_start / window_end: 同一时间窗口可执行的任务边界
      （窗口内任务的 queue_seq 区间）
    - terminal_state: 水位处理终态（正常 / 有冲突 / 已耗尽），
      消费冲突会改变该终态
    """

    queue_version: str = QUEUE_VERSION_LEGACY
    taken_key: tuple[Any, ...] | None = None
    taken_boundary: int = 0
    window_start: int = 0
    window_end: int = 0
    terminal_state: str = WATER_LEVEL_OK
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    updated_at: str = field(default_factory=utc_now_iso)

    def mark_conflict(self, detail: dict[str, Any]) -> None:
        """记录冲突并把水位处理终态改为「有冲突」。"""
        self.conflicts.append({**detail, "at": utc_now_iso()})
        self.terminal_state = WATER_LEVEL_CONFLICT
        self.updated_at = utc_now_iso()

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_version": self.queue_version,
            "taken_key": list(self.taken_key) if self.taken_key else None,
            "taken_boundary": self.taken_boundary,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "terminal_state": self.terminal_state,
            "conflicts": list(self.conflicts),
            "updated_at": self.updated_at,
        }


@dataclass
class QueueSnapshot:
    """任务排队数据快照：某一时刻队列稳定位置与窗口边界的固化视图。

    版本升级后的恢复以快照为准：按快照中的稳定位置续做任务，
    已完成任务保持在终态、不参与重排。
    """

    snapshot_id: int
    queue_version: str
    created_at: str
    positions: list[dict[str, Any]]
    water_level: dict[str, Any]
    window: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "queue_version": self.queue_version,
            "created_at": self.created_at,
            "positions": [dict(p) for p in self.positions],
            "water_level": dict(self.water_level),
            "window": dict(self.window),
        }


class QueueRuntime:
    """队列运行时：队列版本、单调序列号、水位与快照历史。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._snapshot_seq = 0
        self.queue_version = QUEUE_VERSION_LEGACY
        self.water_level = QueueWaterLevel()
        self.snapshots: list[QueueSnapshot] = []
        # 升级策略记录：continue=沿用旧快照完成存量, rebuild=按新规则重建
        self.upgrade_strategy: str | None = None
        self.upgraded_at: str | None = None

    # ---- 稳定队列位置 ----

    def next_seq(self) -> int:
        """分配单调递增的队列序列号（稳定队列位置的兜底排序键）。"""
        with self._lock:
            self._seq += 1
            return self._seq

    def align_seq(self, existing: int) -> None:
        """恢复/迁移时把序列号对齐到已有最大值之后，避免位置冲突。"""
        with self._lock:
            if existing > self._seq:
                self._seq = existing

    # ---- 任务排队数据快照 ----

    def build_snapshot(
        self,
        positions: list[dict[str, Any]],
        window: dict[str, Any],
    ) -> QueueSnapshot:
        """生成任务排队数据快照并固化当前水位。"""
        with self._lock:
            self._snapshot_seq += 1
            snap = QueueSnapshot(
                snapshot_id=self._snapshot_seq,
                queue_version=self.queue_version,
                created_at=utc_now_iso(),
                positions=[dict(p) for p in positions],
                water_level=self.water_level.to_dict(),
                window=dict(window),
            )
            self.snapshots.append(snap)
            return snap

    def latest_snapshot(self) -> QueueSnapshot | None:
        with self._lock:
            return self.snapshots[-1] if self.snapshots else None

    # ---- 水位 ----

    def record_taken_boundary(self, taken_key: tuple[Any, ...],
                              taken_seq: int, window_end: int) -> None:
        """记录已取走边界：调度从当前窗口取走任务后推进水位。

        已取走边界以稳定排序键为准（taken_key），续做时只消费排序键
        大于它的任务；taken_seq 仅用于展示。
        """
        with self._lock:
            wl = self.water_level
            if wl.taken_key is not None and taken_key < wl.taken_key:
                # 水位回退属于冲突：改变水位处理终态
                wl.mark_conflict({
                    "type": "水位回退",
                    "taken_seq": taken_seq,
                    "current_boundary": wl.taken_boundary,
                })
                return
            wl.taken_key = taken_key
            wl.taken_boundary = taken_seq
            wl.window_end = window_end
            wl.updated_at = utc_now_iso()

    def reset_water_level(self) -> None:
        """重建队列后重置水位：边界与窗口由下次消费重新推进。"""
        with self._lock:
            self.water_level.taken_key = None
            self.water_level.taken_boundary = 0
            self.water_level.window_start = 0
            self.water_level.window_end = 0
            self.water_level.updated_at = utc_now_iso()

    def set_window(self, start: int, end: int) -> None:
        with self._lock:
            self.water_level.window_start = start
            self.water_level.window_end = end
            self.water_level.updated_at = utc_now_iso()

    def mark_conflict(self, detail: dict[str, Any]) -> None:
        """消费冲突会改变调度排队水位的处理终态。"""
        with self._lock:
            self.water_level.mark_conflict(detail)

    def mark_exhausted(self) -> None:
        with self._lock:
            # 冲突是更严重的处理终态：队列耗尽不覆盖冲突状态
            if self.water_level.terminal_state != WATER_LEVEL_CONFLICT:
                self.water_level.terminal_state = WATER_LEVEL_EXHAUSTED
                self.water_level.updated_at = utc_now_iso()

    # ---- 版本升级 ----

    def upgrade(self, strategy: str) -> None:
        """升级调度流程到稳定排序版本，记录所选的存量处理策略。"""
        with self._lock:
            self.queue_version = QUEUE_VERSION_STABLE
            self.water_level.queue_version = QUEUE_VERSION_STABLE
            self.water_level.updated_at = utc_now_iso()
            self.upgrade_strategy = strategy
            self.upgraded_at = utc_now_iso()

    # ---- 导出 ----

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "queue_version": self.queue_version,
                "upgrade_strategy": self.upgrade_strategy,
                "upgraded_at": self.upgraded_at,
                "water_level": self.water_level.to_dict(),
                "snapshot_count": len(self.snapshots),
                "latest_snapshot_id": self.snapshots[-1].snapshot_id if self.snapshots else None,
            }
