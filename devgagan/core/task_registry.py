import time
from typing import Dict, Any, List
from dataclasses import dataclass, field
import threading


@dataclass
class TaskEntry:
    user_id: int
    msg_id: int
    link: str
    stage: str  # preparing|downloading|uploading|finalizing
    session: str  # human-readable session label
    started_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    current: int = 0
    total: int = 0
    percent: float = 0.0


class TaskRegistry:
    def __init__(self):
        self._tasks: Dict[str, TaskEntry] = {}
        self._lock = threading.RLock()

    def _key(self, user_id: int, msg_id: int) -> str:
        return f"{user_id}:{msg_id}"

    def start(self, user_id: int, msg_id: int, link: str, stage: str, session: str):
        with self._lock:
            key = self._key(user_id, msg_id)
            self._tasks[key] = TaskEntry(
                user_id=user_id,
                msg_id=msg_id,
                link=link,
                stage=stage,
                session=session,
            )

    def update(self, user_id: int, msg_id: int, *, stage: str | None = None, current: int | None = None, total: int | None = None, session: str | None = None):
        with self._lock:
            key = self._key(user_id, msg_id)
            t = self._tasks.get(key)
            if not t:
                return
            if stage is not None:
                t.stage = stage
            if session is not None:
                t.session = session
            if current is not None:
                t.current = max(0, int(current))
            if total is not None:
                t.total = max(0, int(total))
            if t.total > 0:
                t.percent = round(100.0 * t.current / t.total, 2)
            t.updated_at = time.time()

    def finish(self, user_id: int, msg_id: int):
        with self._lock:
            key = self._key(user_id, msg_id)
            self._tasks.pop(key, None)

    def snapshot(self) -> List[TaskEntry]:
        with self._lock:
            return list(self._tasks.values())


# Global singleton
registry = TaskRegistry()
