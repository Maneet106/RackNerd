import asyncio
import time
import os
from dataclasses import dataclass
from typing import Deque, Callable, Optional, Dict, List, Awaitable
from collections import deque


def _to_int(val: Optional[str], default: int) -> int:
    try:
        return int(val) if val is not None and str(val).strip() != "" else default
    except Exception:
        return default


FREE_DOWNLOAD_CONCURRENCY: int = _to_int(os.getenv("FREE_DOWNLOAD_CONCURRENCY"), 1)
UPDATE_INTERVAL_SECONDS = 7.0  # how often to push queue updates while waiting


@dataclass
class Waiter:
    user_id: int
    link: str
    created_at: float
    fut: asyncio.Future
    last_update_at: float = 0.0
    message_id: Optional[int] = None


class DownloadQueueManager:
    """Global FIFO queue limiting concurrent downloads for non-premium users.

    - Capacity is defined by FREE_DOWNLOAD_CONCURRENCY (env) for non-premium users.
    - Premium/owner/verified users bypass this queue.
    - Provides periodic queue position updates via a user-provided callback.
    """

    def __init__(self, capacity: int):
        if capacity < 1:
            capacity = 1
        self.capacity = capacity
        self._running: int = 0
        self._wait_q: Deque[Waiter] = deque()
        self._cv = asyncio.Condition()
        self._by_user: Dict[int, List[Waiter]] = {}

    @property
    def running(self) -> int:
        return self._running

    @property
    def waiting(self) -> int:
        return len(self._wait_q)

    @property
    def total(self) -> int:
        return self._running + len(self._wait_q)

    def _find_position(self, fut: asyncio.Future) -> int:
        for idx, w in enumerate(self._wait_q, start=1):
            if w.fut is fut:
                return idx
        return -1

    async def acquire(self, user_id: int, link: str, update_cb: Optional[Callable[[str, int, int, int], None]] = None, cancel_check: Optional[Callable[[], Awaitable[bool]]] = None) -> None:
        """Wait for a slot. While waiting, periodically invoke update_cb(link, position, running, total).
        Returns only when a slot is acquired. Caller must later call release().
        """
        async with self._cv:
            if self._running < self.capacity and not self._wait_q:
                self._running += 1
                return

            # enqueue waiter
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            waiter = Waiter(user_id=user_id, link=link, created_at=time.time(), fut=fut)
            self._wait_q.append(waiter)
            self._by_user.setdefault(user_id, []).append(waiter)

            # Periodic queue position updates are disabled to reduce bot edits.

            # Wait until promoted
            while True:
                # Fast path: cancelled while waiting
                if cancel_check is not None:
                    try:
                        if await cancel_check():
                            # Remove this waiter from queue and map
                            try:
                                if waiter in self._wait_q:
                                    self._wait_q.remove(waiter)
                            except Exception:
                                pass
                            try:
                                lst = self._by_user.get(user_id)
                                if lst and waiter in lst:
                                    lst.remove(waiter)
                                    if not lst:
                                        self._by_user.pop(user_id, None)
                            except Exception:
                                pass
                            raise asyncio.CancelledError()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
                # Promote if capacity allows and this waiter is at head
                if self._running < self.capacity and self._wait_q and self._wait_q[0].fut is fut:
                    self._wait_q.popleft()
                    self._running += 1
                    try:
                        lst = self._by_user.get(user_id)
                        if lst and waiter in lst:
                            lst.remove(waiter)
                            if not lst:
                                self._by_user.pop(user_id, None)
                    except Exception:
                        pass
                    try:
                        fut.set_result(True)
                    except Exception:
                        pass
                    # Debug: acquired slot
                    try:
                        print(f"[FREE-QUEUE] ACQUIRE user={user_id} running={self._running} waiting={len(self._wait_q)} capacity={self.capacity}")
                    except Exception:
                        pass
                    return
                # Otherwise wait for a state change
                await self._cv.wait()

    async def release(self) -> None:
        async with self._cv:
            if self._running > 0:
                self._running -= 1
            # Wake up waiters (head will check capacity)
            self._cv.notify_all()
            # Debug: release info
            try:
                print(f"[FREE-QUEUE] RELEASE running={self._running} waiting={len(self._wait_q)} capacity={self.capacity}")
            except Exception:
                pass

    async def cancel_user(self, user_id: int) -> int:
        """Remove any waiting tasks for the user. Returns number removed."""
        removed = 0
        async with self._cv:
            lst = self._by_user.pop(user_id, [])
            if lst:
                for w in list(lst):
                    try:
                        if w in self._wait_q:
                            self._wait_q.remove(w)
                            removed += 1
                        if not w.fut.done():
                            w.fut.set_exception(asyncio.CancelledError())
                    except Exception:
                        continue
            # Notify in case head changed
            self._cv.notify_all()
        return removed

    async def reset(self) -> None:
        """Hard reset the queue state: clear waiting and set running=0.
        Should be called at startup to guarantee clean state after a restart.
        """
        async with self._cv:
            # cancel and clear all waiters
            try:
                while self._wait_q:
                    w = self._wait_q.popleft()
                    try:
                        if not w.fut.done():
                            w.fut.set_exception(asyncio.CancelledError())
                    except Exception:
                        pass
            except Exception:
                pass
            self._by_user.clear()
            self._running = 0
            self._cv.notify_all()


# Global instance
_download_capacity = FREE_DOWNLOAD_CONCURRENCY if isinstance(FREE_DOWNLOAD_CONCURRENCY, int) else 1
download_queue = DownloadQueueManager(_download_capacity)
