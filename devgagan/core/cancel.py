from typing import Set
from asyncio import Lock

class CancelManager:
    """Simple per-user cancel flag manager."""
    def __init__(self) -> None:
        self._cancelled: Set[int] = set()
        self._lock = Lock()

    async def cancel(self, user_id: int) -> None:
        async with self._lock:
            self._cancelled.add(user_id)

    async def clear(self, user_id: int) -> None:
        async with self._lock:
            self._cancelled.discard(user_id)

    async def is_cancelled(self, user_id: int) -> bool:
        async with self._lock:
            return user_id in self._cancelled

# Singleton instance
cancel_manager = CancelManager()
