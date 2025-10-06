import asyncio
import time
from typing import Dict, Any, List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._seq = 0
        # Persistence (best-effort)
        try:
            self._mongo = AsyncIOMotorClient(MONGO_DB)
            self._db = self._mongo["telegram_bot"]
            self._col = self._db["metrics"]
        except Exception:
            self._mongo = None
            self._db = None
            self._col = None

    async def start_task(self, kind: str, user_id: int, username: str, link: str) -> str:
        async with self._lock:
            self._seq += 1
            task_id = f"t{self._seq}-{int(time.time())}"
            doc = {
                "_id": task_id,
                "kind": kind,  # 'download' | 'upload'
                "user_id": user_id,
                "username": username,
                "link": link,
                "started_at": time.time(),
                "status": "running",
                "session_id": None,
            }
            self._tasks[task_id] = doc
            # Persist best-effort
            try:
                if self._col:
                    await self._col.insert_one(doc)
            except Exception:
                pass
            return task_id

    async def bind_session(self, task_id: str, session_id: Optional[str]) -> None:
        async with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["session_id"] = session_id
        try:
            if self._col and session_id:
                await self._col.update_one({"_id": task_id}, {"$set": {"session_id": session_id}})
        except Exception:
            pass

    async def finish_task(self, task_id: str, status: str = "done") -> None:
        async with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["status"] = status
                self._tasks[task_id]["finished_at"] = time.time()
        try:
            if self._col:
                await self._col.update_one({"_id": task_id}, {"$set": {"status": status, "finished_at": time.time()}})
        except Exception:
            pass

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            tasks = list(self._tasks.items())
        active = [t for tid, t in tasks if t.get("status") == "running"]
        downloads = [t for tid, t in tasks if t.get("kind") == "download" and t.get("status") == "running"]
        uploads = [t for tid, t in tasks if t.get("kind") == "upload" and t.get("status") == "running"]
        # Group by session
        per_session: Dict[str, List[Dict[str, Any]]] = {}
        for tid, t in tasks:
            if t.get("status") != "running":
                continue
            sid = t.get("session_id") or "-"
            per_session.setdefault(sid, []).append({"id": tid, **t})
        return {
            "totals": {"active": len(active), "downloads": len(downloads), "uploads": len(uploads)},
            "tasks": [{"id": tid, **t} for tid, t in tasks if t.get("status") == "running"],
            "per_session": per_session,
        }


metrics = MetricsRegistry()
