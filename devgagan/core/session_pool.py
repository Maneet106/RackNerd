import asyncio
import time
import os
import glob
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from pyrogram import Client
from pyrogram.errors import AuthKeyUnregistered, SessionRevoked, UserDeactivated
from config import API_ID, API_HASH
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB


@dataclass
class SessionStats:
    session_id: str
    usage_count: int = 0
    last_used: float = field(default_factory=time.time)
    errors: int = 0
    flood_wait_time: int = 0
    is_active: bool = True
    device_model: str = "iPhone 16 Pro"
    client_started: bool = False
    last_start_error: str = ""

    def increment_usage(self):
        self.usage_count += 1
        self.last_used = time.time()

    def record_error(self, flood_wait_seconds: int = 0):
        self.errors += 1
        if flood_wait_seconds > 0:
            self.flood_wait_time = max(self.flood_wait_time, flood_wait_seconds)


def delete_session_files_from_disk(session_id: str) -> int:
    try:
        files_deleted = 0
        patterns = [
            f"temp_admin_{session_id}.session",
            f"temp_admin_admin_{session_id}.session",
            f"session_{session_id}.session",
            f"{session_id}.session",
            f"temp_admin_{session_id}.session-journal",
            f"temp_admin_admin_{session_id}.session-journal",
            f"session_{session_id}.session-journal",
            f"{session_id}.session-journal",
        ]
        for pattern in patterns:
            if os.path.exists(pattern):
                try:
                    os.remove(pattern)
                    files_deleted += 1
                    print(f"🗑️ SessionPool: Deleted session file: {pattern}")
                except Exception as e:
                    print(f"⚠️ SessionPool: Failed to delete {pattern}: {e}")
        try:
            for file_path in glob.glob(f"*{session_id}*"):
                if file_path.endswith('.session') or file_path.endswith('.session-journal'):
                    try:
                        os.remove(file_path)
                        files_deleted += 1
                        print(f"🗑️ SessionPool: Deleted session file: {file_path}")
                    except Exception as e:
                        print(f"⚠️ SessionPool: Failed to delete {file_path}: {e}")
        except Exception as e:
            print(f"⚠️ SessionPool: Error during glob search: {e}")
        return files_deleted
    except Exception as e:
        print(f"❌ SessionPool: Error deleting session files for {session_id}: {e}")
        return 0


class SessionPool:
    def __init__(self, mongo_uri: str = MONGO_DB, db_name: str = "telegram_bot", collection_name: str = "sessions"):
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client[db_name]
        self.collection = self.db[collection_name]

        self.sessions: Dict[str, Client] = {}
        self.session_stats: Dict[str, SessionStats] = {}
        self.session_locks: Dict[str, asyncio.Lock] = {}

        self.max_errors_before_cooldown = 5
        self.cooldown_period = 300
        self.cooldown_sessions: Dict[str, float] = {}

        self.init_lock = asyncio.Lock()
        # Controls how many simultaneous transfers a single session can handle
        self.session_concurrency = int(os.getenv("SESSION_CONCURRENCY", "3"))
        # Controls internal Pyrogram worker threads per session client
        self.session_workers = int(os.getenv("SESSION_WORKERS", "2"))
        self.session_permits: Dict[str, asyncio.Semaphore] = {}

        # Cache usernames per session to avoid repeated get_me() spam
        self._cached_usernames: Dict[str, str] = {}

        self._cv = asyncio.Condition()
        self._premium_waiters: List[asyncio.Future] = []
        self._free_waiters: List[asyncio.Future] = []

    async def initialize(self):
        async with self.init_lock:
            cursor = self.collection.find({"is_active": True})
            sessions_data = await cursor.to_list(length=100)
            for session_data in sessions_data:
                session_id = session_data["_id"]
                device_model = session_data.get("device_model", "iPhone 16 Pro")
                self.session_stats[session_id] = SessionStats(session_id=session_id, device_model=device_model)
                self.session_locks[session_id] = asyncio.Lock()
                self.session_permits[session_id] = asyncio.Semaphore(self.session_concurrency)
            print(f"Session pool initialized with {len(sessions_data)} sessions")

    async def add_session(self, session_id: str, session_string: str, device_model: str = "iPhone 16 Pro") -> bool:
        try:
            existing = await self.collection.find_one({"_id": session_id})
            if existing:
                await self.collection.update_one(
                    {"_id": session_id},
                    {"$set": {"session_string": session_string, "device_model": device_model, "is_active": True, "last_updated": time.time()}}
                )
            else:
                await self.collection.insert_one({
                    "_id": session_id,
                    "session_string": session_string,
                    "device_model": device_model,
                    "is_active": True,
                    "added_at": time.time(),
                    "last_updated": time.time()
                })
            self.session_stats[session_id] = SessionStats(session_id=session_id, device_model=device_model)
            if session_id not in self.session_locks:
                self.session_locks[session_id] = asyncio.Lock()
            if session_id not in self.session_permits:
                self.session_permits[session_id] = asyncio.Semaphore(self.session_concurrency)
            return True
        except Exception as e:
            print(f"Error adding session {session_id}: {e}")
            return False

    async def remove_session(self, session_id: str) -> bool:
        try:
            files_deleted = delete_session_files_from_disk(session_id)
            if files_deleted > 0:
                print(f"🗑️ SessionPool: Cleaned up {files_deleted} session file(s) for {session_id}")
            await self.collection.update_one({"_id": session_id}, {"$set": {"is_active": False}})
            if session_id in self.sessions:
                try:
                    await self.sessions[session_id].stop()
                except Exception:
                    pass
                del self.sessions[session_id]
            self.session_stats.pop(session_id, None)
            self.session_locks.pop(session_id, None)
            self.cooldown_sessions.pop(session_id, None)
            self.session_permits.pop(session_id, None)
            return True
        except Exception as e:
            print(f"Error removing session {session_id}: {e}")
            return False

    async def get_session(self) -> Tuple[Optional[Client], Optional[str]]:
        current_time = time.time()
        available_sessions = [
            s for s in self.session_stats.keys()
            if s not in self.cooldown_sessions or current_time - self.cooldown_sessions[s] > self.cooldown_period
        ]
        if not available_sessions:
            print("No available sessions in pool")
            return None, None
        await self._cleanup_disconnected_sessions()
        available_sessions.sort(key=lambda s: self.session_stats[s].last_used)
        for session_id in available_sessions:
            if self.session_locks[session_id].locked():
                continue
            try:
                async with self.session_locks[session_id]:
                    if session_id not in self.sessions:
                        session_data = await self.collection.find_one({"_id": session_id})
                        if not session_data or not session_data.get("is_active", False):
                            self.session_stats[session_id].last_start_error = "inactive or missing session document"
                            continue
                        sess_str = session_data.get("session_string")
                        if not sess_str:
                            self.session_stats[session_id].last_start_error = "missing session_string"
                            continue
                        try:
                            client = Client(
                                name=f"session_{session_id}",
                                api_id=API_ID,
                                api_hash=API_HASH,
                                session_string=sess_str,
                                device_model=self.session_stats[session_id].device_model,
                                in_memory=True,
                                no_updates=True,
                                max_concurrent_transmissions=self.session_concurrency,
                                sleep_threshold=60,
                                workers=self.session_workers
                            )
                            await client.start()
                            self.sessions[session_id] = client
                            self.session_stats[session_id].client_started = True
                            self.session_stats[session_id].last_start_error = ""
                            # Cache username once for logging
                            try:
                                me = await client.get_me()
                                self._cached_usernames[session_id] = (me.username or f"user_{me.id}")
                            except Exception:
                                self._cached_usernames[session_id] = "unknown"
                        except Exception as start_err:
                            self.session_stats[session_id].client_started = False
                            self.session_stats[session_id].last_start_error = str(start_err)
                            print(f"❌ Failed to start session client {session_id}: {start_err}")
                            continue
                    sem = self.session_permits.get(session_id)
                    if not sem:
                        sem = asyncio.Semaphore(self.session_concurrency)
                        self.session_permits[session_id] = sem
                    # Non-blocking attempt to acquire a permit for fairness
                    try:
                        await asyncio.wait_for(sem.acquire(), timeout=0.05)
                    except asyncio.TimeoutError:
                        # No permits available immediately
                        continue
                    self.session_stats[session_id].increment_usage()
                    username = self._cached_usernames.get(session_id, "unknown")
                    print(f"🔄 Using session {session_id} (@{username}) for operation")
                    return self.sessions[session_id], session_id
            except Exception as e:
                print(f"❌ Error acquiring lock for session {session_id}: {e}")
        print("❌ No available sessions could be acquired from the pool")
        return None, None

    async def request_session(self, is_premium: bool, timeout: float = 120.0) -> Tuple[Optional[Client], Optional[str]]:
        deadline = time.time() + max(0.0, timeout)
        waiter: asyncio.Future = asyncio.get_running_loop().create_future()
        async with self._cv:
            if is_premium:
                self._premium_waiters.append(waiter)
            else:
                self._free_waiters.append(waiter)
            self._cv.notify_all()
        try:
            while True:
                client, sid = await self._try_acquire_any()
                if client:
                    return client, sid
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None, None
                try:
                    await asyncio.wait_for(waiter, timeout=remaining)
                    # Reset waiter for potential subsequent wake-ups
                    if not waiter.done():
                        waiter.set_result(True)
                    waiter = asyncio.get_running_loop().create_future()
                    async with self._cv:
                        if is_premium:
                            self._premium_waiters.append(waiter)
                        else:
                            self._free_waiters.append(waiter)
                except asyncio.TimeoutError:
                    return None, None
        finally:
            async with self._cv:
                if waiter in self._premium_waiters:
                    self._premium_waiters.remove(waiter)
                if waiter in self._free_waiters:
                    self._free_waiters.remove(waiter)

    async def release_session(self, session_id: str, had_error: bool = False, flood_wait_seconds: int = 0):
        if session_id not in self.session_stats:
            print(f"⚠️ Attempted to release unknown session {session_id}")
            return
        username = self._cached_usernames.get(session_id, "unknown")
        if had_error:
            self.session_stats[session_id].record_error(flood_wait_seconds)
            print(f"⚠️ Session {session_id} (@{username}) released with error")
            if self.session_stats[session_id].errors >= self.max_errors_before_cooldown:
                self.cooldown_sessions[session_id] = time.time()
                print(f"❄️ Session {session_id} (@{username}) placed in cooldown for {self.cooldown_period} seconds")
                self.session_stats[session_id].errors = 0
        else:
            print(f"✅ Session {session_id} (@{username}) released successfully")
        if flood_wait_seconds > 10:
            self.cooldown_sessions[session_id] = time.time()
            cooldown_time = max(flood_wait_seconds * 1.5, self.cooldown_period)
            print(f"⏱️ Session {session_id} (@{username}) placed in cooldown for {cooldown_time} seconds due to flood wait")
        try:
            if session_id in self.session_permits:
                sem = self.session_permits[session_id]
                val_before = getattr(sem, "_value", None)
                in_use_before = (self.session_concurrency - val_before) if isinstance(val_before, int) else "unknown"
                print(f"🔁 Releasing session {session_id} permit | in_use(before)={in_use_before} / concurrency={self.session_concurrency}")
                sem.release()
                val_after = getattr(sem, "_value", None)
                in_use_after = (self.session_concurrency - val_after) if isinstance(val_after, int) else "unknown"
                print(f"✅ Released permit for {session_id} (@{username}) | in_use(after)={in_use_after} / concurrency={self.session_concurrency} | waiters premium={len(self._premium_waiters)} free={len(self._free_waiters)}")
        finally:
            await self._wake_waiters()

    async def _try_acquire_any(self) -> Tuple[Optional[Client], Optional[str]]:
        await self._cleanup_disconnected_sessions()
        current_time = time.time()
        candidates = [
            s for s in self.session_stats.keys()
            if s not in self.cooldown_sessions or current_time - self.cooldown_sessions[s] > self.cooldown_period
        ]
        candidates.sort(key=lambda s: self.session_stats[s].last_used)
        for session_id in candidates:
            if session_id not in self.sessions:
                try:
                    async with self.session_locks[session_id]:
                        if session_id not in self.sessions:
                            session_data = await self.collection.find_one({"_id": session_id})
                            if not session_data or not session_data.get("is_active", False):
                                continue
                            client = Client(
                                name=f"session_{session_id}",
                                api_id=API_ID,
                                api_hash=API_HASH,
                                session_string=session_data["session_string"],
                                device_model=self.session_stats[session_id].device_model,
                                in_memory=True,
                                no_updates=True,
                                max_concurrent_transmissions=self.session_concurrency,
                                sleep_threshold=60,
                                workers=self.session_workers
                            )
                            await client.start()
                            self.sessions[session_id] = client
                except Exception as e:
                    self.session_stats[session_id].client_started = False
                    self.session_stats[session_id].last_start_error = str(e)
                    print(f"❌ Error preparing session {session_id}: {e}")
                    continue

            # Try to acquire a permit
            sem = self.session_permits.get(session_id)
            if not sem:
                sem = asyncio.Semaphore(self.session_concurrency)
                self.session_permits[session_id] = sem
            # Non-blocking permit acquire to avoid blocking this scanner
            try:
                await asyncio.wait_for(sem.acquire(), timeout=0.05)
            except asyncio.TimeoutError:
                continue

            # Record usage and return
            self.session_stats[session_id].increment_usage()
            username = self._cached_usernames.get(session_id, "unknown")
            print(f"🔄 Using session {session_id} (@{username}) for operation")
            return self.sessions[session_id], session_id

        return None, None

    async def _wake_waiters(self):
        """Wake one waiter (premium first)."""
        async with self._cv:
            # Clean completed futures
            self._premium_waiters = [w for w in self._premium_waiters if not w.done()]
            self._free_waiters = [w for w in self._free_waiters if not w.done()]
            target = None
            if self._premium_waiters:
                target = self._premium_waiters.pop(0)
            elif self._free_waiters:
                target = self._free_waiters.pop(0)
            if target and not target.done():
                try:
                    target.set_result(True)
                except Exception:
                    pass
            # Also notify any condition waiters
            self._cv.notify_all()
    
    async def _cleanup_disconnected_sessions(self):
        """Clean up sessions that have been disconnected"""
        disconnected_sessions = []
        
        for session_id, client in list(self.sessions.items()):
            try:
                # Try to check if session is still connected
                if not client.is_connected:
                    disconnected_sessions.append(session_id)
                    continue
                    
                # Test connection with a simple API call
                await asyncio.wait_for(client.get_me(), timeout=5.0)
            except (OSError, ConnectionError, ConnectionResetError, asyncio.TimeoutError) as e:
                print(f"🔌 Session {session_id} disconnected: {e}")
                disconnected_sessions.append(session_id)
            except Exception as e:
                print(f"⚠️ Error checking session {session_id}: {e}")
                disconnected_sessions.append(session_id)
        
        # Remove disconnected sessions
        for session_id in disconnected_sessions:
            try:
                if session_id in self.sessions:
                    await self.sessions[session_id].stop()
                    del self.sessions[session_id]
                    print(f"🗑️ Removed disconnected session {session_id}")
            except Exception as e:
                print(f"⚠️ Error removing session {session_id}: {e}")
                # Force remove from dict even if stop() fails
                if session_id in self.sessions:
                    del self.sessions[session_id]
    
    async def get_all_sessions(self) -> List[Dict]:
        """Get information about all active sessions"""
        cursor = self.collection.find({"is_active": True}).sort("added_at", 1)
        sessions = await cursor.to_list(length=100)
        
        # Enhance with runtime stats
        for session in sessions:
            session_id = session["_id"]
            if session_id in self.session_stats:
                stats = self.session_stats[session_id]
                session["usage_count"] = stats.usage_count
                session["last_used"] = stats.last_used
                session["errors"] = stats.errors
                session["is_in_cooldown"] = session_id in self.cooldown_sessions
                if session["is_in_cooldown"]:
                    cooldown_remaining = self.cooldown_period - (time.time() - self.cooldown_sessions[session_id])
                    session["cooldown_remaining"] = max(0, cooldown_remaining)
        
        return sessions
    
    async def cleanup(self):
        """Stop all active sessions"""
        for session_id, client in list(self.sessions.items()):
            try:
                await client.stop()
            except Exception:
                pass
        
        self.sessions.clear()
        self.session_stats.clear()
        self.session_locks.clear()
        self.cooldown_sessions.clear()

    async def get_diagnostics(self) -> Dict[str, dict]:
        """Return a snapshot of session pool state for diagnostics."""
        diag: Dict[str, dict] = {}
        # Per-session permits info
        for sid, sem in self.session_permits.items():
            try:
                val = getattr(sem, "_value", None)
                in_use = (self.session_concurrency - val) if isinstance(val, int) else None
            except Exception:
                val, in_use = None, None
            stat = self.session_stats.get(sid)
            diag[sid] = {
                "in_use": in_use,
                "concurrency": self.session_concurrency,
                "usage_count": stat.usage_count if stat else None,
                "last_used": stat.last_used if stat else None,
                "errors": stat.errors if stat else None,
                "in_cooldown": sid in self.cooldown_sessions,
                "client_started": stat.client_started if stat else None,
                "last_start_error": stat.last_start_error if stat else None,
            }
        # Waiter counts
        diag["_waiters"] = {
            "premium": len(self._premium_waiters),
            "free": len(self._free_waiters),
        }
        return diag


# Create global instance
session_pool = SessionPool()