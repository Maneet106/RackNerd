"""
Comprehensive File and Memory Cleanup System
Handles cleanup for failed, cancelled, completed downloads and bot restarts
"""

import os
import time
import asyncio
import shutil
import glob
from pathlib import Path
from typing import List, Dict, Set, Optional
import psutil
import gc
from datetime import datetime, timedelta

class FileCleanupManager:
    """Manages cleanup of downloaded files, thumbnails, and temporary files"""
    
    def __init__(self):
        self.downloads_dir = os.path.join(os.getcwd(), "downloads")
        self.thumbnails_dir = os.path.join(os.getcwd(), "thumbnails") 
        self.temp_files: Dict[int, Set[str]] = {}  # user_id -> set of file paths
        self.active_downloads: Dict[int, str] = {}  # user_id -> current download path
        self.thumbnail_links: Dict[str, str] = {}  # thumbnail_path -> video_path
        self.video_thumbnails: Dict[str, Set[str]] = {}  # video_path -> set of thumbnail_paths
        self.cleanup_age_hours = 24  # Clean files older than 24 hours
        
        # Ensure directories exist
        os.makedirs(self.downloads_dir, exist_ok=True)
        os.makedirs(self.thumbnails_dir, exist_ok=True)
        
    def register_file(self, user_id: int, file_path: str, is_temp: bool = True):
        """Register a file for cleanup tracking"""
        if is_temp:
            if user_id not in self.temp_files:
                self.temp_files[user_id] = set()
            self.temp_files[user_id].add(file_path)
            print(f"📝 Registered temp file for cleanup: {file_path}")
        
    def register_active_download(self, user_id: int, file_path: str):
        """Register an active download"""
        self.active_downloads[user_id] = file_path
        print(f"📥 Registered active download: {file_path}")
        
    def unregister_active_download(self, user_id: int):
        """Unregister active download (completed/failed)"""
        if user_id in self.active_downloads:
            file_path = self.active_downloads.pop(user_id)
            print(f"✅ Unregistered active download: {file_path}")
            
    def link_thumbnail_to_video(self, thumbnail_path: str, video_path: str):
        """Link a thumbnail to its corresponding video file"""
        if thumbnail_path and video_path:
            self.thumbnail_links[thumbnail_path] = video_path
            
            if video_path not in self.video_thumbnails:
                self.video_thumbnails[video_path] = set()
            self.video_thumbnails[video_path].add(thumbnail_path)
            
            print(f"🔗 Linked thumbnail {os.path.basename(thumbnail_path)} to video {os.path.basename(video_path)}")
            
    def get_video_thumbnails(self, video_path: str) -> Set[str]:
        """Get all thumbnails linked to a video file"""
        return self.video_thumbnails.get(video_path, set())
            
    async def cleanup_user_files(self, user_id: int, reason: str = "completed"):
        """Clean up all files for a specific user"""
        cleaned_files = []
        
        # Clean temp files for this user
        if user_id in self.temp_files:
            for file_path in self.temp_files[user_id].copy():
                if await self._safe_remove_file(file_path):
                    cleaned_files.append(file_path)
                    self.temp_files[user_id].discard(file_path)
            
            # Remove empty set
            if not self.temp_files[user_id]:
                del self.temp_files[user_id]
        
        # Clean active download if exists
        if user_id in self.active_downloads:
            file_path = self.active_downloads[user_id]
            if await self._safe_remove_file(file_path):
                cleaned_files.append(file_path)
            del self.active_downloads[user_id]
            
        if cleaned_files:
            print(f"🧹 Cleaned {len(cleaned_files)} files for user {user_id} ({reason}): {[os.path.basename(f) for f in cleaned_files]}")
            
        return cleaned_files
        
    async def cleanup_cancelled_download(self, user_id: int):
        """Clean up files when download is cancelled"""
        return await self.cleanup_user_files(user_id, "cancelled")
        
    async def cleanup_failed_download(self, user_id: int, error: str = ""):
        """Clean up files when download fails"""
        print(f"🚫 Cleaning up failed download for user {user_id}: {error}")
        return await self.cleanup_user_files(user_id, "failed")
        
    async def cleanup_completed_download(self, user_id: int):
        """Clean up temporary files after successful upload (keep main file until upload complete)"""
        cleaned_files = []
        
        # Only clean temp files, keep main download until upload is complete
        if user_id in self.temp_files:
            temp_files_copy = self.temp_files[user_id].copy()
            for file_path in temp_files_copy:
                # Only clean thumbnail and other temp files, not the main download
                if "thumb_" in os.path.basename(file_path) or file_path.endswith('.tmp'):
                    if await self._safe_remove_file(file_path):
                        cleaned_files.append(file_path)
                        self.temp_files[user_id].discard(file_path)
        
        if cleaned_files:
            print(f"🧹 Cleaned {len(cleaned_files)} temp files for user {user_id} (completed): {[os.path.basename(f) for f in cleaned_files]}")
            
        return cleaned_files
        
    async def cleanup_after_upload(self, user_id: int, file_path: str):
        """Clean up main download file and its linked thumbnails after successful upload"""
        cleaned_files = []
        
        # Clean all thumbnails linked to this video first
        linked_thumbnails = self.get_video_thumbnails(file_path)
        for thumb_path in linked_thumbnails.copy():
            if await self._safe_remove_file(thumb_path):
                cleaned_files.append(thumb_path)
                print(f"🧹 Cleaned linked thumbnail: {os.path.basename(thumb_path)}")
                
            # Remove from tracking
            self.thumbnail_links.pop(thumb_path, None)
            
        # Clear video thumbnail links
        if file_path in self.video_thumbnails:
            del self.video_thumbnails[file_path]
        
        # Remove the main file
        if await self._safe_remove_file(file_path):
            cleaned_files.append(file_path)
            
        # Remove from active downloads
        if user_id in self.active_downloads and self.active_downloads[user_id] == file_path:
            del self.active_downloads[user_id]
            
        # Remove from temp files if present
        if user_id in self.temp_files:
            self.temp_files[user_id].discard(file_path)
            
        if cleaned_files:
            thumbnails_count = len([f for f in cleaned_files if "thumb_" in os.path.basename(f)])
            print(f"🧹 Cleaned main file + {thumbnails_count} thumbnails after upload for user {user_id}: {os.path.basename(file_path)}")
            
        return cleaned_files
        
    async def cleanup_old_files(self):
        """Clean up old files (older than cleanup_age_hours)"""
        cutoff_time = time.time() - (self.cleanup_age_hours * 3600)
        cleaned_files = []
        
        # Clean downloads directory
        for file_path in glob.glob(os.path.join(self.downloads_dir, "*")):
            try:
                if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
                    if await self._safe_remove_file(file_path):
                        cleaned_files.append(file_path)
            except Exception as e:
                print(f"Error checking file age {file_path}: {e}")
                
        # Clean thumbnails directory  
        for file_path in glob.glob(os.path.join(self.thumbnails_dir, "*")):
            try:
                if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
                    if await self._safe_remove_file(file_path):
                        cleaned_files.append(file_path)
            except Exception as e:
                print(f"Error checking thumbnail age {file_path}: {e}")
                
        if cleaned_files:
            print(f"🧹 Cleaned {len(cleaned_files)} old files (>{self.cleanup_age_hours}h): {[os.path.basename(f) for f in cleaned_files[:5]]}{'...' if len(cleaned_files) > 5 else ''}")
            
        return cleaned_files
        
    async def startup_cleanup(self):
        """Clean up stale files on bot startup"""
        print("🚀 Starting startup file cleanup...")
        
        # Clear all tracking (files from previous session)
        self.temp_files.clear()
        self.active_downloads.clear()
        self.thumbnail_links.clear()
        self.video_thumbnails.clear()
        
        # Clean old files
        cleaned_files = await self.cleanup_old_files()
        
        # Clean any obviously temporary files
        temp_patterns = [
            os.path.join(self.downloads_dir, "thumb_*.jpg"),
            os.path.join(self.downloads_dir, "*.tmp"),
            os.path.join(self.downloads_dir, "temp_*"),
            os.path.join(self.thumbnails_dir, "*.tmp"),
        ]
        
        startup_cleaned = []
        for pattern in temp_patterns:
            for file_path in glob.glob(pattern):
                if await self._safe_remove_file(file_path):
                    startup_cleaned.append(file_path)
                    
        total_cleaned = len(cleaned_files) + len(startup_cleaned)
        if total_cleaned > 0:
            print(f"✅ Startup cleanup complete: {total_cleaned} files removed")
        else:
            print("✅ Startup cleanup complete: No files to clean")
            
        return cleaned_files + startup_cleaned
        
    async def emergency_cleanup(self):
        """Emergency cleanup - remove all files in downloads and thumbnails"""
        print("🚨 Emergency cleanup initiated...")
        
        cleaned_files = []
        
        # Clean downloads directory
        try:
            if os.path.exists(self.downloads_dir):
                for file_path in glob.glob(os.path.join(self.downloads_dir, "*")):
                    if os.path.isfile(file_path):
                        if await self._safe_remove_file(file_path):
                            cleaned_files.append(file_path)
        except Exception as e:
            print(f"Error during emergency cleanup of downloads: {e}")
            
        # Clean thumbnails directory
        try:
            if os.path.exists(self.thumbnails_dir):
                for file_path in glob.glob(os.path.join(self.thumbnails_dir, "*")):
                    if os.path.isfile(file_path):
                        if await self._safe_remove_file(file_path):
                            cleaned_files.append(file_path)
        except Exception as e:
            print(f"Error during emergency cleanup of thumbnails: {e}")
            
        # Clear all tracking
        self.temp_files.clear()
        self.active_downloads.clear()
        self.thumbnail_links.clear()
        self.video_thumbnails.clear()
        
        print(f"🚨 Emergency cleanup complete: {len(cleaned_files)} files removed")
        return cleaned_files
        
    async def _safe_remove_file(self, file_path: str) -> bool:
        """Safely remove a file with error handling"""
        try:
            if file_path and os.path.exists(file_path):
                # Check if file is in use
                if self._is_file_in_use(file_path):
                    print(f"⚠️ File in use, skipping: {os.path.basename(file_path)}")
                    return False
                    
                await asyncio.to_thread(os.remove, file_path)
                return True
        except Exception as e:
            print(f"❌ Error removing file {file_path}: {e}")
            
        return False
        
    def _is_file_in_use(self, file_path: str) -> bool:
        """Check if file is currently in use by another process"""
        try:
            # Try to open file exclusively
            with open(file_path, 'r+b') as f:
                pass
            return False
        except (IOError, OSError):
            return True
            
    def get_cleanup_stats(self) -> Dict:
        """Get current cleanup statistics"""
        total_temp_files = sum(len(files) for files in self.temp_files.values())
        
        downloads_size = 0
        downloads_count = 0
        if os.path.exists(self.downloads_dir):
            for file_path in glob.glob(os.path.join(self.downloads_dir, "*")):
                if os.path.isfile(file_path):
                    downloads_count += 1
                    try:
                        downloads_size += os.path.getsize(file_path)
                    except:
                        pass
                        
        thumbnails_size = 0
        thumbnails_count = 0
        if os.path.exists(self.thumbnails_dir):
            for file_path in glob.glob(os.path.join(self.thumbnails_dir, "*")):
                if os.path.isfile(file_path):
                    thumbnails_count += 1
                    try:
                        thumbnails_size += os.path.getsize(file_path)
                    except:
                        pass
        
        return {
            "tracked_temp_files": total_temp_files,
            "active_downloads": len(self.active_downloads),
            "downloads_count": downloads_count,
            "downloads_size_mb": downloads_size / (1024 * 1024),
            "thumbnails_count": thumbnails_count,
            "thumbnails_size_mb": thumbnails_size / (1024 * 1024),
            "total_files": downloads_count + thumbnails_count,
            "total_size_mb": (downloads_size + thumbnails_size) / (1024 * 1024),
            "linked_thumbnails": len(self.thumbnail_links),
            "videos_with_thumbnails": len(self.video_thumbnails)
        }


class MemoryCleanupManager:
    """Manages memory cleanup and garbage collection"""
    
    def __init__(self):
        self.cleanup_interval = 300  # 5 minutes
        self.memory_threshold_mb = 500  # Clean when memory usage > 500MB
        
    async def cleanup_memory(self, force: bool = False):
        """Clean up memory and run garbage collection"""
        try:
            # Get current memory usage
            process = psutil.Process()
            memory_mb = process.memory_info().rss / (1024 * 1024)
            
            if force or memory_mb > self.memory_threshold_mb:
                print(f"🧠 Memory cleanup triggered (current: {memory_mb:.1f}MB)")
                
                # Force garbage collection
                collected = gc.collect()
                
                # Get memory after cleanup
                new_memory_mb = process.memory_info().rss / (1024 * 1024)
                freed_mb = memory_mb - new_memory_mb
                
                print(f"🧠 Memory cleanup complete: {collected} objects collected, {freed_mb:.1f}MB freed (now: {new_memory_mb:.1f}MB)")
                
                return {
                    "objects_collected": collected,
                    "memory_freed_mb": freed_mb,
                    "memory_before_mb": memory_mb,
                    "memory_after_mb": new_memory_mb
                }
            
        except Exception as e:
            print(f"❌ Error during memory cleanup: {e}")
            
        return None
        
    def get_memory_stats(self) -> Dict:
        """Get current memory statistics"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            
            return {
                "memory_mb": memory_info.rss / (1024 * 1024),
                "memory_percent": process.memory_percent(),
                "cpu_percent": process.cpu_percent(),
                "gc_counts": gc.get_count(),
                "gc_threshold": gc.get_threshold()
            }
        except Exception as e:
            print(f"Error getting memory stats: {e}")
            return {}


class ComprehensiveCleanupManager:
    """Combined file and memory cleanup manager"""
    
    def __init__(self):
        self.file_cleanup = FileCleanupManager()
        self.memory_cleanup = MemoryCleanupManager()
        self.auto_cleanup_task = None
        
    async def start_auto_cleanup(self):
        """Start automatic cleanup task"""
        if self.auto_cleanup_task is None:
            self.auto_cleanup_task = asyncio.create_task(self._auto_cleanup_loop())
            print("🔄 Auto cleanup task started")
            
    async def stop_auto_cleanup(self):
        """Stop automatic cleanup task"""
        if self.auto_cleanup_task:
            self.auto_cleanup_task.cancel()
            try:
                await self.auto_cleanup_task
            except asyncio.CancelledError:
                pass
            self.auto_cleanup_task = None
            print("🛑 Auto cleanup task stopped")
            
    async def _auto_cleanup_loop(self):
        """Automatic cleanup loop"""
        while True:
            try:
                await asyncio.sleep(1800)  # Run every 30 minutes
                
                print("🔄 Running automatic cleanup...")
                
                # Clean old files
                await self.file_cleanup.cleanup_old_files()
                
                # Clean memory if needed
                await self.memory_cleanup.cleanup_memory()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in auto cleanup loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
                
    async def cleanup_for_user(self, user_id: int, reason: str):
        """Clean up everything for a specific user"""
        file_results = await self.file_cleanup.cleanup_user_files(user_id, reason)
        memory_results = await self.memory_cleanup.cleanup_memory()
        
        return {
            "files_cleaned": len(file_results),
            "memory_results": memory_results
        }
        
    async def startup_cleanup(self):
        """Run comprehensive startup cleanup"""
        print("🚀 Starting comprehensive startup cleanup...")
        
        # File cleanup
        file_results = await self.file_cleanup.startup_cleanup()
        
        # Memory cleanup
        memory_results = await self.memory_cleanup.cleanup_memory(force=True)
        
        # Start auto cleanup
        await self.start_auto_cleanup()
        
        return {
            "files_cleaned": len(file_results),
            "memory_results": memory_results
        }
        
    def get_comprehensive_stats(self) -> Dict:
        """Get comprehensive cleanup statistics"""
        file_stats = self.file_cleanup.get_cleanup_stats()
        memory_stats = self.memory_cleanup.get_memory_stats()
        
        return {
            "file_stats": file_stats,
            "memory_stats": memory_stats,
            "timestamp": datetime.now().isoformat()
        }


# Global cleanup manager instance
cleanup_manager = ComprehensiveCleanupManager()
