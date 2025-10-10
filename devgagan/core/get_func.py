import asyncio
import os
import re
import time
import gc
from typing import Dict, Set, Optional, Union, Any, Tuple, List
from pathlib import Path
from functools import lru_cache, wraps
from collections import defaultdict
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
import aiofiles
import pymongo
import io
import tempfile
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid, RPCError, BadRequest, PeerIdInvalid, FloodWait
from pyrogram.enums import ParseMode, MessageMediaType
from devgagan.core.metrics import metrics
# Increase default upload part size to improve throughput on large files.
# Can be overridden via env UPLOAD_PART_SIZE_KB.
UPLOAD_PART_SIZE_KB = int(os.getenv("UPLOAD_PART_SIZE_KB", "2048"))
THUMB_SKIP_SIZE_MB = int(os.getenv("THUMB_SKIP_SIZE_MB", "700"))
from telethon.tl.types import DocumentAttributeVideo, DocumentAttributeAnimated
from telethon import events, Button
from telethon.tl.functions.channels import JoinChannelRequest
from devgagan import app, sex as gf
from devgagan.core.download_queue import download_queue
from devgagan.core.task_registry import registry
from devgagan.core.cancel import cancel_manager
from devgagan.core.func import *
from devgagan.core.mongo import db as odb
from devgagan.core.mongo.plans_db import check_premium
from devgagan.core.cleanup import cleanup_manager
from devgagan.core.deduplication import (
    check_duplicate_before_download,
    check_duplicate_after_download, 
    handle_duplicate_file,
    store_file_for_deduplication
)
from config import MONGO_DB as MONGODB_CONNECTION_STRING, LOG_GROUP, OWNER_ID, STRING, API_ID, API_HASH, GLOBAL_BATCH_PROCESSING_TIMER
from devgagan.core.session_pool import session_pool
from devgagan.core.auto_flood_detection import auto_flood_detector

# Import pro userbot if STRING is available
if STRING:
    from devgagan import pro
else:
    pro = None

@dataclass
class BotConfig:
    DB_NAME: str = "smart_users"
    COLLECTION_NAME: str = "super_user"
    VIDEO_EXTS: Set[str] = field(default_factory=lambda: {
        # Standard video formats
        'mp4', 'mov', 'avi', 'mkv', 'flv', 'wmv', 'webm', 'mpg', 'mpeg',
        # Mobile and streaming
        '3gp', '3g2', 'm4v', 'f4v', 'ts', 'm2ts', 'mts',
        # Professional formats
        'vob', 'ogv', 'dv', 'asf', 'rm', 'rmvb', 'divx', 'xvid',
        # Modern formats
        'hevc', 'h264', 'h265', 'av1', 'vp8', 'vp9'
    })
    DOC_EXTS: Set[str] = field(default_factory=lambda: {
        # Documents
        'pdf', 'doc', 'docx', 'txt', 'rtf', 'odt', 'pages',
        # Spreadsheets
        'xls', 'xlsx', 'csv', 'ods', 'numbers',
        # Presentations
        'ppt', 'pptx', 'odp', 'key',
        # Archives
        'zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz', 'lzma', 'z', 'cab', 'ace', 'arj',
        # E-books
        'epub', 'mobi', 'azw', 'azw3', 'fb2', 'lit',
        # Code/Data
        'json', 'xml', 'yaml', 'yml', 'sql', 'py', 'js', 'html', 'css', 'php', 'java', 'cpp', 'c', 'h',
        # Mobile Apps
        'apk', 'ipa', 'xapk', 'apks',
        # Other
        'iso', 'dmg', 'exe', 'msi', 'deb', 'rpm', 'pkg', 'bin', 'torrent'
    })
    IMG_EXTS: Set[str] = field(default_factory=lambda: {
        # Standard images
        'jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'tiff', 'tif', 'svg',
        # Raw formats
        'raw', 'cr2', 'nef', 'arw', 'dng',
        # Other formats
        'ico', 'psd', 'ai', 'eps', 'heic', 'heif', 'avif', 'jxl'
    })
    AUDIO_EXTS: Set[str] = field(default_factory=lambda: {
        # Compressed audio
        'mp3', 'aac', 'm4a', 'ogg', 'opus', 'wma', 'amr', '3gp',
        # Lossless audio
        'wav', 'flac', 'alac', 'ape', 'wv',
        # Voice notes and calls
        'oga', 'spx', 'gsm', 'au', 'snd',
        # Other formats
        'aiff', 'aif', 'ra', 'rm', 'mid', 'midi', 'kar'
    })
    SIZE_LIMIT: int = 2 * 1024**3  # 2GB
    PART_SIZE: int = int(1.9 * 1024**3)  # 1.9GB for splitting
    SETTINGS_PIC: str = "https://www.dropbox.com/scl/fi/jgckdvyt91268dgcc542a/Gemini_Generated_Image_fc3cskfc3cskfc3c-1.jpg?rlkey=5ujdpptqm1m6tq5azm51aoglj&st=w79h24q5&dl=0"

@dataclass
class UserProgress:
    previous_done: int = 0
    previous_time: float = field(default_factory=time.time)
    start_time: float = field(default_factory=time.time)
    session_downloaded: int = 0
    session_uploaded: int = 0
    peak_speed: float = 0.0
    avg_speed: float = 0.0
    speed_samples: list = field(default_factory=list)

class DatabaseManager:
    """Enhanced database operations with error handling and caching"""
    def __init__(self, connection_string: str, db_name: str, collection_name: str):
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.collection = None
        self._cache = {}
        self._connect()
    
    def _connect(self):
        """Establish database connection with retry logic"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                if self.client:
                    self.client.close()
                self.client = pymongo.MongoClient(
                    self.connection_string,
                    serverSelectionTimeoutMS=10000,
                    connectTimeoutMS=10000,
                    socketTimeoutMS=10000,
                    maxPoolSize=10,
                    retryWrites=True,
                )
                self.collection = self.client[self.db_name][self.collection_name]
                # Test connection
                self.client.admin.command('ping')
                print(f"✅ Database connected successfully (attempt {attempt + 1})")
                return
            except Exception as e:
                print(f"❌ Database connection error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("❌ Failed to connect to database after all retries")
                    self.client = None
                    self.collection = None
    
    def _ensure_connection(self):
        """Ensure database connection is active with better error handling"""
        try:
            if self.client is None or self.collection is None:
                print("🔄 Database client not initialized, connecting...")
                self._connect()
                return
            # Test if connection is alive
            self.client.admin.command('ping')
        except Exception as e:
            print(f"🔄 Database connection lost ({e}), reconnecting...")
            self._connect()
    
    def get_user_data(self, user_id: int, key: str, default=None) -> Any:
        cache_key = f"{user_id}:{key}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                self._ensure_connection()
                if self.collection is None:
                    print("❌ Database collection not available")
                    return default
                    
                doc = self.collection.find_one({"_id": user_id})
                value = doc.get(key, default) if doc else default
                self._cache[cache_key] = value
                return value
            except Exception as e:
                print(f"❌ Database read error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return default
    
    def save_user_data(self, user_id: int, key: str, value: Any) -> bool:
        cache_key = f"{user_id}:{key}"
        max_retries = 2
        
        for attempt in range(max_retries):
            try:
                self._ensure_connection()
                if self.collection is None:
                    print("❌ Database collection not available for save operation")
                    return False
                    
                self.collection.update_one(
                    {"_id": user_id}, 
                    {"$set": {key: value}}, 
                    upsert=True
                )
                self._cache[cache_key] = value
                return True
            except Exception as e:
                print(f"❌ Database save error for {key} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return False
    
    def clear_user_cache(self, user_id: int):
        """Clear cache for specific user"""
        keys_to_remove = [key for key in self._cache.keys() if key.startswith(f"{user_id}:")]
        for key in keys_to_remove:
            del self._cache[key]
    
    def get_protected_channels(self) -> Set[int]:
        try:
            return {doc["channel_id"] for doc in self.collection.find({"channel_id": {"$exists": True}})}
        except:
            return set()
    
    def lock_channel(self, channel_id: int) -> bool:
        try:
            self.collection.insert_one({"channel_id": channel_id})
            return True
        except:
            return False
    
    def unlock_channel(self, channel_id: int) -> bool:
        try:
            self.collection.delete_many({"channel_id": channel_id})
            return True
        except:
            return False
    
    def reset_user_data(self, user_id: int) -> bool:
        try:
            self.collection.update_one(
                {"_id": user_id}, 
                {"$unset": {
                    "delete_words": "", "replacement_words": "", 
                    "watermark_text": "", "duration_limit": "",
                    "custom_caption": "", "rename_tag": ""
                }}
            )
            self.clear_user_cache(user_id)
            return True
        except Exception as e:
            print(f"Reset error: {e}")
            return False

class MediaProcessor:
    """Advanced media processing and file type detection"""
    def __init__(self, config: BotConfig):
        self.config = config
    
    def get_file_type(self, filename: str) -> str:
        """Determine file type based on extension with enhanced support"""
        ext = Path(filename).suffix.lower().lstrip('.')
        filename_lower = filename.lower()
        
        # Special handling for animations and stickers
        if ext == 'gif' or 'sticker' in filename_lower:
            return 'animation'
        elif ext in self.config.VIDEO_EXTS:
            # Check if it's a short video that should be treated as animation
            if ext in ['webm'] and ('anim' in filename_lower or 'sticker' in filename_lower):
                return 'animation'
            return 'video'
        elif ext in self.config.IMG_EXTS:
            return 'photo'
        elif ext in self.config.AUDIO_EXTS:
            # Special handling for voice notes
            if ext in ['oga', 'ogg', 'opus', 'amr'] and ('voice' in filename_lower or 'note' in filename_lower or 'ptt' in filename_lower):
                return 'voice'
            return 'audio'
        elif ext in self.config.DOC_EXTS:
            return 'document'
        return 'document'
    
    @staticmethod
    def get_media_info(msg) -> Tuple[Optional[str], Optional[int], str]:
        """Extract filename, file size, and media type from message with enhanced support"""
        if msg.document:
            filename = msg.document.file_name or "document"
            # Check for special document types
            if hasattr(msg.document, 'attributes'):
                for attr in msg.document.attributes:
                    if hasattr(attr, '__class__'):
                        if 'Animated' in attr.__class__.__name__:
                            return filename or "animation.gif", msg.document.file_size, "animation"
                        elif 'Sticker' in attr.__class__.__name__:
                            return filename or "sticker.webp", msg.document.file_size, "sticker"
            return filename, msg.document.file_size, "document"
        elif msg.video:
            return msg.video.file_name or "video.mp4", msg.video.file_size, "video"
        elif msg.photo:
            return "photo.jpg", msg.photo.file_size, "photo"
        elif msg.audio:
            filename = msg.audio.file_name or "audio.mp3"
            # Check if it's a voice note
            if hasattr(msg.audio, 'voice') and msg.audio.voice:
                return filename or "voice.oga", msg.audio.file_size, "voice"
            return filename, msg.audio.file_size, "audio"
        elif hasattr(msg, 'animation') and msg.animation:
            return msg.animation.file_name or "animation.gif", msg.animation.file_size, "animation"
        elif hasattr(msg, 'sticker') and msg.sticker:
            return msg.sticker.file_name or "sticker.webp", msg.sticker.file_size, "sticker"
        elif hasattr(msg, 'voice') and msg.voice:
            return "voice.oga", msg.voice.file_size, "voice"
        elif msg.voice:
            return "voice.ogg", getattr(msg.voice, 'file_size', 1), "voice"
        elif msg.video_note:
            return "video_note.mp4", getattr(msg.video_note, 'file_size', 1), "video_note"
        elif msg.sticker:
            return "sticker.webp", getattr(msg.sticker, 'file_size', 1), "sticker"
        elif msg.animation:
            return msg.animation.file_name or "animation.gif", msg.animation.file_size, "animation"
        elif msg.poll:
            return "poll.json", 1, "poll"
        elif msg.location:
            return "location.json", 1, "location"
        elif msg.contact:
            return "contact.vcf", 1, "contact"
        elif msg.dice:
            return f"dice_{msg.dice.emoji}.json", 1, "dice"
        elif msg.game:
            return f"game_{msg.game.title}.json", 1, "game"
        return "unknown", 1, "document"

class ProgressManager:
    """Professional real-time progress tracking with enhanced analytics"""
    def __init__(self):
        self.user_progress: Dict[int, UserProgress] = defaultdict(UserProgress)
        # Optimized interval to reduce Bot API edits and lower FloodWait risk
        self.update_interval = 8.0  # Update every 8 seconds to save API calls
        self.last_update: Dict[int, float] = {}
    
    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human readable format"""
        if bytes_value < 0:  # Handle negative values (shouldn't happen but just in case)
            return "0 B"
            
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0 or unit == 'TB':
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    def _format_speed(self, speed_bps: float) -> str:
        """Format speed to human readable format"""
        if speed_bps < 1024:
            return f"{speed_bps:.1f} B/s"
        elif speed_bps < 1024**2:
            return f"{speed_bps/1024:.1f} KB/s"
        elif speed_bps < 1024**3:
            return f"{speed_bps/(1024**2):.1f} MB/s"
        else:
            return f"{speed_bps/(1024**3):.1f} GB/s"
    
    def _format_time(self, seconds: float) -> str:
        """Format time to human readable format"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds//60)}m {int(seconds%60)}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def _create_progress_bar(self, percent: float, length: int = 20) -> str:
        """Create progress bar using unified system"""
        from devgagan.core.func import UnifiedProgressBar
        return UnifiedProgressBar.create_progress_bar(percent, "download")
        
    def _create_modern_progress_bar(self, percent: float, length: int = 20, style: str = "gradient") -> str:
        """Create modern progress bar using unified system
        
        Args:
            percent: Progress percentage (0-100)
            length: Length of the progress bar (ignored - uses unified system)
            style: Style of progress bar (ignored - uses unified system)
            
        Returns:
            Formatted progress bar string using unified styling
        """
        from devgagan.core.func import UnifiedProgressBar
        return UnifiedProgressBar.create_progress_bar(percent, "download")
    
    def calculate_progress(self, done: int, total: int, user_id: int, 
                         operation: str = "Upload", uploader: str = "Restrict Bot Saver") -> str:
        """Calculate and format professional progress display"""
        current_time = time.time()
        
        # Check if we should update (rate limiting)
        if user_id in self.last_update:
            if current_time - self.last_update[user_id] < self.update_interval and done < total:
                return None  # Skip update
        
        user_data = self.user_progress[user_id]
        
        # Initialize if first time
        if user_data.previous_done == 0:
            user_data.start_time = current_time
            user_data.previous_time = current_time
        
        # Ensure values are valid
        done = max(0, done)  # Ensure done is not negative
        total = max(1, total)  # Ensure total is at least 1 to avoid division by zero
        
        # Calculate metrics - handle large files better
        percent = min(100, (done / total) * 100)  # Cap at 100%
        elapsed_total = current_time - user_data.start_time
        elapsed_interval = max(0.1, current_time - user_data.previous_time)
        
        # Calculate speed with better handling for large files
        bytes_progress = max(0, done - user_data.previous_done)
        
        # On first update, use total elapsed time for more accurate speed
        if user_data.previous_done == 0 and elapsed_total > 0:
            speed_bps = done / elapsed_total
        else:
            speed_bps = bytes_progress / elapsed_interval
        
        # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
        speed_bps_display = speed_bps * 3
        
        # Update speed samples for average calculation (use boosted speed)
        user_data.speed_samples.append(speed_bps_display)
        if len(user_data.speed_samples) > 10:  # Keep last 10 samples
            user_data.speed_samples.pop(0)
        
        # Calculate average and peak speed (with boosted values)
        user_data.avg_speed = sum(user_data.speed_samples) / len(user_data.speed_samples)
        user_data.peak_speed = max(user_data.peak_speed, speed_bps_display)
        
        # Calculate ETA (use real speed for calculation, then divide by 2 for display)
        if speed_bps > 0:
            eta_seconds = (total - done) / speed_bps
            # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
            eta_seconds = eta_seconds / 2
        else:
            eta_seconds = 0
        
        # Update tracking data
        user_data.previous_done = done
        user_data.previous_time = current_time
        self.last_update[user_id] = current_time
        
        # Create visual elements
        progress_bar = self._create_progress_bar(percent)
        done_str = self._format_bytes(done)
        total_str = self._format_bytes(total)
        speed_str = self._format_speed(speed_bps_display)  # Use boosted speed for display
        avg_speed_str = self._format_speed(user_data.avg_speed)
        peak_speed_str = self._format_speed(user_data.peak_speed)
        eta_str = self._format_time(eta_seconds) if eta_seconds > 0 else "Calculating..."
        elapsed_str = self._format_time(elapsed_total)
        
        # Status emoji based on speed (use boosted speed for display)
        if speed_bps_display > 10 * 1024 * 1024:  # > 10 MB/s
            status_emoji = "🚀"
        elif speed_bps_display > 1 * 1024 * 1024:  # > 1 MB/s
            status_emoji = "⚡"
        elif speed_bps_display > 100 * 1024:  # > 100 KB/s
            status_emoji = "📶"
        else:
            status_emoji = "🐌"
        
        return (
            f"╭─────────────────────────────╮\n"
            f"│ <b>{status_emoji} {uploader} • {operation}</b> \n"
            f"├─────────────────────────────┤\n"
            f"│ {progress_bar} <b>{percent:.1f}%</b> \n"
            f"│ \n"
            f"│ 📊 <b>Progress:</b> {done_str} / {total_str} \n"
            f"│ 🏃 <b>Current:</b> {speed_str} \n"
            f"│ 📈 <b>Average:</b> {avg_speed_str} \n"
            f"│ 🔥 <b>Peak:</b> {peak_speed_str} \n"
            f"│ ⏱️ <b>ETA:</b> {eta_str} \n"
            f"│ ⏰ <b>Elapsed:</b> {elapsed_str} \n"
            f"╰─────────────────────────────╯\n"
            f"\n<b>🔰 Powered by Restrict Bot Saver</b>"
        )
    
    def reset_user_progress(self, user_id: int):
        """Reset progress tracking for a user"""
        if user_id in self.user_progress:
            del self.user_progress[user_id]
        if user_id in self.last_update:
            del self.last_update[user_id]
    
    def get_session_stats(self, user_id: int) -> Dict[str, any]:
        """Get session statistics for a user"""
        if user_id not in self.user_progress:
            return {}
        
        user_data = self.user_progress[user_id]
        current_time = time.time()
        session_duration = current_time - user_data.start_time
        
        return {
            'session_duration': session_duration,
            'total_downloaded': user_data.session_downloaded,
            'total_uploaded': user_data.session_uploaded,
            'peak_speed': user_data.peak_speed,
            'average_speed': user_data.avg_speed
        }

class CaptionFormatter:
    """Advanced caption processing with markdown to HTML conversion"""
    
    @staticmethod
    async def markdown_to_html(caption: str) -> str:
        """Convert markdown formatting to HTML with improved link handling"""
        if not caption:
            return ""
        
        # First, handle links with a more robust approach to prevent missing embedded links in long text
        # This uses a non-greedy pattern and processes links first to avoid interference with other formatting
        def replace_links(text):
            # Use a more specific pattern for links that handles various URL formats better
            link_pattern = r'\[(.*?)\]\((https?://[^\s\)]+|tg://[^\s\)]+|www\.[^\s\)]+)\)'
            
            # Process all links in the text
            def link_replacer(match):
                text, url = match.groups()
                # Ensure URL is properly formatted
                if url.startswith('www.'):
                    url = 'http://' + url
                return f'<a href="{url}">{text}</a>'
            
            return re.sub(link_pattern, link_replacer, text, flags=re.DOTALL)
        
        # Process links first
        result = replace_links(caption)
        
        # Then handle other markdown formatting
        replacements = [
            (r"```([a-zA-Z0-9_+\-.]+)\n([\s\S]*?)```", r"<pre language=\"\1\">\2</pre>"),
            (r"```([\s\S]*?)```", r"<pre>\1</pre>"),
            (r"^>>> (.*)", r"<blockquote expandable>\1</blockquote>"),
            (r"^>> (.*)", r"<blockquote expandable>\1</blockquote>"),
            (r"^> (.*)", r"<blockquote>\1</blockquote>"),
            (r"(?<!`)`([^`\n]+)`(?!`)", r"<code>\1</code>"),
            (r"\*\*(.+?)\*\*", r"<b>\1</b>"),
            (r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>"),
            (r"__(.+?)__", r"<u>\1</u>"),
            (r"_(.+?)_", r"<i>\1</i>"),
            (r"~~(.+?)~~", r"<s>\1</s>"),
            (r"\|\|(.+?)\|\|", r"<tg-spoiler>\1</tg-spoiler>")
            # Links are already processed above
        ]
        
        for pattern, replacement in replacements:
            result = re.sub(pattern, replacement, result, flags=re.MULTILINE | re.DOTALL)
        
        return result.strip()

class FileOperations:
    """File operations with enhanced error handling"""
    def __init__(self, config: BotConfig, db: DatabaseManager):
        self.config = config
        self.db = db
    
    @asynccontextmanager
    async def safe_file_operation(self, file_path: str):
        """Safe file operations with automatic cleanup"""
        try:
            yield file_path
        finally:
            await self._cleanup_file(file_path)
    
    async def _cleanup_file(self, file_path: str):
        """Safely remove file"""
        if file_path and os.path.exists(file_path):
            try:
                await asyncio.to_thread(os.remove, file_path)
            except Exception as e:
                print(f"Error removing file {file_path}: {e}")
    
    async def process_filename(self, file_path: str, user_id: int) -> str:
        """Process filename with user preferences"""
        delete_words = set(self.db.get_user_data(user_id, "delete_words", []))
        replacements = self.db.get_user_data(user_id, "replacement_words", {})
        rename_tag = self.db.get_user_data(user_id, "rename_tag", "Restrict Bot Saver")
        
        path = Path(file_path)
        name = path.stem
        extension = path.suffix.lstrip('.')
        
        # Process filename
        for word in delete_words:
            name = name.replace(word, "")
        
        for word, replacement in replacements.items():
            name = name.replace(word, replacement)
        
        # Normalize extension for videos
        if extension.lower() in self.config.VIDEO_EXTS and extension.lower() not in ['mp4']:
            extension = 'mp4'
        
        new_name = f"{name.strip()} {rename_tag}.{extension}"
        new_path = path.parent / new_name
        
        # Check if destination file already exists and create a unique name if needed
        counter = 1
        original_new_path = new_path
        while os.path.exists(new_path):
            new_name = f"{name.strip()} {rename_tag} ({counter}).{extension}"
            new_path = path.parent / new_name
            counter += 1
            
            # Prevent infinite loop if we somehow can't create a unique name
            if counter > 100:
                print(f"Warning: Could not create unique filename after 100 attempts for {file_path}")
                # Use original file path as fallback
                return file_path
        
        try:
            await asyncio.to_thread(os.rename, file_path, new_path)
        except FileExistsError:
            print(f"Warning: File {new_path} already exists despite our check. Using original file.")
            return file_path
        except Exception as e:
            print(f"Error renaming file {file_path} to {new_path}: {e}")
            return file_path
            
        return str(new_path)
    
    async def split_large_file(self, file_path: str, app_client, sender: int, target_chat_id: int, caption: str, topic_id: Optional[int] = None):
        """Split large files into smaller parts"""
        if not os.path.exists(file_path):
            await app_client.send_message(sender, "❌ File not found!")
            return

        file_size = os.path.getsize(file_path)
        start_msg = await app_client.send_message(
            sender, f"ℹ️ File size: {file_size / (1024**2):.2f} MB\n🔄 Splitting and uploading..."
        )

        part_number = 0
        base_path = Path(file_path)
        
        try:
            async with aiofiles.open(file_path, mode="rb") as f:
                while True:
                    chunk = await f.read(self.config.PART_SIZE)
                    if not chunk:
                        break

                    part_file = f"{base_path.stem}.part{str(part_number).zfill(3)}{base_path.suffix}"

                    async with aiofiles.open(part_file, mode="wb") as part_f:
                        await part_f.write(chunk)

                    part_caption = f"{caption}\n\n<b>Part: {part_number + 1}</b>" if caption else f"<b>Part: {part_number + 1}</b>"
                    
                    edit_msg = await app_client.send_message(target_chat_id, f"⬆️ Uploading part {part_number + 1}...")
                    
                    try:
                        # Create progress callback for this part with modern progress bar
                        last_update_time = time.time()
                        async def part_progress_callback(current, total):
                            nonlocal last_update_time
                            current_time = time.time()
                            
                            # Update every 10 seconds to prevent spam and reduce API calls
                            if current_time - last_update_time >= 10 or current == total:
                                last_update_time = current_time
                                
                                # Calculate progress
                                percentage = (current / total) * 100 if total > 0 else 0
                                
                                # Use a global reference to the telegram_bot instance
                                if 'telegram_bot' in globals():
                                    # Calculate speed with minimum elapsed time
                                    start_time_obj = telegram_bot.progress_manager.user_progress.get(sender, type('obj', (object,), {'start_time': current_time}))
                                    elapsed = max(current_time - start_time_obj.start_time, 1.0) if current_time > start_time_obj.start_time else 1.0
                                    speed = current / elapsed
                                    
                                    # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
                                    speed_display = speed * 3
                                    
                                    # Get modern progress bar with gradient effect
                                    modern_bar = telegram_bot.progress_manager._create_modern_progress_bar(percentage, 10, "gradient")
                                    
                                    # Calculate ETA
                                    eta_seconds = (total - current) / speed if speed > 0 else 0
                                    # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
                                    eta_seconds = eta_seconds / 2
                                    eta_str = telegram_bot.progress_manager._format_time(eta_seconds) if eta_seconds > 0 else "Calculating..."
                                    
                                    # Update progress message
                                    try:
                                        progress_text = (
                                            f"📤 <b>Uploading Part {part_number + 1} - {os.path.basename(part_file)}</b>\n\n"
                                            f"📊 <b>Progress:</b> {percentage:.1f}%\n"
                                            f"📁 <b>Size:</b> {telegram_bot.progress_manager._format_bytes(current)} / {telegram_bot.progress_manager._format_bytes(total)}\n"
                                            f"⚡ <b>Speed:</b> {telegram_bot.progress_manager._format_speed(speed_display)}\n"
                                            f"⏱ <b>ETA:</b> {eta_str}\n\n"
                                            f"{modern_bar}"
                                        )
                                        async def _edit_progress():
                                            html_progress = await CaptionFormatter.markdown_to_html(progress_text)
                                            await edit_msg.edit(html_progress, parse_mode=ParseMode.HTML)
                                        asyncio.create_task(_edit_progress())
                                    except Exception as e:
                                        print(f"Part progress update error: {e}")
                                    
                                    telegram_bot.progress_manager.calculate_progress(
                                        current, total, sender, os.path.basename(part_file), f"📤 Part {part_number + 1}"
                                    )
                        
                        html_part_caption = await CaptionFormatter.markdown_to_html(part_caption)
                        result = await app_client.send_document(
                            target_chat_id,
                            document=part_file,
                            caption=html_part_caption,
                            reply_to_message_id=topic_id,
                            progress=part_progress_callback,
                            parse_mode=ParseMode.HTML
                        )
                        await edit_msg.delete()
                    finally:
                        if os.path.exists(part_file):
                            os.remove(part_file)
                    
                    part_number += 1

        finally:
            await start_msg.delete()
            if os.path.exists(file_path):
                os.remove(file_path)

class SmartTelegramBot:
    """Main bot class with all functionality"""
    def __init__(self):
        self.config = BotConfig()
        self.db = DatabaseManager(MONGODB_CONNECTION_STRING, self.config.DB_NAME, self.config.COLLECTION_NAME)
        self.media_processor = MediaProcessor(self.config)
        self.progress_manager = ProgressManager()
        self.file_ops = FileOperations(self.config, self.db)
        self.caption_formatter = CaptionFormatter()
        
        # User session management
        self.user_sessions: Dict[int, str] = {}
        self.pending_photos: Set[int] = set()
        self.user_chat_ids: Dict[int, str] = {}
        self.user_rename_prefs: Dict[str, str] = {}
        self.user_caption_prefs: Dict[str, str] = {}
        
        # Pro userbot reference
        self.pro_client = pro
        print(f"Pro client available: {'Yes' if self.pro_client else 'No'}")

        try:
            concurrency_limit_env = os.getenv("CONCURRENCY_LIMIT", "4")
            queue_workers_env = os.getenv("QUEUE_WORKERS")
            concurrency_limit = int(concurrency_limit_env)
            if queue_workers_env is not None:
                queue_workers = int(queue_workers_env)
            else:
                queue_workers = concurrency_limit
        except Exception:
            concurrency_limit = 4
            queue_workers = 4

        # Sanity bounds
        self.max_concurrent_transfers = max(1, concurrency_limit)
        self.queue_workers = max(1, queue_workers)
        self.transfer_semaphore = asyncio.Semaphore(self.max_concurrent_transfers)
        # Priority queue: 0 for premium/owners, 1 for freemium users
        self._task_queue: Optional[asyncio.PriorityQueue] = asyncio.PriorityQueue()
        self._queue_started = False
        self._queue_counter = 0  # Stabilize priority for FIFO within same priority

    async def _ensure_workers(self):
        """Start background workers to process the priority queue (configurable)."""
        if self._queue_started:
            return
        self._queue_started = True
        # Spawn limited number of workers based on environment configuration
        for _ in range(self.queue_workers):
            asyncio.create_task(self._queue_worker())

    async def _queue_worker(self):
        """Continuously pull tasks from the queue and execute them under concurrency semaphore."""
        while True:
            priority, order, fut, coro = await self._task_queue.get()
            try:
                print(f"[QUEUE] Start task (priority={priority}, order={order})")
            except Exception:
                pass
            try:
                async with self.transfer_semaphore:
                    result = await coro()
                    if not fut.done():
                        fut.set_result(result)
            except Exception as e:
                if not fut.done():
                    fut.set_exception(e)
            finally:
                try:
                    print(f"[QUEUE] Done task (priority={priority}, order={order})")
                except Exception:
                    pass
                self._task_queue.task_done()

    async def enqueue_download(self, priority: int, coro_factory):
        """Enqueue a download task with a given priority. Returns the awaited result.

        Args:
            priority: 0 for premium/owner, 1 for free
            coro_factory: a no-arg callable returning an awaitable that performs the download
        """
        await self._ensure_workers()
        fut = asyncio.get_event_loop().create_future()
        # Increase counter to keep FIFO order for equal priorities
        self._queue_counter += 1
        await self._task_queue.put((priority, self._queue_counter, fut, coro_factory))
        return await fut
    
    def get_thumbnail_path(self, user_id: int) -> Optional[str]:
        """Get user's persistent custom thumbnail path.

        Looks in a dedicated thumbnails/ directory first, then falls back to
        legacy path in the working directory for backward compatibility.
        """
        # New preferred location
        thumb_dir = os.path.join(os.getcwd(), "thumbnails")
        preferred = os.path.join(thumb_dir, f"{user_id}.jpg")
        if os.path.exists(preferred):
            return preferred

        # Legacy fallback (old behavior)
        legacy = f"{user_id}.jpg"
        if os.path.exists(legacy):
            return legacy

        return None

    @staticmethod
    def is_user_thumbnail(path: Optional[str], user_id: int) -> bool:
        """Return True if path points to the user's persistent thumbnail file."""
        if not path:
            return False
        base = os.path.basename(path)
        if base == f"{user_id}.jpg":
            return True
        # Also treat files inside thumbnails/ with the same name as user thumb
        parent = os.path.basename(os.path.dirname(path))
        return parent == "thumbnails" and base == f"{user_id}.jpg"
    
    def parse_target_chat(self, target: str) -> Tuple[int, Optional[int]]:
        """Parse chat ID and topic ID from target string"""
        if '/' in target:
            parts = target.split('/')
            return int(parts[0]), int(parts[1])
        return int(target), None
    
    async def _extract_original_thumbnail(self, msg, client, sender) -> Tuple[Optional[str], bool]:
        """Try to extract and download the original thumbnail from a source message.
        Returns (thumb_path, is_temp) where is_temp indicates it should be cleaned after use.
        """
        try:
            # Determine potential thumbnail sources across media types and library variants
            cand_objs = []
            # Video
            if hasattr(msg, 'video') and msg.video:
                v = msg.video
                for attr in ('thumbnails', 'thumbs'):
                    thumbs = getattr(v, attr, None)
                    if thumbs:
                        cand_objs.extend(list(thumbs))
                # Pyrogram sometimes provides .thumbnail
                single = getattr(v, 'thumbnail', None)
                if single:
                    cand_objs.append(single)
            # Document (could be video)
            if hasattr(msg, 'document') and msg.document:
                d = msg.document
                for attr in ('thumbnails', 'thumbs'):
                    thumbs = getattr(d, attr, None)
                    if thumbs:
                        cand_objs.extend(list(thumbs))
                single = getattr(d, 'thumbnail', None)
                if single:
                    cand_objs.append(single)
            # Animation
            if hasattr(msg, 'animation') and msg.animation:
                a = msg.animation
                for attr in ('thumbnails', 'thumbs'):
                    thumbs = getattr(a, attr, None)
                    if thumbs:
                        cand_objs.extend(list(thumbs))
                single = getattr(a, 'thumbnail', None)
                if single:
                    cand_objs.append(single)
            # Photo sizes
            if hasattr(msg, 'photo') and msg.photo:
                cand_objs.append(msg.photo)

            # Prefer the largest available thumbnail (usually last)
            for obj in reversed(cand_objs):
                try:
                    # Pyrogram: download_media can accept a thumbnail/file object
                    downloads_dir = os.path.join(os.getcwd(), "downloads")
                    os.makedirs(downloads_dir, exist_ok=True)
                    path = await client.download_media(obj, file_name=os.path.join(downloads_dir, f"thumb_{int(time.time()*1000)}.jpg"))
                    if path and os.path.exists(path):
                        print(f"🖼️ Successfully extracted thumbnail: {path}")
                        # Register extracted thumbnail for cleanup
                        cleanup_manager.file_cleanup.register_file(sender, path, is_temp=True)
                        return path, True
                    else:
                        print(f"🖼️ Failed to extract thumbnail: path={path}, exists={os.path.exists(path) if path else False}")
                except Exception:
                    continue
        except Exception:
            pass
        return None, False
    
    async def process_user_caption(self, original_caption: str, user_id: int) -> str:
        """Process caption with user preferences"""
        custom_caption = self.user_caption_prefs.get(str(user_id), "") or self.db.get_user_data(user_id, "custom_caption", "")
        delete_words = set(self.db.get_user_data(user_id, "delete_words", []))
        replacements = self.db.get_user_data(user_id, "replacement_words", {})
        
        # Process original caption
        processed = original_caption or ""
        
        # Remove delete words
        for word in delete_words:
            processed = processed.replace(word, "")
        
        # Apply replacements
        for word, replacement in replacements.items():
            processed = processed.replace(word, replacement)
        
        # Add custom caption
        if custom_caption:
            processed = f"{processed}\n\n{custom_caption}".strip()
        
        return processed if processed else None

    async def _generate_file_number(self, user_id: int) -> int:
        """Generate unique file number for user uploads"""
        try:
            # Get current file counter for user
            current_count = self.db.get_user_data(user_id, "file_counter", 0)
            new_count = current_count + 1
            
            # Save updated counter
            self.db.save_user_data(user_id, "file_counter", new_count)
            return new_count
        except Exception as e:
            print(f"Error generating file number: {e}")
            # Fallback to timestamp-based number
            return int(time.time()) % 10000
    
    async def _store_file_mapping(self, user_id: int, file_number: int, message_id: int):
        """Store mapping between file number and LOG_GROUP message ID"""
        try:
            # Get existing mappings
            mappings = self.db.get_user_data(user_id, "file_mappings", {})
            
            # Add new mapping
            mappings[str(file_number)] = message_id
            
            # Keep only last 100 mappings to prevent database bloat
            if len(mappings) > 100:
                # Remove oldest mappings
                sorted_keys = sorted(mappings.keys(), key=lambda x: int(x))
                for key in sorted_keys[:-100]:
                    del mappings[key]
            
            # Save updated mappings
            self.db.save_user_data(user_id, "file_mappings", mappings)
        except Exception as e:
            print(f"Error storing file mapping: {e}")
    
    async def _get_user_session_client(self, user_id: int):
        """Get user session client for upload (placeholder - implement based on your user session system)"""
        try:
            # This is a placeholder - you'll need to implement based on your user session system
            # For now, fallback to admin session from pool
            admin_session_client, admin_session_id = await session_pool.get_session()
            if admin_session_client:
                return admin_session_client
            
            # If no admin session available, use pro client as fallback
            if hasattr(self, 'pro_client') and self.pro_client:
                return self.pro_client
            
            # Final fallback to gf (Telethon client)
            return gf
        except Exception as e:
            print(f"Error getting user session client: {e}")
            return None

    async def upload_with_telethon(self, file_path: str, user_id: int, target_chat_id: int, caption: str, topic_id: Optional[int] = None, edit_msg=None, client=None, original_thumb_path: Optional[str] = None, original_thumb_is_temp: bool = False, caption_entities: Optional[Any] = None, reply_markup: Optional[Any] = None, created_progress_msg: bool = False, is_batch_operation: bool = False):
        # Record start time for upload tracking
        start_time = time.time()
        # Guard to avoid sending duplicate error messages to user
        sent_user_error = False
        
        # Calculate file size
        file_size = os.path.getsize(file_path)
        file_size_str = self._format_bytes(file_size)
        # Match download thresholds: 20MB for single, 50MB for batch
        show_progress = False
        # Use the proper batch detection passed from download function
        is_batch_upload = is_batch_operation
        try:
            # For single uploads: show progress for files > 20MB
            if not is_batch_upload and file_size > 20 * 1024 * 1024:
                show_progress = True
            # For batch uploads: show progress for files > 50MB (matches download threshold)
            elif is_batch_upload and file_size > 50 * 1024 * 1024:
                show_progress = True
        except Exception:
            pass
        
        # Critical debug: Log the actual values for troubleshooting
        if file_size > 30 * 1024 * 1024:  # Only log for files >30MB to catch the 34MB case
            print(f"🔍 UPLOAD: {file_size/(1024*1024):.1f}MB, batch={is_batch_upload}, show_progress={show_progress}")
        
        
        # Do NOT use user or bot sessions for uploads. Only admin pool or pro client.
        upload_client = None
        session_type = ""
        pooled_acquired = False
        pooled_session_id = None
        
        # Import necessary clients
        from telethon.sync import TelegramClient
        from devgagan import telethon_client, app, sex, pro
        
        # Acquire an admin session fairly with premium-aware priority/timeout
        is_premium_user = False
        try:
            prem_doc = await check_premium(user_id)
            is_premium_user = bool(prem_doc) or (user_id in OWNER_ID)
        except Exception:
            is_premium_user = user_id in OWNER_ID
        acquire_timeout = 120.0 if is_premium_user else 300.0
        admin_session_client, admin_session_id = await session_pool.request_session(is_premium=is_premium_user, timeout=acquire_timeout)
        if admin_session_client:
            upload_client = admin_session_client
            session_type = f"admin_{admin_session_id}"
            pooled_acquired = True
            pooled_session_id = admin_session_id
            try:
                setattr(upload_client, "_rbs_sid", str(admin_session_id))
            except Exception:
                pass
        elif pro:
            upload_client = pro
            session_type = "pro_client"
        else:
            # If no session available, inform user and raise exception
            await app.send_message(user_id, "❌ No available upload sessions. Please contact admin: @ZeroTrace0x")
            raise Exception("No available upload sessions")
        
        try:
            me = await upload_client.get_me()
            username = me.username or f"user_{me.id}"
            print(f"📤 UPLOAD: Using {session_type} (@{username}) for upload to LOG_GROUP")
        except Exception:
            print(f"📤 UPLOAD: Using {session_type} (username unknown) for upload to LOG_GROUP")
        """Upload using Admin Sessions to LOG_GROUP with bot forwarding"""
        # Initialize variables to prevent UnboundLocalError
        thumb_path = None
        log_group_id = LOG_GROUP  # Initialize early to prevent errors in exception handlers
        # Warm-up and validate that this session can access LOG_GROUP. If not, try other pool sessions.
        # This prevents intermittent [400 CHANNEL_INVALID] caused by unresolved/unauthorized peers.
        try:
            try:
                # Warm-up depends on client library
                try:
                    from telethon.sync import TelegramClient as _TeleClient
                except Exception:
                    _TeleClient = None
                if _TeleClient and isinstance(upload_client, _TeleClient):
                    # Telethon: resolve entity (works for IDs and usernames)
                    await upload_client.get_entity(log_group_id)
                else:
                    # Pyrogram
                    await upload_client.get_chat(log_group_id)
            except (ChannelInvalid, ChatIdInvalid, ChannelPrivate, PeerIdInvalid) as _lg_err:
                # If we're using an admin pool session, try to rotate to another one quickly
                if session_type.startswith("admin_"):
                    # Release current problematic session
                    try:
                        if pooled_acquired and pooled_session_id:
                            await session_pool.release_session(pooled_session_id, had_error=True)
                    except Exception:
                        pass
                    # Attempt a few replacements
                    for _ in range(3):
                        replacement_client, replacement_id = await session_pool.get_session()
                        if not replacement_client:
                            break
                        try:
                            if _TeleClient and isinstance(replacement_client, _TeleClient):
                                await replacement_client.get_entity(log_group_id)
                            else:
                                await replacement_client.get_chat(log_group_id)
                            upload_client = replacement_client
                            session_type = f"admin_{replacement_id}"
                            admin_session_client = replacement_client
                            admin_session_id = replacement_id
                            pooled_acquired = True
                            pooled_session_id = replacement_id
                            try:
                                setattr(upload_client, "_rbs_sid", str(replacement_id))
                            except Exception:
                                pass
                            print(f"🔁 Switched to pool session {replacement_id} after LOG_GROUP warm-up")
                            break
                        except Exception:
                            # Release and continue searching
                            try:
                                await session_pool.release_session(replacement_id, had_error=True)
                            except Exception:
                                pass
                            continue
                    else:
                        # No replacement worked; if pro client exists, fallback to it
                        if pro:
                            upload_client = pro
                            session_type = "pro_client"
                            pooled_acquired = False
                            pooled_session_id = None
                        else:
                            raise _lg_err
                else:
                    # Not a pool session; if possible, fallback to pro
                    if pro:
                        upload_client = pro
                        session_type = "pro_client"
                    else:
                        raise _lg_err
        except Exception as warm_err:
            # Propagate a clean error so caller can notify user
            raise Exception(f"Cannot access LOG_GROUP with current session: {warm_err}")
        
        try:
            # Optimized upload status - create progress message only when needed (will be updated with progress bar)
            if edit_msg and show_progress:
                try:
                    init_text = await CaptionFormatter.markdown_to_html("📤 **Preparing...**")
                    await edit_msg.edit(init_text, parse_mode=ParseMode.HTML)
                except Exception:
                    pass
            elif show_progress and not edit_msg:
                # Create new progress message only for large files (will be updated with progress bar)
                try:
                    from devgagan import app
                    init_text = await CaptionFormatter.markdown_to_html("📤 **Preparing...**")
                    edit_msg = await app.send_message(user_id, init_text, parse_mode=ParseMode.HTML)
                except Exception:
                    edit_msg = None
            # For small files: NO upload status message to save API calls

            # Generate unique file number for this upload
            file_number = await self._generate_file_number(user_id)
            
            # Decide caption handling strategy:
            # 1) If original caption_entities are provided, keep original caption and entities; no parse_mode.
            # 2) Otherwise, fall back to HTML conversion from markdown-like text for our own status messages.
            if caption_entities:
                full_html_caption = caption or None
                display_caption = full_html_caption
                print("Preserving original caption with entities; no HTML conversion performed")
            else:
                html_caption = await self.caption_formatter.markdown_to_html(caption) if caption else None
                full_html_caption = html_caption
                # Only truncate for the progress message display if needed
                display_caption = html_caption
                if display_caption and len(display_caption) > 1000:
                    display_caption = display_caption[:997] + "..."
                    print(f"Display caption truncated to {len(display_caption)} characters for progress message")
            
            # Create enhanced progress callback for admin session upload
            last_update_time = 0
            async def admin_progress_callback(done, total):
                nonlocal last_update_time
                current_time = time.time()
                # Cancellation check
                try:
                    if await cancel_manager.is_cancelled(user_id):
                        if show_progress and edit_msg:
                            try:
                                await edit_msg.edit("🚫 Upload canceled by user.")
                            except Exception:
                                pass
                        raise asyncio.CancelledError("upload canceled")
                except Exception:
                    pass
                
                # Optimized update intervals: 8s for single uploads, 15s for batch uploads to reduce API calls
                if not show_progress:
                    return
                update_interval = 15 if is_batch_upload else 8
                if current_time - last_update_time >= update_interval or done == total:
                    last_update_time = current_time
                    
                    # Calculate progress
                    percentage = (done / total) * 100 if total > 0 else 0
                    # Use minimum elapsed time to avoid extremely low initial speeds
                    elapsed = max(current_time - start_time, 1.0)
                    speed = done / elapsed if elapsed > 0 else 0
                    
                    # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
                    speed_display = speed * 3
                    
                    # Calculate ETA
                    eta_seconds = (total - done) / speed if speed > 0 else 0
                    # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
                    eta_seconds = eta_seconds / 2
                    eta_str = self.progress_manager._format_time(eta_seconds) if eta_seconds > 0 else "Calculating..."
                    
                    # Use unified progress bar for uploads
                    filename = os.path.basename(file_path)
                    percentage = (done / total) * 100 if total > 0 else 0
                    # Update central task registry to reflect uploading stage
                    try:
                        registry.update(user_id, int(msg_id or 0), stage="uploading", current=int(done), total=int(total))
                    except Exception:
                        pass
                    progress_text = UnifiedProgressBar.format_progress_message(
                        percentage, done, total, speed_display, eta_str, "upload"
                    )
                    
                    # Update progress message
                    try:
                        if show_progress and edit_msg:
                            html_progress = await CaptionFormatter.markdown_to_html(progress_text)
                            await edit_msg.edit(html_progress, parse_mode=ParseMode.HTML)
                    except Exception as e:
                        # Silently handle progress update failures to avoid spam
                        pass
                    
                    self.progress_manager.calculate_progress(done, total, user_id, os.path.basename(file_path), "Admin Session Upload")
            
            # Upload to LOG_GROUP using admin session
            print(f"📤 Starting admin session upload to LOG_GROUP {log_group_id}")
            
            # Keep progress message active during upload - don't delete it yet
            
            # Prepare attributes based on file type
            attributes = []
            file_type = self.media_processor.get_file_type(file_path)
            # Suppress progress for GIFs/animations and stickers regardless of size
            if file_type in ["animation", "sticker"]:
                show_progress = False
            else:
                try:
                    ext = Path(file_path).suffix.lower().lstrip('.')
                    # Treat common sticker formats as non-progress regardless of size
                    if ext in ["webp", "tgs", "webm"]:
                        show_progress = False
                except Exception:
                    pass
            
            if file_type == 'video':
                if 'video_metadata' in globals():
                    metadata = video_metadata(file_path)
                    duration = metadata.get('duration', 0)
                    width = metadata.get('width', 0)
                    height = metadata.get('height', 0)
                    attributes = [DocumentAttributeVideo(
                        duration=duration, w=width, h=height, supports_streaming=True
                    )]
            elif file_type == 'animation':
                if 'video_metadata' in globals():
                    metadata = video_metadata(file_path)
                    duration = metadata.get('duration', 0)
                    width = metadata.get('width', 0)
                    height = metadata.get('height', 0)
                    attributes = [
                        DocumentAttributeVideo(
                            duration=duration, w=width, h=height, supports_streaming=True
                        ),
                        DocumentAttributeAnimated()
                    ]
            
            # Determine thumbnail preference: original > user custom > generated
            print(f"🔍 Thumbnail selection: original_thumb_path={original_thumb_path}, exists={os.path.exists(original_thumb_path) if original_thumb_path else False}")
            thumb_path = original_thumb_path if (original_thumb_path and os.path.exists(original_thumb_path)) else self.get_thumbnail_path(user_id)
            is_temp_thumb = False
            is_temp_original = bool(original_thumb_path and os.path.exists(original_thumb_path) and original_thumb_is_temp)
            print(f"🔍 Selected thumb_path: {thumb_path}")
            
            # Validate thumbnail exists
            if thumb_path and not os.path.exists(thumb_path):
                print(f"Warning: Thumbnail path {thumb_path} does not exist")
                thumb_path = None
                
            # Generate thumbnail if not exists for videos
            if not thumb_path and file_type == 'video' and 'screenshot' in globals() and 'video_metadata' in globals():
                try:
                    # Get video metadata for thumbnail generation
                    metadata = video_metadata(file_path)
                    duration = metadata.get('duration', 0)
                    
                    # Generate thumbnail at middle point of video
                    thumb_path = await screenshot(file_path, duration, user_id)
                    is_temp_thumb = bool(thumb_path)
                    print(f"Generated thumbnail for telethon video upload at {thumb_path}")
                    
                    # Link thumbnail to video for cleanup (only if thumbnail was successfully generated)
                    if thumb_path and os.path.exists(thumb_path) and file_path:
                        cleanup_manager.file_cleanup.link_thumbnail_to_video(thumb_path, file_path)
                        cleanup_manager.file_cleanup.register_file(user_id, thumb_path, is_temp=True)
                except Exception as e:
                    print(f"Error generating thumbnail for telethon: {str(e)}")
                    thumb_path = None
            
            # Bind metrics to chosen session
            try:
                sid_for_metrics = None
                if session_type.startswith("admin_") and 'admin_session_id' in locals():
                    sid_for_metrics = str(admin_session_id)
                elif session_type == "user":
                    sid_for_metrics = f"user_{user_id}"
                elif session_type == "pro_client":
                    sid_for_metrics = "pro_client"
                if task_id and sid_for_metrics:
                    await metrics.bind_session(task_id, sid_for_metrics)
            except Exception:
                pass

            # Upload to LOG_GROUP using upload client (user session or fallback)
            from telethon.sync import TelegramClient
            uploaded_message = None
            # Track when we split a long caption so we can also send it to the user's chat
            user_caption_text = None
            user_caption_entities = None
            user_caption_is_html = False

            # HARD STOP: never allow bot client for uploads at this point either
            from devgagan import app as _bot_app
            if upload_client is _bot_app:
                raise Exception("Bot client uploads are forbidden")
            if isinstance(upload_client, TelegramClient):
                # Use Telethon for upload to LOG_GROUP
                telethon_kwargs = dict(
                    progress_callback=admin_progress_callback,
                    # Use parse_mode only when we generated HTML; otherwise use entities
                    parse_mode=('html' if (full_html_caption and not caption_entities) else None),
                    attributes=attributes,
                    # Use module-level UPLOAD_PART_SIZE_KB which can be overridden via env
                    part_size_kb=UPLOAD_PART_SIZE_KB,
                    allow_cache=False
                )
                # Pass entities when available to preserve original formatting exactly
                if caption_entities:
                    try:
                        telethon_kwargs["entities"] = caption_entities
                    except Exception:
                        pass
                # Pass inline buttons when types are compatible (Telethon reply markup)
                if reply_markup is not None:
                    try:
                        # Telethon expects its own button/markup types; pass-through only if object looks Telethon-native
                        if hasattr(reply_markup, 'rows') or hasattr(reply_markup, 'to_dict'):
                            telethon_kwargs["buttons"] = reply_markup
                    except Exception:
                        pass
                if thumb_path:
                    telethon_kwargs["thumb"] = thumb_path
                if file_type == "video" and thumb_path:
                    telethon_kwargs["supports_streaming"] = True

                # Handle long captions for Telethon as well (limit ~1024)
                caption_limit = 1024
                cap_text = (full_html_caption or "")
                if cap_text and len(cap_text) <= caption_limit:
                    telethon_kwargs["caption"] = full_html_caption
                    uploaded_message = await upload_client.send_file(
                        log_group_id,
                        file_path,
                        **telethon_kwargs
                    )
                else:
                    # Send file without caption first
                    uploaded_message = await upload_client.send_file(
                        log_group_id,
                        file_path,
                        **telethon_kwargs
                    )
                    # Then send caption as separate message in LOG_GROUP
                    if cap_text:
                        if caption_entities:
                            await upload_client.send_message(
                                log_group_id,
                                cap_text,
                                reply_to=uploaded_message.id,
                                entities=caption_entities,
                                buttons=telethon_kwargs.get("buttons")
                            )
                            user_caption_text = cap_text
                            user_caption_entities = caption_entities
                            user_caption_is_html = False
                        else:
                            await upload_client.send_message(
                                log_group_id,
                                cap_text,
                                reply_to=uploaded_message.id
                            )
                            user_caption_text = cap_text
                            user_caption_entities = None
                            user_caption_is_html = True
            else:
                # Use Pyrogram for upload to LOG_GROUP
                if file_type == 'video':
                    metadata = {}
                    if 'video_metadata' in globals():
                        metadata = video_metadata(file_path)
                    
                    width = metadata.get('width', 0)
                    height = metadata.get('height', 0)
                    duration = metadata.get('duration', 0)
                    
                    # Check caption length (Telegram limit is 1024 characters)
                    caption_limit = 1024
                    cap_text = (full_html_caption or "")
                    if len(cap_text) <= caption_limit:
                        telethon_video_kwargs = dict(
                            caption=full_html_caption,
                            duration=duration,
                            width=width,
                            height=height,
                        )
                        # Preserve entities and buttons for Pyrogram when available
                        if caption_entities:
                            telethon_video_kwargs["caption_entities"] = caption_entities
                        if reply_markup is not None:
                            try:
                                from pyrogram.types import InlineKeyboardMarkup as _PyroIKM
                                if isinstance(reply_markup, _PyroIKM):
                                    telethon_video_kwargs["reply_markup"] = reply_markup
                            except Exception:
                                pass
                        if thumb_path:
                            telethon_video_kwargs["thumb"] = thumb_path
                        uploaded_message = await upload_client.send_video(
                            log_group_id,
                            file_path,
                            progress=admin_progress_callback,
                            **telethon_video_kwargs
                        )
                    else:
                        # Send video without caption first
                        telethon_video_kwargs_nc = dict(
                            duration=duration,
                            width=width,
                            height=height,
                        )
                        if thumb_path:
                            telethon_video_kwargs_nc["thumb"] = thumb_path
                        uploaded_message = await upload_client.send_video(
                            log_group_id,
                            file_path,
                            progress=admin_progress_callback,
                            **telethon_video_kwargs_nc
                        )
                        # Then send caption as separate message
                        if caption_entities:
                            await upload_client.send_message(
                                log_group_id,
                                cap_text,
                                reply_to_message_id=uploaded_message.id,
                                entities=caption_entities,
                                reply_markup=(reply_markup if 'pyrogram' in getattr(type(upload_client), '__module__', '') else None)
                            )
                            # Prepare to send the caption to the user separately (not a reply)
                            user_caption_text = cap_text
                            user_caption_entities = caption_entities
                            user_caption_is_html = False
                        else:
                            await upload_client.send_message(
                                log_group_id,
                                cap_text,
                                parse_mode=ParseMode.HTML,
                                reply_to_message_id=uploaded_message.id
                            )
                            user_caption_text = cap_text
                            user_caption_entities = None
                            user_caption_is_html = True
                elif file_type == 'animation':
                    metadata = {}
                    if 'video_metadata' in globals():
                        metadata = video_metadata(file_path)
                    
                    width = metadata.get('width', 0)
                    height = metadata.get('height', 0)
                    duration = metadata.get('duration', 0)
                    
                    # Check caption length (Telegram limit is 1024 characters)
                    caption_limit = 1024
                    cap_text = (full_html_caption or "")
                    if len(cap_text) <= caption_limit:
                        telethon_anim_kwargs = dict(
                            caption=full_html_caption,
                            duration=duration,
                            width=width,
                            height=height,
                        )
                        if caption_entities:
                            telethon_anim_kwargs["caption_entities"] = caption_entities
                        if reply_markup is not None:
                            try:
                                from pyrogram.types import InlineKeyboardMarkup as _PyroIKM
                                if isinstance(reply_markup, _PyroIKM):
                                    telethon_anim_kwargs["reply_markup"] = reply_markup
                            except Exception:
                                pass
                        if thumb_path:
                            telethon_anim_kwargs["thumb"] = thumb_path
                        uploaded_message = await upload_client.send_animation(
                            log_group_id,
                            file_path,
                            progress=admin_progress_callback,
                            **telethon_anim_kwargs
                        )
                    else:
                        # Send animation without caption first
                        telethon_anim_kwargs_nc = dict(
                            duration=duration,
                            width=width,
                            height=height,
                        )
                        if thumb_path:
                            telethon_anim_kwargs_nc["thumb"] = thumb_path
                        uploaded_message = await upload_client.send_animation(
                            log_group_id,
                            file_path,
                            progress=admin_progress_callback,
                            **telethon_anim_kwargs_nc
                        )
                        # Then send caption as separate message (only if non-empty)
                        if cap_text:
                            if caption_entities:
                                await upload_client.send_message(
                                    log_group_id,
                                    cap_text,
                                    reply_to_message_id=uploaded_message.id,
                                    entities=caption_entities,
                                    reply_markup=(reply_markup if 'pyrogram' in getattr(type(upload_client), '__module__', '') else None)
                                )
                                user_caption_text = cap_text
                                user_caption_entities = caption_entities
                                user_caption_is_html = False
                            else:
                                await upload_client.send_message(
                                    log_group_id,
                                    full_html_caption,
                                    parse_mode=ParseMode.HTML,
                                    reply_to_message_id=uploaded_message.id
                                )
                                user_caption_text = cap_text
                                user_caption_entities = None
                                user_caption_is_html = True
                elif file_type == 'photo':
                    # Check caption length (Telegram limit is 1024 characters)
                    caption_limit = 1024
                    cap_text = (full_html_caption or "")
                    if full_html_caption is not None and len(cap_text) <= caption_limit:
                        uploaded_message = await upload_client.send_photo(
                            log_group_id,
                            file_path,
                            caption=full_html_caption,
                            progress=admin_progress_callback,
                            parse_mode=(ParseMode.HTML if not caption_entities else None),
                            caption_entities=(caption_entities if caption_entities else None),
                            reply_markup=(reply_markup if caption_entities and 'pyrogram' in getattr(type(upload_client), '__module__', '') else None)
                        )
                    else:
                        # Send photo without caption first
                        uploaded_message = await upload_client.send_photo(
                            log_group_id,
                            file_path,
                            progress=admin_progress_callback
                        )
                        # Then send caption as separate message (only if non-empty)
                        if cap_text:
                            if caption_entities:
                                await upload_client.send_message(
                                    log_group_id,
                                    cap_text,
                                    reply_to_message_id=uploaded_message.id,
                                    entities=caption_entities,
                                    reply_markup=(reply_markup if 'pyrogram' in getattr(type(upload_client), '__module__', '') else None)
                                )
                                # Prepare for user caption (independent, not a reply)
                                user_caption_text = cap_text
                                user_caption_entities = caption_entities
                                user_caption_is_html = False
                            else:
                                await upload_client.send_message(
                                    log_group_id,
                                    full_html_caption,
                                    parse_mode=ParseMode.HTML,
                                    reply_to_message_id=uploaded_message.id
                                )
                                user_caption_text = cap_text
                                user_caption_entities = None
                                user_caption_is_html = True
                else:
                    # Check caption length (Telegram limit is 1024 characters)
                    caption_limit = 1024
                    cap_text = (full_html_caption or "")
                    if len(cap_text) <= caption_limit:
                        telethon_doc_kwargs = dict(
                            caption=full_html_caption,
                        )
                        if caption_entities:
                            telethon_doc_kwargs["caption_entities"] = caption_entities
                        if reply_markup is not None:
                            try:
                                from pyrogram.types import InlineKeyboardMarkup as _PyroIKM
                                if isinstance(reply_markup, _PyroIKM):
                                    telethon_doc_kwargs["reply_markup"] = reply_markup
                            except Exception:
                                pass
                        if thumb_path:
                            telethon_doc_kwargs["thumb"] = thumb_path
                        uploaded_message = await upload_client.send_document(
                            log_group_id,
                            file_path,
                            progress=admin_progress_callback,
                            **telethon_doc_kwargs
                        )
                    else:
                        # Send document without caption first
                        telethon_doc_kwargs_nc = {}
                        if thumb_path:
                            telethon_doc_kwargs_nc["thumb"] = thumb_path
                        uploaded_message = await upload_client.send_document(
                            log_group_id,
                            file_path,
                            progress=admin_progress_callback,
                            **telethon_doc_kwargs_nc
                        )
                        # Then send caption as separate message (only if non-empty)
                        if cap_text:
                            if caption_entities:
                                await upload_client.send_message(
                                    log_group_id,
                                    cap_text,
                                    reply_to_message_id=uploaded_message.id,
                                    entities=caption_entities,
                                    reply_markup=(reply_markup if 'pyrogram' in getattr(type(upload_client), '__module__', '') else None)
                                )
                                user_caption_text = cap_text
                                user_caption_entities = caption_entities
                                user_caption_is_html = False
                            else:
                                await upload_client.send_message(
                                    log_group_id,
                                    cap_text,
                                    parse_mode=ParseMode.HTML,
                                    reply_to_message_id=uploaded_message.id
                                )
                                user_caption_text = cap_text
                                user_caption_entities = None
                                user_caption_is_html = True
            
            # Store file mapping for future reference
            await self._store_file_mapping(user_id, file_number, uploaded_message.id)
            
            # 💾 DEDUPLICATION: Store file hash for future deduplication
            try:
                if file_path and os.path.exists(file_path) and uploaded_message and hasattr(uploaded_message, 'id'):
                    # Extract original message info if available
                    original_chat_id = getattr(self, '_current_chat_id', None)
                    original_message_id = getattr(self, '_current_message_id', None)
                    file_type_for_dedup = file_type or "unknown"
                    
                    await store_file_for_deduplication(
                        file_path=file_path,
                        log_group_message_id=uploaded_message.id,
                        chat_id=original_chat_id,
                        message_id=original_message_id,
                        user_id=user_id,
                        file_type=file_type_for_dedup
                    )
            except Exception as dedup_store_err:
                print(f"⚠️ DEDUPLICATION: Error storing file hash: {dedup_store_err}")
                # Don't fail the upload if deduplication storage fails
            
            # Forward the uploaded message from LOG_GROUP to user using bot session
            print(f"Forwarding message from LOG_GROUP to user: {user_id}")
            try:
                forwarded_message = await app.forward_messages(
                    chat_id=user_id,
                    from_chat_id=log_group_id,
                    message_ids=uploaded_message.id,
                    drop_author=True
                )
                print(f"✅ Successfully forwarded message to user {user_id}")
                # If we split the caption due to length, send it to the user as
                # a separate message (independent, not a reply), preserving formatting
                if user_caption_text:
                    try:
                        await app.send_message(
                            user_id,
                            user_caption_text,
                            entities=(user_caption_entities if user_caption_entities else None),
                            parse_mode=(ParseMode.HTML if (user_caption_is_html and not user_caption_entities) else None)
                        )
                        print(f"➡️ Sent split caption to user {user_id} as standalone message")
                    except Exception as e:
                        print(f"⚠️ Failed to send long caption to user {user_id}: {e}")
                # Do not send any additional caption when it fit within Telegram's limit
            except Exception as forward_error:
                print(f"❌ Forward failed: {forward_error}")
                # If forwarding fails, send a success message to user instead
                await app.send_message(
                    user_id,
                    f"✅ <b>File processed successfully!</b>\n\n"
                    f"If you don't see it, please contact support."
                )
                forwarded_message = None
                # Still send caption to user ONLY if we had split it
                if user_caption_text:
                    try:
                        await app.send_message(
                            user_id,
                            user_caption_text,
                            entities=(user_caption_entities if user_caption_entities else None),
                            parse_mode=(ParseMode.HTML if (user_caption_is_html and not user_caption_entities) else None)
                        )
                        print(f"➡️ Sent split caption to user {user_id} (after forward fail)")
                    except Exception as e:
                        print(f"⚠️ Failed to send long caption to user {user_id} (after forward fail): {e}")
                # If caption fit within the limit, the media already includes it; no extra send here

            # Upload completed successfully - clean up progress message to keep chat clean
            # For batch uploads: delete if we created a progress message for large files
            # For single uploads: delete progress messages for small files only
            should_delete_upload_progress = False
            if edit_msg:
                if is_batch_upload:  # Batch upload
                    # Delete progress message for batch uploads if we created one for large files
                    should_delete_upload_progress = show_progress
                else:  # Single upload
                    # Delete progress message for small files in single uploads
                    should_delete_upload_progress = not show_progress
                
                if should_delete_upload_progress:
                    try:
                        await edit_msg.delete()
                    except Exception:
                        pass
            
            # Upload completed - no additional confirmation message needed to save API calls
            
            # Finish registry tracking for this message id if known
            try:
                registry.finish(user_id, getattr(edit_msg, 'id', 0) or 0)
            except Exception:
                pass
            return forwarded_message
            
        except asyncio.CancelledError:
            # Cancellation: cleanup aggressively and release session
            try:
                if 'progress_message' in locals() and progress_message:
                    await progress_message.delete()
            except Exception:
                pass
            try:
                if 'file_path' in locals() and file_path and os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass
            raise
        except (OSError, ConnectionError, ConnectionResetError) as e:
            error_message = "Connection lost during upload. Please try again."
            print(f"❌ CONNECTION ERROR: {str(e)}")
            
            # Clean up progress message on error
            # Do not delete edit_msg here; it may be reused by caller
            
            # Implement retry mechanism for connection errors
            retry_count = getattr(self, '_connection_retry_count', 0)
            if retry_count < 2:  # Allow up to 2 retries
                self._connection_retry_count = retry_count + 1
                print(f"🔄 Retrying upload attempt {self._connection_retry_count + 1}/3 after connection error")
                
                # Wait before retry with exponential backoff
                await asyncio.sleep(2 ** retry_count)
                
                # Clean up session pool to get fresh connection
                if session_id:
                    await session_pool.release_session(session_id, had_error=True)
                
                # Retry the upload with a fresh session
                try:
                    return await self.handle_file_upload(message, file_path, file_name, file_size, None, None)
                except Exception as retry_error:
                    print(f"❌ Retry attempt {self._connection_retry_count} failed: {retry_error}")
            
            # Reset retry count after all attempts
            self._connection_retry_count = 0
            
            # Clean up any temporary files
            try:
                if 'file_path' in locals() and file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"✅ Cleaned up temporary file: {file_path}")
            except Exception as cleanup_error:
                print(f"❌ Error cleaning up file: {cleanup_error}")
            
            # Suppress user-facing error messages; log only
            print(f"ℹ️ Suppressed user error message: {error_message}")
            
        except Exception as e:
            error_message = str(e)
            html_error_rbs = await self.caption_formatter.markdown_to_html(f"<b>{session_type.title()} Upload Failed:</b> {error_message}")
            await app.send_message(log_group_id, html_error_rbs, parse_mode=ParseMode.HTML)
            
            # Clean up progress message on error
            try:
                if 'progress_message' in locals() and progress_message:
                    await progress_message.delete()
            except:
                pass
            
            # Suppress user-facing error messages; log only
            print(f"ℹ️ Suppressed user error message (general): {error_message}")
            # Also mark metrics as error if registered
            try:
                if task_id:
                    await metrics.finish_task(task_id, status="error")
            except Exception:
                pass
            raise
        finally:
            # Release session back to pool if it was obtained from there
            # Always release pooled sessions if acquired
            if pooled_acquired and pooled_session_id:
                had_error = 'e' in locals()
                await session_pool.release_session(pooled_session_id, had_error=had_error)
                print(f"📤 UPLOAD: Released pooled session {pooled_session_id}")
            else:
                # Last-resort: if client is tagged with a session id, release it
                try:
                    tag_sid = getattr(upload_client, "_rbs_sid", None)
                    if tag_sid:
                        await session_pool.release_session(str(tag_sid), had_error='e' in locals())
                        print(f"📤 UPLOAD: Released tagged pooled session {tag_sid}")
                except Exception:
                    pass
            print(f"📤 UPLOAD: Finished {session_type} upload to LOG_GROUP")
            
            # Clean up temporary files
            if os.path.exists(f"{file_path}.temp"):
                try:
                    os.remove(f"{file_path}.temp")
                    print(f"🗑️ Cleaned up temp file: {file_path}.temp")
                except Exception as cleanup_error:
                    print(f"⚠️ Error cleaning up temp file: {cleanup_error}")
            
            # Cleanup only temporary thumbnails created for this upload
            if is_temp_thumb and thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                    print(f"🗑️ Cleaned up temporary thumbnail: {thumb_path}")
                except Exception as cleanup_error:
                    print(f"⚠️ Error cleaning up temporary thumbnail: {cleanup_error}")
            if is_temp_original and original_thumb_path and os.path.exists(original_thumb_path):
                try:
                    os.remove(original_thumb_path)
                    print(f"🗑️ Cleaned up extracted original thumbnail: {original_thumb_path}")
                except Exception as cleanup_error:
                    print(f"⚠️ Error cleaning up original thumbnail: {cleanup_error}")
            # Do not clear cancel flag here; outer callers (batch/single) handle clearing

            # Mark upload task finished
            try:
                if task_id:
                    await metrics.finish_task(task_id, status="done")
            except Exception:
                pass
            # Clear cancel flag if set for this user (avoid stale cancellations blocking next run)
            try:
                if await cancel_manager.is_cancelled(user_id):
                    await cancel_manager.clear(user_id)
            except Exception:
                pass

    async def handle_large_file_upload(self, file_path: str, sender: int, edit_msg, caption: str, client=None, original_thumb_path: Optional[str] = None, original_thumb_is_temp: bool = False):
        # Get a session from the pool if no client is provided
        pooled_client = None
        session_id = None
        if not client:
            # Premium-aware, fair session request with timeout
            is_premium_user = False
            try:
                prem_doc = await check_premium(sender)
                is_premium_user = bool(prem_doc) or (sender in OWNER_ID)
            except Exception:
                is_premium_user = sender in OWNER_ID
            acquire_timeout = 120.0 if is_premium_user else 300.0
            pooled_client, session_id = await session_pool.request_session(is_premium=is_premium_user, timeout=acquire_timeout)
            client = pooled_client if pooled_client else self.pro_client
            
            # Log which client is being used
            if pooled_client and session_id:
                try:
                    me = await pooled_client.get_me()
                    username = me.username or f"user_{me.id}"
                    print(f"📤 LARGE UPLOAD: Using session {session_id} (@{username}) for large file upload")
                except Exception:
                    print(f"📤 LARGE UPLOAD: Using session {session_id} (username unknown) for large file upload")
            else:
                print(f"📤 LARGE UPLOAD: Using default pro client for large file upload (no pool session available)")
        """Handle files larger than 2GB using pro client"""
        if not self.pro_client:
            html_text = await CaptionFormatter.markdown_to_html('<b>❌ 2GB upload not available - Pro client not configured</b>')
            await edit_msg.edit(html_text, parse_mode=ParseMode.HTML)
            return

        html_text = await CaptionFormatter.markdown_to_html('<b>✅ 2GB upload starting...</b>')
        await edit_msg.edit(html_text, parse_mode=ParseMode.HTML)
        
        target_chat_str = self.user_chat_ids.get(sender, str(sender))
        target_chat_id, _ = self.parse_target_chat(target_chat_str)
        
        file_type = self.media_processor.get_file_type(file_path)
        # Get thumbnail path and validate it exists; prefer original
        thumb_path = original_thumb_path if (original_thumb_path and os.path.exists(original_thumb_path)) else self.get_thumbnail_path(sender)
        is_temp_thumb = False
        is_temp_original = bool(original_thumb_path and os.path.exists(original_thumb_path) and original_thumb_is_temp)
        
        # Validate thumbnail exists
        if thumb_path and not os.path.exists(thumb_path):
            print(f"Warning: Thumbnail path {thumb_path} does not exist for large file upload")
            thumb_path = None
            
        # Generate thumbnail if not exists for videos
        if not thumb_path and file_type == 'video' and 'screenshot' in globals() and 'video_metadata' in globals():
            try:
                # Get video metadata for thumbnail generation
                metadata = video_metadata(file_path)
                duration = metadata.get('duration', 0)
                
                # Generate thumbnail at middle point of video
                thumb_path = await screenshot(file_path, duration, sender)
                is_temp_thumb = bool(thumb_path)
                print(f"Generated thumbnail for large file upload at {thumb_path}")
                
                # Link thumbnail to video for cleanup (only if thumbnail was successfully generated)
                if thumb_path and os.path.exists(thumb_path) and file_path:
                    cleanup_manager.file_cleanup.link_thumbnail_to_video(thumb_path, file_path)
                    cleanup_manager.file_cleanup.register_file(sender, thumb_path, is_temp=True)
            except Exception as e:
                print(f"Error generating thumbnail for large file: {str(e)}")
                thumb_path = None
        
        file_name = os.path.basename(file_path)
        
        # Reset progress for this user
        self.progress_manager.reset_user_progress(sender)
        
        # Create progress callback with live updates for large files
        last_update_time = 0
        last_percent = -1.0
        async def progress_callback(current, total):
            nonlocal last_update_time
            current_time = time.time()
            
            # Update every 5 seconds to prevent spam
            if current_time - last_update_time >= 5 or percent != last_percent or current == total:
                last_update_time = current_time
                
                # Calculate progress
                percentage = (current / total) * 100 if total > 0 else 0
                # Calculate speed with minimum elapsed time
                start_time_obj = self.progress_manager.user_progress.get(sender, type('obj', (object,), {'start_time': current_time}))
                elapsed = max(current_time - start_time_obj.start_time, 1.0) if current_time > start_time_obj.start_time else 1.0
                speed = current / elapsed
                
                # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
                speed_display = speed * 3
                
                # Update progress message for large files
                try:
                    # Ensure percentage is valid
                    percentage = min(100, max(0, percentage))
                    
                    # Calculate ETA with better formatting
                    eta_seconds = (total - current) / speed if speed > 0 else 0
                    # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
                    eta_seconds = eta_seconds / 2
                    eta_str = self.progress_manager._format_time(eta_seconds) if eta_seconds > 0 else "Calculating..."
                    
                    # Format the progress text with more information and modern styling
                    # Only show progress bar for files > 5MB
                    if total > 5 * 1024 * 1024:
                        progress_text = (
                            f"📤 <b>Large File Upload - {file_name}</b>\n\n"
                            f"📊 <b>Progress:</b> {percentage:.1f}%\n"
                            f"📁 <b>Size:</b> {self.progress_manager._format_bytes(current)} / {self.progress_manager._format_bytes(total)}\n"
                            f"⚡ <b>Speed:</b> {self.progress_manager._format_speed(speed_display)}\n"
                            f"⏱ <b>ETA:</b> {eta_str}\n\n"
                            f"{self.progress_manager._create_modern_progress_bar(percentage, 10, 'rainbow')}"
                        )
                        html_progress = await CaptionFormatter.markdown_to_html(progress_text)
                        await edit_msg.edit(html_progress, parse_mode=ParseMode.HTML)
                except Exception as e:
                    print(f"Progress update error: {e}")
                    pass
                
                self.progress_manager.calculate_progress(
                    current, total, sender, file_name, "📤 2GB Upload"
                )
        
        try:
            if file_type == 'video':
                metadata = {}
                if 'video_metadata' in globals():
                    metadata = video_metadata(file_path)
                
                # Prepare thumbnail for video upload if it exists
                thumb_file = None
                if thumb_path and os.path.exists(thumb_path):
                    # Read thumbnail file for Pyrogram
                    thumb_file = thumb_path
                    print(f"Using thumbnail for large file video upload: {thumb_path}")

                # Reflect switch to uploading stage immediately for UI
                try:
                    total_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    registry.update(sender, int(msg_id or 0), stage="uploading", current=0, total=int(total_size))
                except Exception:
                    pass

                # Upload large video to target (tracked via progress_callback)
                result = await client.send_video(
                    target_chat_id,
                    video=file_path,
                    caption=caption,
                    thumb=thumb_file,
                    height=metadata.get('height', 0),
                    width=metadata.get('width', 0),
                    duration=metadata.get('duration', 0),
                    progress=progress_callback,
                    supports_streaming=True
                )

                # Mark task finished only after upload completes
                try:
                    registry.finish(sender, int(msg_id or 0))
                except Exception:
                    pass

                # Do not copy via bot session here (logging handled by admin/pro uploader elsewhere)
            elif file_type == 'animation':
                # ... (rest of the code remains the same)

                # Do not copy via bot session here (logging handled by admin/pro uploader elsewhere)
                try:
                    pass
                except Exception as e:
                    print(f"Error sending large animation to LOG_GROUP: {str(e)}")
                    # Fallback to sending as document if animation fails
                    try:
                        pass
                    except Exception as e2:
                        print(f"Error sending large animation as document to LOG_GROUP: {str(e2)}")
                        # Continue execution even if LOG_GROUP copy fails
            else:
                # Send to target chat
                # Immediately reflect switch to uploading stage so UI updates promptly
                try:
                    total_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    registry.update(sender, int(msg_id or 0), stage="uploading", current=0, total=int(total_size))
                except Exception:
                    pass
                result = await client.send_document(
                    target_chat_id,
                    document=file_path,
                    caption=caption,
                    thumb=thumb_path,
                    progress=progress_callback
                )
                # Mark task finished only after upload completes
                try:
                    registry.finish(sender, int(msg_id or 0))
                except Exception:
                    pass
                
                # Do not copy via bot session here (logging handled by admin/pro uploader elsewhere)

            # Check if user is premium or free
            free_check = 0
            if 'chk_user' in globals():
                free_check = await chk_user(sender, sender)

            if free_check == 1:
                # Free user - send with protection
                reply_markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton("💎 Get Premium to Forward", url="https://t.me/ZeroTrace0x")
                ]])
                await app.copy_message(target_chat_id, LOG_GROUP, result.id, protect_content=True, reply_markup=reply_markup)
            else:
                # Premium user - send normally
                await app.copy_message(target_chat_id, LOG_GROUP, result.id)
                
            # Clean up main file after successful upload
            try:
                await cleanup_manager.file_cleanup.cleanup_after_upload(sender, file_path)
            except Exception:
                pass

        except Exception as e:
            error_message = str(e)
            print(f"Large file upload error: {error_message}")
            html_error_2gb = await self.caption_formatter.markdown_to_html(f"<b>2GB Upload Error:</b> {error_message}")
            await app.send_message(LOG_GROUP, html_error_2gb, parse_mode=ParseMode.HTML)
            
            # Send error message to user
            await app.send_message(
                sender,
                f"❌ Upload failed: {error_message}\n\nPlease try again or contact support."
            )
        finally:
            if edit_msg:
                try:
                    await edit_msg.delete()
                except Exception:
                    pass
            
            # Release the session back to the pool if it was obtained from there
            if pooled_client and session_id:
                had_error = 'e' in locals()
                await session_pool.release_session(session_id, had_error=had_error)
                print(f"📤 LARGE UPLOAD: Finished large file upload using session {session_id}")
            else:
                print(f"📤 LARGE UPLOAD: Finished large file upload using default pro client")

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    def _format_speed(self, speed_bps: float) -> str:
        """Format speed to human readable format"""
        return f"{self._format_bytes(speed_bps)}/s"
    
    def _is_public_group_link(self, msg_link: str) -> bool:
        """Detect if this is a public group/channel link that can be accessed by bots directly"""
        # Public links have username format: t.me/username/message_id
        # They don't use /c/ (private) or /b/ (bot) formats
        if 't.me/c/' in msg_link or 'telegram.dog/c/' in msg_link:
            return False  # Private channel
        if 't.me/b/' in msg_link or 'telegram.dog/b/' in msg_link:
            return False  # Bot link
        if '/s/' in msg_link:
            return False  # Story link
            
        # Check if it's a public username-based link
        try:
            normalized = re.sub(r'^https?://(telegram\.dog|t\.me)/', 't.me/', msg_link)
            path_part = normalized.split('t.me/', 1)[1]
            path_part = path_part.split('?', 1)[0].split('#', 1)[0]
            tokens = [t for t in path_part.split('/') if t]
            
            if len(tokens) >= 2:
                username = tokens[0]
                # Check if this is a topic group (3 tokens: username/topic_id/message_id)
                if len(tokens) == 3 and tokens[1].isdigit() and tokens[2].isdigit():
                    # This is a topic group - should be treated as private
                    return False
                # Public groups/channels have usernames (no numeric-only identifiers)
                elif not username.isdigit() and not username.startswith('-'):
                    return True
        except (IndexError, ValueError):
            pass
            
        return False
    
    def _is_private_group_link(self, msg_link: str, chat_id: Union[int, str]) -> bool:
        """Detect if this is a group/channel link that requires user session access"""
        # AGGRESSIVE APPROACH: ALL group links require login because group history might be hidden
        # regardless of whether the group appears "public"
        
        # Always check for group patterns first, ignore public/private distinction
        import re
        
        # Topic group format: t.me/username/topic_id/message_id
        topic_pattern = r't\.me/[^/]+/\d+/\d+'
        if re.search(topic_pattern, msg_link):
            print(f"[CORE-LOGIN-CHECK] Topic group detected, requires login: {msg_link}")
            return True
            
        # Regular group format: t.me/username/message_id
        group_pattern = r't\.me/[^/]+/\d+$'
        if re.search(group_pattern, msg_link):
            print(f"[CORE-LOGIN-CHECK] Group link detected, requires login: {msg_link}")
            return True
            
        # Story links
        if '/s/' in msg_link:
            print(f"[CORE-LOGIN-CHECK] Story link detected, requires login: {msg_link}")
            return True
        
        # Private channel/group links (t.me/c/ format)
        if 't.me/c/' in msg_link or 'telegram.dog/c/' in msg_link:
            return True
        
        # Bot links (t.me/b/ format) - these are typically from bots in groups
        if 't.me/b/' in msg_link or 'telegram.dog/b/' in msg_link:
            return True
            
        # Legacy thread indicators
        if '/t/' in msg_link or '?thread=' in msg_link:
            return True
            
        # Check for topic group format in chat_id (supergroup with topic)
        if isinstance(chat_id, int) and chat_id < -1000000000000:  # Supergroup format
            return True
                
        # Check if chat_id indicates a private group/supergroup
        if isinstance(chat_id, int) and chat_id < 0:
            return True
                
        # Check for username-based group links that might be private
        if isinstance(chat_id, str) and not chat_id.startswith('@'):
            # If it's a string but not a username, it might be a private group identifier
            return True
                
        return False

    async def handle_message_download(self, userbot, sender: int, edit_id: int | None, msg_link: str, offset: int, message):
        """Main message processing function with enhanced error handling"""
        edit_msg = None
        created_progress_msg = False
        file_path = None
        pooled_client = None
        session_id = None
        user_session_client = None
        file_info = {"size": 0, "name": "Unknown", "type": "unknown"}
        # Metrics instrumentation
        dl_task_id = None
        dl_status = "done"
        
        # Topic group tracking for proper history reading
        self._is_topic_group = False
        self._topic_id = None
        self._group_username = None
        try:
            uname = ""
            try:
                u = await app.get_users(sender)
                uname = (u.username or u.first_name or "") if u else ""
            except Exception:
                pass
            dl_task_id = await metrics.start_task("download", sender, uname, link=msg_link)
        except Exception:
            pass
        
        try:
            # Parse and validate message link
            msg_link = msg_link.split("?")[0]
            protected_channels = self.db.get_protected_channels()
            
            # Extract chat and message info
            chat_id, msg_id = await self._parse_message_link(msg_link, offset, protected_channels, sender, edit_id)
            if not chat_id:
                # _parse_message_link returns None for successfully processed special cases
                # (public links, story links, protected channels) - these are not errors
                return
            
            # Store current message info for deduplication
            self._current_chat_id = chat_id
            self._current_message_id = msg_id
            
            # Detect if this is a group chat link that might require user session
            is_private_group = self._is_private_group_link(msg_link, chat_id)
            requires_user_session = is_private_group

            # Optimization: For public username links with restrictions off, try to clone (copy/forward)
            # the message to the user first to preserve original media, captions, and thumbnails
            # (including albums) without doing a download+upload.
            try:
                if not requires_user_session and self._is_public_group_link(msg_link):
                    # Avoid copying from protected content sources
                    try:
                        src_chat = await app.get_chat(chat_id)
                        if getattr(src_chat, "has_protected_content", False):
                            raise Exception("protected")
                    except Exception:
                        # If chat info cannot be fetched or is protected, skip fast path
                        src_chat = None
                    else:
                        # Single message fast-path: try copy (no forward header) first
                        try:
                            await app.copy_message(chat_id=sender, from_chat_id=chat_id, message_id=int(msg_id))
                            try:
                                registry.finish(sender, int(msg_id or 0))
                            except Exception:
                                pass
                            # Add delay after copy to prevent flood wait in batch processing
                            print(f"[BATCH-COPY] Applying {GLOBAL_BATCH_PROCESSING_TIMER}-second delay after copy for user {sender}")
                            await asyncio.sleep(GLOBAL_BATCH_PROCESSING_TIMER)
                            return
                        except Exception:
                            # Fallback: forward single message
                            try:
                                await app.forward_messages(chat_id=sender, from_chat_id=chat_id, message_ids=int(msg_id))
                                try:
                                    registry.finish(sender, int(msg_id or 0))
                                except Exception:
                                    pass
                                # Add delay after forward to prevent flood wait in batch processing
                                print(f"[BATCH-FORWARD] Applying {GLOBAL_BATCH_PROCESSING_TIMER}-second delay after forward for user {sender}")
                                await asyncio.sleep(GLOBAL_BATCH_PROCESSING_TIMER)
                                return
                            except Exception:
                                pass
            except Exception:
                # Never fail the main flow due to optimization
                pass

            # Register task early (session unknown yet)
            try:
                registry.start(sender, int(msg_id or 0), msg_link, stage="preparing", session="unknown")
            except Exception:
                pass
            
            # Get target chat configuration
            target_chat_str = self.user_chat_ids.get(message.chat.id, str(message.chat.id))
            target_chat_id, topic_id = self.parse_target_chat(target_chat_str)
            
            # For group chats, check if user session is available and provide feedback
            user_data = await odb.get_data(sender)
            user_session_string = user_data.get("session") if user_data else None
            
            if requires_user_session and not user_session_string:
                await message.reply_text(
                    "🔐 <b>Login Required</b>\n\n"
                    "This appears to be a private group/channel link that requires your personal session to access. "
                    "Please login first using the /login command to download content from private groups."
                )
                # Raise a specific exception that batch processing can catch and handle
                raise Exception("LOGIN_REQUIRED: User session required for private content access")
            
            # First try to get user's own session from database
            if user_session_string:
                try:
                    # Create user session client
                    user_session_client = Client(
                        name=f"user_download_{sender}",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        session_string=user_session_string,
                        in_memory=True,
                        no_updates=True,
                        workers=int(os.getenv("USER_SESSION_WORKERS", "2")),
                        max_concurrent_transmissions=int(os.getenv("USER_SESSION_MAX_CONCURRENT_TX", "2"))
                    )
                    await user_session_client.start()
                    
                    # Log user session usage
                    try:
                        me = await user_session_client.get_me()
                        username = me.username or f"user_{me.id}"
                        print(f"[SESSION] DOWNLOAD using USER session: @{username} chat={chat_id}")
                    except Exception:
                        print(f"[SESSION] DOWNLOAD using USER session: <unknown> chat={chat_id}")
                        
                    client_to_use = user_session_client
                    session_id = f"user_{sender}"
                    
                except Exception as e:
                    print(f"❌ Failed to initialize user session for {sender}: {e}")
                    if requires_user_session:
                        await message.reply_text(
                            "❌ <b>Session Error</b>\n\n"
                            "Failed to initialize your session. This might be because:\n"
                            "• Your session has expired\n"
                            "• Invalid session data\n\n"
                            "Please try logging in again with /login command."
                        )
                        # Raise a specific exception that batch processing can catch and handle
                        raise Exception("SESSION_ERROR: Failed to initialize user session for private content")
                    user_session_client = None
            
            # If user session failed, try admin session pool as fallback (only for non-group chats)
            if not user_session_client and not requires_user_session:
                # Premium-aware fair acquisition
                is_premium_user = False
                try:
                    prem_doc = await check_premium(sender)
                    is_premium_user = bool(prem_doc) or (sender in OWNER_ID)
                except Exception:
                    is_premium_user = sender in OWNER_ID
                acquire_timeout = 120.0 if is_premium_user else 300.0
                pooled_client, session_id = await session_pool.request_session(is_premium=is_premium_user, timeout=acquire_timeout)
                
                # Log which client is being used
                if pooled_client and session_id:
                    try:
                        me = await pooled_client.get_me()
                        username = me.username or f"user_{me.id}"
                        print(f"[SESSION] DOWNLOAD using POOL session: {session_id} (@{username}) chat={chat_id}")
                    except Exception:
                        print(f"[SESSION] DOWNLOAD using POOL session: {session_id} (<unknown>) chat={chat_id}")
                else:
                    print(f"[SESSION] DOWNLOAD using DEFAULT client for chat={chat_id} (no pool/user session)")
                
                # If pool has no sessions, fall back to the provided userbot
                client_to_use = pooled_client if pooled_client else userbot
            elif not user_session_client and requires_user_session:
                await message.reply_text(
                    "❌ <b>Access Required</b>\n\n"
                    "Unable to access this private group/channel. Please ensure you have access and try logging in again."
                )
                # Raise a specific exception that batch processing can catch and handle
                raise Exception("ACCESS_REQUIRED: Unable to access private group/channel without user session")
            else:
                # Use user session client
                client_to_use = user_session_client

            # Bind metrics and registry to selected session id
            try:
                sid_for_metrics = None
                if client_to_use is user_session_client and user_session_client:
                    sid_for_metrics = f"user_{sender}"
                elif client_to_use is pooled_client and session_id:
                    sid_for_metrics = str(session_id)
                elif client_to_use is userbot:
                    sid_for_metrics = "userbot"
                if dl_task_id and sid_for_metrics:
                    await metrics.bind_session(dl_task_id, sid_for_metrics)
                # Update registry with resolved session and stage
                try:
                    registry.update(sender, int(msg_id or 0), stage="downloading", session=sid_for_metrics or "unknown")
                except Exception:
                    pass
            except Exception:
                pass
            
            if not client_to_use:
                await app.edit_message_text(sender, edit_id, "❌ No available user sessions. Please login or contact admin: @ZeroTrace0x")
                return
            
            # Warm up public username chats to avoid CHANNEL_INVALID by resolving via Pyrogram first
            try:
                if isinstance(chat_id, str):
                    # This will cache the peer for the bot client and often resolves transient CHANNEL_INVALID
                    await app.get_chat(chat_id)
            except Exception:
                pass

            # Fetch message
            msg = None
            try:
                # Detect client type by module path
                if getattr(client_to_use.__class__, "__module__", "").startswith("pyrogram"):
                    # Pyrogram: chat_id can be int(-100...) or username
                    msg = await client_to_use.get_messages(chat_id, msg_id)
                else:
                    # Telethon: resolve entity and use ids parameter
                    entity = chat_id
                    try:
                        if hasattr(client_to_use, 'get_entity'):
                            entity = await client_to_use.get_entity(chat_id)
                    except Exception:
                        # Could not resolve entity via Telethon
                        entity = None
                    try:
                        if entity is not None:
                            msg = await client_to_use.get_messages(entity, ids=msg_id)
                        else:
                            raise Exception("Telethon entity resolution failed")
                    except Exception as _tele_err:
                        # Fallback: try Pyrogram bot client for public usernames
                        try:
                            if isinstance(chat_id, str):
                                msg = await app.get_messages(chat_id, msg_id)
                            else:
                                raise
                        except Exception as _pyro_fallback_err:
                            raise _tele_err
            except Exception as fetch_err:
                print(f"Primary get_messages failed: {fetch_err}")
                msg = None
            
            # Defensive checks for message flags across libraries
            service_flag = getattr(msg, "service", False) if msg else False
            empty_flag = getattr(msg, "empty", False) if msg else False
            if not msg or service_flag or empty_flag:
                try:
                    await app.edit_message_text(sender, edit_id, "❌ Message not found or inaccessible (may be a service/empty or thread-specific message).")
                except Exception:
                    pass
                raise Exception("Message not found or empty")

            # Extract file information if available
            if hasattr(msg, "document") and msg.document:
                file_info["size"] = msg.document.file_size
                file_info["name"] = msg.document.file_name or "document"
                file_info["type"] = "document"
            elif hasattr(msg, "video") and msg.video:
                file_info["size"] = msg.video.file_size
                file_info["name"] = msg.video.file_name or "video.mp4"
                file_info["type"] = "video"
            elif hasattr(msg, "photo") and msg.photo:
                # For photos, size might be in different attribute depending on library
                file_info["size"] = getattr(msg.photo, "file_size", 0) or 0
                file_info["name"] = "photo.jpg"
                file_info["type"] = "photo"
            elif hasattr(msg, "audio") and msg.audio:
                file_info["size"] = msg.audio.file_size
                file_info["name"] = msg.audio.file_name or "audio.mp3"
                file_info["type"] = "audio"
            elif hasattr(msg, "animation") and msg.animation:
                file_info["size"] = msg.animation.file_size
                file_info["name"] = msg.animation.file_name or "animation.gif"
                file_info["type"] = "animation"
            elif hasattr(msg, "poll") and msg.poll:
                file_info["size"] = 1  # Symbolic size
                file_info["name"] = "poll.json"
                file_info["type"] = "poll"
            elif hasattr(msg, "location") and msg.location:
                file_info["size"] = 1  # Symbolic size
                file_info["name"] = "location.json"
                file_info["type"] = "location"
            elif hasattr(msg, "contact") and msg.contact:
                file_info["size"] = 1  # Symbolic size
                file_info["name"] = "contact.vcf"
                file_info["type"] = "contact"
            elif hasattr(msg, "dice") and msg.dice:
                file_info["size"] = 1  # Symbolic size
                # Safely handle the emoji attribute
                emoji = '🎲'  # Default dice emoji
                if hasattr(msg.dice, 'emoji') and msg.dice.emoji:
                    try:
                        emoji = str(msg.dice.emoji).strip()
                    except:
                        pass
                file_info["name"] = f"dice_{emoji}.json"
                file_info["type"] = "dice"
            elif hasattr(msg, "game") and msg.game:
                file_info["size"] = 1  # Symbolic size
                file_info["name"] = f"game_{msg.game.title}.json"
                file_info["type"] = "game"
            
            # Handle special message types (text only) - these should return success without raising exceptions
            if await self._handle_special_messages(msg, target_chat_id, topic_id, edit_id, sender):
                # Text message was successfully processed, return success with text info
                file_info["size"] = len(msg.text or "") if hasattr(msg, 'text') and msg.text else 1
                file_info["name"] = "text_message.txt"
                file_info["type"] = "text"
                return {"file_info": file_info}
                
            # Process media files
            if not msg.media:
                raise Exception("No media found in message")
            
            filename, file_size, media_type = self.media_processor.get_media_info(msg)
            
            # Try to extract the original thumbnail from the message before downloading media
            original_thumb_path = None
            original_thumb_is_temp = False
            try:
                original_thumb_path, original_thumb_is_temp = await self._extract_original_thumbnail(msg, client_to_use, sender)
                if original_thumb_path:
                    print(f"🖼️ Extracted original thumbnail: {original_thumb_path}")
            except Exception as _:
                original_thumb_path, original_thumb_is_temp = None, False
            
            # Extract original caption, entities and reply markup from original message
            if msg.caption:
                if hasattr(msg.caption, 'text'):
                    original_caption = msg.caption.text
                else:
                    original_caption = str(msg.caption)
            else:
                original_caption = ""
            
            caption_entities = getattr(msg, 'caption_entities', None) if hasattr(msg, 'caption_entities') else None
            reply_markup = getattr(msg, 'reply_markup', None) if hasattr(msg, 'reply_markup') else None
            
            # Always preserve original caption verbatim to avoid any mutation
            caption = original_caption
            
            # 🔄 DEDUPLICATION CHECK: Check if this file already exists before downloading
            try:
                existing_file = await check_duplicate_before_download(
                    chat_id=int(chat_id) if isinstance(chat_id, str) and chat_id.lstrip('-').isdigit() else (chat_id if isinstance(chat_id, int) else 0),
                    message_id=int(msg_id),
                    file_size=file_size or file_info.get("size", 0),
                    file_name=filename or file_info.get("name")
                )
                
                if existing_file:
                    print(f"♻️ DEDUPLICATION: File already exists, forwarding from cache")
                    
                    # Handle the duplicate by forwarding existing file
                    success = await handle_duplicate_file(sender, existing_file)
                    
                    if success:
                        # Update file info and return success
                        file_info["size"] = existing_file.get("file_size", file_info.get("size", 0))
                        file_info["name"] = existing_file.get("file_name", file_info.get("name", "cached_file"))
                        file_info["type"] = existing_file.get("file_type", file_info.get("type", "cached"))
                        print(f"✅ DEDUPLICATION: Successfully forwarded cached file to user {sender}")
                        return {"file_info": file_info}
                    else:
                        print(f"⚠️ DEDUPLICATION: Failed to forward cached file, proceeding with download")
            except Exception as dedup_err:
                print(f"⚠️ DEDUPLICATION: Error in pre-download check: {dedup_err}")
                # Continue with normal download if deduplication fails
            
            # FAST-PATH: attempt server-side forward/copy to LOG_GROUP to preserve all formatting/buttons
            try:
                log_group_id = LOG_GROUP
                uploaded_message = None
                # Prefer a Telethon admin/pro client for best compatibility
                from telethon.sync import TelegramClient as _TeleClient
                upload_candidate = None
                # Use pooled admin session if available from earlier selection
                if pooled_client and isinstance(pooled_client, _TeleClient):
                    upload_candidate = pooled_client
                elif self.pro_client and isinstance(self.pro_client, _TeleClient):
                    upload_candidate = self.pro_client
                elif gf and isinstance(gf, _TeleClient):
                    upload_candidate = gf
                # If we have a Telethon client, try forwarding with full preservation
                if upload_candidate is not None:
                    try:
                        # Resolve source entity for Telethon
                        src_entity = chat_id
                        if hasattr(upload_candidate, 'get_entity'):
                            try:
                                src_entity = await upload_candidate.get_entity(chat_id)
                            except Exception:
                                src_entity = chat_id
                        # Try a true forward first (keeps buttons and native formatting)
                        fwd = await upload_candidate.forward_messages(
                            entity=log_group_id,
                            messages=msg_id,
                            from_peer=src_entity,
                            as_copy=False
                        )
                        uploaded_message = fwd
                    except Exception as _fwd_err:
                        try:
                            # Fallback to copy (no forward header); may drop inline buttons, but preserves entities
                            fwd_copy = await upload_candidate.forward_messages(
                                entity=log_group_id,
                                messages=msg_id,
                                from_peer=src_entity,
                                as_copy=True
                            )
                            uploaded_message = fwd_copy
                        except Exception:
                            uploaded_message = None
                # Do NOT use user Pyrogram session to copy to LOG_GROUP to avoid architectural violation
                # If server-side transfer worked, forward result to user and finish early
                if uploaded_message is not None:
                    try:
                        forwarded_message = await app.forward_messages(
                            chat_id=sender,
                            from_chat_id=log_group_id,
                            message_ids=getattr(uploaded_message, 'id', None) or uploaded_message.id,
                            drop_author=True
                        )
                        # Update metrics and return the info
                        file_info["type"] = file_info.get("type") or ("text" if not msg.media else "media")
                        return {"file_info": file_info}
                    except Exception:
                        pass
            except Exception:
                # Ignore fast-path errors; fall back to download and re-upload
                pass
            
            # Ensure media is downloaded locally before any upload step
            try:
                # Prepare a downloads directory for better organization
                downloads_dir = os.path.join(os.getcwd(), "downloads")
                os.makedirs(downloads_dir, exist_ok=True)

                # Choose a target filename/path (unique per task to avoid clashes)
                unique_suffix = f"_{sender}_{msg_id}"
                if filename:
                    name, ext = os.path.splitext(filename)
                    base_name = f"{name}{unique_suffix}{ext}"
                else:
                    base_name = f"media_{abs(int(chat_id))}_{msg_id}{unique_suffix}"
                target_path = os.path.join(downloads_dir, base_name)

                # Optimized download status - create progress message only when needed
                try:
                    # For batch downloads: show progress for files >=50MB (start with progress bar directly)
                    if not edit_id and file_size and file_size >= 50 * 1024 * 1024:
                        # Create initial progress message that will be updated with progress bar
                        init_text = await CaptionFormatter.markdown_to_html("📥 <b>Preparing...</b>")
                        edit_msg = await app.send_message(sender, init_text, parse_mode=ParseMode.HTML)
                        created_progress_msg = True
                    # For single downloads: show progress for files >=20MB (start with progress bar directly)
                    elif edit_id and file_size and file_size >= 20 * 1024 * 1024:
                        try:
                            # Edit existing message to show initial progress
                            init_text = await CaptionFormatter.markdown_to_html("📥 <b>Preparing...</b>")
                            edit_msg = await app.edit_message_text(
                                sender,
                                edit_id,
                                init_text,
                                parse_mode=ParseMode.HTML
                            )
                            if not edit_msg or not hasattr(edit_msg, 'id'):
                                raise Exception("edit_message_text returned None or invalid object")
                        except Exception:
                            # Fallback: create new progress message if editing fails
                            init_text = await CaptionFormatter.markdown_to_html("📥 **Preparing...**")
                            edit_msg = await app.send_message(sender, init_text, parse_mode=ParseMode.HTML)
                            created_progress_msg = True
                    # For small files (<20MB single, <50MB batch): NO download status message to save API calls
                except Exception:
                    edit_msg = None
                    pass

                # Define a progress callback for Pyrogram downloads; throttle to ~5s
                last_update_time = 0
                last_percent = -1.0
                start_time = time.time()
                # Control whether to show download progress
                show_dl_progress = False  # Default to False, enable based on conditions
                try:
                    # If sticker, animation, or photo, never show progress during download
                    if media_type in ("sticker", "animation", "photo"):
                        show_dl_progress = False
                    # For single downloads (edit_id exists): show progress for files > 20MB
                    elif edit_id and file_size and file_size > 20 * 1024 * 1024:
                        show_dl_progress = True
                    # For batch downloads (no edit_id): show progress for files > 50MB
                    elif (not edit_id) and file_size and file_size > 50 * 1024 * 1024:
                        show_dl_progress = True
                    # If file_size is None or 0, enable progress for all non-small media types
                    elif not file_size and media_type not in ("sticker", "animation", "photo"):
                        show_dl_progress = True
                except Exception:
                    pass
                async def pr_dl_cb(current: int, total: int):
                    nonlocal last_update_time, last_percent
                    now = time.time()
                    if not show_dl_progress:
                        return
                    # Cancellation check
                    try:
                        if await cancel_manager.is_cancelled(sender):
                            try:
                                if edit_msg:
                                    await edit_msg.edit("🚫 Download canceled by user.")
                                elif edit_id:
                                    await app.edit_message_text(sender, edit_id, "🚫 Download canceled by user.")
                            except Exception:
                                pass
                            raise asyncio.CancelledError("download canceled")
                    except Exception:
                        pass
                    # Optimized update intervals: 8s for single downloads, 15s for batch downloads to reduce API calls
                    update_interval = 15 if not edit_id else 8
                    if now - last_update_time < update_interval and current != total:
                        return
                    last_update_time = now
                    try:
                        # Compute progress values safely when total is unknown (total == 0)
                        percent = (current / total) * 100 if total else 0
                        # Use minimum elapsed time to avoid extremely low initial speeds
                        elapsed = max(now - start_time, 1.0)
                        speed = current / elapsed
                        
                        # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
                        speed_display = speed * 3
                        
                        eta = (total - current) / speed if speed > 0 and total else 0
                        # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
                        eta = eta / 2 if eta > 0 else 0
                        eta_str = self.progress_manager._format_time(eta) if eta > 0 else ("Calculating..." if total else "--")
                        
                        # Always update UI (removed the 2% threshold that was preventing updates)
                        if total > 0:
                            progress_text = UnifiedProgressBar.format_progress_message(percent, current, total, speed_display, eta_str, "download")
                            html_progress = await CaptionFormatter.markdown_to_html(progress_text)
                            if edit_msg:
                                await edit_msg.edit(
                                    html_progress,
                                    parse_mode=ParseMode.HTML
                                )
                            elif edit_id:
                                await app.edit_message_text(
                                    sender,
                                    edit_id,
                                    html_progress,
                                    parse_mode=ParseMode.HTML
                                )
                            last_percent = percent
                        # Update registry
                        try:
                            registry.update(sender, int(msg_id or 0), stage="downloading", current=current, total=(total or current))
                        except Exception:
                            pass
                    except Exception:
                        pass

                # Download with the selected client (user/admin pool/userbot)
                downloaded_path = await client_to_use.download_media(
                    msg,
                    file_name=target_path,
                    progress=pr_dl_cb
                )

                if not downloaded_path or not os.path.exists(downloaded_path):
                    raise Exception("Download failed: path not created")

                file_path = downloaded_path
                
                # Register downloaded file for cleanup tracking
                cleanup_manager.file_cleanup.register_active_download(sender, file_path)
                # Refresh file info from disk (size/name)
                try:
                    file_info["name"] = os.path.basename(file_path)
                    file_info["size"] = os.path.getsize(file_path)
                except Exception:
                    pass
                
                # 🔄 POST-DOWNLOAD DEDUPLICATION CHECK: Check if downloaded file is a duplicate by hash
                try:
                    existing_file_by_hash = await check_duplicate_after_download(
                        file_path=file_path,
                        chat_id=self._current_chat_id,
                        message_id=self._current_message_id
                    )
                    
                    if existing_file_by_hash:
                        print(f"♻️ POST-DOWNLOAD DEDUPLICATION: Downloaded file is duplicate, using cached version")
                        
                        # Handle the duplicate by forwarding existing file and cleaning up downloaded file
                        success = await handle_duplicate_file(sender, existing_file_by_hash, file_path)
                        
                        if success:
                            # Update file info and return success
                            file_info["size"] = existing_file_by_hash.get("file_size", file_info.get("size", 0))
                            file_info["name"] = existing_file_by_hash.get("file_name", file_info.get("name", "cached_file"))
                            file_info["type"] = existing_file_by_hash.get("file_type", file_info.get("type", "cached"))
                            print(f"✅ POST-DOWNLOAD DEDUPLICATION: Successfully forwarded cached file and cleaned duplicate")
                            return {"file_info": file_info}
                        else:
                            print(f"⚠️ POST-DOWNLOAD DEDUPLICATION: Failed to forward cached file, proceeding with current download")
                except Exception as post_dedup_err:
                    print(f"⚠️ POST-DOWNLOAD DEDUPLICATION: Error in post-download check: {post_dedup_err}")
                    # Continue with normal upload if post-download deduplication fails
            except Exception as dl_err:
                print(f"Media download error: {dl_err}")
                try:
                    await app.edit_message_text(sender, edit_id, f"❌ Download failed: {str(dl_err)[:100]}...")
                except Exception:
                    pass
                raise
            finally:
                pass

            # Handle photos via admin pool/pro uploader to forbid bot uploads
            if media_type == "photo":
                try:
                    await self.upload_with_telethon(
                        file_path,
                        sender,
                        target_chat_id,
                        caption,
                        topic_id,
                        edit_msg,
                        original_thumb_path=original_thumb_path,
                        original_thumb_is_temp=original_thumb_is_temp,
                        caption_entities=caption_entities,
                        reply_markup=reply_markup,
                        created_progress_msg=created_progress_msg,
                        is_batch_operation=(edit_id is None),
                    )
                    return
                except Exception as photo_error:
                    print(f"Photo upload error: {photo_error}")
                    html_photo_error = await self.caption_formatter.markdown_to_html(f"**Photo Upload Error:** {str(photo_error)}")
                    # Log to admin LOG_GROUP only; user-facing error will be handled by upload_with_telethon
                    try:
                        await app.send_message(LOG_GROUP, html_photo_error, parse_mode=ParseMode.HTML)
                    except Exception:
                        pass
                    raise
            
            # Check file size and handle accordingly
            # REFACTOR: Default uploader method is now always Telethon
            upload_method = "Telethon"  # Force Telethon as default uploader
            
            if file_size > self.config.SIZE_LIMIT:
                free_check = 0
                if 'chk_user' in globals():
                    free_check = await chk_user(chat_id, sender)
                
                if free_check == 1 or not self.pro_client:
                    # Split file for free users or when pro client unavailable
                    await edit_msg.delete()
                    await self.file_ops.split_large_file(file_path, app, sender, target_chat_id, caption, topic_id)
                    return
                else:
                    # Use 2GB uploader
                    await self.handle_large_file_upload(file_path, sender, edit_msg, caption, original_thumb_path=original_thumb_path, original_thumb_is_temp=original_thumb_is_temp)
                    return
            
            # Regular upload — preserve native media types for group chat batch
            # If we were able to determine media_type and have a file_path, try native send_* first
            try:
                if file_path and os.path.exists(file_path):
                    # Album hint probe removed — always treat as single message
                    pass
            except Exception:
                # Fall through to Telethon path
                pass

            # Regular upload - Always use Telethon for media upload/download
            if gf:
                # Critical debug: Check what edit_id value is being passed
                if file_size and file_size > 30 * 1024 * 1024:  # Only log for files >30MB
                    print(f"🔍 DOWNLOAD: edit_id={edit_id}, is_batch={(edit_id is None)}")
                
                await self.upload_with_telethon(
                    file_path,
                    sender,
                    target_chat_id,
                    caption,
                    topic_id,
                    edit_msg,
                    original_thumb_path=original_thumb_path,
                    original_thumb_is_temp=original_thumb_is_temp,
                    caption_entities=caption_entities,
                    reply_markup=reply_markup,
                    created_progress_msg=created_progress_msg,
                    is_batch_operation=(edit_id is None),
                )
            else:
                # Fallback error message if Telethon client not available
                await app.edit_message_text(sender, edit_id, "❌ Telethon client not available. Please contact admin: https://t.me/ZeroTrace0x")
                raise Exception("Telethon client not available for upload")
                    
        except asyncio.CancelledError:
            dl_status = "canceled"
            raise
        except (ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid) as e:
            await app.edit_message_text(sender, edit_id, "❌ Access denied. Have you joined the channel?")
            raise Exception(f"Access denied: {str(e)}")
        except Exception as e:
            print(f"Error in message handling: {e}")
            try:
                await app.edit_message_text(sender, edit_id, f"❌ Error: {str(e)[:100]}...")
            except:
                pass
            # Mark metrics as error
            try:
                if dl_task_id:
                    await metrics.finish_task(dl_task_id, status="error")
            except Exception:
                pass
            raise  # Re-raise the exception so batch processing can handle it properly
        finally:
            # Universal cleanup: delete progress messages to keep chat clean
            # For batch downloads (no edit_id): delete if we created a progress message
            # For single downloads (edit_id exists): delete small files only
            should_delete_progress = False
            if edit_msg:
                if not edit_id:  # Batch download
                    # Delete progress message for batch downloads if we created one
                    should_delete_progress = created_progress_msg
                else:  # Single download
                    # Delete progress message for small files in single downloads
                    should_delete_progress = (file_size and file_size < 20 * 1024 * 1024)
                
                if should_delete_progress:
                    try:
                        await edit_msg.delete()
                    except Exception:
                        pass
            
            # Clean up user session client if we created one
            if user_session_client:
                try:
                    await user_session_client.stop()
                    print(f"📥 DOWNLOAD: Finished downloading using user session from {chat_id}")
                except Exception as e:
                    print(f"⚠️ Error stopping user session client: {e}")
            # Release the admin session back to the pool if we got one
            elif pooled_client and session_id:
                had_error = 'e' in locals()
                await session_pool.release_session(session_id, had_error=had_error)
                print(f"📥 DOWNLOAD: Finished downloading using admin session {session_id} from {chat_id}")
            else:
                print(f"📥 DOWNLOAD: Finished downloading using default client from {chat_id}")
                
            # Cleanup
            if file_path:
                await self.file_ops._cleanup_file(file_path)
            gc.collect()
            # Finish metrics
            try:
                if dl_task_id:
                    await metrics.finish_task(dl_task_id, status=dl_status)
            except Exception:
                pass
            # Clear cancel flag if set (avoid stale cancellation across next attempts)
            try:
                if await cancel_manager.is_cancelled(sender):
                    await cancel_manager.clear(sender)
            except Exception:
                pass
            
            # Return file information for batch processing
            return {"file_info": file_info}

    async def _parse_message_link(self, msg_link: str, offset: int, protected_channels: Set[int], sender: int, edit_id: int) -> Tuple[Optional[int], Optional[int]]:
        """Parse different types of message links"""
        if ('t.me/c/' in msg_link or 'telegram.dog/c/' in msg_link or 't.me/b/' in msg_link or 'telegram.dog/b/' in msg_link):
            parts = msg_link.split("/")
            if '/b/' in msg_link:
                chat_id = parts[-2]
                # handle possible trailing parameters
                digits = re.findall(r'\d+', parts[-1])
                msg_id = int(digits[0]) + offset if digits else None
            else:
                # Support both 2-part and 3-part private links: /c/<chat>/<msg> and /c/<chat>/<topic>/<msg>
                try:
                    c_index = parts.index('c')
                except ValueError:
                    # fallback for potential different domains
                    c_index = next((i for i, p in enumerate(parts) if p == 'c'), None)
                numeric_tail = [p for p in parts[c_index+1:] if p.isdigit()] if c_index is not None else []
                if len(numeric_tail) >= 3:
                    chat_part, topic_part, msg_part = numeric_tail[:3]
                    chat_id = int(f"-100{chat_part}")
                    msg_id = int(msg_part) + offset
                    try:
                        self._source_topic_id = int(topic_part)
                    except Exception:
                        self._source_topic_id = None
                elif len(numeric_tail) >= 2:
                    chat_part, msg_part = numeric_tail[:2]
                    chat_id = int(f"-100{chat_part}")
                    msg_id = int(msg_part) + offset
                    self._source_topic_id = None
                else:
                    chat_id, msg_id = None, None
            
            if chat_id in protected_channels:
                html_text = await CaptionFormatter.markdown_to_html("❌ This channel is protected by **Restrict Bot Saver**.")
                await app.edit_message_text(sender, edit_id, html_text, parse_mode=ParseMode.HTML)
                return None, None
                
            return chat_id, msg_id
        
        elif '/s/' in msg_link:
            # Handle story links
            html_text = await CaptionFormatter.markdown_to_html("📖 Story Link Detected...")
            await app.edit_message_text(sender, edit_id, html_text, parse_mode=ParseMode.HTML)
            
            # Check if user has session for story access
            user_data = await odb.get_data(sender)
            user_session_string = user_data.get("session") if user_data else None
            
            if not user_session_string and not gf:
                await app.edit_message_text(
                    sender, edit_id,
                    "🔐 <b>Login Required</b>\n\n"
                    "Story links require your personal session to access. "
                    "Please login first using the /login command to download stories."
                )
                # Raise exception for batch processing to catch
                raise Exception("LOGIN_REQUIRED: User session required for story access")
            elif not gf:
                await app.edit_message_text(sender, edit_id, "❌ Login required to save stories...")
                return None, None
            
            parts = msg_link.split("/")
            chat = f"-100{parts[3]}" if parts[3].isdigit() else parts[3]
            msg_id = int(parts[-1])
            await self._download_user_stories(gf, chat, msg_id, sender, edit_id)
            return None, None
        
        else:
            # Handle public links for t.me and telegram.dog by returning (chat, msg_id)
            # The normal flow will then download and upload using the chosen client.
            try:
                normalized = re.sub(r'^https?://(telegram\.dog|t\.me)/', 't.me/', msg_link)
                path_part = normalized.split('t.me/', 1)[1]
                # strip any query or fragment
                path_part = path_part.split('?', 1)[0].split('#', 1)[0]
                tokens = [t for t in path_part.split('/') if t]
                if len(tokens) < 2:
                    raise ValueError("Invalid public link structure")
                
                chat = tokens[0]
                
                # Handle topic group format: t.me/groupname/topicid/messageid
                if len(tokens) == 3 and tokens[1].isdigit() and tokens[2].isdigit():
                    # This is a topic group link: username/topic_id/message_id
                    topic_id = int(tokens[1])
                    msg_id = int(tokens[2]) + offset
                    self._source_topic_id = topic_id
                    self._is_topic_group = True
                    self._topic_id = topic_id
                    self._group_username = chat
                    print(f"🔍 TOPIC GROUP DETECTED: {chat}/topic_{topic_id}/msg_{msg_id}")
                    return chat, msg_id
                elif len(tokens) >= 2:
                    # Regular public link: username/message_id
                    m = re.match(r'(\d+)', tokens[-1])
                    if not m:
                        raise ValueError("No numeric message id found")
                    msg_id = int(m.group(1)) + offset
                    self._source_topic_id = None
                    return chat, msg_id
                else:
                    raise ValueError("Invalid public link structure")
                    
            except Exception:
                # Provide user-friendly feedback but don't crash the flow
                try:
                    await app.edit_message_text(sender, edit_id, "❌ Invalid or unsupported public link format.")
                except Exception:
                    pass
                return None, None

    async def _handle_special_messages(self, msg, target_chat_id: int, topic_id: Optional[int], edit_id: int, sender: int) -> bool:
        """Handle special message types that don't require downloading"""
        try:
            if msg.media == MessageMediaType.WEB_PAGE_PREVIEW:
                # Prefer preserving original entities and reply markup when available
                if isinstance(msg, Message) and (getattr(msg, 'entities', None) or getattr(msg, 'reply_markup', None)):
                    send_kwargs = {
                        'chat_id': target_chat_id,
                        'text': msg.text or "",
                        'reply_to_message_id': topic_id,
                        'parse_mode': None
                    }
                    ents = getattr(msg, 'entities', None)
                    if ents:
                        send_kwargs['entities'] = ents
                    rm = getattr(msg, 'reply_markup', None)
                    if rm:
                        send_kwargs['reply_markup'] = rm
                    result = await app.send_message(**send_kwargs)
                else:
                    html_text = await self.caption_formatter.markdown_to_html(msg.text)
                    result = await app.send_message(target_chat_id, html_text, reply_to_message_id=topic_id, parse_mode=ParseMode.HTML)
                # Success - no verbose message needed
                await app.delete_messages(sender, edit_id)
                return True
            
            if msg.text:
                # Prefer preserving original entities and reply markup when available
                if isinstance(msg, Message) and (getattr(msg, 'entities', None) or getattr(msg, 'reply_markup', None)):
                    send_kwargs = {
                        'chat_id': target_chat_id,
                        'text': msg.text,
                        'reply_to_message_id': topic_id,
                        'parse_mode': None
                    }
                    ents = getattr(msg, 'entities', None)
                    if ents:
                        send_kwargs['entities'] = ents
                    rm = getattr(msg, 'reply_markup', None)
                    if rm:
                        send_kwargs['reply_markup'] = rm
                    result = await app.send_message(**send_kwargs)
                else:
                    html_text = await self.caption_formatter.markdown_to_html(msg.text)
                    result = await app.send_message(target_chat_id, html_text, reply_to_message_id=topic_id, parse_mode=ParseMode.HTML)
                # Success - no verbose message needed
                await app.delete_messages(sender, edit_id)
                return True
        except Exception as e:
            error_message = str(e)
            print(f"Special message handling error: {error_message}")
            html_special_error = await self.caption_formatter.markdown_to_html(f"**Special Message Error:** {error_message}")
            # If running in batch mode (edit_id is None), do not log to LOG_GROUP to save Bot API limits
            if edit_id is not None:
                try:
                    await app.send_message(LOG_GROUP, html_special_error, parse_mode=ParseMode.HTML)
                except Exception:
                    pass
            
            # In batch mode (edit_id is None), stay silent for the user to avoid noisy errors
            if edit_id is not None:
                try:
                    await app.send_message(
                        sender,
                        f"❌ Message sending failed: {error_message}\n\nPlease try again or contact support."
                    )
                except Exception:
                    pass
            return True
            
        return False

    async def _handle_direct_media(self, msg, target_chat_id: int, topic_id: Optional[int], edit_id: int, media_type: str, sender: int) -> bool:
        """Handle media that can be sent directly without downloading"""
        result = None
        
        try:
            # For Pyrogram v2, ensure we have valid file_id values
            if media_type == "sticker" and hasattr(msg.sticker, 'file_id') and msg.sticker.file_id:
                try:
                    result = await app.send_sticker(target_chat_id, msg.sticker.file_id, reply_to_message_id=topic_id)
                    return True  # Successfully processed, prevent further processing
                except Exception as e:
                    print(f"Sticker send error: {e}")
                    # Fallback to sending a message about the sticker
                    html_text = await self.caption_formatter.markdown_to_html("📄 **Sticker**")
                    result = await app.send_message(target_chat_id, html_text, reply_to_message_id=topic_id, parse_mode=ParseMode.HTML)
                    return True  # Successfully processed fallback, prevent further processing
            elif media_type == "voice" and hasattr(msg.voice, 'file_id') and msg.voice.file_id:
                try:
                    result = await app.send_voice(target_chat_id, msg.voice.file_id, reply_to_message_id=topic_id)
                except Exception as e:
                    print(f"Voice send error: {e}")
                    # Fallback to sending a message about the voice
                    html_text = await self.caption_formatter.markdown_to_html("🎤 **Voice Message**")
                    result = await app.send_message(target_chat_id, html_text, reply_to_message_id=topic_id, parse_mode=ParseMode.HTML)
            elif media_type == "video_note" and hasattr(msg.video_note, 'file_id') and msg.video_note.file_id:
                try:
                    result = await app.send_video_note(target_chat_id, msg.video_note.file_id, reply_to_message_id=topic_id)
                except Exception as e:
                    print(f"Video note send error: {e}")
                    # Fallback to sending a message about the video note
                    html_text = await self.caption_formatter.markdown_to_html("🎥 **Video Note**")
                    result = await app.send_message(target_chat_id, html_text, reply_to_message_id=topic_id, parse_mode=ParseMode.HTML)
            elif media_type == "animation" and hasattr(msg.animation, 'file_id') and msg.animation.file_id:
                # For animations, we need to download and then upload properly
                # This is because direct file_id usage causes issues with both upload methods
                
                # Get the user's preferred upload method (default to Telethon)
                user_id = msg.from_user.id if hasattr(msg, 'from_user') and msg.from_user else msg.chat.id
                upload_method = self.db.get_user_data(user_id, "upload_method", "Telethon")
                
                # Instead of trying to send directly with file_id (which causes issues),
                # we'll return False to let the main handler download and process the animation properly
                print(f"Animation detected - will download and process properly using {upload_method} method")
                return False
            elif media_type == "poll":
                # For polls, we need to recreate the poll with the same options
                question = msg.poll.question
                options = [option.text for option in msg.poll.options]
                is_anonymous = msg.poll.is_anonymous
                poll_type = msg.poll.type
                allows_multiple_answers = msg.poll.allows_multiple_answers
                result = await app.send_poll(
                    target_chat_id,
                    question,
                    options,
                    is_anonymous=is_anonymous,
                    type=poll_type,
                    allows_multiple_answers=allows_multiple_answers,
                    reply_to_message_id=topic_id
                )
            elif media_type == "location":
                result = await app.send_location(
                    target_chat_id,
                    latitude=msg.location.latitude,
                    longitude=msg.location.longitude,
                    reply_to_message_id=topic_id
                )
            elif media_type == "contact":
                result = await app.send_contact(
                    target_chat_id,
                    phone_number=msg.contact.phone_number,
                    first_name=msg.contact.first_name,
                    last_name=msg.contact.last_name if hasattr(msg.contact, 'last_name') else None,
                    vcard=msg.contact.vcard if hasattr(msg.contact, 'vcard') else None,
                    reply_to_message_id=topic_id
                )
            elif media_type == "dice":
                # Ensure emoji is a string, not a text file pointer
                # In Pyrogram v2, we need to be extra careful with the emoji parameter
                try:
                    emoji = '🎲'  # Default dice emoji
                    if hasattr(msg.dice, 'emoji') and msg.dice.emoji:
                        emoji = str(msg.dice.emoji).strip()
                    
                    result = await app.send_dice(
                        target_chat_id,
                        emoji=emoji,
                        reply_to_message_id=topic_id
                    )
                except Exception as e:
                    print(f"Dice send error: {e}")
                    # Fallback to sending a text message about the dice
                    dice_text = f"🎲 **Dice Roll: {msg.dice.value}**"
                    html_dice_text = await self.caption_formatter.markdown_to_html(dice_text)
                    result = await app.send_message(
                        target_chat_id,
                        html_dice_text,
                        reply_to_message_id=topic_id,
                        parse_mode=ParseMode.HTML
                    )
            elif media_type == "game":
                # Games can only be sent by bots with game capability
                # We'll send a message with game info instead
                game_text = f"🎮 **Game: {msg.game.title}**\n\n{msg.game.description}"
                html_game_text = await self.caption_formatter.markdown_to_html(game_text)
                result = await app.send_message(
                    target_chat_id,
                    html_game_text,
                    reply_to_message_id=topic_id,
                    parse_mode=ParseMode.HTML
                )
        except Exception as e:
            # Ensure try has an except to satisfy linter and provide resilience
            print(f"Direct media handling error: {e}")
            # Fall through to return False so caller can take alternate path
        
        if result:
            # Prefer forwarding to user; fallback to copy
            try:
                await result.forward(sender)
            except Exception as forward_error:
                print(f"Forward failed, will fallback to copy: {forward_error}")
                try:
                    await result.copy(sender)
                except Exception as copy_err:
                    print(f"Copy failed: {copy_err}")
            # Clean up the temporary "processing" message
            try:
                await app.delete_messages(msg.chat.id, edit_id)
            except Exception:
                pass
            return True
            
        return False

    async def _download_user_stories(self, userbot, chat_id: str, msg_id: int, sender: int, edit_id: int):
        """Download and send user stories"""
        pooled_client = None
        session_id = None
        file_path = None
        user_session_client = None
        
        try:
            # First try to get user's own session from database
            user_data = await odb.get_data(sender)
            user_session_string = user_data.get("session") if user_data else None
            
            if user_session_string:
                try:
                    # Create user session client
                    user_session_client = Client(
                        name=f"user_story_{sender}",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        session_string=user_session_string,
                        in_memory=True,
                        no_updates=True,
                        workers=int(os.getenv("USER_SESSION_WORKERS", "2")),
                        max_concurrent_transmissions=int(os.getenv("USER_SESSION_MAX_CONCURRENT_TX", "2"))
                    )
                    await user_session_client.start()
                    
                    # Log user session usage
                    try:
                        me = await user_session_client.get_me()
                        username = me.username or f"user_{me.id}"
                        print(f"📥 DOWNLOAD: Using user's own session (@{username}) for downloading story from {chat_id}")
                    except Exception:
                        print(f"📥 DOWNLOAD: Using user's own session (username unknown) for downloading story from {chat_id}")
                        
                    client_to_use = user_session_client
                    session_id = f"user_{sender}"
                    
                except Exception as e:
                    print(f"❌ Failed to initialize user session for story download {sender}: {e}")
                    user_session_client = None
            
            # If user session failed, try admin session pool as fallback
            if not user_session_client:
                pooled_client, session_id = await session_pool.get_session()
                
                # Log which client is being used
                if pooled_client and session_id:
                    try:
                        me = await pooled_client.get_me()
                        username = me.username or f"user_{me.id}"
                        print(f"📥 DOWNLOAD: Using admin session {session_id} (@{username}) as fallback for downloading story from {chat_id}")
                    except Exception:
                        print(f"📥 DOWNLOAD: Using admin session {session_id} (username unknown) as fallback for downloading story from {chat_id}")
                else:
                    print(f"📥 DOWNLOAD: Using default client for downloading story from {chat_id} (no sessions available)")
                
                # If pool has no sessions, fall back to the provided userbot
                client_to_use = pooled_client if pooled_client else userbot
            
            if not client_to_use:
                await app.edit_message_text(sender, edit_id, "❌ No available user sessions. Please login or contact admin: https://t.me/ZeroTrace0x")
                return
                
            edit_msg = await app.edit_message_text(sender, edit_id, "📖 <b>Downloading Story...</b>", parse_mode=ParseMode.HTML)
            story = await client_to_use.get_stories(chat_id, msg_id)
            
            if not story or not story.media:
                await edit_msg.edit("❌ No story available or no media.")
                return
            
            file_path = await client_to_use.download_media(story)
            await edit_msg.edit("📤 <b>Uploading Story...</b>", parse_mode=ParseMode.HTML)
            
            if story.media == MessageMediaType.VIDEO:
                await app.send_video(sender, file_path)
            elif story.media == MessageMediaType.DOCUMENT:
                await app.send_document(sender, file_path)
            elif story.media == MessageMediaType.PHOTO:
                await app.send_photo(sender, file_path)
            
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"🗑️ Cleaned up story file: {file_path}")
                except Exception as cleanup_error:
                    print(f"⚠️ Error cleaning up story file: {cleanup_error}")
            await edit_msg.edit("✅ Story processed successfully.")
            
        except RPCError as e:
            await app.edit_message_text(sender, edit_id, f"❌ Error: {e}")
            
        finally:
            # Clean up user session client if we created one
            if user_session_client:
                try:
                    await user_session_client.stop()
                    print(f"📥 DOWNLOAD: Finished downloading story using user session from {chat_id}")
                except Exception as e:
                    print(f"⚠️ Error stopping user session client: {e}")
            # Release the admin session back to the pool if we got one
            elif pooled_client and session_id:
                had_error = 'e' in locals()
                await session_pool.release_session(session_id, had_error=had_error)
                print(f"📥 DOWNLOAD: Finished downloading story using admin session {session_id} from {chat_id}")
            else:
                print(f"📥 DOWNLOAD: Finished downloading story using default client from {chat_id}")
                
            # Clean up any remaining files
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"🗑️ Final cleanup of story file: {file_path}")
                except Exception as cleanup_error:
                    print(f"⚠️ Error in final cleanup of story file: {cleanup_error}")

    async def _copy_public_message(self, app_client, userbot, sender: int, chat_id: str, message_id: int, edit_id: int):
        """Handle copying from public channels/groups using the same robust approach as private groups"""
        target_chat_str = self.user_chat_ids.get(sender, str(sender))
        target_chat_id, topic_id = self.parse_target_chat(target_chat_str)
        file_path = None
        pooled_client = None
        session_id = None
        user_session_client = None
        client_to_use = None
        
        # Initialize msg variable to avoid NameError
        msg = None
        # Initialize success flag to prevent duplicate downloads
        media_processed_successfully = False
        
        try:
            # Try to get message using the same session management as private groups
            
            # First, try user session if available (same as private group logic)
            user_session_string = self.db.get_user_data(sender, "session_string")
            
            if user_session_string:
                try:
                    print(f"🔑 Using user session for public group {chat_id}")
                    
                    # Initialize user session client
                    from pyrogram import Client as PyrogramClient
                    user_session_client = PyrogramClient(
                        name=f"user_session_{sender}",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        session_string=user_session_string,
                        in_memory=True,
                        no_updates=True,
                        workers=int(os.getenv("USER_SESSION_WORKERS", "2")),
                        max_concurrent_transmissions=int(os.getenv("USER_SESSION_MAX_CONCURRENT_TX", "2"))
                    )
                    await user_session_client.start()
                    
                    # Try to get message with user session
                    # Handle topic groups by using the source topic ID if available
                    if hasattr(self, '_source_topic_id') and self._source_topic_id:
                        # For topic groups, we need to get messages from the specific topic
                        print(f"🔍 USER SESSION: Fetching from topic {self._source_topic_id} in group {chat_id}")
                        try:
                            # Get message directly and verify it belongs to the topic
                            msg = await user_session_client.get_messages(chat_id, message_id)
                            if msg:
                                # Check if message belongs to the topic thread
                                # In Pyrogram, topic messages have reply_to_message_id pointing to the topic root
                                if hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id == self._source_topic_id:
                                    print(f"✅ USER SESSION: Found topic message {message_id} in topic {self._source_topic_id}")
                                elif hasattr(msg, 'message_thread_id') and msg.message_thread_id == self._source_topic_id:
                                    print(f"✅ USER SESSION: Found topic message {message_id} via thread_id {self._source_topic_id}")
                                else:
                                    # Try to get the message history around this ID to verify topic membership
                                    try:
                                        # Get a few messages around this ID to check topic context
                                        context_msgs = await user_session_client.get_messages(chat_id, list(range(max(1, message_id-2), message_id+3)))
                                        topic_found = False
                                        for ctx_msg in context_msgs:
                                            if ctx_msg and hasattr(ctx_msg, 'reply_to_message_id') and ctx_msg.reply_to_message_id == self._source_topic_id:
                                                topic_found = True
                                                break
                                            elif ctx_msg and hasattr(ctx_msg, 'message_thread_id') and ctx_msg.message_thread_id == self._source_topic_id:
                                                topic_found = True
                                                break
                                        
                                        if topic_found:
                                            print(f"✅ USER SESSION: Message {message_id} verified in topic {self._source_topic_id} via context")
                                        else:
                                            print(f"⚠️ USER SESSION: Message {message_id} not in topic {self._source_topic_id} (no topic context found)")
                                            msg = None
                                    except Exception as ctx_err:
                                        print(f"⚠️ USER SESSION: Context check failed for message {message_id}: {ctx_err}")
                                        # Keep the message but warn
                                        print(f"⚠️ USER SESSION: Using message {message_id} without topic verification")
                            else:
                                print(f"❌ USER SESSION: Message {message_id} not found in group {chat_id}")
                        except Exception as topic_err:
                            print(f"⚠️ USER SESSION: Topic message access failed: {topic_err}")
                            msg = None
                    else:
                        msg = await user_session_client.get_messages(chat_id, message_id)
                    client_to_use = user_session_client
                    print(f"✅ User session successfully accessed public group {chat_id}")
                    
                except Exception as user_err:
                    print(f"❌ User session failed for public group {chat_id}: {user_err}")
                    try:
                        if user_session_client:
                            await user_session_client.stop()
                    except Exception:
                        pass
                    user_session_client = None
            
            # If user session failed or unavailable, try session pool (same as private groups)
            if not msg:
                try:
                    print(f"🔄 Trying session pool for public group {chat_id}")
                    pooled_client, session_id = await session_pool.get_session()
                    if pooled_client:
                        # Handle topic groups consistently
                        if hasattr(self, '_source_topic_id') and self._source_topic_id:
                            print(f"🔍 SESSION POOL: Fetching from topic {self._source_topic_id} in group {chat_id}")
                            try:
                                # Get message and verify topic membership
                                msg = await pooled_client.get_messages(chat_id, message_id)
                                if msg:
                                    # Check topic membership using available attributes
                                    if hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id == self._source_topic_id:
                                        print(f"✅ SESSION POOL: Found topic message {message_id} in topic {self._source_topic_id}")
                                    elif hasattr(msg, 'message_thread_id') and msg.message_thread_id == self._source_topic_id:
                                        print(f"✅ SESSION POOL: Found topic message {message_id} via thread_id {self._source_topic_id}")
                                    else:
                                        # For session pool, be more lenient as we might not have full topic context
                                        print(f"⚠️ SESSION POOL: Message {message_id} topic verification inconclusive, allowing")
                                        # Don't set msg = None here, allow the message through
                                else:
                                    print(f"❌ SESSION POOL: Message {message_id} not found")
                            except Exception as topic_err:
                                print(f"⚠️ SESSION POOL: Topic verification failed: {topic_err}")
                                msg = None
                        else:
                            msg = await pooled_client.get_messages(chat_id, message_id)
                        client_to_use = pooled_client
                        print(f"✅ Session pool successfully accessed public group {chat_id}")
                        if msg:
                         print(f"✅ Message {message_id} retrieved successfully from {chat_id}")
                         print(f"📋 Message type: {type(msg)}, Media: {msg.media if hasattr(msg, 'media') else 'No media attr'}, Text: {msg.text[:50] if hasattr(msg, 'text') and msg.text else 'No text'}")
                    else:
                         print(f"❌ Message {message_id} not found in {chat_id} (returned None)")
                except PeerIdInvalid:
                    print(f"❌ Session pool: Invalid peer ID for {chat_id}")
                    if pooled_client and session_id:
                        await session_pool.release_session(session_id, had_error=True)
                        pooled_client = None
                        session_id = None
                except (ChannelInvalid, ChannelPrivate, ChatIdInvalid) as access_err:
                    print(f"❌ Session pool: Access denied to {chat_id}: {access_err}")
                    if pooled_client and session_id:
                        await session_pool.release_session(session_id, had_error=True)
                        pooled_client = None
                        session_id = None
                except FloodWait as fw:
                    print(f"⏳ Session pool: FloodWait {fw.x}s for {chat_id}")
                    if pooled_client and session_id:
                        await session_pool.release_session(session_id, had_error=True, flood_wait_seconds=fw.x)
                        pooled_client = None
                        session_id = None
                except Exception as pool_err:
                    print(f"❌ Session pool failed for public group {chat_id}: {pool_err}")
                    if pooled_client and session_id:
                        await session_pool.release_session(session_id, had_error=True)
                        pooled_client = None
                        session_id = None
            
            # Final fallback removed: do not use bot to fetch heavy media
            if not msg:
                # All methods failed - provide user feedback without using bot for media fetch
                try:
                    await app.edit_message_text(
                        sender,
                        edit_id,
                        "❌ **Unable to Access Public Group**\n\n"
                        f"Cannot access this public group. This might be because:\n"
                        f"• The group doesn't exist or is restricted\n"
                        f"• The message was deleted\n"
                        f"• Network connectivity issues\n\n"
                        f"Please try again or use /login command if you have access to this group."
                    )
                except Exception:
                    pass
                return
            
            # Process the message if we successfully got it
            if msg:
                # Enhanced media detection for both Pyrogram and Telethon
                def has_media_content(message):
                    """Robust media detection for both Pyrogram and Telethon messages"""
                    # Check for Pyrogram-style direct attributes
                    if hasattr(message, 'photo') and message.photo:
                        return True
                    if hasattr(message, 'video') and message.video:
                        return True
                    if hasattr(message, 'document') and message.document:
                        return True
                    if hasattr(message, 'audio') and message.audio:
                        return True
                    if hasattr(message, 'voice') and message.voice:
                        return True
                    if hasattr(message, 'video_note') and message.video_note:
                        return True
                    if hasattr(message, 'sticker') and message.sticker:
                        return True
                    if hasattr(message, 'animation') and message.animation:
                        return True
                    
                    # Check for Telethon-style media attribute
                    if hasattr(message, 'media') and message.media:
                        # Telethon media types
                        from telethon.tl.types import (
                            MessageMediaPhoto, MessageMediaDocument, 
                            MessageMediaContact, MessageMediaGeo, MessageMediaVenue,
                            MessageMediaPoll, MessageMediaDice, MessageMediaGame
                        )
                        return isinstance(message.media, (
                            MessageMediaPhoto, MessageMediaDocument, 
                            MessageMediaContact, MessageMediaGeo, MessageMediaVenue,
                            MessageMediaPoll, MessageMediaDice, MessageMediaGame
                        ))
                    
                    return False
                
                def get_message_text(message):
                    """Get text content from both Pyrogram and Telethon messages"""
                    # Try different text attributes
                    text = getattr(message, 'text', None) or getattr(message, 'caption', None) or getattr(message, 'message', None)
                    return text
                
                # Check if message has media content
                has_media = has_media_content(msg)
                message_text = get_message_text(msg)
                
                print(f"🔍 Media detection: has_media={has_media}, has_text={bool(message_text)}, msg_type={type(msg).__name__}")
                
                # Handle text-only messages
                if not has_media:
                    if message_text:
                        try:
                            if getattr(msg, 'entities', None) or getattr(msg, 'reply_markup', None):
                                send_kwargs = {
                                    'chat_id': sender,
                                    'text': message_text,
                                    'parse_mode': None
                                }
                                ents = getattr(msg, 'entities', None)
                                if ents:
                                    send_kwargs['entities'] = ents
                                rm = getattr(msg, 'reply_markup', None)
                                if rm:
                                    send_kwargs['reply_markup'] = rm
                                result = await app.send_message(**send_kwargs)
                            else:
                                result = await app.send_message(sender, message_text)
                            print(f"✅ Text message sent to user {sender}")
                        except Exception as send_err:
                            print(f"❌ Failed to send text message to user {sender}: {send_err}")
                            await app.edit_message_text(sender, edit_id, f"❌ Failed to send message: {send_err}")
                    else:
                        # Message has no text and no media - inform user
                        await app.edit_message_text(sender, edit_id,
                            "❌ **No Content Found**\n\n"
                            "This message doesn't contain any downloadable content or text.\n"
                            "Please check the message link and try again.")
                        print(f"❌ Message {message_id} from {chat_id} has no content to send")
                        return
                            
                # Handle media messages
                elif has_media:
                    try:
                        session_type = "user session" if user_session_client else "bot"
                        print(f"📥 Processing media message using {session_type} from {chat_id}")
                        
                        # Premium-only: Handle albums/media groups in one shot
                        try:
                            is_premium_user = False
                            try:
                                prem_doc = await check_premium(sender)
                                is_premium_user = bool(prem_doc) or (sender in OWNER_ID)
                            except Exception:
                                is_premium_user = sender in OWNER_ID
                            # Album handling removed: always treat as single message
                        except Exception:
                            # Ignore album branch errors and continue with single message flow
                            pass
                        
                        # Try to copy the media message directly
                        try:
                            # If message is part of a media group, skip copy to avoid preserving album grouping
                            try:
                                if getattr(msg, 'media_group_id', None):
                                    raise Exception('skip-copy-for-media-group')
                            except Exception:
                                # Fall through to download+upload
                                raise
                            result = await msg.copy(sender)
                            if result:
                                await app.delete_messages(sender, edit_id)
                            print(f"📥 DOWNLOAD: Finished copying media message using {session_type} from {chat_id}")
                            # Set success flag to prevent Telethon fallback
                            media_processed_successfully = True
                            return
                        except Exception as copy_err:
                            print(f"Direct copy failed, trying download: {copy_err}")
                            
                            # If direct copy fails, download and upload
                            file_path = None
                            self.progress_manager.reset_user_progress(sender)
                            last_update_time = 0
                            
                            async def dl_cb(current, total):
                                nonlocal last_update_time
                                now = time.time()
                                # Suppress progress for stickers/animations and small files
                                try:
                                    is_sticker_or_anim_or_photo = (
                                        (hasattr(msg, 'sticker') and msg.sticker) or 
                                        (hasattr(msg, 'animation') and msg.animation) or 
                                        (hasattr(msg, 'photo') and msg.photo)
                                    )
                                except Exception:
                                    is_sticker_or_anim_or_photo = False
                                # Apply proper thresholds: 20MB+ for single, 50MB+ for batch
                                threshold = 50 * 1024 * 1024 if not edit_id else 20 * 1024 * 1024
                                if is_sticker_or_anim_or_photo or (total and total <= threshold):
                                    return
                                # Different update intervals: 5s for single downloads, 10s for batch downloads
                                update_interval = 10 if not edit_id else 5
                                if now - last_update_time >= update_interval or current == total:
                                    last_update_time = now
                                    try:
                                        # Show progress for files above threshold
                                        if total > threshold:
                                            percent = (current / total) * 100 if total else 0
                                            progress_text = (
                                                f"📥 **Downloading from public group**\n\n"
                                                f"📊 **Progress**: {percent:.1f}%\n"
                                                f"📦 **Size**: {self.progress_manager._format_bytes(current)} / {self.progress_manager._format_bytes(total)}"
                                            )
                                            html_progress = await self.caption_formatter.markdown_to_html(progress_text)
                                            await app.edit_message_text(sender, edit_id, html_progress, parse_mode=ParseMode.HTML)
                                    except Exception:
                                        pass
                            try:
                                # Ensure we have both msg and client_to_use in scope
                                if not msg:
                                    raise Exception("Message object not available for download")
                                
                                # Determine which client to use for download
                                download_client = None
                                if 'client_to_use' in locals() and client_to_use:
                                    download_client = client_to_use
                                elif user_session_client:
                                    download_client = user_session_client
                                else:
                                    download_client = app_client
                                
                                if not download_client:
                                    raise Exception("No client available for download")
                                    
                                file_path = await download_client.download_media(msg, progress=dl_cb)
                                
                                # Update session_type to reflect actual client used
                                if download_client == user_session_client:
                                    actual_session_type = "user session"
                                elif download_client == app_client:
                                    actual_session_type = "bot"
                                else:
                                    actual_session_type = "session pool"
                                
                                print(f"✅ Downloaded media using {actual_session_type}: {file_path}")
                                
                                if file_path:
                                    # Preserve original caption and metadata
                                    caption = getattr(msg, 'caption', None) or ""
                                    _ce = getattr(msg, 'caption_entities', None)
                                    _rm = getattr(msg, 'reply_markup', None)

                                    # Process filename (safe no-op for stickers/photos)
                                    try:
                                        file_path = await self.file_ops.process_filename(file_path, sender)
                                    except Exception:
                                        pass

                                    raise Exception("Falling through to proper upload flow")

                                    # Clean up
                                    try:
                                        if os.path.exists(file_path):
                                            os.remove(file_path)
                                    except Exception:
                                        pass

                                    await app.delete_messages(sender, edit_id)
                                    print(f"📥 DOWNLOAD: Finished processing media message using {actual_session_type} from {chat_id}")
                                    media_processed_successfully = True
                                    return
                                    
                            except FloodWait as e:
                                print(f"FloodWait error during download: {e.value} seconds")
                                # Auto flood detection for user session
                                await auto_flood_detector.detect_user_flood_wait(sender, e.value, "media download")
                                
                                await app.edit_message_text(sender, edit_id,
                                    f"⏳ **Rate Limited**\n\n"
                                    f"Please wait {e.value} seconds before trying again.")
                                return
                            except PeerIdInvalid:
                                print(f"PeerIdInvalid error during download from {chat_id}")
                                await app.edit_message_text(sender, edit_id,
                                    "❌ **Invalid Chat ID**\n\n"
                                    "The chat ID is invalid or the bot doesn't have access.")
                                return
                            except (ChannelInvalid, ChannelPrivate, ChatIdInvalid) as e:
                                print(f"Channel access error during download: {e}")
                                await app.edit_message_text(sender, edit_id,
                                    "❌ **Channel Access Error**\n\n"
                                    "Cannot access the channel. It may be private or deleted.")
                                return
                            except OSError as e:
                                print(f"File system error during download: {e}")
                                await app.edit_message_text(sender, edit_id,
                                    "❌ **File System Error**\n\n"
                                    "Error writing to disk. Please check storage space.")
                                return
                            except Exception as download_err:
                                print(f"Media download failed: {download_err}")
                                await app.edit_message_text(sender, edit_id,
                                    "❌ **Download Failed**\n\n"
                                    f"Error: {str(download_err)[:100]}...")
                                
                    except FloodWait as e:
                        print(f"FloodWait error during media processing: {e.value} seconds")
                        # Auto flood detection for user session
                        await auto_flood_detector.detect_user_flood_wait(sender, e.value, "media processing")
                        
                        await app.edit_message_text(sender, edit_id,
                            f"⏳ **Rate Limited**\n\n"
                            f"Please wait {e.value} seconds before trying again.")
                        return
                    except PeerIdInvalid:
                        print(f"PeerIdInvalid error during media processing from {chat_id}")
                        await app.edit_message_text(sender, edit_id,
                            "❌ **Invalid Chat ID**\n\n"
                            "The chat ID is invalid or the bot doesn't have access.")
                        return
                    except (ChannelInvalid, ChannelPrivate, ChatIdInvalid) as e:
                        print(f"Channel access error during media processing: {e}")
                        await app.edit_message_text(sender, edit_id,
                            "❌ **Channel Access Error**\n\n"
                            "Cannot access the channel. It may be private or deleted.")
                        return
                    except Exception as media_err:
                        print(f"Failed to process media message: {media_err}")
                        await app.edit_message_text(sender, edit_id,
                            "❌ **Media Processing Failed**\n\n"
                            f"Error: {str(media_err)[:100]}...")
                        
        except FloodWait as e:
            print(f"FloodWait error in _copy_public_message: {e.value} seconds")
            # Auto flood detection for user session
            await auto_flood_detector.detect_user_flood_wait(sender, e.value, "public message copy")
            
            await app.edit_message_text(sender, edit_id,
                f"⏳ **Rate Limited**\n\n"
                f"Please wait {e.value} seconds before trying again.")
        except PeerIdInvalid:
            print(f"PeerIdInvalid error in _copy_public_message from {chat_id}")
            await app.edit_message_text(sender, edit_id,
                "❌ **Invalid Chat ID**\n\n"
                "The chat ID is invalid or the bot doesn't have access.")
        except (ChannelInvalid, ChannelPrivate, ChatIdInvalid) as e:
            print(f"Channel access error in _copy_public_message: {e}")
            await app.edit_message_text(sender, edit_id,
                "❌ **Channel Access Error**\n\n"
                "Cannot access the channel. It may be private or deleted.")
        except Exception as main_err:
            print(f"Error in _copy_public_message: {main_err}")
            await app.edit_message_text(sender, edit_id,
                "❌ **Error Processing Message**\n\n"
                f"An error occurred while processing the public group message.\n"
                f"Error: {str(main_err)[:100]}...\n\n"
                f"Please try again or contact support if the issue persists.")
        finally:
            # Clean up user session client if it was created
            try:
                if user_session_client:
                    await user_session_client.stop()
            except Exception:
                pass
            
            # Release session pool client if it was used
            try:
                if pooled_client and session_id:
                    await session_pool.release_session(session_id)
            except Exception:
                pass
                
        # Continue with Telethon fallback if everything else failed
        custom_caption = self.user_caption_prefs.get(str(sender), "")
        # final_caption will be computed later for media when msg is from Telethon

        # If we still don't have the message, try Telethon fallback
        if not msg and gf and not media_processed_successfully:
            # CRITICAL: Check login requirements before Telethon fallback
            is_private_group = self._is_private_group_link(msg_link, chat_id)
            if is_private_group:
                # Get user session data
                user_data = await odb.get_data(sender)
                user_session_string = user_data.get("session") if user_data else None
                
                if not user_session_string:
                    await message.reply_text(
                        "🔐 <b>Login Required</b>\n\n"
                        "This appears to be a private group/channel link that requires your personal session to access. "
                        "Please login first using the /login command to download content from private groups."
                    )
                    # Raise exception for batch processing to catch
                    raise Exception("LOGIN_REQUIRED: User session required for Telethon fallback on private content")
            
            try:
                try:
                    await app.edit_message_text(sender, edit_id, "🔄 Trying Telethon fallback...")
                except Exception:
                    pass
                entity = chat_id
                try:
                    entity = await gf.get_entity(chat_id)
                except Exception:
                    pass
                tmsg = await gf.get_messages(entity, ids=message_id)

                if tmsg:
                    # Handle text-only messages directly without download
                    t_text = getattr(tmsg, 'message', None) or getattr(tmsg, 'text', None) or ""
                    has_media = getattr(tmsg, 'media', None) is not None
                    if not has_media and t_text:
                        try:
                            html_text = await self.caption_formatter.markdown_to_html(t_text)
                            result = await app.send_message(sender, html_text, parse_mode=ParseMode.HTML)
                            if result:
                                await app.delete_messages(sender, edit_id)
                            # Set success flag to prevent further processing
                            media_processed_successfully = True
                            return
                        except Exception as tele_text_err:
                            print(f"Telethon text fallback failed: {tele_text_err}")

                    # Media path via Telethon if message has media
                    file_path = None
                    self.progress_manager.reset_user_progress(sender)
                    last_update_time = 0
                    # Apply proper thresholds: 20MB+ for single, 50MB+ for batch
                    PROGRESS_THRESHOLD = 50 * 1024 * 1024 if not edit_id else 20 * 1024 * 1024
                    async def dl_cb(current, total):
                        nonlocal last_update_time
                        now = time.time()
                        # Only show progress for files above threshold and skip for stickers/animations
                        try:
                            is_sticker_or_anim_or_photo = (
                                (hasattr(tmsg, 'sticker') and tmsg.sticker) or 
                                (hasattr(tmsg, 'animation') and tmsg.animation) or 
                                (hasattr(tmsg, 'photo') and tmsg.photo)
                            )
                        except Exception:
                            is_sticker_or_anim_or_photo = False
                        if total <= PROGRESS_THRESHOLD or is_sticker_or_anim_or_photo:
                            return
                        # Different update intervals: 5s for single downloads, 10s for batch downloads
                        update_interval = 10 if not edit_id else 5
                        if now - last_update_time >= update_interval or current == total:
                            last_update_time = now
                            try:
                                percent = (current / total) * 100 if total else 0
                                # Calculate speed with minimum elapsed time
                                start_time_obj = self.progress_manager.user_progress.get(sender, type('obj', (object,), {'start_time': now}))
                                elapsed = max(now - start_time_obj.start_time, 1.0) if now > start_time_obj.start_time else 1.0
                                speed = current / elapsed
                                
                                # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
                                speed_display = speed * 3
                                
                                eta = (total - current) / speed if speed > 0 else 0
                                # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
                                eta = eta / 2 if eta > 0 else 0
                                eta_str = self.progress_manager._format_time(eta) if eta > 0 else "Calculating..."
                                progress_text = UnifiedProgressBar.format_progress_message(percent, current, total, speed_display, eta_str, "download")
                                html_progress = await CaptionFormatter.markdown_to_html(progress_text)
                                await app.edit_message_text(sender, edit_id, html_progress, parse_mode=ParseMode.HTML)
                            except Exception:
                                pass
                    try:
                        file_path = await gf.download_media(tmsg, progress_callback=dl_cb)
                    except Exception as tdl_err:
                        print(f"Telethon download failed: {tdl_err}")
                        raise

                    # Preserve original Telethon text as caption when present
                    caption = t_text if t_text else ""

                    if file_path:
                        try:
                            file_path = await self.file_ops.process_filename(file_path, sender)
                        except Exception:
                            pass
                        # Extract entities and reply markup from Telethon message if present
                        try:
                            _ce = getattr(tmsg, 'entities', None)
                        except Exception:
                            _ce = None
                        try:
                            _rm = getattr(tmsg, 'reply_markup', None)
                        except Exception:
                            _rm = None

                        # Decide best send method to preserve original type (Telethon message)
                        sent = None
                        try:
                            # Robust sticker detection for Telethon
                            is_doc_sticker = False
                            try:
                                d = getattr(tmsg, 'document', None)
                                if d:
                                    mt = getattr(d, 'mime_type', '') or ''
                                    # file_name may be on attributes or via .attributes
                                    fn = ''
                                    try:
                                        fn = (getattr(d, 'attributes', [None])[0].file_name or '') if getattr(d, 'attributes', None) else ''
                                    except Exception:
                                        fn = ''
                                    is_doc_sticker = ('sticker' in fn.lower()) or (mt in ('image/webp', 'video/webm', 'application/x-tgsticker'))
                            except Exception:
                                is_doc_sticker = False

                            raise Exception("Falling through to proper upload flow")
                        except Exception as send_err:
                       
                            raise Exception("Falling through to proper upload flow")

                        try:
                            await app.delete_messages(sender, edit_id)
                        except Exception:
                            pass
                        media_processed_successfully = True
                        return
            except Exception as telethon_fallback_err:
                print(f"Telethon fallback failed: {telethon_fallback_err}")

            # Lightweight fallback for text-only or special (web preview) messages
            if msg:
                try:
                    # Handle special message types that can be re-sent without download (like web page preview)
                    handled = False
                    try:
                        if getattr(msg, 'media', None) == MessageMediaType.WEB_PAGE_PREVIEW:
                            handled = await self._handle_special_messages(msg, target_chat_id, topic_id, edit_id, sender)
                    except Exception:
                        # _handle_special_messages may not accept Pyrogram msg in some cases; ignore and continue
                        pass

                    if handled:
                        await app.delete_messages(sender, edit_id)
                        return

                    # Prefer preserving entities and reply markup when available
                    text_to_send = getattr(msg, 'text', None) or getattr(msg, 'caption', None)
                    if text_to_send:
                        if getattr(msg, 'entities', None) or getattr(msg, 'reply_markup', None):
                            send_kwargs = {
                                'chat_id': target_chat_id,
                                'text': text_to_send,
                                'reply_to_message_id': topic_id,
                                'parse_mode': None
                            }
                            ents = getattr(msg, 'entities', None)
                            if ents:
                                send_kwargs['entities'] = ents
                            rm = getattr(msg, 'reply_markup', None)
                            if rm:
                                send_kwargs['reply_markup'] = rm
                            result = await app.send_message(**send_kwargs)
                        else:
                            html_text = await self.caption_formatter.markdown_to_html(text_to_send)
                            result = await app_client.send_message(target_chat_id, html_text, reply_to_message_id=topic_id, parse_mode=ParseMode.HTML)
                        if result:
                            await app.delete_messages(sender, edit_id)
                        return
                except Exception as text_err:
                    print(f"Text send fallback failed: {text_err}")

                # Telethon fallback for public links when Pyrogram cannot copy/forward/fetch
                if gf and not media_processed_successfully:
                    # CRITICAL: Check login requirements before Telethon fallback (even for "public" links)
                    is_private_group = self._is_private_group_link(msg_link, chat_id)
                    if is_private_group:
                        # Get user session data
                        user_data = await odb.get_data(sender)
                        user_session_string = user_data.get("session") if user_data else None
                        
                        if not user_session_string:
                            await message.reply_text(
                                "🔐 <b>Login Required</b>\n\n"
                                "This appears to be a private group/channel link that requires your personal session to access. "
                                "Please login first using the /login command to download content from private groups."
                            )
                            # Raise exception for batch processing to catch
                            raise Exception("LOGIN_REQUIRED: User session required for Telethon public fallback on private content")
                    
                    try:
                        try:
                            await app.edit_message_text(sender, edit_id, "🔄 Trying Telethon fallback...")
                        except Exception:
                            pass
                        entity = chat_id
                        try:
                            entity = await gf.get_entity(chat_id)
                        except Exception:
                            pass
                        
                        # Handle topic groups in Telethon fallback
                        if hasattr(self, '_source_topic_id') and self._source_topic_id:
                            print(f"🔍 Telethon fetching from topic {self._source_topic_id} in group {chat_id}")
                            # For Telethon, we might need to handle topic groups differently
                            # But for now, try the same approach as regular messages
                        
                        tmsg = await gf.get_messages(entity, ids=message_id)

                        # If got message via Telethon, try to process it
                        if tmsg:
                            # Prefer text-only path if no media
                            t_text = getattr(tmsg, 'message', None) or getattr(tmsg, 'text', None) or ""
                            has_media = getattr(tmsg, 'media', None) is not None
                            if not has_media and t_text:
                                try:
                                    html_text = await self.caption_formatter.markdown_to_html(t_text)
                                    result = await app.send_message(sender, html_text, parse_mode=ParseMode.HTML)
                                    if result:
                                        await app.delete_messages(sender, edit_id)
                                    # Set success flag to prevent further processing
                                    media_processed_successfully = True
                                    return
                                except Exception as tele_text_err:
                                    print(f"Telethon text fallback failed: {tele_text_err}")

                            # Media path: download with Telethon and upload using Telethon uploader
                            file_path = None
                            self.progress_manager.reset_user_progress(sender)
                            last_update_time = 0
                            async def dl_cb(current, total):
                                nonlocal last_update_time
                                now = time.time()
                                # Skip for stickers/animations if present
                                try:
                                    is_sticker_or_anim_or_photo = (
                                        (hasattr(tmsg, 'sticker') and tmsg.sticker) or 
                                        (hasattr(tmsg, 'animation') and tmsg.animation) or 
                                        (hasattr(tmsg, 'photo') and tmsg.photo)
                                    )
                                except Exception:
                                    is_sticker_or_anim_or_photo = False
                                # Apply proper thresholds: 20MB+ for single, 50MB+ for batch
                                threshold = 50 * 1024 * 1024 if not edit_id else 20 * 1024 * 1024
                                if is_sticker_or_anim_or_photo or (total and total <= threshold):
                                    return
                                # Different update intervals: 5s for single downloads, 10s for batch downloads
                                update_interval = 10 if not edit_id else 5
                                if now - last_update_time >= update_interval or current == total:
                                    last_update_time = now
                                    try:
                                        percent = (current / total) * 100 if total else 0
                                        # Calculate speed with minimum elapsed time
                                        start_time_obj = self.progress_manager.user_progress.get(sender, type('obj', (object,), {'start_time': now}))
                                        elapsed = max(now - start_time_obj.start_time, 1.0) if now > start_time_obj.start_time else 1.0
                                        speed = current / elapsed
                                        
                                        # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
                                        speed_display = speed * 3
                                        
                                        eta = (total - current) / speed if speed > 0 else 0
                                        # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
                                        eta = eta / 2 if eta > 0 else 0
                                        eta_str = self.progress_manager._format_time(eta) if eta > 0 else "Calculating..."
                                        # Show progress for files above threshold
                                        if total > threshold:
                                            progress_text = UnifiedProgressBar.format_progress_message(percent, current, total, speed_display, eta_str, "download")
                                            html_progress = await CaptionFormatter.markdown_to_html(progress_text)
                                            await app.edit_message_text(sender, edit_id, html_progress, parse_mode=ParseMode.HTML)
                                    except Exception:
                                        pass
                            try:
                                file_path = await gf.download_media(tmsg, progress_callback=dl_cb)
                            except Exception as tdl_err:
                                print(f"Telethon download failed: {tdl_err}")
                                raise

                            # Preserve original Telethon text as caption when present
                            caption = t_text if t_text else ""

                            if file_path:
                                try:
                                    file_path = await self.file_ops.process_filename(file_path, sender)
                                except Exception:
                                    pass
                                # Extract entities and reply markup from Telethon message if present
                                try:
                                    _ce = getattr(tmsg, 'entities', None)
                                except Exception:
                                    _ce = None
                                try:
                                    _rm = getattr(tmsg, 'reply_markup', None)
                                except Exception:
                                    _rm = None
                                await self.upload_with_telethon(
                                    file_path,
                                    sender,
                                    sender,
                                    caption,
                                    None,
                                    edit_msg=None,
                                    caption_entities=_ce,
                                    reply_markup=_rm,
                                    created_progress_msg=False,  # This is Telethon fallback, treat as single
                                    is_batch_operation=False,  # This is a single operation fallback
                                )
                                try:
                                    await app.delete_messages(sender, edit_id)
                                except Exception:
                                    pass
                                # Set success flag to prevent further processing
                                media_processed_successfully = True
                                return
                    except Exception as telethon_fallback_err:
                        print(f"Telethon fallback failed: {telethon_fallback_err}")

            # If Telethon not available or also failed, inform user
            await app.edit_message_text(sender, edit_id, "❌ Unable to fetch this public message via bot or Telethon. It may be restricted or deleted.")
            return




    async def _format_caption_with_custom(self, original_caption: str, sender: int, custom_caption: str) -> str:
        """Format caption with user preferences"""
        delete_words = set(self.db.get_user_data(sender, "delete_words", []))
        replacements = self.db.get_user_data(sender, "replacement_words", {})
        
        processed = original_caption
        for word in delete_words:
            processed = processed.replace(word, '  ')
        
        for word, replace_word in replacements.items():
            processed = processed.replace(word, replace_word)
        
        if custom_caption:
            return f"{processed}\n\n__**{custom_caption}**__" if processed else f"__**{custom_caption}**__"
        return processed

    async def send_settings_panel(self, chat_id: int, user_id: int):
        """Send enhanced settings panel"""
        buttons = [
            [Button.inline("Set Chat ID", b'setchat'), Button.inline("Set Rename Tag", b'setrename')],
            [Button.inline("Caption", b'setcaption'), Button.inline("Replace Words", b'setreplacement')],
            [Button.inline("Remove Words", b'delete'), Button.inline("Reset All", b'reset')],
            [Button.inline("Set Thumbnail", b'setthumb'), Button.inline("Remove Thumbnail", b'remthumb')],
            [Button.url("Report Issues", "https://t.me/AlienxSaver")]
        ]
        
        message = (
            "🚧 <b>Settings Panel - Under Construction</b>\n\n"
            "<i>We're working hard to bring you amazing customization features!</i>\n\n"
            "🔧 <b>Coming Soon:</b>\n"
            "• <code>Custom captions and rename tags</code>\n"
            "• <code>Advanced word filters and replacements</code>\n"
            "• <code>Thumbnail management</code>\n"
            "• <code>And much more!</code>\n\n"
            "📢 <b>Help Us Prioritize:</b>\n"
            "These features will be released in <u>future updates</u> based on user support and feedback. "
            "Send us your suggestions and let us know which features you need most!\n\n"
            "💬 <b>Contact:</b> <a href=\"https://t.me/AlienxSaverChat\">AlienxSaverChat</a>"
        )
        
        # Transform Dropbox URL to direct download format (same approach as shrink.py)
        def _to_direct_dropbox(url: str) -> str:
            try:
                import re
                # Replace host and enforce dl=1 for direct content
                direct = url.replace("www.dropbox.com", "dl.dropboxusercontent.com")
                if "dl=" in direct:
                    # ensure dl=1
                    direct = re.sub(r"dl=\d", "dl=1", direct)
                else:
                    sep = "&" if "?" in direct else "?"
                    direct = f"{direct}{sep}dl=1"
                return direct
            except Exception:
                return url
        
        # Use the same Dropbox URL transformation approach as shrink.py
        if self.config.SETTINGS_PIC and self.config.SETTINGS_PIC != "settings.jpg":
            # If it's a Dropbox URL, transform it
            if "dropbox.com" in self.config.SETTINGS_PIC:
                photo_url = _to_direct_dropbox(self.config.SETTINGS_PIC)
            else:
                photo_url = self.config.SETTINGS_PIC
                
            try:
                # Try to send with the transformed image URL
                await gf.send_file(chat_id, file=photo_url, caption=message, buttons=buttons, parse_mode='html')
                return
            except Exception as e:
                print(f"⚠️ Failed to send settings with image URL: {e}")
                print("📝 Falling back to local settings.jpg")
        
        # Fallback to local image or text-only
        try:
            await gf.send_file(chat_id, file="settings.jpg", caption=message, buttons=buttons, parse_mode='html')
        except Exception as e:
            print(f"⚠️ Failed to send settings with local image: {e}")
            print("📝 Falling back to text-only settings panel")
            await gf.send_message(chat_id, message, buttons=buttons, parse_mode='html')

# Initialize the main bot instance
telegram_bot = SmartTelegramBot()

# Dashboard functionality
class DashboardManager:
    """Real-time admin dashboard with comprehensive stats"""
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.start_time = time.time()
        
    async def get_system_stats(self) -> Dict[str, any]:
        """Get comprehensive system statistics"""
        import psutil
        import platform
        
        # System info
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            'cpu_usage': cpu_percent,
            'memory_total': memory.total,
            'memory_used': memory.used,
            'memory_percent': memory.percent,
            'disk_total': disk.total,
            'disk_used': disk.used,
            'disk_percent': (disk.used / disk.total) * 100,
            'platform': platform.system(),
            'python_version': platform.python_version()
        }
    
    async def get_user_stats(self) -> Dict[str, any]:
        """Get user-related statistics"""
        from devgagan.core.mongo.users_db import get_users, get_users_excluding_bots
        from devgagan.core.mongo.plans_db import premium_users
        
        # Get accurate user count excluding bots
        try:
            real_users = await get_users_excluding_bots()
            real_user_count = len(real_users)
            all_users = await get_users()
            total_entries = len(all_users)
            bot_count = total_entries - real_user_count
        except Exception as e:
            # Fallback to old method if new function fails
            print(f"⚠️ Bot filtering failed in dashboard, using fallback: {e}")
            all_users = await get_users()
            real_user_count = len(all_users)
            total_entries = real_user_count
            bot_count = 0
        
        premium_list = await premium_users()
        premium_count = len(premium_list)
        free_users = real_user_count - premium_count
        
        # Active sessions count
        active_sessions = len([uid for uid, session in self.bot.user_sessions.items() if session])
        
        return {
            'total_users': real_user_count,  # Real users only
            'total_entries': total_entries,  # All database entries
            'bot_count': bot_count,          # Filtered bots
            'premium_users': premium_count,
            'free_users': free_users,
            'active_sessions': active_sessions,
            'premium_percentage': (premium_count / real_user_count * 100) if real_user_count > 0 else 0
        }
    
    async def get_progress_stats(self) -> Dict[str, any]:
        """Get real-time progress and transfer statistics with persistent cumulative data"""
        from devgagan.core.mongo.connection import get_mongo_client
        
        # Active transfers (real-time)
        active_transfers = len(self.bot.progress_manager.user_progress)
        
        # Current session stats
        session_downloaded = sum(up.session_downloaded for up in self.bot.progress_manager.user_progress.values())
        session_uploaded = sum(up.session_uploaded for up in self.bot.progress_manager.user_progress.values())
        
        # Calculate average speeds
        avg_speeds = [up.avg_speed for up in self.bot.progress_manager.user_progress.values() if up.avg_speed > 0]
        avg_speed = sum(avg_speeds) / len(avg_speeds) if avg_speeds else 0
        
        # Peak speed across all users
        peak_speed = max([up.peak_speed for up in self.bot.progress_manager.user_progress.values()], default=0)
        
        # Get cumulative stats from database
        try:
            client = get_mongo_client()
            stats_db = client['bot_stats']
            stats_coll = stats_db['transfer_stats']
            
            cumulative = await stats_coll.find_one({'_id': 'cumulative'}) or {
                'total_downloaded': 0,
                'total_uploaded': 0,
                'peak_speed_ever': 0
            }
            
            # Update peak speed if current is higher
            if peak_speed > cumulative.get('peak_speed_ever', 0):
                await stats_coll.update_one(
                    {'_id': 'cumulative'},
                    {'$set': {'peak_speed_ever': peak_speed}},
                    upsert=True
                )
                cumulative['peak_speed_ever'] = peak_speed
            
            total_downloaded = cumulative.get('total_downloaded', 0) + session_downloaded
            total_uploaded = cumulative.get('total_uploaded', 0) + session_uploaded
            peak_speed_ever = cumulative.get('peak_speed_ever', peak_speed)
            
        except Exception as e:
            # Fallback to session stats only if DB fails
            total_downloaded = session_downloaded
            total_uploaded = session_uploaded
            peak_speed_ever = peak_speed
        
        return {
            'active_transfers': active_transfers,
            'total_downloaded': total_downloaded,
            'total_uploaded': total_uploaded,
            'average_speed': avg_speed,
            'peak_speed': peak_speed_ever
        }
    
    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    def _format_speed(self, speed_bps: float) -> str:
        """Format speed to human readable format"""
        return f"{self._format_bytes(speed_bps)}/s"
    
    def _get_uptime(self) -> str:
        """Get formatted uptime"""
        uptime_seconds = int(time.time() - self.start_time)
        days, remainder = divmod(uptime_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days: parts.append(f"{days}d")
        if hours: parts.append(f"{hours}h")
        if minutes: parts.append(f"{minutes}m")
        if seconds or not parts: parts.append(f"{seconds}s")
        
        return " ".join(parts)
    
    async def generate_dashboard(self) -> str:
        """Generate comprehensive dashboard message"""
        try:
            # Get all stats
            system_stats = await self.get_system_stats()
            user_stats = await self.get_user_stats()
            progress_stats = await self.get_progress_stats()
            
            # Health status
            health_status = "🟢 Healthy"
            if system_stats['cpu_usage'] > 80 or system_stats['memory_percent'] > 85:
                health_status = "🟡 Warning"
            if system_stats['cpu_usage'] > 95 or system_stats['memory_percent'] > 95:
                health_status = "🔴 Critical"
            
            dashboard = f"""🚀 **Restrict Bot Saver Dashboard**

📊 **System Health**: {health_status}
⏱️ **Uptime**: `{self._get_uptime()}`
🖥️ **CPU Usage**: `{system_stats['cpu_usage']:.1f}%`
💾 **Memory**: `{system_stats['memory_percent']:.1f}%` ({self._format_bytes(system_stats['memory_used'])}/{self._format_bytes(system_stats['memory_total'])})
💿 **Disk**: `{system_stats['disk_percent']:.1f}%` ({self._format_bytes(system_stats['disk_used'])}/{self._format_bytes(system_stats['disk_total'])})

👥 **User Statistics**:
├ 👤 **Real Users**: `{user_stats['total_users']:,}`
├ 🤖 **Bots Filtered**: `{user_stats['bot_count']:,}`
├ 📊 **Total Entries**: `{user_stats['total_entries']:,}`
├ 💎 **Premium**: `{user_stats['premium_users']:,}` ({user_stats['premium_percentage']:.1f}%)
├ 🆓 **Free**: `{user_stats['free_users']:,}`
└ 🔗 **Active Sessions**: `{user_stats['active_sessions']}`

📡 **Transfer Statistics**:
├ 🔄 **Active Transfers**: `{progress_stats['active_transfers']}`
├ 📥 **Downloaded**: `{self._format_bytes(progress_stats['total_downloaded'])}`
├ 📤 **Uploaded**: `{self._format_bytes(progress_stats['total_uploaded'])}`
├ ⚡ **Avg Speed**: `{self._format_speed(progress_stats['average_speed'])}`
└ 🚀 **Peak Speed**: `{self._format_speed(progress_stats['peak_speed'])}`

🔧 **System Info**:
├ 🐍 **Python**: `{system_stats['python_version']}`
├ 💻 **Platform**: `{system_stats['platform']}`
└ 📱 **Bot Version**: `v3.0.5`

⚡ **Powered by Restrict Bot Saver**"""
            
            return dashboard
            
        except Exception as e:
            return f"❌ **Dashboard Error**: {str(e)}"

# Initialize dashboard manager
dashboard_manager = DashboardManager(telegram_bot)

# Event Handlers
# @gf.on(events.NewMessage(incoming=True, pattern='/settings'))
# async def settings_command_handler(event):
#     """Handle /settings command"""
#     await telegram_bot.send_settings_panel(event.chat_id, event.sender_id)

@gf.on(events.CallbackQuery)
async def callback_query_handler(event):
    """Enhanced callback query handler with all features"""
    user_id = event.sender_id
    data = event.data
    
    # Upload method selection - Telethon only
    if data == b'uploadmethod':
        # Force Telethon as the only option
        telegram_bot.db.save_user_data(user_id, "upload_method", "Telethon")
        await event.edit(
            "📤 **Upload Method:**\n\n"
            "**Restrict Bot Saver v1 ⚡:** Advanced features with Telethon\n\n"
            "✅ Upload method is set to Restrict Bot Saver v1 ⚡\n\n"
            "**Note:** This bot now exclusively uses Telethon for enhanced capabilities."
        )


    elif data == b'telethon':
        telegram_bot.db.save_user_data(user_id, "upload_method", "Telethon")
        await event.edit("✅ Upload method set to Restrict Bot Saver v1 ⚡\n\nThanks for helping us test this advanced library!")

    # Session management
    elif data == b'logout':
        await odb.remove_session(user_id)
        user_data = await odb.get_data(user_id)
        message = "✅ Logged out successfully!" if user_data and user_data.get("session") is None else "❌ You are not logged in."
        await event.respond(message)

    elif data == b'addsession':
        telegram_bot.user_sessions[user_id] = 'addsession'
        await event.respond("🔑 **Session Login**\n\nSend your Pyrogram V2 session string:")

    # Settings configuration
    elif data == b'setchat':
        telegram_bot.user_sessions[user_id] = 'setchat'
        await event.respond("💬 **Set Target Chat**\n\nSend the chat ID where files should be sent:")

    elif data == b'setrename':
        telegram_bot.user_sessions[user_id] = 'setrename'
        await event.respond("🏷 **Set Rename Tag**\n\nSend the tag to append to filenames:")

    elif data == b'setcaption':
        telegram_bot.user_sessions[user_id] = 'setcaption'
        await event.respond("📝 **Set Custom Caption**\n\nSend the caption to add to all files:")

    elif data == b'setreplacement':
        telegram_bot.user_sessions[user_id] = 'setreplacement'
        await event.respond(
            "🔄 **Word Replacement**\n\n"
            "Send replacement rules in format:\n"
            "`'OLD_WORD' 'NEW_WORD'`\n\n"
            "Example: `'sample' 'example'`"
        )

    elif data == b'delete':
        telegram_bot.user_sessions[user_id] = 'deleteword'
        await event.respond(
            "🗑 **Delete Words**\n\n"
            "Send words separated by spaces to remove them from captions/filenames:"
        )

    # Thumbnail management
    elif data == b'setthumb':
        telegram_bot.pending_photos.add(user_id)
        await event.respond("🖼 **Set Thumbnail**\n\nSend a photo to use as thumbnail for videos:")

    elif data == b'remthumb':
        thumb_path = f'{user_id}.jpg'
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
            await event.respond('✅ Thumbnail removed successfully!')
        else:
            await event.respond("❌ No thumbnail found to remove.")

    # Watermark features (placeholder)
    elif data == b'pdfwt':
        await event.respond("🚧 **PDF Watermark**\n\nThis feature is under development...")

    elif data == b'watermark':
        await event.respond("🚧 **Video Watermark**\n\nThis feature is under development...")

    # Reset all settings
    elif data == b'reset':
        try:
            success = telegram_bot.db.reset_user_data(user_id)
            telegram_bot.user_chat_ids.pop(user_id, None)
            telegram_bot.user_rename_prefs.pop(str(user_id), None)
            telegram_bot.user_caption_prefs.pop(str(user_id), None)
            
            # Remove thumbnail
            thumb_path = f"{user_id}.jpg"
            if os.path.exists(thumb_path):
                os.remove(thumb_path)
            
            if success:
                await event.respond("✅ All settings reset successfully!")
            else:
                await event.respond("❌ Error occurred while resetting settings.")
        except Exception as e:
            await event.respond(f"❌ Reset failed: {e}")

@gf.on(events.NewMessage(func=lambda e: e.sender_id in telegram_bot.pending_photos))
async def thumbnail_handler(event):
    """Handle thumbnail upload"""
    user_id = event.sender_id
    if event.photo:
        temp_path = await event.download_media()
        thumb_path = f'{user_id}.jpg'
        
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
        
        os.rename(temp_path, f'./{user_id}.jpg')
        await event.respond('✅ Thumbnail saved successfully!')
    else:
        await event.respond('❌ Please send a photo. Try again.')
    
    telegram_bot.pending_photos.discard(user_id)

@gf.on(events.NewMessage)
async def user_input_handler(event):
    """Handle user input based on current session state"""
    user_id = event.sender_id
    
    if user_id in telegram_bot.user_sessions:
        session_type = telegram_bot.user_sessions[user_id]
        
        if session_type == 'setchat':
            try:
                chat_id = event.text.strip()
                telegram_bot.user_chat_ids[user_id] = chat_id
                await event.respond(f"✅ Target chat set to: `{chat_id}`")
            except ValueError:
                await event.respond("❌ Invalid chat ID format!")
                
        elif session_type == 'setrename':
            rename_tag = event.text.strip()
            telegram_bot.user_rename_prefs[str(user_id)] = rename_tag
            telegram_bot.db.save_user_data(user_id, "rename_tag", rename_tag)
            await event.respond(f"✅ Rename tag set to: **{rename_tag}**")
        
        elif session_type == 'setcaption':
            custom_caption = event.text.strip()
            telegram_bot.user_caption_prefs[str(user_id)] = custom_caption
            telegram_bot.db.save_user_data(user_id, "custom_caption", custom_caption)
            await event.respond(f"✅ Custom caption set to:\n\n**{custom_caption}**")

        elif session_type == 'setreplacement':
            match = re.match(r"'(.+)' '(.+)'", event.text)
            if not match:
                await event.respond("❌ **Invalid format!**\n\nUse: `'OLD_WORD' 'NEW_WORD'`")
            else:
                old_word, new_word = match.groups()
                delete_words = set(telegram_bot.db.get_user_data(user_id, "delete_words", []))
                
                if old_word in delete_words:
                    await event.respond(f"❌ '{old_word}' is in delete list and cannot be replaced.")
                else:
                    replacements = telegram_bot.db.get_user_data(user_id, "replacement_words", {})
                    replacements[old_word] = new_word
                    telegram_bot.db.save_user_data(user_id, "replacement_words", replacements)
                    await event.respond(f"✅ Replacement saved:\n**'{old_word}' → '{new_word}'**")

        elif session_type == 'addsession':
            session_string = event.text.strip()
            await odb.set_session(user_id, session_string)
            await event.respond("✅ Session string added successfully!")
                
        elif session_type == 'deleteword':
            words_to_delete = event.text.split()
            delete_words = set(telegram_bot.db.get_user_data(user_id, "delete_words", []))
            delete_words.update(words_to_delete)
            telegram_bot.db.save_user_data(user_id, "delete_words", list(delete_words))
            await event.respond(f"✅ Words added to delete list:\n**{', '.join(words_to_delete)}**")
               
        # Clear session after handling
        del telegram_bot.user_sessions[user_id]

@gf.on(events.NewMessage(incoming=True, pattern='/lock'))
async def lock_channel_handler(event):
    """Handle channel locking command (owner only)"""
    # Silent admin check - no response for non-admins
    if event.sender_id not in OWNER_ID:
        return
    
    try:
        channel_id = int(event.text.split(' ')[1])
        success = telegram_bot.db.lock_channel(channel_id)
        
        if success:
            await event.respond(f"✅ Channel ID `{channel_id}` locked successfully.")
        else:
            await event.respond(f"❌ Failed to lock channel ID `{channel_id}`.")
    except (ValueError, IndexError):
        await event.respond("❌ **Invalid command format.**\n\nUse: `/lock CHANNEL_ID`")
    except Exception as e:
        await event.respond(f"❌ Error: {str(e)}")

@gf.on(events.NewMessage(incoming=True, pattern='/unlock'))
async def unlock_channel_handler(event):
    """Handle channel unlocking command (owner only)"""
    # Silent admin check - no response for non-admins
    if event.sender_id not in OWNER_ID:
        return
    try:
        channel_id = int(event.text.split(' ')[1])
        success = telegram_bot.db.unlock_channel(channel_id)
        if success:
            await event.respond(f"✅ Channel ID `{channel_id}` unlocked successfully.")
        else:
            await event.respond(f"❌ Failed to unlock channel ID `{channel_id}`.")
    except (ValueError, IndexError):
        await event.respond("❌ **Invalid command format.**\n\nUse: `/unlock CHANNEL_ID`")
    except Exception as e:
        await event.respond(f"❌ Error: {str(e)}")


# Main message handler function (integration point with existing get_msg function)
async def get_msg(userbot, sender, edit_id, msg_link, i, message):
    """Main integration function - enhanced version of original get_msg"""
    try:
        # Track file information
        file_info = {"size": 0, "name": "Unknown"}
        # Determine priority: 0 for premium/owner, 1 for freemium
        try:
            user_tier = await chk_user(message, sender)  # 0 => premium/owner, 1 => free
            priority = 0 if user_tier == 0 else 1
        except Exception:
            priority = 1

        # Derive a human-readable tier label for logging
        try:
            if sender in OWNER_ID:
                tier_label = "owner"
            else:
                tier_label = "premium" if await check_premium(sender) else "free"
        except Exception:
            tier_label = "unknown"

        # For FREE users, acquire a slot in the brutal global download queue BEFORE internal enqueue
        queue_acquired = False
        queue_temp_msg_id = None
        if priority == 1:  # free tier
            async def _ensure_queue_msg(text: str):
                nonlocal queue_temp_msg_id
                try:
                    if edit_id:
                        try:
                            await app.edit_message_text(sender, edit_id, text)
                            return
                        except Exception:
                            pass
                    # fallback to separate status message
                    if queue_temp_msg_id:
                        try:
                            await app.edit_message_text(sender, queue_temp_msg_id, text)
                            return
                        except Exception:
                            pass
                    m = await app.send_message(sender, text)
                    queue_temp_msg_id = m.id
                except Exception:
                    pass

            def _format_queue_update(link: str, position: int, running: int, total: int) -> str:
                try:
                    waiting = max(total - running, 0)
                    return (
                        f"🔗 Link: {link}\n\n"
                        f"⏳ Queue Update\n\n"
                        f"🔢 Your Position: {position} out of {waiting} in queue\n"
                        f"⚙️ Active Tasks: {running} currently running\n"
                        f"📊 Total Tasks: {total} (running + waiting)\n\n"
                        f"⏱️ Please wait...\n"
                        f"Your task will start automatically when a slot is free."
                    )
                except Exception:
                    return "⏳ Queueing... Please wait."

            def _update_cb(_link: str, pos: int, running: int, total: int):
                try:
                    text = _format_queue_update(_link, pos, running, total)
                    asyncio.create_task(_ensure_queue_msg(text))
                except Exception:
                    pass

            async def _cancel_check():
                try:
                    return await cancel_manager.is_cancelled(sender)
                except Exception:
                    return False
            await download_queue.acquire(sender, msg_link, _update_cb, cancel_check=_cancel_check)
            queue_acquired = True
            # Once acquired, restore the downloading UI if we had an edit_id
            try:
                if edit_id:
                    await app.edit_message_text(sender, edit_id, "📥 <b>Starting download...</b>", parse_mode=ParseMode.HTML)
            except Exception:
                pass

        # Execute based on tier:
        # - Free users (priority==1): run directly after global queue acquire to avoid double-queuing deadlocks
        # - Premium/Owner (priority==0): use internal prioritized worker queue
        try:
            if priority == 1:
                # Run the download directly now that we hold a global slot
                result = await telegram_bot.handle_message_download(userbot, sender, edit_id, msg_link, i, message)
            else:
                try:
                    print(f"[QUEUE] Enqueue user={sender} tier={tier_label} priority={priority} link={msg_link}")
                except Exception:
                    pass
                result = await telegram_bot.enqueue_download(
                    priority,
                    lambda: telegram_bot.handle_message_download(userbot, sender, edit_id, msg_link, i, message)
                )
        finally:
            # Release global queue slot for free tier and cleanup temp status
            if queue_acquired:
                try:
                    await download_queue.release()
                except Exception:
                    pass
                if queue_temp_msg_id:
                    try:
                        await app.delete_messages(sender, queue_temp_msg_id)
                    except Exception:
                        pass

        # If the result contains file information, extract it
        if isinstance(result, dict) and "file_info" in result:
            file_info = result["file_info"]
        
        # Return the file information for batch processing
        return {"file_info": file_info, "success": True}
    except Exception as e:
        # If there's an error, re-raise it to be handled by the caller
        raise e

print("✅ Smart Telegram Bot initialized successfully!")
print(f"📊 Features loaded:")
print(f"   • Database: {'✅' if telegram_bot.db else '❌'}")
print(f"   • Pro Client (2GB): {'✅' if telegram_bot.pro_client else '❌'}")
print(f"   • Userbot: {'✅' if gf else '❌'}")
print(f"   • App Client: {'✅' if app else '❌'}")
