"""
File Hashing Database Module for Telegram Bot
Provides deduplication functionality to avoid re-downloading identical files
"""

import hashlib
import os
import time
from typing import Optional, Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB

class FileHashManager:
    """Manages file hashing and deduplication in MongoDB"""
    
    def __init__(self):
        self.client = AsyncIOMotorClient(MONGO_DB)
        self.db = self.client["telegram_bot"]
        self.collection = self.db["file_hashes"]
        self._initialized = False
    
    async def initialize(self):
        """Initialize the collection with proper indexes"""
        if self._initialized:
            return
            
        try:
            # Create indexes for efficient queries
            await self.collection.create_index("file_hash", unique=True)
            await self.collection.create_index("file_size")
            await self.collection.create_index("file_name")
            await self.collection.create_index([("file_size", 1), ("file_hash", 1)])
            # Note: created_at index is created below as TTL index
            
            # TTL index to automatically clean old entries (90 days)
            try:
                await self.collection.create_index("created_at", expireAfterSeconds=90*24*60*60)
            except Exception as ttl_err:
                # Index might already exist - check if it's an options conflict
                if "IndexOptionsConflict" in str(ttl_err) or "equivalent index already exists" in str(ttl_err):
                    # Index exists but with different options, silently continue
                    # The existing TTL index (7776000 seconds = 90 days) is what we want anyway
                    pass
                else:
                    # Different error, re-raise
                    raise ttl_err
            
            self._initialized = True
            print("✅ File hash database initialized with indexes")
        except Exception as e:
            print(f"⚠️ Warning: Could not create file hash indexes: {e}")
            self._initialized = True  # Continue anyway
    
    def _calculate_file_hash(self, file_path: str, chunk_size: int = 8192) -> str:
        """
        Calculate SHA-256 hash of a file efficiently
        
        Args:
            file_path: Path to the file
            chunk_size: Size of chunks to read (default 8KB)
            
        Returns:
            SHA-256 hash as hex string
        """
        sha256_hash = hashlib.sha256()
        
        try:
            with open(file_path, "rb") as f:
                # Read file in chunks to handle large files efficiently
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            print(f"❌ Error calculating hash for {file_path}: {e}")
            return None
    
    def _calculate_message_hash(self, chat_id: int, message_id: int, file_size: int) -> str:
        """
        Calculate a unique hash for a Telegram message file
        This helps identify the same file from the same message
        
        Args:
            chat_id: Telegram chat ID
            message_id: Telegram message ID  
            file_size: File size in bytes
            
        Returns:
            SHA-256 hash as hex string
        """
        # Create a unique identifier for this specific message file
        message_identifier = f"{chat_id}:{message_id}:{file_size}"
        return hashlib.sha256(message_identifier.encode()).hexdigest()
    
    async def check_file_exists(self, file_path: str = None, chat_id: int = None, 
                               message_id: int = None, file_size: int = None) -> Optional[Dict[str, Any]]:
        """
        Check if a file already exists in the database
        
        Args:
            file_path: Local file path (for hash calculation)
            chat_id: Telegram chat ID
            message_id: Telegram message ID
            file_size: File size in bytes
            
        Returns:
            Dictionary with file info if found, None otherwise
        """
        await self.initialize()
        
        try:
            # Method 1: Check by file hash (most accurate)
            if file_path and os.path.exists(file_path):
                file_hash = self._calculate_file_hash(file_path)
                if file_hash:
                    result = await self.collection.find_one({"file_hash": file_hash})
                    if result:
                        print(f"🔍 Found duplicate file by hash: {file_hash[:16]}...")
                        return result
            
            # Method 2: Check by message hash (for pre-download checking)
            if chat_id and message_id and file_size:
                message_hash = self._calculate_message_hash(chat_id, message_id, file_size)
                result = await self.collection.find_one({"message_hash": message_hash})
                if result:
                    print(f"🔍 Found duplicate message: chat={chat_id}, msg={message_id}")
                    return result
            
            # Method 3: Check by file size and name (less accurate but fast)
            if file_size and file_path:
                file_name = os.path.basename(file_path)
                result = await self.collection.find_one({
                    "file_size": file_size,
                    "file_name": file_name
                })
                if result:
                    print(f"🔍 Found potential duplicate by size+name: {file_name} ({file_size} bytes)")
                    return result
                    
        except Exception as e:
            print(f"❌ Error checking file existence: {e}")
        
        return None
    
    async def store_file_hash(self, file_path: str, log_group_message_id: int,
                             chat_id: int = None, message_id: int = None,
                             user_id: int = None, additional_info: Dict = None) -> bool:
        """
        Store file hash information in the database
        
        Args:
            file_path: Path to the downloaded file
            log_group_message_id: Message ID in LOG_GROUP where file was uploaded
            chat_id: Original Telegram chat ID
            message_id: Original Telegram message ID
            user_id: User who requested the download
            additional_info: Additional metadata to store
            
        Returns:
            True if stored successfully, False otherwise
        """
        await self.initialize()
        
        try:
            if not os.path.exists(file_path):
                print(f"❌ Cannot store hash: file not found: {file_path}")
                return False
            
            # Calculate file hash
            file_hash = self._calculate_file_hash(file_path)
            if not file_hash:
                return False
            
            # Get file information
            file_stat = os.stat(file_path)
            file_size = file_stat.st_size
            file_name = os.path.basename(file_path)
            
            # Calculate message hash if message info provided
            message_hash = None
            if chat_id and message_id:
                message_hash = self._calculate_message_hash(chat_id, message_id, file_size)
            
            # Prepare document
            doc = {
                "file_hash": file_hash,
                "file_name": file_name,
                "file_size": file_size,
                "log_group_message_id": log_group_message_id,
                "created_at": time.time(),
                "created_by_user": user_id,
                "original_chat_id": chat_id,
                "original_message_id": message_id,
                "message_hash": message_hash,
                "file_path_when_stored": file_path,  # For debugging
            }
            
            # Add additional info if provided
            if additional_info:
                doc.update(additional_info)
            
            # Store in database (upsert to handle duplicates)
            await self.collection.update_one(
                {"file_hash": file_hash},
                {"$set": doc},
                upsert=True
            )
            
            print(f"💾 Stored file hash: {file_hash[:16]}... -> LOG_GROUP msg {log_group_message_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error storing file hash: {e}")
            return False
    
    async def get_duplicate_info(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a duplicate file
        
        Args:
            file_hash: SHA-256 hash of the file
            
        Returns:
            Dictionary with file information if found
        """
        await self.initialize()
        
        try:
            result = await self.collection.find_one({"file_hash": file_hash})
            return result
        except Exception as e:
            print(f"❌ Error getting duplicate info: {e}")
            return None
    
    async def forward_existing_file(self, app, user_id: int, log_group_message_id: int) -> bool:
        """
        Forward an existing file from LOG_GROUP to user
        
        Args:
            app: Pyrogram app instance
            user_id: Target user ID
            log_group_message_id: Message ID in LOG_GROUP to forward
            
        Returns:
            True if forwarded successfully, False otherwise
        """
        try:
            from config import LOG_GROUP
            
            # Forward the message from LOG_GROUP to user
            forwarded_message = await app.forward_messages(
                chat_id=user_id,
                from_chat_id=LOG_GROUP,
                message_ids=log_group_message_id,
                drop_author=True
            )
            
            if forwarded_message:
                print(f"📤 Forwarded existing file (LOG_GROUP msg {log_group_message_id}) to user {user_id}")
                return True
            else:
                print(f"❌ Failed to forward existing file to user {user_id}")
                return False
                
        except Exception as e:
            print(f"❌ Error forwarding existing file: {e}")
            return False
    
    async def cleanup_old_hashes(self, days_old: int = 90) -> int:
        """
        Clean up old hash entries (manual cleanup if TTL index is not working)
        
        Args:
            days_old: Remove entries older than this many days
            
        Returns:
            Number of entries removed
        """
        await self.initialize()
        
        try:
            cutoff_time = time.time() - (days_old * 24 * 60 * 60)
            result = await self.collection.delete_many({
                "created_at": {"$lt": cutoff_time}
            })
            
            deleted_count = result.deleted_count
            if deleted_count > 0:
                print(f"🧹 Cleaned up {deleted_count} old file hash entries (>{days_old} days)")
            
            return deleted_count
            
        except Exception as e:
            print(f"❌ Error cleaning up old hashes: {e}")
            return 0
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about stored file hashes
        
        Returns:
            Dictionary with statistics
        """
        await self.initialize()
        
        try:
            total_files = await self.collection.count_documents({})
            
            # Get size statistics
            pipeline = [
                {"$group": {
                    "_id": None,
                    "total_size": {"$sum": "$file_size"},
                    "avg_size": {"$avg": "$file_size"},
                    "max_size": {"$max": "$file_size"},
                    "min_size": {"$min": "$file_size"}
                }}
            ]
            
            size_stats = await self.collection.aggregate(pipeline).to_list(1)
            size_info = size_stats[0] if size_stats else {}
            
            # Get recent activity (last 7 days)
            week_ago = time.time() - (7 * 24 * 60 * 60)
            recent_files = await self.collection.count_documents({
                "created_at": {"$gte": week_ago}
            })
            
            return {
                "total_files": total_files,
                "total_size_bytes": size_info.get("total_size", 0),
                "average_size_bytes": size_info.get("avg_size", 0),
                "largest_file_bytes": size_info.get("max_size", 0),
                "smallest_file_bytes": size_info.get("min_size", 0),
                "files_last_7_days": recent_files
            }
            
        except Exception as e:
            print(f"❌ Error getting hash statistics: {e}")
            return {}

# Global instance
file_hash_manager = FileHashManager()
