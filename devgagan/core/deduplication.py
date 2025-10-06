"""
File Deduplication Module
Integrates file hashing with the existing download system to avoid duplicate downloads
"""

import os
import time
from typing import Optional, Dict, Any, Tuple
from devgagan.core.mongo.file_hash_db import file_hash_manager
from devgagan import app

class DeduplicationManager:
    """Manages file deduplication logic"""
    
    def __init__(self):
        self.enabled = True
        self.stats = {
            "duplicates_found": 0,
            "duplicates_forwarded": 0,
            "downloads_saved": 0,
            "bytes_saved": 0
        }
    
    async def check_before_download(self, chat_id: int, message_id: int, 
                                   file_size: int, file_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Check if file already exists before downloading
        
        Args:
            chat_id: Telegram chat ID
            message_id: Telegram message ID
            file_size: File size in bytes
            file_name: Optional file name
            
        Returns:
            Dictionary with existing file info if found, None otherwise
        """
        if not self.enabled:
            return None
            
        try:
            # Check by message hash (most efficient for pre-download check)
            existing_file = await file_hash_manager.check_file_exists(
                chat_id=chat_id,
                message_id=message_id, 
                file_size=file_size
            )
            
            if existing_file:
                self.stats["duplicates_found"] += 1
                self.stats["downloads_saved"] += 1
                self.stats["bytes_saved"] += file_size
                
                print(f"🔄 DEDUPLICATION: Found existing file for chat={chat_id}, msg={message_id}")
                print(f"   📊 Saved download: {file_size:,} bytes")
                
                return existing_file
                
        except Exception as e:
            print(f"❌ Error in pre-download deduplication check: {e}")
        
        return None
    
    async def check_after_download(self, file_path: str, chat_id: int = None, 
                                  message_id: int = None) -> Optional[Dict[str, Any]]:
        """
        Check if downloaded file is a duplicate (by file hash)
        
        Args:
            file_path: Path to downloaded file
            chat_id: Optional Telegram chat ID
            message_id: Optional Telegram message ID
            
        Returns:
            Dictionary with existing file info if duplicate found, None otherwise
        """
        if not self.enabled or not os.path.exists(file_path):
            return None
            
        try:
            file_size = os.path.getsize(file_path)
            
            # Check by file hash (most accurate)
            existing_file = await file_hash_manager.check_file_exists(file_path=file_path)
            
            if existing_file:
                self.stats["duplicates_found"] += 1
                self.stats["bytes_saved"] += file_size
                
                print(f"🔄 DEDUPLICATION: Downloaded file is duplicate of existing file")
                print(f"   📊 Wasted download: {file_size:,} bytes (will reuse existing)")
                
                return existing_file
                
        except Exception as e:
            print(f"❌ Error in post-download deduplication check: {e}")
        
        return None
    
    async def handle_duplicate_found(self, user_id: int, existing_file: Dict[str, Any], 
                                   original_file_path: str = None) -> bool:
        """
        Handle when a duplicate file is found - forward existing file to user
        
        Args:
            user_id: User ID to send file to
            existing_file: Dictionary with existing file information
            original_file_path: Path to originally downloaded file (for cleanup)
            
        Returns:
            True if successfully handled, False otherwise
        """
        try:
            log_group_message_id = existing_file.get("log_group_message_id")
            if not log_group_message_id:
                print("❌ No LOG_GROUP message ID found in existing file record")
                return False
            
            # Forward existing file from LOG_GROUP to user
            success = await file_hash_manager.forward_existing_file(
                app, user_id, log_group_message_id
            )
            
            if success:
                self.stats["duplicates_forwarded"] += 1
                
                # Clean up the duplicate downloaded file if it exists
                if original_file_path and os.path.exists(original_file_path):
                    try:
                        os.remove(original_file_path)
                        print(f"🧹 Cleaned up duplicate downloaded file: {os.path.basename(original_file_path)}")
                    except Exception as e:
                        print(f"⚠️ Could not clean up duplicate file: {e}")
                
                # Note: Removed user notification to keep the experience seamless
                # The file is forwarded silently without extra messages
                
                return True
            else:
                print("❌ Failed to forward existing file to user")
                return False
                
        except Exception as e:
            print(f"❌ Error handling duplicate file: {e}")
            return False
    
    async def store_new_file(self, file_path: str, log_group_message_id: int,
                           chat_id: int = None, message_id: int = None,
                           user_id: int = None, file_type: str = None) -> bool:
        """
        Store information about a newly uploaded file
        
        Args:
            file_path: Path to the file
            log_group_message_id: Message ID in LOG_GROUP where file was uploaded
            chat_id: Original Telegram chat ID
            message_id: Original Telegram message ID
            user_id: User who requested the download
            file_type: Type of file (video, document, etc.)
            
        Returns:
            True if stored successfully, False otherwise
        """
        if not self.enabled:
            return True  # Don't fail if deduplication is disabled
            
        try:
            additional_info = {}
            if file_type:
                additional_info["file_type"] = file_type
            
            success = await file_hash_manager.store_file_hash(
                file_path=file_path,
                log_group_message_id=log_group_message_id,
                chat_id=chat_id,
                message_id=message_id,
                user_id=user_id,
                additional_info=additional_info
            )
            
            if success:
                print(f"💾 Stored file hash for future deduplication: {os.path.basename(file_path)}")
            
            return success
            
        except Exception as e:
            print(f"❌ Error storing new file hash: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics"""
        return {
            "enabled": self.enabled,
            "duplicates_found": self.stats["duplicates_found"],
            "duplicates_forwarded": self.stats["duplicates_forwarded"],
            "downloads_saved": self.stats["downloads_saved"],
            "bytes_saved": self.stats["bytes_saved"],
            "mb_saved": round(self.stats["bytes_saved"] / (1024 * 1024), 2),
            "gb_saved": round(self.stats["bytes_saved"] / (1024 * 1024 * 1024), 2)
        }
    
    def reset_stats(self):
        """Reset deduplication statistics"""
        self.stats = {
            "duplicates_found": 0,
            "duplicates_forwarded": 0,
            "downloads_saved": 0,
            "bytes_saved": 0
        }
        print("📊 Deduplication statistics reset")
    
    def enable(self):
        """Enable deduplication"""
        self.enabled = True
        print("✅ File deduplication enabled")
    
    def disable(self):
        """Disable deduplication"""
        self.enabled = False
        print("❌ File deduplication disabled")

# Global instance
deduplication_manager = DeduplicationManager()

# Utility functions for easy integration
async def check_duplicate_before_download(chat_id: int, message_id: int, 
                                        file_size: int, file_name: str = None) -> Optional[Dict[str, Any]]:
    """Convenience function to check for duplicates before download"""
    return await deduplication_manager.check_before_download(chat_id, message_id, file_size, file_name)

async def check_duplicate_after_download(file_path: str, chat_id: int = None, 
                                       message_id: int = None) -> Optional[Dict[str, Any]]:
    """Convenience function to check for duplicates after download"""
    return await deduplication_manager.check_after_download(file_path, chat_id, message_id)

async def handle_duplicate_file(user_id: int, existing_file: Dict[str, Any], 
                              original_file_path: str = None) -> bool:
    """Convenience function to handle duplicate files"""
    return await deduplication_manager.handle_duplicate_found(user_id, existing_file, original_file_path)

async def store_file_for_deduplication(file_path: str, log_group_message_id: int,
                                     chat_id: int = None, message_id: int = None,
                                     user_id: int = None, file_type: str = None) -> bool:
    """Convenience function to store file hash"""
    return await deduplication_manager.store_new_file(
        file_path, log_group_message_id, chat_id, message_id, user_id, file_type
    )
