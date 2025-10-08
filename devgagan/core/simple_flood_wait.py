"""
Simple Flood Wait Management System
- Stores flood waits in MongoDB
- Prevents downloads during flood wait
- Admin commands: /flood and /unflood
- Survives restarts and reboots
"""

import re
from datetime import datetime, timedelta
from devgagan.core.mongo.connection import get_collection

# MongoDB collection for flood waits
flood_waits_db = get_collection("flood_management", "active_flood_waits")

class SimpleFloodWaitManager:
    """Simple flood wait management with MongoDB persistence"""
    
    @staticmethod
    def parse_time_duration(duration_str):
        """
        Parse flexible time duration formats:
        - 20s = 20 seconds
        - 20m = 20 minutes  
        - 20h = 20 hours
        - 20d = 20 days
        - 300000 = 300000 seconds (plain number)
        """
        duration_str = str(duration_str).strip().lower()
        
        # If it's just a number, treat as seconds
        if duration_str.isdigit():
            return int(duration_str)
        
        # Parse time with units
        match = re.match(r'^(\d+)([smhd])$', duration_str)
        if not match:
            raise ValueError(f"Invalid time format: {duration_str}. Use formats like: 20s, 30m, 2h, 1d")
        
        value = int(match.group(1))
        unit = match.group(2)
        
        if unit == 's':
            return value
        elif unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        else:
            raise ValueError(f"Invalid time unit: {unit}")
    
    @staticmethod
    def format_duration(seconds):
        """Format seconds into human readable duration"""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            if remaining_seconds > 0:
                return f"{minutes}m {remaining_seconds}s"
            return f"{minutes}m"
        elif seconds < 86400:
            hours = seconds // 3600
            remaining_minutes = (seconds % 3600) // 60
            remaining_seconds = seconds % 60
            result = f"{hours}h"
            if remaining_minutes > 0:
                result += f" {remaining_minutes}m"
            if remaining_seconds > 0:
                result += f" {remaining_seconds}s"
            return result
        else:
            days = seconds // 86400
            remaining_hours = (seconds % 86400) // 3600
            remaining_minutes = (seconds % 3600) // 60
            remaining_seconds = seconds % 60
            result = f"{days}d"
            if remaining_hours > 0:
                result += f" {remaining_hours}h"
            if remaining_minutes > 0:
                result += f" {remaining_minutes}m"
            if remaining_seconds > 0:
                result += f" {remaining_seconds}s"
            return result
    
    @staticmethod
    async def apply_flood_wait(user_id: int, seconds: int, admin_id: int):
        """Apply flood wait to user"""
        try:
            # Calculate expiry time
            now = datetime.utcnow()
            expires_at = now + timedelta(seconds=seconds)
            
            # Store in MongoDB (upsert to replace existing)
            await flood_waits_db.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "applied_at": now,
                        "expires_at": expires_at,
                        "seconds": seconds,
                        "admin_id": admin_id,
                        "active": True
                    }
                },
                upsert=True
            )
            
            print(f"✅ Applied {seconds}s flood wait to user {user_id} by admin {admin_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error applying flood wait: {e}")
            return False
    
    @staticmethod
    async def remove_flood_wait(user_id: int, admin_id: int):
        """Remove flood wait from user"""
        try:
            # Remove from MongoDB
            result = await flood_waits_db.delete_one({"user_id": user_id})
            
            if result.deleted_count > 0:
                print(f"✅ Removed flood wait from user {user_id} by admin {admin_id}")
                return True
            else:
                print(f"⚠️ No active flood wait found for user {user_id}")
                return False
                
        except Exception as e:
            print(f"❌ Error removing flood wait: {e}")
            return False
    
    @staticmethod
    async def check_flood_wait(user_id: int):
        """Check if user has active flood wait"""
        try:
            # Get flood wait from MongoDB
            flood_wait = await flood_waits_db.find_one({"user_id": user_id, "active": True})
            
            if not flood_wait:
                return False, 0
            
            # Check if expired
            now = datetime.utcnow()
            expires_at = flood_wait["expires_at"]
            
            if now >= expires_at:
                # Expired, remove it
                await flood_waits_db.delete_one({"user_id": user_id})
                print(f"🕐 Flood wait expired for user {user_id}, removed automatically")
                return False, 0
            
            # Still active, calculate remaining seconds
            remaining = expires_at - now
            remaining_seconds = int(remaining.total_seconds())
            
            return True, remaining_seconds
            
        except Exception as e:
            print(f"❌ Error checking flood wait: {e}")
            return False, 0
    
    @staticmethod
    async def get_flood_wait_message(user_id: int):
        """Get flood wait message for user"""
        is_flood_waited, seconds_remaining = await SimpleFloodWaitManager.check_flood_wait(user_id)
        
        if is_flood_waited:
            return f"[420 FLOOD_WAIT_X] : ⏳ A wait of {seconds_remaining} seconds is required. Please try again after {seconds_remaining} seconds due to Telegram's flood control."
        
        return None
    
    @staticmethod
    async def get_all_active_flood_waits():
        """Get all active flood waits"""
        try:
            now = datetime.utcnow()
            
            # Get all active flood waits
            cursor = flood_waits_db.find({"active": True})
            active_waits = []
            
            async for flood_wait in cursor:
                expires_at = flood_wait["expires_at"]
                
                # Check if expired
                if now >= expires_at:
                    # Remove expired ones
                    await flood_waits_db.delete_one({"user_id": flood_wait["user_id"]})
                    continue
                
                # Calculate remaining time
                remaining = expires_at - now
                remaining_seconds = int(remaining.total_seconds())
                
                active_waits.append({
                    "user_id": flood_wait["user_id"],
                    "applied_at": flood_wait["applied_at"],
                    "expires_at": expires_at,
                    "remaining_seconds": remaining_seconds,
                    "total_seconds": flood_wait["seconds"],
                    "admin_id": flood_wait["admin_id"]
                })
            
            # Sort by remaining time (least time first)
            active_waits.sort(key=lambda x: x["remaining_seconds"])
            return active_waits
            
        except Exception as e:
            print(f"❌ Error getting active flood waits: {e}")
            return []

# Global instance
flood_manager = SimpleFloodWaitManager()
