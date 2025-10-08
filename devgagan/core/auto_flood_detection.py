"""
Auto Flood Wait Detection System
- Detects when user sessions get flood waits
- Automatically applies flood wait using existing system
- Cancels ongoing downloads and batches
- Notifies admins of automatic flood waits
"""

import asyncio
from datetime import datetime
from devgagan.core.simple_flood_wait import flood_manager
from devgagan.core.cancel import cancel_manager
from config import OWNER_ID, AUTO_FLOODWAIT, AUTO_FLOOD_TIME, LOG_GROUP
from devgagan import app

# Global settings for auto flood wait
auto_flood_settings = {
    "enabled": AUTO_FLOODWAIT,
    "flood_time": AUTO_FLOOD_TIME
}

class AutoFloodDetection:
    """Automatic flood wait detection and handling"""
    
    @staticmethod
    async def detect_user_flood_wait(user_id: int, flood_wait_seconds: int, context: str = "download"):
        """
        Detect and handle user session flood wait
        
        Args:
            user_id: User who got flood waited
            flood_wait_seconds: Duration of flood wait from Telegram
            context: Context where flood wait occurred (download, batch, etc.)
        """
        if not auto_flood_settings["enabled"]:
            print(f"🔍 AUTO FLOOD: Disabled - ignoring {flood_wait_seconds}s flood wait for user {user_id} during {context}")
            return
        
        try:
            print(f"🚨 AUTO FLOOD DETECTION: User {user_id} got {flood_wait_seconds}s flood wait during {context}")
            
            # Cancel all user operations immediately
            print(f"🛑 AUTO FLOOD: Cancelling all operations for user {user_id}")
            await cancel_manager.cancel(user_id)
            
            # Clear user from active processes (import here to avoid circular imports)
            try:
                from devgagan.modules.main import users_loop, process_start_times
                if user_id in users_loop:
                    users_loop[user_id] = False
                    process_start_times.pop(user_id, None)
                    print(f"🔧 Cleared active process for user {user_id} due to auto flood wait")
            except ImportError:
                pass
            
            # Apply automatic flood wait using configured time
            auto_flood_time = auto_flood_settings["flood_time"]
            print(f"⏰ AUTO FLOOD: Applying {auto_flood_time}s flood wait to user {user_id} (original: {flood_wait_seconds}s)")
            success = await flood_manager.apply_flood_wait(user_id, auto_flood_time, 0)  # 0 = automatic system
            
            if success:
                print(f"✅ AUTO FLOOD: Successfully applied {auto_flood_time}s flood wait to user {user_id}")
                print(f"📧 AUTO FLOOD: Notifying admins about automatic flood wait for user {user_id}")
                
                # Notify admins only (no user notification)
                await AutoFloodDetection._notify_admins(user_id, flood_wait_seconds, auto_flood_time, context)
                
            else:
                print(f"❌ AUTO FLOOD: Failed to apply automatic flood wait to user {user_id}")
                
        except Exception as e:
            print(f"❌ Error in auto flood detection for user {user_id}: {e}")
    
    @staticmethod
    async def _notify_admins(user_id: int, original_flood_seconds: int, applied_flood_seconds: int, context: str):
        """Notify admins about automatic flood wait application"""
        try:
            # Get user info
            try:
                user_info = await app.get_users(user_id)
                username = user_info.username or f"user_{user_info.id}"
                name = f"{user_info.first_name or ''} {user_info.last_name or ''}".strip() or "Unknown"
            except:
                username = "unknown"
                name = "Unknown User"
            
            admin_message = (
                f"🚨 <b>Auto Flood Wait Applied</b>\n\n"
                f"👤 <b>User:</b> {name} (@{username})\n"
                f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
                f"📍 <b>Context:</b> {context}\n\n"
                f"⚡ <b>Original Flood:</b> {original_flood_seconds}s\n"
                f"🛑 <b>Applied Flood:</b> {applied_flood_seconds:,}s ({flood_manager.format_duration(applied_flood_seconds)})\n\n"
                f"<b>Admin Commands:</b>\n"
                f"• Check: <code>/checkflood {user_id}</code>\n"
                f"• Remove: <code>/unflood {user_id}</code>"
            )
            
            # Send to all admins
            admin_count = 0
            for admin_id in OWNER_ID:
                try:
                    await app.send_message(admin_id, admin_message, parse_mode="HTML")
                    admin_count += 1
                    print(f"📧 AUTO FLOOD: Notified admin {admin_id} about user {user_id}")
                except Exception as e:
                    print(f"⚠️ AUTO FLOOD: Could not notify admin {admin_id}: {e}")
            
            print(f"✅ AUTO FLOOD: Notified {admin_count}/{len(OWNER_ID)} admins about user {user_id}")
            
            # Also send to log group if configured
            if LOG_GROUP:
                try:
                    await app.send_message(LOG_GROUP, admin_message, parse_mode="HTML")
                    print(f"📧 AUTO FLOOD: Sent notification to log group for user {user_id}")
                except Exception as e:
                    print(f"⚠️ AUTO FLOOD: Could not send to log group: {e}")
                    
        except Exception as e:
            print(f"❌ Error notifying admins: {e}")

# Global instance
auto_flood_detector = AutoFloodDetection()
