"""
Simple Flood Wait Admin Commands
- /flood <user_id> <seconds> - Apply flood wait
- /unflood <user_id> - Remove flood wait
"""

from pyrogram import filters
from pyrogram.enums import ParseMode
from devgagan import app
from config import OWNER_ID
from devgagan.core.simple_flood_wait import flood_manager
from devgagan.core.cancel import cancel_manager

# Import users_loop to clear active processes
try:
    from devgagan.modules.main import users_loop, process_start_times
except ImportError:
    users_loop = {}
    process_start_times = {}

@app.on_message(filters.command("flood") & filters.private)
async def flood_command(_, message):
    """Apply flood wait to user: /flood <user_id> <seconds>"""
    user_id = message.from_user.id
    
    # Admin only
    if user_id not in OWNER_ID:
        return
    
    try:
        # Parse command
        parts = message.text.split()
        if len(parts) != 3:
            await message.reply(
                "❌ <b>Usage:</b> <code>/flood &lt;user_id&gt; &lt;duration&gt;</code>\n\n"
                "<b>Examples:</b>\n"
                "• <code>/flood 123456789 3600</code> (3600 seconds)\n"
                "• <code>/flood 123456789 20s</code> (20 seconds)\n"
                "• <code>/flood 123456789 30m</code> (30 minutes)\n"
                "• <code>/flood 123456789 2h</code> (2 hours)\n"
                "• <code>/flood 123456789 1d</code> (1 day)",
                parse_mode=ParseMode.HTML
            )
            return
        
        try:
            target_user_id = int(parts[1])
        except ValueError:
            await message.reply("❌ Invalid user ID. Must be a number.")
            return
            
        duration_input = parts[2]
        
        # Parse flexible time duration
        seconds = flood_manager.parse_time_duration(duration_input)
        
        if seconds <= 0:
            await message.reply("❌ Duration must be positive")
            return
        
        # Cancel all user operations first
        await cancel_manager.cancel(target_user_id)
        
        # Clear user from active processes
        if target_user_id in users_loop:
            users_loop[target_user_id] = False
            process_start_times.pop(target_user_id, None)
            print(f"🔧 Cleared active process for user {target_user_id} due to flood wait")
        
        # Apply flood wait
        success = await flood_manager.apply_flood_wait(target_user_id, seconds, user_id)
        
        if success:
            time_display = flood_manager.format_duration(seconds)
            
            await message.reply(
                f"✅ <b>Flood Wait Applied</b>\n\n"
                f"👤 <b>User ID:</b> <code>{target_user_id}</code>\n"
                f"⏰ <b>Duration:</b> {time_display} ({seconds:,} seconds)\n"
                f"🛑 <b>All operations cancelled</b>\n\n"
                f"<b>Unflood command:</b>\n"
                f"<code>/unflood {target_user_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply("❌ Failed to apply flood wait")
            
    except ValueError as e:
        if "Invalid time format" in str(e):
            await message.reply(f"❌ {str(e)}", parse_mode=ParseMode.HTML)
        else:
            await message.reply("❌ Invalid user ID. Must be a number.")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("unflood") & filters.private)
async def unflood_command(_, message):
    """Remove flood wait from user: /unflood <user_id>"""
    user_id = message.from_user.id
    
    # Admin only
    if user_id not in OWNER_ID:
        return
    
    try:
        # Parse command
        parts = message.text.split()
        if len(parts) != 2:
            await message.reply("❌ Usage: `/unflood <user_id>`\nExample: `/unflood 123456789`")
            return
        
        try:
            target_user_id = int(parts[1])
        except ValueError:
            await message.reply("❌ Invalid user ID. Must be a number.")
            return
        
        # Remove flood wait
        success = await flood_manager.remove_flood_wait(target_user_id, user_id)
        
        if success:
            await message.reply(
                f"✅ <b>Flood Wait Removed</b>\n\n"
                f"👤 <b>User ID:</b> <code>{target_user_id}</code>\n"
                f"🔓 <b>User can now download</b>\n\n"
                f"<b>Flood command:</b>\n"
                f"<code>/flood {target_user_id} &lt;duration&gt;</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply(f"⚠️ No active flood wait found for user <code>{target_user_id}</code>", parse_mode=ParseMode.HTML)
            
    except ValueError:
        await message.reply("❌ Invalid user ID. Must be a number.")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("checkflood") & filters.private)
async def check_flood_command(_, message):
    """Check flood wait status for user: /checkflood <user_id>"""
    user_id = message.from_user.id
    
    # Admin only
    if user_id not in OWNER_ID:
        return
    
    try:
        # Parse command
        parts = message.text.split()
        if len(parts) != 2:
            await message.reply("❌ Usage: `/checkflood <user_id>`\nExample: `/checkflood 123456789`")
            return
        
        target_user_id = int(parts[1])
        
        # Check flood wait
        is_flood_waited, seconds_remaining = await flood_manager.check_flood_wait(target_user_id)
        
        if is_flood_waited:
            time_display = flood_manager.format_duration(seconds_remaining)
            
            await message.reply(
                f"🚫 <b>User is Flood Waited</b>\n\n"
                f"👤 <b>User ID:</b> <code>{target_user_id}</code>\n"
                f"⏰ <b>Remaining:</b> {time_display} ({seconds_remaining:,} seconds)\n"
                f"🛑 <b>Downloads blocked</b>\n\n"
                f"<b>Unflood command:</b>\n"
                f"<code>/unflood {target_user_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply(
                f"✅ <b>User is Free</b>\n\n"
                f"👤 <b>User ID:</b> <code>{target_user_id}</code>\n"
                f"🔓 <b>No flood wait active</b>\n"
                f"📥 <b>Can download normally</b>\n\n"
                f"<b>Flood command:</b>\n"
                f"<code>/flood {target_user_id} &lt;duration&gt;</code>",
                parse_mode=ParseMode.HTML
            )
            
    except ValueError:
        await message.reply("❌ Invalid user ID. Must be a number.")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("floodcheck") & filters.private)
async def flood_check_all_command(_, message):
    """List all active flood waits: /floodcheck"""
    user_id = message.from_user.id
    
    # Admin only
    if user_id not in OWNER_ID:
        return
    
    try:
        # Get all active flood waits
        active_waits = await flood_manager.get_all_active_flood_waits()
        
        if not active_waits:
            await message.reply(
                "✅ <b>No Active Flood Waits</b>\n\n"
                "🔓 All users can download normally",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Build the message
        flood_message = f"🚫 <b>Active Flood Waits ({len(active_waits)})</b>\n\n"
        
        for i, wait in enumerate(active_waits, 1):
            user_id_display = wait["user_id"]
            remaining_seconds = wait["remaining_seconds"]
            time_display = flood_manager.format_duration(remaining_seconds)
            
            flood_message += (
                f"<b>{i}.</b> User ID: <code>{user_id_display}</code>\n"
                f"   ⏰ Remaining: {time_display} ({remaining_seconds:,}s)\n"
                f"   🔓 Unflood: <code>/unflood {user_id_display}</code>\n\n"
            )
        
        
        # Split message if too long
        if len(flood_message) > 4000:
            # Send in chunks
            lines = flood_message.split('\n')
            current_chunk = ""
            
            for line in lines:
                if len(current_chunk + line + '\n') > 4000:
                    await message.reply(current_chunk, parse_mode=ParseMode.HTML)
                    current_chunk = line + '\n'
                else:
                    current_chunk += line + '\n'
            
            if current_chunk.strip():
                await message.reply(current_chunk, parse_mode=ParseMode.HTML)
        else:
            await message.reply(flood_message, parse_mode=ParseMode.HTML)
            
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")
