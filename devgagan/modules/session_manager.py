import time
import random
import string
import os
import glob
from pyrogram import filters, Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from devgagan import app
from config import OWNER_ID, API_ID, API_HASH
from devgagan.core.session_pool import session_pool
from pyrogram.errors import (
    ApiIdInvalid,
    PhoneNumberInvalid,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    SessionPasswordNeeded,
    PasswordHashInvalid,
    FloodWait
)

def generate_random_session_id(length=8):
    """Generate a random session ID for admin sessions"""
    characters = string.ascii_letters + string.digits
    return 'admin_' + ''.join(random.choice(characters) for _ in range(length))


async def delete_admin_session_files(session_id):
    """Delete admin session files from disk"""
    try:
        files_deleted = 0
        
        # Look for session files with the session_id pattern
        # Admin sessions are typically stored as temp_admin_admin_*.session or session_*.session
        session_patterns = [
            f"temp_admin_{session_id}.session",
            f"temp_admin_admin_{session_id}.session", 
            f"session_{session_id}.session",
            f"{session_id}.session"
        ]
        
        # Also check for journal files
        journal_patterns = [
            f"temp_admin_{session_id}.session-journal",
            f"temp_admin_admin_{session_id}.session-journal",
            f"session_{session_id}.session-journal", 
            f"{session_id}.session-journal"
        ]
        
        all_patterns = session_patterns + journal_patterns
        
        for pattern in all_patterns:
            if os.path.exists(pattern):
                try:
                    os.remove(pattern)
                    files_deleted += 1
                    print(f"🗑️ Deleted session file: {pattern}")
                except Exception as e:
                    print(f"⚠️ Failed to delete {pattern}: {e}")
        
        # Also search for any files containing the session_id in their name
        # This handles cases where the naming pattern might be different
        try:
            for file_path in glob.glob(f"*{session_id}*"):
                if file_path.endswith('.session') or file_path.endswith('.session-journal'):
                    try:
                        os.remove(file_path)
                        files_deleted += 1
                        print(f"🗑️ Deleted session file: {file_path}")
                    except Exception as e:
                        print(f"⚠️ Failed to delete {file_path}: {e}")
        except Exception as e:
            print(f"⚠️ Error during glob search: {e}")
        
        return files_deleted
        
    except Exception as e:
        print(f"❌ Error deleting session files for {session_id}: {e}")
        return 0


@app.on_message(filters.command("sessions"))
async def list_sessions_command(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    """List all admin sessions in the pool with their stats"""
    try:
        # Send initial loading message
        loading_msg = await message.reply_text("🔄 <b>Loading Session Pool Data...</b>")
        
        # Get all sessions
        sessions = await session_pool.get_all_sessions()
        
        if not sessions:
            session_text = "❌ <b>No sessions found in the pool.</b>\n\nClick the ➕ Add Session button below to add your first session!"
            # Still show the management buttons even when no sessions
            keyboard = [
                [InlineKeyboardButton("➕ Add Session", callback_data="add_session")],
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_sessions")],
                [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")]
            ]
            await loading_msg.edit_text(
                session_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        # Format session information
        current_time = time.time()
        session_text = "🔑 <b>Admin Session Pool Status</b>\n\n"
        
        for idx, session in enumerate(sessions, 1):
            session_id = session["_id"]
            is_active = session.get("is_active", False)
            usage_count = session.get("usage_count", 0)
            last_used = session.get("last_used", 0)
            errors = session.get("errors", 0)
            is_in_cooldown = session.get("is_in_cooldown", False)
            device_model = session.get("device_model", "Unknown")
            
            # Format last used time
            if last_used > 0:
                last_used_ago = current_time - last_used
                if last_used_ago < 60:
                    last_used_str = f"{int(last_used_ago)} seconds ago"
                elif last_used_ago < 3600:
                    last_used_str = f"{int(last_used_ago/60)} minutes ago"
                else:
                    last_used_str = f"{int(last_used_ago/3600)} hours ago"
            else:
                last_used_str = "Never"
            
            # Format cooldown info
            cooldown_str = ""
            if is_in_cooldown:
                cooldown_remaining = session.get("cooldown_remaining", 0)
                cooldown_str = f"⏳ Cooldown: {int(cooldown_remaining)} seconds remaining\n"
            
            # Status emoji
            status_emoji = "🟢" if is_active and not is_in_cooldown else "🟠" if is_active and is_in_cooldown else "🔴"
            
            session_text += f"{status_emoji} <b>Session #{idx}</b>\n"
            session_text += f"ID: <code>{session_id}</code>\n"
            session_text += f"Device: {device_model}\n"
            session_text += f"Usage: {usage_count} requests\n"
            session_text += f"Last Used: {last_used_str}\n"
            session_text += f"Errors: {errors}\n"
            session_text += cooldown_str
            session_text += "\n"
        
        # Add management buttons and remove buttons for each session
        keyboard = [
            [InlineKeyboardButton("➕ Add Session", callback_data="add_session")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_sessions")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")]
        ]
        
        # Add remove buttons for each session
        if sessions:
            keyboard.append([InlineKeyboardButton("🗑️ Remove Session", callback_data="show_remove_options")])
        
        await loading_msg.edit_text(
            session_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        await message.reply_text(f"❌ <b>Error listing sessions</b>: {str(e)}")


@app.on_message(filters.command("addsession"))
async def add_session_command(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    """Add a new admin session to the pool (legacy method)"""
    try:
        # Check command format
        if len(message.command) < 3:
            await message.reply_text(
                "❌ <b>Invalid format</b>\n\n"
                "<b>Legacy Method:</b>\n"
                "<code>/addsession session_id session_string [device_model]</code>\n\n"
                "<b>🆕 Recommended Method:</b>\n"
                "Use <code>/sessions</code> and click the \"➕ Add Session\" button for an interactive login flow!\n\n"
                "Example (legacy):\n<code>/addsession admin123 1BQANOTEuMTA4LjU2LjE5MAAAAHXtbJC8ZY9Cw4TZsj2w9plK0YzF... iPhone 13 Pro</code>"
            )
            return
        
        # Extract parameters
        session_id = message.command[1]
        session_string = message.command[2]
        device_model = " ".join(message.command[3:]) if len(message.command) > 3 else "iPhone 16 Pro"
        
        # Add session to pool
        success = await session_pool.add_session(session_id, session_string, device_model)
        
        if success:
            await message.reply_text(
                f"✅ <b>Session added successfully!</b>\n\n"
                f"ID: <code>{session_id}</code>\n"
                f"Device: {device_model}\n\n"
                f"💡 <b>Tip:</b> Next time, use <code>/sessions</code> and click \"➕ Add Session\" for an easier interactive login process!\n\n"
                f"Use /sessions to view all sessions."
            )
        else:
            await message.reply_text("❌ <b>Failed to add session</b>\n\nPlease check the session string and try again.")
        
    except Exception as e:
        await message.reply_text(f"❌ <b>Error adding session</b>: {str(e)}")


@app.on_message(filters.command("removesession"))
async def remove_session_command(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    """Remove an admin session from the pool (legacy method)"""
    try:
        # Check command format
        if len(message.command) != 2:
            await message.reply_text(
                "❌ <b>Invalid format</b>\n\n"
                "<b>Legacy Method:</b>\n"
                "<code>/removesession session_id</code>\n\n"
                "<b>🆕 Recommended Method:</b>\n"
                "Use <code>/sessions</code> and click the \"🗑️ Remove Session\" button for an easier interface!\n\n"
                "Example (legacy):\n<code>/removesession admin123</code>\n\n"
                "Use /sessions to view all session IDs."
            )
            return
        
        # Extract parameters
        session_id = message.command[1]
        
        # Delete session files from disk first
        files_deleted = await delete_admin_session_files(session_id)
        
        # Remove session from pool
        success = await session_pool.remove_session(session_id)
        
        if success:
            if files_deleted > 0:
                await message.reply_text(
                    f"✅ <b>Session</b> <code>{session_id}</code> <b>removed successfully!</b>\n\n"
                    f"🗑️ <b>Cleaned up {files_deleted} session file(s) from disk.</b>\n\n"
                    f"💡 <b>Tip:</b> Next time, use <code>/sessions</code> and click \"🗑️ Remove Session\" for an easier interface!"
                )
            else:
                await message.reply_text(
                    f"✅ <b>Session</b> <code>{session_id}</code> <b>removed successfully!</b>\n\n"
                    f"ℹ️ <b>No session files found on disk to clean up.</b>\n\n"
                    f"💡 <b>Tip:</b> Next time, use <code>/sessions</code> and click \"🗑️ Remove Session\" for an easier interface!"
                )
        else:
            await message.reply_text(f"❌ <b>Failed to remove session</b> <code>{session_id}</code>\n\nSession may not exist.")
        
    except Exception as e:
        await message.reply_text(f"❌ <b>Error removing session</b>: {str(e)}")


# Callback handlers for inline buttons
@app.on_callback_query(filters.regex("^refresh_sessions$"))
async def refresh_sessions_callback(_, callback_query):
    """Refresh the sessions list"""
    # Silent admin check - no response for non-admins
    if callback_query.from_user.id not in OWNER_ID:
        return
    try:
        await callback_query.answer("Refreshing sessions...")
        await list_sessions_command(None, callback_query.message)
    except Exception as e:
        await callback_query.message.reply_text(f"❌ <b>Error refreshing sessions</b>: {str(e)}")


@app.on_callback_query(filters.regex("^add_session$"))
async def add_session_callback(_, callback_query):
    """Start interactive login flow to add a new admin session"""
    try:
        await callback_query.answer()
        user_id = callback_query.from_user.id
        
        # Silent admin check - no response for non-admins
        if user_id not in OWNER_ID:
            return
        
        await callback_query.message.reply_text(
            "🔐 <b>Add New Admin Session</b>\n\n"
            "I'll guide you through the login process to add a new admin session to the upload pool.\n\n"
            "⚠️ <b>Important:</b> This will create a new admin session for uploading purposes.\n\n"
            "Please provide your phone number with country code (e.g., +1234567890):"
        )
        
        # Start the interactive login process
        await start_admin_login_flow(_, callback_query.message, user_id)
        
    except Exception as e:
        await callback_query.message.reply_text(f"❌ <b>Error starting login flow</b>: {str(e)}")


async def start_admin_login_flow(client, message, user_id):
    """Start the interactive login flow for adding admin sessions"""
    try:
        # Ask for phone number
        number = await client.ask(
            user_id, 
            'Please enter your phone number along with the country code.\nExample: +19876543210', 
            filters=filters.text,
            timeout=300
        )
        phone_number = number.text
        
        # Generate unique session ID for admin
        session_id = generate_random_session_id()
        
        try:
            await message.reply("📲 Sending OTP...")
            temp_client = Client(f"temp_admin_{session_id}", API_ID, API_HASH, in_memory=True)
            await temp_client.connect()
        except Exception as e:
            await message.reply(f"❌ Failed to send OTP {e}. Please wait and try again later.")
            return
            
        try:
            code = await temp_client.send_code(phone_number)
        except ApiIdInvalid:
            await message.reply('❌ Invalid combination of API ID and API HASH. Please restart the session.')
            return
        except PhoneNumberInvalid:
            await message.reply('❌ Invalid phone number. Please restart the session.')
            return
            
        try:
            otp_code = await client.ask(
                user_id, 
                "Please check for an OTP in your official Telegram account. Once received, enter the OTP in the following format:\nIf the OTP is <code>12345</code>, please enter it as <code>1 2 3 4 5</code>.", 
                filters=filters.text, 
                timeout=600
            )
        except TimeoutError:
            await message.reply('⏰ Time limit of 10 minutes exceeded. Please restart the session.')
            return
            
        phone_code = otp_code.text.replace(" ", "")
        
        try:
            await temp_client.sign_in(phone_number, code.phone_code_hash, phone_code)
        except PhoneCodeInvalid:
            await message.reply('❌ Invalid OTP. Please restart the session.')
            return
        except PhoneCodeExpired:
            await message.reply('❌ Expired OTP. Please restart the session.')
            return
        except SessionPasswordNeeded:
            try:
                two_step_msg = await client.ask(
                    user_id, 
                    'Your account has two-step verification enabled. Please enter your password.', 
                    filters=filters.text, 
                    timeout=300
                )
            except TimeoutError:
                await message.reply('⏰ Time limit of 5 minutes exceeded. Please restart the session.')
                return
                
            try:
                password = two_step_msg.text
                await temp_client.check_password(password=password)
            except PasswordHashInvalid:
                await two_step_msg.reply('❌ Invalid password. Please restart the session.')
                return
                
        # Export session string
        string_session = await temp_client.export_session_string()
        
        # Get user info for device model
        try:
            me = await temp_client.get_me()
            device_model = f"Admin Session - {me.first_name or 'Unknown'}"
        except:
            device_model = "Admin Session - iPhone 16 Pro"
            
        await temp_client.disconnect()
        
        # Add session to admin pool
        success = await session_pool.add_session(session_id, string_session, device_model)
        
        if success:
            await otp_code.reply(
                f"✅ <b>Admin session added successfully!</b>\n\n"
                f"Session ID: <code>{session_id}</code>\n"
                f"Device: {device_model}\n\n"
                f"This session is now available in the admin upload pool.\n\n"
                f"Use /sessions to view all sessions."
            )
        else:
            await otp_code.reply("❌ <b>Failed to add session to admin pool</b>\n\nPlease try again later.")
            
    except Exception as e:
        await message.reply(f"❌ <b>Error during login flow</b>: {str(e)}")


@app.on_callback_query(filters.regex("^show_remove_options$"))
async def show_remove_options_callback(_, callback_query):
    """Show list of sessions that can be removed"""
    try:
        await callback_query.answer()
        user_id = callback_query.from_user.id
        
        # Silent admin check - no response for non-admins
        if user_id not in OWNER_ID:
            return
        
        # Get all sessions
        sessions = await session_pool.get_all_sessions()
        
        if not sessions:
            await callback_query.message.reply_text("❌ <b>No sessions found to remove.</b>")
            return
            
        # Create keyboard with remove buttons for each session
        keyboard = []
        for idx, session in enumerate(sessions, 1):
            session_id = session["_id"]
            device_model = session.get("device_model", "Unknown")
            button_text = f"🗑️ Remove {session_id[:12]}... ({device_model[:15]}...)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"remove_session:{session_id}")])
            
        # Add cancel/back controls
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="refresh_sessions")])
        keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")])
        
        await callback_query.message.reply_text(
            "🗑️ <b>Select a session to remove:</b>\n\n"
            "⚠️ <b>Warning:</b> This action cannot be undone!",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        await callback_query.message.reply_text(f"❌ <b>Error showing remove options</b>: {str(e)}")


@app.on_callback_query(filters.regex("^remove_session:(.+)$"))
async def remove_session_callback(_, callback_query):
    """Remove a session from the callback button"""
    try:
        user_id = callback_query.from_user.id
        
        # Silent admin check - no response for non-admins
        if user_id not in OWNER_ID:
            return
            
        session_id = callback_query.data.split(":")[1]
        await callback_query.answer(f"Removing session {session_id}...")
        
        # Delete session files from disk first
        files_deleted = await delete_admin_session_files(session_id)
        
        # Remove session from pool
        success = await session_pool.remove_session(session_id)
        
        if success:
            if files_deleted > 0:
                await callback_query.message.reply_text(
                    f"✅ <b>Session</b> <code>{session_id}</code> <b>removed successfully!</b>\n\n"
                    f"🗑️ <b>Cleaned up {files_deleted} session file(s) from disk.</b>"
                )
            else:
                await callback_query.message.reply_text(
                    f"✅ <b>Session</b> <code>{session_id}</code> <b>removed successfully!</b>\n\n"
                    f"ℹ️ <b>No session files found on disk to clean up.</b>"
                )
            # Refresh the sessions list
            await list_sessions_command(None, callback_query.message)
        else:
            await callback_query.message.reply_text(f"❌ <b>Failed to remove session</b> <code>{session_id}</code>")
        
    except Exception as e:
        await callback_query.message.reply_text(f"❌ <b>Error removing session</b>: {str(e)}")