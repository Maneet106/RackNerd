import time
import random
import string
import asyncio
import re
from pyrogram import filters, Client
from pyrogram.enums import MessagesFilter, ChatType
from devgagan import app, userrbot
from config import (
    API_ID,
    API_HASH,
    FREEMIUM_LIMIT,
    PREMIUM_LIMIT,
    OWNER_ID,
    DEFAULT_SESSION,
    FREE_SINGLE_WAIT_SECONDS,
    FREE_BATCH_WAIT_SECONDS,
)
from pyrogram.errors import FloodWait
from devgagan.core.func import subscribe, chk_user, get_link
from devgagan.core.mongo import db
from devgagan.modules.shrink import *
from devgagan.core.cancel import cancel_manager
from devgagan.core.get_func import get_msg
from devgagan.core.simple_flood_wait import flood_manager

# Global userbot request queue for flood protection
userbot_queue = asyncio.Queue()
userbot_processing = False

# Queued userbot message fetching to prevent concurrent flood waits
async def get_messages_queued(userbot, channel_ref, msg_ids, batch_num):
    """Queue userbot requests to prevent concurrent API calls and flood waits"""
    global userbot_processing
    
    # Add request to queue
    request_data = {
        'userbot': userbot,
        'channel_ref': channel_ref, 
        'msg_ids': msg_ids,
        'batch_num': batch_num,
        'result': None,
        'error': None,
        'done': asyncio.Event()
    }
    
    await userbot_queue.put(request_data)
    
    # Start processing if not already running
    if not userbot_processing:
        asyncio.create_task(process_userbot_queue())
    
    # Wait for our request to be processed
    await request_data['done'].wait()
    
    if request_data['error']:
        raise request_data['error']
    
    return request_data['result']

# Process userbot queue with proper rate limiting
async def process_userbot_queue():
    """Process userbot requests one at a time with rate limiting"""
    global userbot_processing
    userbot_processing = True
    
    try:
        while not userbot_queue.empty():
            request = await userbot_queue.get()
            
            try:
                # Add delay between requests for rate limiting
                await asyncio.sleep(0.3)  # 300ms between userbot calls (optimized for 100 users)
                
                # Make the actual API call
                result = await request['userbot'].get_messages(
                    request['channel_ref'], 
                    request['msg_ids']
                )
                request['result'] = result
                
                # Log progress every 5 batches
                if request['batch_num'] % 5 == 0:
                    print(f"🔄 USERBOT QUEUE: Processed batch {request['batch_num']} (queue size: {userbot_queue.qsize()})")
                
            except FloodWait as fw:
                print(f"🛡️ USERBOT QUEUE: Flood wait {fw.value}s detected, pausing queue...")
                await asyncio.sleep(fw.value + 1)
                # Retry the request after flood wait
                try:
                    result = await request['userbot'].get_messages(
                        request['channel_ref'], 
                        request['msg_ids']
                    )
                    request['result'] = result
                except Exception as retry_err:
                    request['error'] = retry_err
            except Exception as e:
                request['error'] = e
            
            # Mark request as done
            request['done'].set()
            userbot_queue.task_done()
            
    finally:
        userbot_processing = False
from devgagan.core.task_registry import registry
from devgagan.core.cleanup import cleanup_manager

users_loop = {}
interval_set = {}
batch_mode = {}
# Track process start times for better stale detection
process_start_times = {}
# Debug flag to control forwarding debug logs
DEBUG_FORWARD = False

# Startup cleanup function
async def cleanup_stale_processes():
    """Clear any stale process flags on bot startup"""
    try:
        stale_count = len(users_loop)
        if stale_count > 0:
            print(f"🔧 Clearing {stale_count} stale process flags on startup")
            users_loop.clear()
            process_start_times.clear()
            
        # Run comprehensive cleanup
        await cleanup_manager.startup_cleanup()
        
    except Exception as e:
        print(f"Error during startup cleanup: {e}")

# Run cleanup on import
try:
    asyncio.create_task(cleanup_stale_processes())
except Exception:
    # If event loop is not running, cleanup will happen on first use
    pass

# Simple in-memory anti-spam limiter (token bucket-like)
# Keeps recent timestamps per user and enforces per-window limits
from collections import deque
_user_hits = {}


def _rate_limit_allow(user_id: int, is_premium: bool) -> tuple[bool, int]:
    """Return (allowed, wait_seconds). Premium users get higher limits.

    Free: 3 actions / 60s
    Premium/Owner/Verified: 15 actions / 60s
    """
    window = 60
    limit = 15 if is_premium else 3
    q = _user_hits.setdefault(user_id, deque())
    now = int(time.time())
    # drop old
    while q and now - q[0] >= window:
        q.popleft()
    if len(q) < limit:
        q.append(now)
        return True, 0
    # compute wait
    wait = window - (now - q[0])
    return False, max(wait, 1)

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    """Process and upload link with proper success/failure tracking"""
    start_time = time.time()
    file_info = {"size": 0, "name": "Unknown"}
    
    # Track seen/attempted message IDs to avoid duplicate processing (from main loop and backfill)
    seen_ids = set()

    try:
        # Call get_msg and track if it completes successfully
        result = await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        
        # Extract file information if available
        if isinstance(result, dict) and "file_info" in result:
            file_info = result["file_info"]
        
        # Determine if the processed message had downloadable media
        media_type = str(file_info.get("type", "unknown")).lower()
        non_downloadable_types = {"text", "poll", "location", "contact", "dice", "game"}
        is_downloadable = media_type in {"document", "video", "photo", "audio", "animation", "voice", "video_note"}
        
        # If it's not downloadable content, mark as text processed so batch won't count it
        if not is_downloadable:
            processing_time = time.time() - start_time
            processing_time_str = f"{processing_time:.1f}s" if processing_time < 60 else f"{processing_time/60:.1f}m"
            try:
                if msg_id:
                    await app.delete_messages(user_id, msg_id)
            except Exception:
                pass
            # Use the special message to signal batch to skip counting
            return True, "Text message processed", file_info, processing_time_str
        
        # If we reach here, the download/upload was successful
        try:
            await app.delete_messages(user_id, msg_id)
        except Exception:
            pass
        
        # Calculate processing time
        processing_time = time.time() - start_time
        processing_time_str = f"{processing_time:.1f}s" if processing_time < 60 else f"{processing_time/60:.1f}m"
        
        # Reduced sleep time for batch processing efficiency
        await asyncio.sleep(2)
        return True, None, file_info, processing_time_str
    except Exception as e:
        # Handle the error and return the error message
        error_msg = str(e)
        print(f"Error in process_and_upload_link: {error_msg}")
        
        # Calculate processing time even for errors
        processing_time = time.time() - start_time
        processing_time_str = f"{processing_time:.1f}s" if processing_time < 60 else f"{processing_time/60:.1f}m"
        
        # Special case: text messages are technically successful but not media downloads
        if "Text message processed" in error_msg:
            return True, error_msg, file_info, processing_time_str
        
        # Categorize errors for better user feedback
        if "No media found" in error_msg:
            error_type = "no_media"
        elif "Message not found" in error_msg or "message you specified doesn't exist" in error_msg:
            error_type = "not_found"
        elif "Access denied" in error_msg or "not authorized" in error_msg:
            error_type = "access_denied"
        elif "flood" in error_msg.lower() or "wait" in error_msg.lower():
            error_type = "flood_wait"
        else:
            error_type = "general"
            
        return False, error_msg, file_info, processing_time_str, error_type


def _format_seconds(sec: int) -> str:
    if sec < 60:
        return f"{sec}s"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


async def _render_running_tasks() -> str:
    now = int(time.time())
    snapshot = registry.snapshot()
    # Keep only actively running tasks; hide items that look completed (current >= total when total > 0)
    active = []
    for t in snapshot:
        try:
            if t.total > 0 and t.current >= t.total:
                continue
            if str(t.stage).lower() in {"preparing", "downloading", "uploading", "finalizing"}:
                active.append(t)
        except Exception:
            continue

    count = len(active)
    if count == 0:
        return "✅ <b>No running tasks right now.</b>"

    # Group by user for batch/single labeling
    from collections import defaultdict
    by_user = defaultdict(list)
    for t in active:
        by_user[t.user_id].append(t)

    lines = [f"🚀 <b>Currently Running Tasks:</b> <code>{count}</code>", ""]
    for uid, items in by_user.items():
        # Sort most recently updated first
        try:
            items.sort(key=lambda x: x.updated_at, reverse=True)
        except Exception:
            pass
        latest = items[0]
        multi = len(items) > 1
        mode_label = "Batch" if multi else "Single"
        extra = f" (+{len(items)-1} more)" if multi else ""
        header = f"• 👤 <b>User</b>: <code>{uid}</code> | 📦 <b>Mode</b>: <code>{mode_label}</code>{extra}"
        lines.append(header)
        # Show only the most recent active task for this user to avoid duplicates/stale entries
        t = latest
        dur = _format_seconds(now - int(getattr(t, 'started_at', now)))
        total = getattr(t, 'total', 0) or 0
        current = getattr(t, 'current', 0) or 0
        percent = getattr(t, 'percent', 0.0) or 0.0
        pct = f"{percent:.1f}%" if total > 0 else "--"
        size = f"{current}/{total}" if total > 0 else f"{current}"
        link = getattr(t, 'link', '') or ''
        display_link = (link[:60] + "…") if len(str(link)) > 60 else link
        link_html = f"<a href=\"{link}\">{display_link}</a>" if link else "-"
        lines.append(
            (
                f"  ├─ 🆔 <b>Msg</b>: <code>{getattr(t, 'msg_id', '-') }</code> | 🔗 {link_html}\n"
                f"    ⏳ <b>Stage</b>: <code>{getattr(t, 'stage', '-') }</code> | 🧰 <b>Session</b>: <code>{getattr(t, 'session', '-') }</code> | ⏱ <b>Time</b>: <code>{dur}</code> | 📊 <b>Progress</b>: <code>{pct}</code> (<code>{size}</code>)"
            )
        )
        lines.append("")
    return "\n".join(lines)


@app.on_message(filters.command("runningtasks") & filters.private)
async def running_tasks_cmd(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    text = await _render_running_tasks()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="rt:refresh")]])
    await message.reply_text(text, disable_web_page_preview=True, parse_mode=ParseMode.HTML, reply_markup=kb)

@app.on_callback_query(filters.regex(r"^rt:refresh$"))
async def on_runningtasks_refresh(_, callback_query):
    # Silent admin check - no response for non-admins
    if callback_query.from_user.id not in OWNER_ID:
        return
    try:
        text = await _render_running_tasks()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="rt:refresh")]])
        await app.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.id,
            text=text,
            disable_web_page_preview=True,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        await callback_query.answer("Updated")
    except Exception:
        # If unable to edit (deleted), silently ignore
        try:
            await callback_query.answer("Unable to refresh", show_alert=False)
        except Exception:
            pass

# Function to check if the user can proceed
async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  # Premium or owner users can always proceed
        return True, None

    now = datetime.now()

    # Check if the user is on cooldown
    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds
            return False, (
                f"Please wait {remaining_time} seconds(s) before sending another link.\n\n"
                f"Tip: Use /upgrade for instant access and bigger batches."
            )
        else:
            del interval_set[user_id]  # Cooldown expired, remove user from interval set

    return True, None

async def set_interval(user_id, seconds=45):
    now = datetime.now()
    # Set the cooldown interval for the user (value is in seconds)
    interval_set[user_id] = now + timedelta(seconds=seconds)
    

@app.on_message(
    filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+|tg://openmessage\?user_id=\w+&message_id=\d+')
    & filters.private
)
async def single_link(_, message):
    user_id = message.chat.id

    # Check subscription and batch mode
    if await subscribe(_, message) == 1 or user_id in batch_mode:
        return

    # Check flood wait first
    flood_message = await flood_manager.get_flood_wait_message(user_id)
    if flood_message:
        await message.reply(flood_message)
        return

    # Anti-spam: allow more for paid/verified/owner
    is_prem = (await chk_user(message, user_id) == 0) or (user_id in OWNER_ID) or (await is_user_verified(user_id))
    ok, wait = _rate_limit_allow(user_id, is_prem)
    if not ok:
        await message.reply(
            f"Slow down 🛑 Please wait {wait}s before sending the next request.\n\nTip: Get instant processing with Premium — use /upgrade"
        )
        return

    # Check if user is already in a loop
    if users_loop.get(user_id, False):
        # Enhanced stale process detection
        process_start_time = process_start_times.get(user_id, 0)
        current_time = time.time()
        time_elapsed = current_time - process_start_time
        
        # If process has been running for more than 5 minutes, consider it stale
        if time_elapsed > 300:  # 5 minutes
            print(f"🔧 Clearing stale process for user {user_id} (running for {time_elapsed:.1f}s)")
            users_loop[user_id] = False
            process_start_times.pop(user_id, None)
            # Clear any stale cancel flags too
            try:
                await cancel_manager.clear(user_id)
            except Exception:
                pass
        else:
            await message.reply(
                f"You already have an ongoing process. Please wait for it to finish or cancel it with /cancel.\n\n⏱️ Process started {time_elapsed:.0f} seconds ago."
            )
            # Enhanced safety mechanism with longer delay
            try:
                async def _safety_clear():
                    await asyncio.sleep(10)  # Increased from 2 to 10 seconds
                    # Double-check if process is still marked as active
                    if users_loop.get(user_id, False):
                        current_time_check = time.time()
                        elapsed_check = current_time_check - process_start_times.get(user_id, current_time_check)
                        # If still active after 10 more seconds and no recent activity, clear it
                        if elapsed_check > 15:  # Total 15+ seconds since start
                            print(f"🔧 Safety clearing potentially stuck process for user {user_id}")
                            users_loop[user_id] = False
                            process_start_times.pop(user_id, None)
                            try:
                                await cancel_manager.clear(user_id)
                            except Exception:
                                pass
            
                asyncio.create_task(_safety_clear())
            except Exception:
                pass
            return

    # Check freemium limits
    if await chk_user(message, user_id) == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Use /upgrade to get access immediately.")
        return

    # Check cooldown
    can_proceed, response_message = await check_interval(user_id, await chk_user(message, user_id))
    if not can_proceed:
        await message.reply(response_message)
        return

    # Add user to the loop with timestamp tracking
    users_loop[user_id] = True
    process_start_times[user_id] = time.time()

    link = message.text if "tg://openmessage" in message.text else get_link(message.text)
    
    # Early check for login requirements before starting any processing
    requires_login, login_error_msg = await check_login_required(link, user_id)
    if requires_login:
        # Clear ongoing flag and timestamp since we are returning early
        try:
            users_loop[user_id] = False
            process_start_times.pop(user_id, None)
        except Exception:
            pass
        
        # Send login required message with login button
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Login", callback_data="auth:go_login")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
        ])
        await message.reply(login_error_msg, reply_markup=kb, disable_web_page_preview=True)
        return
    
    # Try forwarding first for public and forwardable content (without forward tag)
    try:
        if link and await try_forward_first(link, user_id):
            # Successful forward; set interval ONLY for free users and exit without download/upload
            try:
                if (await chk_user(message, user_id)) == 1 and not await is_user_verified(user_id):
                    await set_interval(user_id, seconds=FREE_SINGLE_WAIT_SECONDS)
            except Exception:
                pass
            # Clear ongoing flag and timestamp since we are returning early
            try:
                users_loop[user_id] = False
                process_start_times.pop(user_id, None)
            except Exception:
                pass
            return
    except Exception:
        # Silently ignore and fall back to existing flow
        pass
    # Start with visible minimal UI; get_func will edit to "📥 Downloading media..." with cancel button
    msg = await message.reply("📥 <b>Downloading media...</b>", parse_mode=ParseMode.HTML)
    success_download = False
    userbot = await initialize_userbot(user_id)
    try:
        if await is_normal_tg_link(link):
            result = await process_and_upload_link(userbot, user_id, msg.id, link, 0, message)
            # result may be (True, None, file_info, time) on success
            try:
                if isinstance(result, tuple) and len(result) >= 2:
                    ok = bool(result[0])
                    err = result[1]
                    # Consider True with no error message as success
                    success_download = ok and (err is None or err == "Text message processed")
                    
                    # 🚀 IMMEDIATE CLEANUP: Clear process flag as soon as content is delivered
                    if success_download:
                        try:
                            users_loop[user_id] = False
                            process_start_times.pop(user_id, None)
                            print(f"✅ Immediate cleanup: Process flag cleared for user {user_id} after successful delivery")
                            
                            # Clean up temporary files after successful delivery
                            await cleanup_manager.cleanup_for_user(user_id, "completed")
                        except Exception:
                            pass
            except Exception:
                success_download = False
            # Apply cooldown ONLY for free users after a successful single download
            try:
                if (await chk_user(message, user_id)) == 1 and not await is_user_verified(user_id):
                    await set_interval(user_id, seconds=FREE_SINGLE_WAIT_SECONDS)
            except Exception:
                pass
        else:
            await process_special_links(userbot, user_id, msg, link)
            # 🚀 IMMEDIATE CLEANUP: Clear process flag after special links processing
            try:
                users_loop[user_id] = False
                process_start_times.pop(user_id, None)
                print(f"✅ Immediate cleanup: Process flag cleared for user {user_id} after special links processing")
                
                # Clean up temporary files after special links processing
                await cleanup_manager.cleanup_for_user(user_id, "completed")
            except Exception:
                pass
            
    except FloodWait as fw:
        await msg.edit_text(
            f'Try again after {fw.x} seconds due to floodwait from Telegram.\n\nTip: Premium users get priority handling — use /upgrade.'
        )
    except Exception as e:
        error_msg = str(e)
        # Check for login-related errors
        if any(keyword in error_msg for keyword in ["LOGIN_REQUIRED", "SESSION_ERROR", "ACCESS_REQUIRED"]):
            # Login-related error - show appropriate message with login button
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔑 Login", callback_data="auth:go_login")],
                [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
            ])
            await msg.edit_text(
                "🔐 <b>Login Required</b>\n\n"
                "This content requires your personal session to access. "
                "Please login first using the /login command.",
                reply_markup=kb,
                disable_web_page_preview=True
            )
        else:
            # Hide technical errors from users
            await msg.edit_text("⚠️ Unable to process this link. Please try again later or check if the link is valid.")
        print(f"Single link processing error for user {user_id}: {e}")
        
        # Clean up files after failed download
        try:
            await cleanup_manager.cleanup_for_user(user_id, f"failed: {str(e)[:50]}")
        except Exception:
            pass
    finally:
        # Fallback cleanup: Only clear if not already cleared by immediate cleanup
        if users_loop.get(user_id, False):
            users_loop[user_id] = False
            process_start_times.pop(user_id, None)
            print(f"🔧 Fallback cleanup: Process flag cleared for user {user_id} in finally block")
        # Only delete the progress message when we actually succeeded.
        # Keep informative messages (login required, access denied, invalid link) visible.
        if success_download:
            try:
                await msg.delete()
            except Exception:
                pass


async def check_login_required(link: str, user_id: int) -> tuple[bool, str]:
    """Check if a link requires user login before attempting download.
    Returns (requires_login, error_message)
    """
    try:
        # AGGRESSIVE APPROACH: ALL group links require login because group history might be hidden
        # regardless of whether the group appears "public"
        
        # Check if it's ANY group/channel link that requires user session
        requires_login = any(identifier in link for identifier in [
            't.me/c/',          # Private channel links
            'telegram.dog/c/',  # Private telegram.dog links
            't.me/b/',          # Bot links (often in private groups)
            'telegram.dog/b/',  # Bot telegram.dog links
            '/s/',              # Story links (require user session)
            'tg://openmessage', # Deep links
            'joinchat',         # Invite links
            't.me/+',           # New invite link format
        ])
        
        # Check for ALL group formats (both public and private groups)
        if not requires_login:
            import re
            # Topic group format: t.me/username/topic_id/message_id
            topic_pattern = r't\.me/[^/]+/\d+/\d+'
            is_topic_group = bool(re.search(topic_pattern, link))
            
            # Regular group format: t.me/username/message_id (could be public group with hidden history)
            group_pattern = r't\.me/[^/]+/\d+$'
            is_group_link = bool(re.search(group_pattern, link))
            
            # Legacy thread indicators
            is_thread_link = ('/t/' in link or '?thread=' in link)
            
            requires_login = is_topic_group or is_group_link or is_thread_link
            
            # Debug logging
            if is_topic_group:
                print(f"[LOGIN-CHECK] Detected topic group link: {link}")
            elif is_group_link:
                print(f"[LOGIN-CHECK] Detected group link (history might be hidden): {link}")
            elif is_thread_link:
                print(f"[LOGIN-CHECK] Detected thread link: {link}")
        
        if requires_login:
            # Check if user has a session
            from devgagan.core.mongo import db
            user_data = await db.get_data(user_id)
            user_session_string = user_data.get("session") if user_data else None
            
            if not user_session_string:
                return True, (
                    "🔐 <b>Login Required</b>\n\n"
                    "Group/channel links require your personal session to access "
                    "because group history might be hidden even in public groups. "
                    "Please login first using the /login command."
                )
        
        return False, ""
    except Exception:
        return False, ""

async def initialize_userbot(user_id): # this ensure the single startup .. even if logged in or not
    data = await db.get_data(user_id)
    if data and data.get("session"):
        try:
            device = 'iPhone 16 Pro' # added gareebi text
            userbot = Client(
                "userbot",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=data.get("session"),
                in_memory=True  # Use in-memory storage to prevent SQLite DB closing issues
            )
            await userbot.start()
            return userbot
        except Exception as e:
            # Session appears invalid/expired. Clear it so /login doesn't falsely say already logged in.
            try:
                print(f"[LOGIN-DEBUG] Failed to start userbot for {user_id}: {e}. Clearing stored session.")
                await db.remove_session(user_id)
            except Exception:
                pass
            await app.send_message(user_id, "Login Expired re do login")
            return None
    else:
        if DEFAULT_SESSION:
            return userrbot
        else:
            return None


async def is_normal_tg_link(link: str) -> bool:
    """Check if the link is a standard Telegram link that doesn't require special handling."""
    # Enhanced list of special identifiers that require userbot or special handling
    special_identifiers = [
        't.me/+',           # Invite links
        't.me/c/',          # Private channel links
        't.me/b/',          # Bot links
        't.me/s/',          # Story links
        'tg://openmessage', # Deep links
        'telegram.dog/c/',  # Private telegram.dog links
        'joinchat'          # Legacy invite links
    ]
    
    # Check if it's a Telegram link
    is_telegram_link = any(domain in link.lower() for domain in ['t.me/', 'telegram.me/', 'telegram.dog/'])
    
    # Check if it requires special handling
    is_special_format = any(identifier in link for identifier in special_identifiers)
    
    # Normal links are Telegram links that don't require special handling
    return is_telegram_link and not is_special_format
    
async def process_special_links(userbot, user_id, msg, link):
    """Process special Telegram links with enhanced error handling"""
    if userbot is None:
        # Persistent, clear CTA to login
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Login", callback_data="auth:go_login")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
        ])
        return await msg.edit_text(
            "🔐 Login required for restricted/private content.\n\nUse /login and then try again.",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    
    # Handle invite links
    if 't.me/+' in link or 'joinchat' in link:
        await msg.edit_text("🔗 Invite link detected. Attempting to join...")
        result = await userbot_join(userbot, link)
        await msg.edit_text(result)
        return
        
    # Handle all special link formats
    special_patterns = ['t.me/c/', 't.me/b/', '/s/', 'tg://openmessage', 'telegram.dog']
    if any(sub in link for sub in special_patterns):
        try:
            # Directly start processing; get_func will display downloading UI
            await process_and_upload_link(userbot, user_id, msg.id, link, 0, msg)
            # Cooldown ONLY for free users after special-links single processing
            try:
                if (await chk_user(message, user_id)) == 1 and not await is_user_verified(user_id):
                    await set_interval(user_id, seconds=FREE_SINGLE_WAIT_SECONDS)
            except Exception:
                pass
            return
        except Exception as e:
            error_msg = str(e)
            # Check for login-related errors first
            if any(keyword in error_msg for keyword in ["LOGIN_REQUIRED", "SESSION_ERROR", "ACCESS_REQUIRED"]):
                # Login-related error - show appropriate message with login button
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔑 Login", callback_data="auth:go_login")],
                    [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
                ])
                await msg.edit_text(
                    "🔐 <b>Login Required</b>\n\n"
                    "This content requires your personal session to access. "
                    "Please login first using the /login command.",
                    reply_markup=kb,
                    disable_web_page_preview=True
                )
            elif "flood" in error_msg.lower() or "wait" in error_msg.lower():
                wait_time = re.search(r'\d+', error_msg)
                wait_msg = f" (wait {wait_time.group()} seconds)" if wait_time else ""
                await msg.edit_text(f"⏳ Rate limit exceeded{wait_msg}. Please try again later.")
            elif "not found" in error_msg.lower() or "doesn't exist" in error_msg.lower():
                await msg.edit_text("❌ Message not found or deleted.")
            elif "access" in error_msg.lower() or "not authorized" in error_msg.lower():
                await msg.edit_text(
                    "🔒 Access denied — this content may be restricted/private.\n\nJoin the chat if needed and use /login to grant access, then try again."
                )
            else:
                await msg.edit_text("⚠️ Unable to process this link. Please try again later.")
                print(f"Special link processing error: {error_msg}")
            return
            
    await msg.edit_text("❌ Invalid link format. Please check and try again.")

async def _parse_public_link(link: str):
    """Parse a public channel/group message link into (chat_ref, message_id).
    Returns (str|None, int|None). Only handles public username links like
    https://t.me/username/123 or telegram.dog/username/123. Private /c/ links are ignored.
    """
    try:
        l = link.strip()
        if not l:
            return None, None
        l_low = l.lower()
        # Reject known special/private formats
        if any(x in l_low for x in ["/c/", "/b/", "/s/", "tg://openmessage", "joinchat", "/+/"]):
            return None, None
        # Must be a telegram domain
        if not any(d in l_low for d in ["t.me/", "telegram.me/", "telegram.dog/"]):
            return None, None
        # Extract parts after domain
        try:
            parts = l.split("//", 1)[-1].split("/")
            # parts[0] = domain, parts[1] = username, parts[2] = message id
            if len(parts) >= 3:
                username = parts[1]
                mid_part = parts[2]
                # Strip query params if any
                mid_part = mid_part.split("?")[0]
                if username and mid_part.isdigit():
                    return username, int(mid_part)
        except Exception:
            return None, None
        return None, None
    except Exception:
        return None, None

async def try_forward_first(link: str, user_id: int):
    """Try to forward (copy) a public message directly with bot session without forward mark.
    Returns True if forwarded, else False. Never raises.
    """
    try:
        if DEBUG_FORWARD:
            print(f"[FORWARD-DEBUG] Checking link for forwarding: {link} -> user {user_id}")
        chat_ref, msg_id = await _parse_public_link(link)
        if not chat_ref or not msg_id:
            if DEBUG_FORWARD:
                print(f"[FORWARD-DEBUG] Not a public message link or unable to parse: {link}")
            return False
        # Check if source chat allows forwarding (protected content)
        try:
            src_chat = await app.get_chat(chat_ref)
            # Disable forwarding for group/supergroup entirely; use user session download instead
            try:
                if getattr(src_chat, "type", None) in (ChatType.SUPERGROUP, ChatType.GROUP):
                    if DEBUG_FORWARD:
                        print(f"[FORWARD-DEBUG] Source is a group/supergroup: {chat_ref}; skipping forward/copy")
                    return False
            except Exception:
                # If type is unavailable, fall through to protected content checks
                pass
            if getattr(src_chat, "has_protected_content", False):
                if DEBUG_FORWARD:
                    print(f"[FORWARD-DEBUG] Source chat has protected content: {chat_ref}")
                return False
            # Only allow copy for channels (broadcast), never for groups/supergroups
            if getattr(src_chat, "type", None) != ChatType.CHANNEL:
                if DEBUG_FORWARD:
                    print(f"[FORWARD-DEBUG] Non-channel source ({getattr(src_chat, 'type', None)}); skipping forward/copy")
                return False
        except Exception:
            # If we cannot fetch chat info, do not risk forwarding; fall back to download path
            if DEBUG_FORWARD:
                print(f"[FORWARD-DEBUG] Could not fetch chat info for {chat_ref}; skipping forward/copy")
            return False
        # Attempt copy to avoid forward tag (channels only)
        try:
            await app.copy_message(chat_id=user_id, from_chat_id=chat_ref, message_id=msg_id)
            if DEBUG_FORWARD:
                print(f"[FORWARD-DEBUG] Copy success: {chat_ref}/{msg_id} -> {user_id}")
            return True
        except Exception as e:
            # Forward restricted or otherwise failed
            if DEBUG_FORWARD:
                print(f"[FORWARD-DEBUG] Copy failed for {chat_ref}/{msg_id}: {e}")
            return False
    except Exception:
        if DEBUG_FORWARD:
            print(f"[FORWARD-DEBUG] Unexpected error in try_forward_first for link: {link}")
        return False

@app.on_message(filters.command("batch") & filters.private)
async def batch_link(_, message):
    join = await subscribe(_, message)
    if join == 1:
        return
    user_id = message.chat.id
    
    # Check flood wait first
    flood_message = await flood_manager.get_flood_wait_message(user_id)
    if flood_message:
        await message.reply(flood_message)
        return
    
    # Check if a batch process is already running
    if users_loop.get(user_id, False):
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete."
        )
        return
        
    # Start timing the batch process
    batch_start_time = time.time()

    freecheck = await chk_user(message, user_id)

    # Anti-spam for batch trigger
    is_prem = (freecheck == 0) or (user_id in OWNER_ID) or (await is_user_verified(user_id))
    ok, wait = _rate_limit_allow(user_id, is_prem)
    if not ok:
        await app.send_message(
            message.chat.id,
            f"Slow down 🛑 Please wait {wait}s before starting another batch.\n\nTip: Premium users skip waits and get higher limits — use /upgrade"
        )
        return
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    max_batch_size = FREEMIUM_LIMIT if freecheck == 1 else PREMIUM_LIMIT

    # Ensure any stale cancel flag is cleared before starting
    try:
        if await cancel_manager.is_cancelled(user_id):
            await cancel_manager.clear(user_id)
    except Exception:
        pass

    # Mark user as in a batch session to avoid collisions
    users_loop[user_id] = True
    process_start_times[user_id] = time.time()

    # Start link input with enhanced validation (single-shot; auto-cancel on commands)
    message_id = None
    base_url = None
    start_id = None
    prompt = (
        "🔗 Please send the start link.\n\n"
        "• Examples: https://t.me/channel/123 or https://t.me/c/123456789/456"
    )
    start = await app.ask(message.chat.id, prompt)
    if not getattr(start, "text", None):
        users_loop.pop(user_id, None)
        return
    # Silent auto-cancel if user sends a command during prompt
    if start.text.strip().startswith("/"):
        users_loop.pop(user_id, None)
        try:
            await cancel_manager.clear(user_id)
        except Exception:
            pass
        return

    start_text = start.text.strip()
    validated_link = get_link(start_text)
    if not validated_link:
        # End immediately on invalid input
        users_loop.pop(user_id, None)
        await app.send_message(message.chat.id, "❌ Invalid link. Batch cancelled.")
        return

    # Extract message ID from various link formats
    # Handle different Telegram link formats
    if '/c/' in validated_link:  # Private channel or topic: t.me/c/<chat>/<msg> OR t.me/c/<chat>/<topic>/<msg>
        parts = validated_link.split('/c/')
        if len(parts) > 1:
            channel_and_msg = parts[1].split('/')
            # Support 3-part private topic links: /c/<chat>/<topic>/<msg>
            if len(channel_and_msg) >= 3 and channel_and_msg[1].isdigit() and channel_and_msg[2].isdigit():
                # topic group: keep topic fixed in base_url, increment only message id
                message_id = int(channel_and_msg[2])
                base_url = f"{parts[0]}/c/{channel_and_msg[0]}/{channel_and_msg[1]}"
            # Support 2-part private links: /c/<chat>/<msg>
            elif len(channel_and_msg) >= 2 and channel_and_msg[1].isdigit():
                message_id = int(channel_and_msg[1])
                base_url = f"{parts[0]}/c/{channel_and_msg[0]}"
    elif '/b/' in validated_link:  # Bot link: t.me/b/1234567890/123
        parts = validated_link.split('/b/')
        if len(parts) > 1:
            bot_and_msg = parts[1].split('/')
            if len(bot_and_msg) >= 2 and bot_and_msg[1].isdigit():
                message_id = int(bot_and_msg[1])
                base_url = f"{parts[0]}/b/{bot_and_msg[0]}"
    elif '/s/' in validated_link:  # Story link: t.me/s/1234567890/123
        parts = validated_link.split('/s/')
        if len(parts) > 1:
            story_and_msg = parts[1].split('/')
            if len(story_and_msg) >= 2 and story_and_msg[1].isdigit():
                message_id = int(story_and_msg[1])
                base_url = f"{parts[0]}/s/{story_and_msg[0]}"
    elif 'telegram.dog' in validated_link:  # Telegram.dog links
        if '/c/' in validated_link:
            parts = validated_link.split('/c/')
            if len(parts) > 1:
                channel_and_msg = parts[1].split('/')
                # Support 3-part private topic links on telegram.dog as well
                if len(channel_and_msg) >= 3 and channel_and_msg[1].isdigit() and channel_and_msg[2].isdigit():
                    message_id = int(channel_and_msg[2])
                    base_url = f"{parts[0]}/c/{channel_and_msg[0]}/{channel_and_msg[1]}"
                elif len(channel_and_msg) >= 2 and channel_and_msg[1].isdigit():
                    message_id = int(channel_and_msg[1])
                    base_url = f"{parts[0]}/c/{channel_and_msg[0]}"
        else:
            # Regular telegram.dog link
            s = validated_link.split("/")[-1]
            if s.isdigit():
                message_id = int(s)
                base_url = "/".join(validated_link.split("/")[:-1])
    else:  # Regular public channel: t.me/channel/123 (includes topic groups)
        s = validated_link.split("/")[-1]
        if s.isdigit():
            message_id = int(s)
            base_url = "/".join(validated_link.split("/")[:-1])
            # Debug logging for topic groups
            if bool(re.search(r't\.me/[^/]+/\d+/\d+', validated_link)):
                print(f"🔍 TOPIC BASE_URL DEBUG: For link {validated_link}, set base_url={base_url}, message_id={message_id}")

    if not message_id:
        users_loop.pop(user_id, None)
        await app.send_message(message.chat.id, "❌ Invalid message ID in link. Batch cancelled.")
        return

    cs = message_id
    start_id = validated_link  # Use the validated link

    # Early check for login requirements before starting batch processing
    requires_login, login_error_msg = await check_login_required(validated_link, user_id)
    if requires_login:
        users_loop.pop(user_id, None)
        # Send login required message with login button
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Login", callback_data="auth:go_login")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
        ])
        await app.send_message(user_id, login_error_msg, reply_markup=kb, disable_web_page_preview=True)
        return

    # Number of messages input (single-shot; auto-cancel on commands)
    num_prompt = (
        f"🔢 How many messages do you want to process?\n"
        f"> Max limit {max_batch_size}"
    )
    num_messages = await app.ask(message.chat.id, num_prompt)
    if not getattr(num_messages, "text", None):
        users_loop.pop(user_id, None)
        return
    if num_messages.text.strip().startswith("/"):
        users_loop.pop(user_id, None)
        try:
            await cancel_manager.clear(user_id)
        except Exception:
            pass
        return
    try:
        cl = int(num_messages.text.strip())
        if not (1 <= cl <= max_batch_size):
            raise ValueError()
    except Exception:
        users_loop.pop(user_id, None)
        await app.send_message(message.chat.id, "❌ Invalid number. Batch cancelled.")
        return

    # Validate and interval check
    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        users_loop.pop(user_id, None)
        return
        
    # Initialize userbot early so we can resolve caps for private channels
    userbot = await initialize_userbot(user_id)

    # Resolve channel reference and detect last message id to avoid scanning past the end
    channel_ref = None
    last_message_id_cap = None
    last_downloadable_id_cap = None  # Smart cap: last message that actually has downloadable media
    try:
        # Derive channel reference from the start link
        # For /c/<internalId>/, convert to -100<internalId>
        if '/c/' in start_id:
            internal = base_url.split('/c/')[1].split('/')[0]
            if internal.isdigit():
                channel_ref = int(f"-100{internal}")
        else:
            # For regular public channels, extract username between t.me/ and last '/'
            parts = base_url.split('/')
            if len(parts) >= 4 and parts[2] in ("t.me", "telegram.me", "telegram.dog"):
                channel_ref = parts[3]
        # If this is a private /c/ link or a group/supergroup and no userbot -> require login
        if userbot is None:
            try:
                needs_user_session = False
                if '/c/' in start_id:
                    needs_user_session = True
                elif channel_ref is not None:
                    # Try resolve chat type using bot; for public usernames this works
                    chat_info = await app.get_chat(channel_ref)
                    if getattr(chat_info, 'type', None) in (ChatType.SUPERGROUP, ChatType.GROUP):
                        needs_user_session = True
                if needs_user_session:
                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔑 Login", callback_data="auth:go_login")],
                        [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
                    ])
                    await app.send_message(
                        user_id,
                        "🔐 Login required to process this chat.\n\nUse /login and then start the batch again.",
                        reply_markup=kb,
                        disable_web_page_preview=True,
                    )
                    users_loop.pop(user_id, None)
                    return
            except Exception:
                # If chat info can't be resolved, proceed; downstream will handle errors gracefully
                pass
        # Try to get last message id using userbot first
        if channel_ref is not None:
            try:
                if userbot:
                    hist = userbot.get_chat_history if hasattr(userbot, 'get_chat_history') else None
                    if hist:
                        async for m in userbot.get_chat_history(channel_ref, limit=1):
                            last_message_id_cap = m.id
                            break
                    else:
                        # Fallback older Pyrogram: use get_history-like via search_messages
                        msgs = await userbot.search_messages(channel_ref, limit=1)
                        if msgs:
                            last_message_id_cap = msgs[0].id
                # If still unknown, try bot for public usernames
                if last_message_id_cap is None and isinstance(channel_ref, str):
                    async for m in app.get_chat_history(channel_ref, limit=1):
                        last_message_id_cap = m.id
                        break
            except Exception:
                pass
    except Exception:
        pass

    # Fast compute last downloadable cap using search_messages on media types (no downloads)
    async def _get_last_media_id(client, chat) -> int | None:
        try:
            latest_ids = []
            for f in [MessagesFilter.DOCUMENT, MessagesFilter.VIDEO, MessagesFilter.PHOTO, MessagesFilter.AUDIO, MessagesFilter.ANIMATION]:
                try:
                    msgs = []
                    async for m in client.search_messages(chat, filter=f, limit=1):
                        msgs.append(m)
                        break
                    if msgs:
                        latest_ids.append(msgs[0].id)
                except Exception:
                    continue
            return max(latest_ids) if latest_ids else None
        except Exception:
            return None

    # Get last media id at or before a given cap using offset_id (so we don't consider newer media beyond cap)
    async def _get_last_media_at_or_before(client, chat, cap_id: int) -> int | None:
        try:
            latest_ids = []
            for f in [MessagesFilter.DOCUMENT, MessagesFilter.VIDEO, MessagesFilter.PHOTO, MessagesFilter.AUDIO, MessagesFilter.ANIMATION]:
                try:
                    msgs = []
                    async for m in client.search_messages(chat, filter=f, offset_id=cap_id + 1, limit=1):
                        msgs.append(m)
                        break
                    if msgs:
                        latest_ids.append(msgs[0].id)
                except Exception:
                    continue
            return max(latest_ids) if latest_ids else None
        except Exception:
            return None

    try:
        # Prefer user session for groups/supergroups and private chats to ensure visibility
        probe_client = userbot if userbot else app
        if channel_ref is not None:
            # Try to detect chat type to choose the safest client
            try:
                # Prefer userbot to fetch chat info; fallback to bot
                info_client = userbot if userbot else app
                chat_info = await info_client.get_chat(channel_ref)
                chat_type = getattr(chat_info, 'type', None)
                if chat_type in (ChatType.SUPERGROUP, ChatType.GROUP):
                    probe_client = userbot if userbot else app
                elif chat_type == ChatType.CHANNEL:
                    # Channels may be fine via bot, but userbot also works; keep current probe_client
                    pass
            except Exception:
                # If type cannot be resolved, keep userbot if available
                probe_client = userbot if userbot else app

                    # For topic groups, we need special handling to get the actual last message in the topic
            if bool(re.search(r't\.me/[^/]+/\d+/\d+', validated_link)):  # Topic group pattern
                try:
                    # Extract topic ID from the link
                    topic_match = re.search(r't\.me/([^/]+)/(\d+)/(\d+)', validated_link)
                    if topic_match:
                        group_username = topic_match.group(1)
                        topic_id = int(topic_match.group(2))
                        print(f"🔍 TOPIC GROUP BATCH: Detecting last message in topic {topic_id} of {group_username}")
                        
                        # Use user session to get topic history
                        if userbot:
                            try:
                                print(f"🔍 TOPIC GROUP: Scanning for messages in topic {topic_id}...")
                                
                                # Method 1: Try to get messages by scanning a range around the start message
                                topic_history = []
                                # Make topic_history available globally for batch processing
                                globals()['current_topic_history'] = topic_history
                                
                                # Smart dynamic scanning based on batch size and user expectations
                                start_msg_id = int(topic_match.group(3))
                                
                                # OPTIMIZED scanning - reduced API calls for better scaling
                                if cl <= 10:  # Small batch (1-10)
                                    scan_behind = 5   # Reduced from 10
                                    scan_ahead = 25   # Reduced from 50
                                elif cl <= 50:  # Medium batch (11-50)
                                    scan_behind = 10  # Reduced from 20
                                    scan_ahead = 75   # Reduced from 100
                                elif cl <= 200:  # Large batch (51-200)
                                    scan_behind = 20  # Reduced from 30
                                    scan_ahead = 150  # Reduced from 200
                                else:  # Huge batch (201+)
                                    scan_behind = 30  # Reduced from 50
                                    scan_ahead = 200  # Reduced from 300
                                
                                start_scan = max(1, start_msg_id - scan_behind)
                                end_scan = start_msg_id + scan_ahead
                                
                                print(f"🔍 TOPIC GROUP: ULTRA FAST scanning for batch size {cl}")
                                print(f"🔍 TOPIC GROUP: Scanning range {start_scan} to {end_scan} ({end_scan - start_scan + 1} messages)")
                                
                                # ULTRA AGGRESSIVE early detection - stop immediately when enough found
                                early_stop_threshold = min(cl, 30)  # Stop at exactly what we need or 30 max
                                print(f"🏁 TOPIC GROUP: Will stop at {early_stop_threshold} messages (ULTRA FAST)")
                                
                                # Get messages in batches to find topic messages (with queue-based flood protection)
                                messages_found_count = 0
                                batch_count = 0
                                
                                # Queue all batch requests to prevent concurrent userbot usage
                                batch_requests = []
                                for batch_start in range(start_scan, end_scan, 30):
                                    batch_end = min(batch_start + 29, end_scan)
                                    msg_ids = list(range(batch_start, batch_end + 1))
                                    batch_requests.append((batch_start, batch_end, msg_ids))
                                
                                print(f"📋 USERBOT QUEUE: Queuing {len(batch_requests)} batch requests for topic detection")
                                
                                for batch_start, batch_end, msg_ids in batch_requests:
                                    try:
                                        batch_count += 1
                                        
                                        # Queue-based userbot access with automatic rate limiting
                                        messages = await get_messages_queued(userbot, channel_ref, msg_ids, batch_count)
                                        
                                        if not isinstance(messages, list):
                                            messages = [messages] if messages else []
                                        
                                        for msg in messages:
                                            if msg and msg.id:
                                                # Check if message belongs to this topic
                                                belongs_to_topic = False
                                                
                                                # Check reply_to_message_id
                                                if hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id == topic_id:
                                                    belongs_to_topic = True
                                                # Check message_thread_id
                                                elif hasattr(msg, 'message_thread_id') and msg.message_thread_id == topic_id:
                                                    belongs_to_topic = True
                                                # Check if it's the topic creation message
                                                elif msg.id == topic_id:
                                                    belongs_to_topic = True
                                                
                                                if belongs_to_topic:
                                                    topic_history.append(msg.id)
                                                    messages_found_count += 1
                                                    if messages_found_count <= 10 or messages_found_count % 20 == 0:
                                                        print(f"📍 TOPIC GROUP: Found message {msg.id} in topic {topic_id} (total: {messages_found_count})")
                                                    
                                                    # Early stop if we found enough messages
                                                    if messages_found_count >= early_stop_threshold:
                                                        print(f"🏁 TOPIC GROUP: ULTRA FAST STOP! Found {messages_found_count} messages")
                                                        break
                                    
                                    except Exception as batch_err:
                                        # Handle flood wait specifically
                                        if "flood" in str(batch_err).lower() or "wait" in str(batch_err).lower():
                                            print(f"🛡️ FLOOD WAIT: Detected flood wait, pausing for 15 seconds...")
                                            await asyncio.sleep(15)
                                            # Try to continue after flood wait
                                            continue
                                        else:
                                            print(f"⚠️ TOPIC GROUP: Batch scan error for {batch_start}-{batch_end}: {batch_err}")
                                            continue
                                    
                                    # Break outer loop if early stop was triggered
                                    if messages_found_count >= early_stop_threshold:
                                        break
                                
                                if topic_history:
                                    # Sort to get the actual range of messages in this topic
                                    topic_history.sort()
                                    
                                    # For hybrid mode, extend the cap beyond what we found to allow more scanning
                                    if len(topic_history) < cl:  # If we didn't find enough, allow more scanning
                                        # Extend BOTH caps to allow hybrid mode to continue scanning (more conservative)
                                        extension = min(200, max(50, cl))  # Much more conservative extension
                                        last_message_id_cap = max(topic_history) + extension
                                        last_downloadable_id_cap = last_message_id_cap  # Same extension for both
                                        print(f"🔄 TOPIC HYBRID PREP: Extended both caps to {last_message_id_cap} (extension: {extension}) for continued scanning")
                                    else:
                                        last_message_id_cap = max(topic_history)
                                        last_downloadable_id_cap = last_message_id_cap
                                        print(f"🎯 TOPIC COMPLETE: Found enough messages, caps at {last_message_id_cap}")
                                    print(f"✅ TOPIC GROUP: Found {len(topic_history)} messages, range: {min(topic_history)} to {max(topic_history)}")
                                    print(f"📋 TOPIC RANGE: Setting caps - message: {last_message_id_cap}, downloadable: {last_downloadable_id_cap}")
                                    # Store for batch processing
                                    globals()['current_topic_history'] = topic_history
                                else:
                                    print(f"⚠️ TOPIC GROUP: No messages found in topic {topic_id} via API scan")
                                    print(f"🔍 TOPIC GROUP: Trying alternative method - get_chat_history")
                                    
                                    # Alternative method: Use get_chat_history and filter
                                    try:
                                        history_count = 0
                                        async for msg in userbot.get_chat_history(channel_ref, limit=100):
                                            if msg and msg.id >= int(topic_match.group(3)):
                                                # Check if this could be a topic message
                                                if (hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id == topic_id) or \
                                                   (hasattr(msg, 'message_thread_id') and msg.message_thread_id == topic_id) or \
                                                   msg.id == topic_id:
                                                    topic_history.append(msg.id)
                                                    history_count += 1
                                                    print(f"📍 TOPIC HISTORY: Found message {msg.id}")
                                        
                                        if topic_history:
                                            topic_history.sort()
                                            last_message_id_cap = max(topic_history)
                                            last_downloadable_id_cap = last_message_id_cap
                                            print(f"✅ TOPIC HISTORY: Found {len(topic_history)} messages via history, last: {last_message_id_cap}")
                                            # Store for batch processing
                                            globals()['current_topic_history'] = topic_history
                                        else:
                                            print(f"⚠️ TOPIC HISTORY: Still no messages found, using safe extended range")
                                            # Final fallback - ultra conservative range
                                            fallback_range = min(300, max(50, cl))  # Max 300, at least 50, or 1x batch size
                                            last_message_id_cap = int(topic_match.group(3)) + fallback_range
                                            last_downloadable_id_cap = last_message_id_cap
                                            print(f"📋 TOPIC FINAL FALLBACK: Setting last_message_id_cap to {last_message_id_cap} (range: {fallback_range})")
                                    
                                    except Exception as history_err:
                                        print(f"❌ TOPIC HISTORY: Failed: {history_err}")
                                        # Final fallback - reasonable range based on batch size
                                        fallback_range = min(500, max(100, cl))  # Max 500, at least 100, or 1x batch size
                                        last_message_id_cap = int(topic_match.group(3)) + fallback_range
                                        last_downloadable_id_cap = last_message_id_cap
                                        print(f"📋 TOPIC FINAL FALLBACK: Setting last_message_id_cap to {last_message_id_cap} (range: {fallback_range})")
                            except Exception as topic_err:
                                print(f"❌ TOPIC GROUP: Failed to get topic history: {topic_err}")
                                # Fallback - reasonable range based on batch size
                                fallback_range = min(500, max(100, cl))  # Max 500, at least 100, or 1x batch size
                                print(f"📋 TOPIC ERROR FALLBACK: Using extended range for topic scanning (range: {fallback_range})")
                                last_message_id_cap = int(topic_match.group(3)) + fallback_range
                                last_downloadable_id_cap = last_message_id_cap
                        else:
                            print(f"⚠️ TOPIC GROUP: No userbot available for topic history")
                            # Without userbot, allow reasonable range based on batch size
                            fallback_range = min(500, max(100, cl))  # Max 500, at least 100, or 1x batch size
                            last_message_id_cap = int(topic_match.group(3)) + fallback_range
                            last_downloadable_id_cap = last_message_id_cap
                            print(f"📋 TOPIC NO-USERBOT: Setting last_message_id_cap to {last_message_id_cap} (range: {fallback_range})")
                    else:
                        # Not a topic group, use original logic
                        overall_last_media = await _get_last_media_id(probe_client, channel_ref)
                        last_downloadable_id_cap = await _get_last_media_at_or_before(probe_client, channel_ref, last_message_id_cap) if last_message_id_cap is not None else overall_last_media
                except Exception as e:
                    print(f"❌ TOPIC GROUP DETECTION ERROR: {e}")
                    # Fallback to original logic
                    overall_last_media = await _get_last_media_id(probe_client, channel_ref)
                    last_downloadable_id_cap = await _get_last_media_at_or_before(probe_client, channel_ref, last_message_id_cap) if last_message_id_cap is not None else overall_last_media
            else:
                # Regular channel/group - use original logic
                overall_last_media = await _get_last_media_id(probe_client, channel_ref)
                last_downloadable_id_cap = await _get_last_media_at_or_before(probe_client, channel_ref, last_message_id_cap) if last_message_id_cap is not None else overall_last_media
        else:
            last_downloadable_id_cap = None
    except Exception:
        last_downloadable_id_cap = None
    # Debug: print detected caps
    try:
        print(f"[BATCH] Initial caps: last_msg_id_cap={last_message_id_cap}, last_dl_id_cap={last_downloadable_id_cap}, channel_ref={channel_ref}")
        if is_topic_group_batch:
            print(f"[BATCH] TOPIC MODE: Hybrid scanning enabled for topic group processing")
    except Exception:
        pass

    # New CTA button (with Cancel) and HTML start message
    cta_btn = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete"),
            InlineKeyboardButton("Open AlienxSaver", url="https://t.me/AlienxSaver")
        ]
    ])
    start_html = (
        f"📦 <b>Batch Processing Started!</b> ⚡️\n\n"
        f"⏳ Progress: <b>0/{cl}</b>\n\n"
        f"🚀 Sit back and relax while we handle everything!"
    )
    pin_msg = await app.send_message(user_id, start_html, reply_markup=cta_btn, disable_web_page_preview=True)
    # Pin for both sides when applicable
    try:
        await pin_msg.pin(both_sides=True)
    except Exception as e:
        print(f"Failed to pin message for user {user_id}: {e}")

    # --- Preflight for t.me/c private groups/supergroups: detect hidden history early ---
    # We run this AFTER sending the initial pin so the batch appears to start immediately.
    try:
        is_private_c = '/c/' in start_id and isinstance(channel_ref, (int,))
        if is_private_c:
            async def _has_accessible_media_at_id(client, chat, mid: int) -> bool:
                try:
                    if mid is None or mid <= 0:
                        return False
                    m = await client.get_messages(chat, mid)
                    if not m:
                        return False
                    # Check for downloadable media without downloading
                    return any([
                        bool(getattr(m, 'document', None)),
                        bool(getattr(m, 'video', None)),
                        bool(getattr(m, 'photo', None)),
                        bool(getattr(m, 'audio', None)),
                        bool(getattr(m, 'animation', None)),
                        bool(getattr(m, 'voice', None)),
                        bool(getattr(m, 'video_note', None)),
                    ])
                except Exception:
                    return False

            probe_client = userbot if userbot else app
            # Build a small probe set around the start to avoid long invisible spans
            probe_ids = {cs, cs + 1, cs + 4, cs + 16}
            try:
                if last_message_id_cap is not None:
                    probe_ids.add(min(cs + 64, last_message_id_cap))
                else:
                    probe_ids.add(cs + 64)
            except Exception:
                pass

            accessible_hits = 0
            for pid in sorted(x for x in probe_ids if isinstance(x, int) and x > 0):
                ok = await _has_accessible_media_at_id(probe_client, channel_ref, pid)
                if ok:
                    accessible_hits += 1
                    break

            if accessible_hits == 0 and (last_downloadable_id_cap is None or last_downloadable_id_cap <= cs):
                # Early abort: likely hidden prehistory or no visible media ahead from this start
                try:
                    warn_html = (
                        f"⚠️ <b>No visible media detected ahead from this start point.</b>\n\n"
                        f"This may be due to hidden group history or insufficient access.\n\n"
                        f"👉 Try a newer start message, or use single download for specific items."
                    )
                    await pin_msg.edit_text(warn_html, reply_markup=cta_btn, disable_web_page_preview=True)
                except Exception:
                    pass
                # Set a reasonable cooldown and exit gracefully
                await set_interval(user_id, seconds=60)
                users_loop.pop(user_id, None)
                try:
                    if await cancel_manager.is_cancelled(user_id):
                        await cancel_manager.clear(user_id)
                except Exception:
                    pass
                return
    except Exception:
        # If preflight fails, continue with normal flow
        pass

    users_loop[user_id] = True
    processed_count = 0
    consecutive_failures = 0
    # Adaptive gap probing parameters (exponential stride probing)
    # Use stricter thresholds for t.me/c (group-like) to avoid long invisible scans
    is_private_c_runtime = '/c/' in start_id
    max_consecutive_failures = 3 if is_private_c_runtime else 5  # trigger sparse probing after N consecutive gaps
    base_step = 8                   # starting probe stride
    max_step = 256                  # cap stride size to avoid huge leaps
    step = base_step
    # Periodic scanning progress updates when no successful downloads for a while
    last_progress_edit = time.time()
    last_processed_for_scan = 0
    # Track the beginning of the most recent jump window to perform bounded backfill
    last_jump_start = None  # None means no active jump window
    
    # Track seen/attempted message IDs to avoid duplicate processing across main loop and backfill
    seen_ids = set()
    # Track processed message IDs with their results to prevent duplicate downloads
    processed_messages = {}  # {message_id: {'success': bool, 'hash': str, 'timestamp': float}}
    # Track consecutive empty messages for topic groups to detect end of topic
    consecutive_empty_messages = 0
    # Minimum number of attempts to check within a topic before allowing early-stop
    min_topic_attempts = 20
    topic_attempts = 0  # Counts attempts in topic sequential scan mode
    # Track early stop due to no downloadable media to notify user
    early_stop_no_media = False
    early_stop_next_id = None
    # Dynamic consecutive empty threshold - ULTRA AGGRESSIVE for deleted content
    if cl <= 10:
        max_consecutive_empty = 3   # Small batches - stop immediately
    elif cl <= 50:
        max_consecutive_empty = 4   # Medium batches - very quick stop
    elif cl <= 200:
        max_consecutive_empty = 5   # Large batches - still quick stop
    else:
        max_consecutive_empty = 6   # Huge batches - maximum 6 deleted messages
    # Detect public topic links for both t.me and telegram.dog
    is_topic_group_batch = bool(re.search(r'(t\.me|telegram\.dog)/[^/]+/\d+/\d+', validated_link))
    
    # Extract topic info for consistent session usage
    topic_info = None
    topic_message_queue = []  # Queue of actual topic messages to process
    process_only_last_topic_msg = False  # when true, disable hybrid and process only last topic message
    if is_topic_group_batch:
        # Support both t.me and telegram.dog public topic links
        topic_match = re.search(r'(?:t\.me|telegram\.dog)/([^/]+)/(\d+)/(\d+)', validated_link)
        if topic_match:
            topic_info = {
                'username': topic_match.group(1),
                'topic_id': int(topic_match.group(2)),
                'start_msg_id': int(topic_match.group(3))
            }
            print(f"🔍 BATCH TOPIC INFO: {topic_info}")
            # Optimization: For PUBLIC topics (not /c/), peek the latest message strictly via user session and process only that
            try:
                if '/c/' not in start_id and userbot is not None:
                    topic_id = topic_info['topic_id']
                    # Iterate recent history and pick the newest message that belongs to this topic
                    newest_topic_msg_id = None
                    fetched = 0
                    last_offset = 0
                    # Try up to 3 pages of 200 messages each (lightweight)
                    for _ in range(3):
                        async for msg in userbot.get_chat_history(topic_info['username'], offset_id=last_offset, limit=200):
                            fetched += 1
                            last_offset = msg.id
                            if msg and msg.id:
                                if (hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id == topic_id) or \
                                   (hasattr(msg, 'message_thread_id') and msg.message_thread_id == topic_id) or \
                                   msg.id == topic_id:
                                    newest_topic_msg_id = msg.id
                                    break
                        if newest_topic_msg_id:
                            break
                    if newest_topic_msg_id:
                        print(f"🎯 PUBLIC TOPIC PEEK: Latest topic message detected: {newest_topic_msg_id} (fetched={fetched})")
                        topic_message_queue = [newest_topic_msg_id]
                        # Cap scanning strictly to this id
                        last_message_id_cap = newest_topic_msg_id
                        last_downloadable_id_cap = newest_topic_msg_id
                        process_only_last_topic_msg = True
                    else:
                        print(f"⚠️ PUBLIC TOPIC PEEK: No recent topic messages found via history; falling back to standard hybrid scan")
            except Exception as peek_err:
                print(f"⚠️ PUBLIC TOPIC PEEK ERROR: {peek_err}")
            
            # If we have topic history from detection, use it for smart processing
            if 'current_topic_history' in globals() and globals()['current_topic_history']:
                topic_history = globals()['current_topic_history']
                # Filter messages >= start message and sort
                topic_message_queue = [msg_id for msg_id in topic_history if msg_id >= topic_info['start_msg_id']]
                topic_message_queue.sort()
                
                # Smart user expectation management
                found_count = len(topic_message_queue)
                if found_count == 0:
                    print(f"⚠️ TOPIC SMART QUEUE: No messages found >= {topic_info['start_msg_id']}, using sequential scan")
                elif found_count < cl:
                    print(f"🎯 TOPIC SMART QUEUE: Found {found_count} messages (less than requested {cl})")
                    print(f"🎯 TOPIC STRATEGY: Will process all {found_count} topic messages + scan for more if needed")
                    print(f"🎯 TOPIC MESSAGES: {topic_message_queue[:10]}{'...' if len(topic_message_queue) > 10 else ''}")
                else:
                    print(f"🎯 TOPIC SMART QUEUE: Found {found_count} messages (enough for batch size {cl})")
                    print(f"🎯 TOPIC MESSAGES: {topic_message_queue[:10]}{'...' if len(topic_message_queue) > 10 else ''}")
            else:
                print(f"⚠️ TOPIC FALLBACK: No topic history available, using sequential scan")

    try:
        # Smart batch processing with gap detection and long-jump handling
        i = cs
        
        # Binary-search style backfill within a bounded window to avoid scanning every link
        async def binary_backfill(range_start: int, range_end: int):
            nonlocal processed_count, cl
            if range_start is None or range_end is None:
                return
            if range_start > range_end:
                return
            # Stack-based DFS over midpoints to reduce calls
            stack = [(range_start, range_end)]
            while stack and processed_count < cl:
                # Early cancel inside backfill to improve responsiveness
                try:
                    if user_id not in users_loop or not users_loop.get(user_id, False) or await cancel_manager.is_cancelled(user_id):
                        return
                except Exception:
                    pass
                l, r = stack.pop()
                if l > r:
                    continue
                mid = (l + r) // 2
                # Skip if already attempted/processed
                if mid in seen_ids:
                    # Explore left and right halves next
                    if l <= mid - 1:
                        stack.append((l, mid - 1))
                    if mid + 1 <= r:
                        stack.append((mid + 1, r))
                    continue
                try:
                    bf_url = f"{base_url}/{mid}"
                    bf_link = get_link(bf_url)
                    is_normal_link_bf = await is_normal_tg_link(bf_link)
                    is_special_link_bf = any(x in bf_link for x in ['t.me/b/', 't.me/c/', 'tg://openmessage', '/s/', 'telegram.dog', 'joinchat'])
                    if is_normal_link_bf or is_special_link_bf:
                        seen_ids.add(mid)
                        bf_result = await process_and_upload_link(userbot, user_id, None, bf_link, 0, message)
                        if bf_result and bf_result[0]:
                            bf_success, bf_err, bf_info, bf_time = bf_result
                            if not (bf_err and "Text message processed" in bf_err):
                                processed_count += 1
                                # Update main progress pin only on success
                                progress_html = (
                                    f"📦 <b>Batch Processing</b>\n\n"
                                    f"⏳ Progress: <b>{processed_count}/{cl}</b>\n\n"
                                    f"🚀 Sit back and relax while we handle everything!"
                                )
                                try:
                                    await pin_msg.edit_text(progress_html, reply_markup=cta_btn, disable_web_page_preview=True)
                                except Exception:
                                    pass
                    # Explore left and right halves next
                    if l <= mid - 1:
                        stack.append((l, mid - 1))
                    if mid + 1 <= r:
                        stack.append((mid + 1, r))
                except Exception as e:
                    # Check for login-related errors in backfill too
                    error_msg = str(e)
                    if any(keyword in error_msg for keyword in ["LOGIN_REQUIRED", "SESSION_ERROR", "ACCESS_REQUIRED"]):
                        # Login required - stop backfill immediately
                        return
                    # Record failed backfill attempt to prevent retries
                    processed_messages[mid] = {
                        'success': False,
                        'error': f'Backfill error: {error_msg}',
                        'timestamp': time.time(),
                        'file_info': None
                    }
                    # Ignore other backfill failures silently
                    pass
        # Stop once we have cl successful downloads, not attempts, and do not pass last message id cap
        # For topic groups, use smart message queue if available
        topic_queue_index = 0
        use_topic_queue = is_topic_group_batch and len(topic_message_queue) > 0
        hybrid_mode = is_topic_group_batch and len(topic_message_queue) > 0 and len(topic_message_queue) < cl
        # If we're processing only the last topic message, disable hybrid to stop after that single message
        if process_only_last_topic_msg:
            hybrid_mode = False
        
        if use_topic_queue:
            if hybrid_mode:
                print(f"🎯 TOPIC BATCH: Using HYBRID mode - {len(topic_message_queue)} known messages + sequential scan for more")
            else:
                print(f"🎯 TOPIC BATCH: Using SMART QUEUE mode with {len(topic_message_queue)} messages")
        
        while processed_count < cl:
            # Stop if we've passed the last message id or last downloadable id (when known)
            # But be more lenient for hybrid mode that's still searching
            if last_message_id_cap is not None and i > last_message_id_cap:
                # Check if we're in hybrid mode and haven't found enough yet
                if is_topic_group_batch and processed_count < cl:
                    # Allow extra scanning in hybrid mode if we haven't found enough
                    extension = min(200, max(50, (cl - processed_count) * 20))  # Scale extension based on remaining need
                    print(f"[BATCH] HYBRID: Extending message cap by {extension} (found {processed_count}/{cl}) at i={i}")
                    # Extend the cap
                    last_message_id_cap += extension
                    print(f"[BATCH] HYBRID: New message cap: {last_message_id_cap}")
                else:
                    # We've reached the end of the channel; stop scanning
                    try:
                        print(f"[BATCH] Early stop: passed last_message_id_cap at i={i}")
                    except Exception:
                        pass
                    break
            if last_downloadable_id_cap is not None and i > last_downloadable_id_cap:
                # Check if we're in hybrid mode and haven't found enough yet
                if is_topic_group_batch and processed_count < cl:
                    # Allow extra scanning in hybrid mode if we haven't found enough
                    extension = min(200, max(50, (cl - processed_count) * 20))  # Scale extension based on remaining need
                    print(f"[BATCH] HYBRID: Extending downloadable cap by {extension} (found {processed_count}/{cl}) at i={i}")
                    # Extend both caps
                    last_downloadable_id_cap += extension
                    last_message_id_cap += extension
                    print(f"[BATCH] HYBRID: New caps - downloadable: {last_downloadable_id_cap}, message: {last_message_id_cap}")
                else:
                    # No more downloadable messages in the remaining range
                    try:
                        print(f"[BATCH] Early stop: passed last_downloadable_id_cap at i={i} (found {processed_count}/{cl})")
                    except Exception:
                        pass
                    break
            # Check if batch was cancelled (via command or inline cancel)
            cancelled = False
            if user_id not in users_loop or not users_loop[user_id]:
                cancelled = True
            else:
                try:
                    if await cancel_manager.is_cancelled(user_id):
                        cancelled = True
                except Exception:
                    pass
            if cancelled:
                users_loop[user_id] = False
                try:
                    await cancel_manager.clear(user_id)
                except Exception:
                    pass
                try:
                    await app.send_message(message.chat.id, "Batch processing was cancelled.")
                except Exception:
                    pass
                break
            
            # Smart gap detection - adaptively probe ahead with exponential stride
            if consecutive_failures >= max_consecutive_failures:
                # Start/extend a jump window and move forward by current stride
                if last_jump_start is None:
                    last_jump_start = i
                i += step
                # Exponentially increase stride up to a cap for very long empty regions
                step = min(max_step, step * 2)
                consecutive_failures = 0
                continue
            
            # For topic groups with smart queue, use the actual topic message IDs
            if use_topic_queue:
                if topic_queue_index >= len(topic_message_queue):
                    if hybrid_mode:
                        # Switch to sequential mode after exhausting known topic messages
                        print(f"🎯 TOPIC HYBRID: Completed {len(topic_message_queue)} known messages, switching to sequential scan")
                        use_topic_queue = False
                        # Continue with the next message after the last processed one
                        i = max(topic_message_queue) + 1
                        print(f"🔄 TOPIC HYBRID: Continuing sequential scan from message {i}")
                        # Reset consecutive counter for fresh start
                        consecutive_empty_messages = 0
                    else:
                        print(f"🎯 TOPIC BATCH: Completed all {len(topic_message_queue)} topic messages")
                        break
                
                if use_topic_queue:  # Still in queue mode
                    i = topic_message_queue[topic_queue_index]
                    topic_queue_index += 1
                    print(f"🎯 TOPIC BATCH: Processing message {i} ({topic_queue_index}/{len(topic_message_queue)})")
                else:
                    # We've switched to sequential mode, reset consecutive empty counter
                    if topic_queue_index > 0:  # Just switched from queue mode
                        consecutive_empty_messages = 0
                        print(f"🔄 TOPIC HYBRID: Reset consecutive counter, continuing from i={i}")
            
            # Generate URL based on the original link format
            if '/c/' in start_id:  # Private channel
                url = f"{base_url}/{i}"
            elif '/b/' in start_id:  # Bot link
                url = f"{base_url}/{i}"
            elif '/s/' in start_id:  # Story link
                url = f"{base_url}/{i}"
            elif 'telegram.dog' in start_id:  # Telegram.dog
                url = f"{base_url}/{i}"
            else:  # Regular public channel (includes topic groups)
                url = f"{base_url}/{i}"
            
            # Debug logging for topic groups to verify correct URL generation
            if is_topic_group_batch and i <= 5:  # Only log first few for debugging
                print(f"🔍 TOPIC URL DEBUG: Generated {url} from base_url={base_url}, i={i}")
            
            link = get_link(url)

            # Skip duplicates - check both seen_ids and processed_messages
            if i in seen_ids or i in processed_messages:
                if i in processed_messages:
                    print(f"[BATCH] SKIP: Message {i} already processed with result: {processed_messages[i]['success']}")
                if not use_topic_queue:
                    i += 1
                continue
            
            # For topic groups, handle early stop logic based on mode with smarter deleted message detection
            if is_topic_group_batch and not use_topic_queue:
                # Check for early stop due to consecutive empty/deleted messages
                if consecutive_empty_messages >= max_consecutive_empty and topic_attempts >= min_topic_attempts:
                    try:
                        if hybrid_mode:
                            print(f"[BATCH] TOPIC HYBRID: Early stop after {consecutive_empty_messages} consecutive empty messages at i={i}")
                        else:
                            print(f"[BATCH] TOPIC GROUP: Early stop after {consecutive_empty_messages} consecutive empty messages at i={i}")
                    except Exception:
                        pass
                    # Mark early stop due to no-media for user notification
                    early_stop_no_media = True
                    early_stop_next_id = (i + 1) if not use_topic_queue else None
                    break
                
                # Additional check: if we've hit many deleted messages in a row, likely at end of topic
                if consecutive_empty_messages >= 3 and i > 10 and topic_attempts >= min_topic_attempts:  # Guard with min attempts
                    try:
                        print(f"[BATCH] TOPIC: Likely reached end of topic - {consecutive_empty_messages} consecutive deleted messages at i={i}")
                    except Exception:
                        pass
                    early_stop_no_media = True
                    early_stop_next_id = (i + 1) if not use_topic_queue else None
                    break
            
            # Determine if this is a normal or special link
            is_normal_link = await is_normal_tg_link(link)
            is_special_link = any(x in link for x in ['t.me/b/', 't.me/c/', 'tg://openmessage', '/s/', 'telegram.dog', 'joinchat'])
            
            if is_normal_link or is_special_link:
                # First, try forwarding directly (copy) for public forwardable content
                try:
                    if is_normal_link and await try_forward_first(link, user_id):
                        # Count as processed success without download/upload
                        processed_count += 1
                        consecutive_failures = 0
                        step = base_step
                        # Update progress pin
                        progress_html = (
                            f"📦 <b>Batch Processing</b>\n\n"
                            f"⏳ Progress: <b>{processed_count}/{cl}</b>\n\n"
                            f"🚀 Sit back and relax while we handle everything!"
                        )
                        try:
                            await pin_msg.edit_text(progress_html, reply_markup=cta_btn, disable_web_page_preview=True)
                        except Exception:
                            pass
                        last_processed_for_scan = processed_count
                        last_progress_edit = time.time()
                        # Move to next message id (only for non-topic queue mode)
                        if not use_topic_queue:
                            i += 1
                        continue
                    
                    # Respect cancel before processing this candidate
                    try:
                        if user_id not in users_loop or not users_loop.get(user_id, False) or await cancel_manager.is_cancelled(user_id):
                            users_loop[user_id] = False
                            try:
                                await cancel_manager.clear(user_id)
                            except Exception:
                                pass
                            try:
                                await app.send_message(message.chat.id, "Batch processing was cancelled.")
                            except Exception:
                                pass
                            break
                    except Exception:
                        pass

                    # Create a list to store successful downloads for consolidated message
                    if not hasattr(message, 'successful_downloads'):
                        message.successful_downloads = []
                    
                    # Use None for msg_id to avoid deleting the pinned progress message
                    seen_ids.add(i)
                    
                    # Record processing attempt
                    processing_start = time.time()
                    result = await process_and_upload_link(userbot, user_id, None, link, 0, message)
                    
                    # Handle result for topic group early stop detection and duplicate prevention
                    if result:
                        success, err, info, time_str = result
                        
                        # Record the processing result
                        processed_messages[i] = {
                            'success': success,
                            'error': err,
                            'timestamp': processing_start,
                            'file_info': info
                        }
                        
                        if success and not (err and "Text message processed" in err):
                            consecutive_empty_messages = 0  # Reset counter on successful download
                            print(f"[BATCH] SUCCESS: Message {i} processed successfully")
                        elif err and ("Message not found" in err or "empty" in err.lower() or "NoneType" in err):
                            consecutive_empty_messages += 1
                            if is_topic_group_batch:
                                print(f"[BATCH] TOPIC: Deleted/Empty message {i}, consecutive: {consecutive_empty_messages}/{max_consecutive_empty}")
                                # For deleted messages, be ULTRA aggressive about stopping
                                if consecutive_empty_messages >= max(2, max_consecutive_empty // 3) and (not is_topic_group_batch or topic_attempts >= min_topic_attempts):
                                    print(f"[BATCH] TOPIC: Too many deleted messages ({consecutive_empty_messages}), likely reached end of topic")
                                    early_stop_no_media = True
                                    early_stop_next_id = (i + 1) if not use_topic_queue else None
                                    break
                        elif err and "Text message processed" in err:
                            consecutive_empty_messages += 1
                            if is_topic_group_batch:
                                print(f"[BATCH] TOPIC: Text message {i} (no media), consecutive: {consecutive_empty_messages}/{max_consecutive_empty}")
                        else:
                            consecutive_empty_messages += 1  # Count other errors as empty for topic groups
                            if is_topic_group_batch:
                                print(f"[BATCH] TOPIC: Error on message {i}: {err}, consecutive: {consecutive_empty_messages}/{max_consecutive_empty}")
                    else:
                        consecutive_empty_messages += 1
                        processed_messages[i] = {
                            'success': False,
                            'error': 'No result returned',
                            'timestamp': processing_start,
                            'file_info': None
                        }
                        if is_topic_group_batch:
                            print(f"[BATCH] TOPIC: No result for message {i}, consecutive: {consecutive_empty_messages}/{max_consecutive_empty}")
                    # Count an attempt in topic sequential scan mode (only when not using the prebuilt queue)
                    if is_topic_group_batch and not use_topic_queue:
                        topic_attempts += 1
                    
                    # Respect cancel immediately after processing
                    try:
                        if user_id not in users_loop or not users_loop.get(user_id, False) or await cancel_manager.is_cancelled(user_id):
                            users_loop[user_id] = False
                            try:
                                await cancel_manager.clear(user_id)
                            except Exception:
                                pass
                            try:
                                await app.send_message(message.chat.id, "Batch processing was cancelled.")
                            except Exception:
                                pass
                            break
                    except Exception:
                        pass
                    # Compute success flag clearly to avoid ambiguous else binding
                    is_success = bool(result and result[0])
                    
                    # Unpack the result based on success/failure
                    if is_success:  # Success
                        success, error_msg, file_info, processing_time = result
                        # Track last success index for dynamic end detection
                        last_success_i = i
                        
                        # Check if it was a text message or media download
                        if error_msg and "Text message processed" in error_msg:
                            # Count text/link messages as processed to advance batch
                            processed_count += 1
                        else:
                            # Media download - count in processed total
                            processed_count += 1
                            # Reset failure counter and stride on success
                            consecutive_failures = 0
                            step = base_step
                            
                            # Format file size for display
                            file_size = file_info.get("size", 0)
                            if file_size > 0:
                                if file_size >= 1024*1024*1024:
                                    size_str = f"{file_size/(1024*1024*1024):.2f} GB"
                                elif file_size >= 1024*1024:
                                    size_str = f"{file_size/(1024*1024):.2f} MB"
                                else:
                                    size_str = f"{file_size/1024:.2f} KB"
                            else:
                                size_str = "Unknown size"
                                
                            # Get file name
                            file_name = file_info.get("name", "Unknown")
                            if len(file_name) > 30:
                                file_name = file_name[:27] + "..."
                            
                            # Store successful download info for consolidated message
                            message.successful_downloads.append({
                                "message_id": i,
                                "file_name": file_name,
                                "size": size_str,
                                "time": processing_time
                            })
                            
                            # Update main progress pin with digits only and CTA
                            progress_html = (
                                f"📦 <b>Batch Processing</b>\n\n"
                                f"⏳ Progress: <b>{processed_count}/{cl}</b>\n\n"
                                f"🚀 Sit back and relax while we handle everything!"
                            )
                            try:
                                await pin_msg.edit_text(progress_html, reply_markup=cta_btn, disable_web_page_preview=True)
                            except Exception:
                                pass
                            last_processed_for_scan = processed_count
                            last_progress_edit = time.time()

                            # Bounded backfill via binary probing within last jump window to ensure no content missed
                            if last_jump_start is not None and last_jump_start < (i - 1):
                                backfill_start = max(last_jump_start, i - (step * 2))
                                backfill_end = i - 1
                                await binary_backfill(backfill_start, backfill_end)
                                # Close the jump window after backfill
                                last_jump_start = None
                    else:
                        # Failure
                        success, error_msg, file_info, processing_time, error_type = result
                        consecutive_failures += 1  # Increment failure counter
                        
                        # Handle flood wait silently but respect the delay
                        if error_type == "flood_wait":
                            wait_time = re.search(r'\d+', error_msg)
                            await asyncio.sleep(int(wait_time.group()) if wait_time else 30)
                        
                        # Silently skip all errors - no per-item edits to save limits
                except Exception as e:
                    error_msg = str(e)
                    # Check for login-related errors that should break the batch immediately
                    if any(keyword in error_msg for keyword in ["LOGIN_REQUIRED", "SESSION_ERROR", "ACCESS_REQUIRED"]):
                        # Login required - break the batch loop immediately
                        try:
                            login_error_html = (
                                f"🔐 <b>Batch Stopped - Login Required</b>\n\n"
                                f"⚠️ Private content detected that requires login.\n\n"
                                f"📝 Use <code>/login</code> command first, then restart batch.\n\n"
                                f"✅ Processed: <b>{processed_count}</b> messages before stopping."
                            )
                            await pin_msg.edit_text(login_error_html, reply_markup=cta_btn, disable_web_page_preview=True)
                        except Exception:
                            pass
                        # Break the batch processing loop
                        break
                    else:
                        # Silently handle other unexpected errors
                        consecutive_failures += 1
                
                # Add delay between downloads to prevent flooding
                await asyncio.sleep(2)
            else:
                # Silently skip invalid links
                consecutive_failures += 1
                # Move to next message (only for non-topic queue mode)
                if not use_topic_queue:
                    i += 1

            # Periodic scanning update when no progress to reassure the user
            try:
                now_ts = time.time()
                if processed_count == last_processed_for_scan and (now_ts - last_progress_edit) >= (30 if is_private_c_runtime else 45):
                    scan_html = (
                        f"📦 <b>Batch Processing</b>\n\n"
                        f"⏳ Progress: <b>{processed_count}/{cl}</b>\n\n"
                        f"🔍 Scanning... checked up to <b>{i}</b>."
                    )
                    try:
                        await pin_msg.edit_text(scan_html, reply_markup=cta_btn, disable_web_page_preview=True)
                    except Exception:
                        pass
                    last_progress_edit = now_ts
            except Exception:
                pass
            
            # Dynamic early-stop when no more content ahead (when last_downloadable_id_cap unknown)
            if last_downloadable_id_cap is None and consecutive_failures >= max_consecutive_failures * 2:
                # Probe a bounded forward window using binary backfill to detect any remaining content ahead
                probe_start = i
                probe_end = min(i + (step * 4), last_message_id_cap or (i + step * 4))
                gains_before = processed_count
                await binary_backfill(probe_start, probe_end)
                if processed_count == gains_before:
                    # No more downloadable messages detected ahead; stop early
                    try:
                        print(f"[BATCH] Early stop: no gains in probe [{probe_start},{probe_end}] after prolonged empty span")
                    except Exception:
                        pass
                    early_stop_no_media = True
                    early_stop_next_id = i + 1
                    break
                # Reset consecutive failures since some area ahead may have content
                consecutive_failures = 0

            # If we already hit the last downloadable in the region, stop as soon as we pass it
            if last_downloadable_id_cap is not None and 'last_success_i' in locals() and last_success_i >= last_downloadable_id_cap and i > last_success_i:
                try:
                    print(f"[BATCH] Stop: last_success_i={last_success_i} >= last_downloadable_id_cap={last_downloadable_id_cap}")
                except Exception:
                    pass
                break

            # If approaching end-of-channel cap and currently in stride mode, do a final backfill up to cap
            if last_message_id_cap is not None and last_jump_start is not None and (i + step) > last_message_id_cap:
                bf_gain_before = processed_count
                backfill_start = max(last_jump_start, i - (step * 2))
                backfill_end = last_message_id_cap
                await binary_backfill(backfill_start, backfill_end)
                # If no gains from final backfill and we've hit the end cap, stop
                if processed_count == bf_gain_before:
                    break
                # Close jump window post backfill
                last_jump_start = None

            # Move to next candidate message id (only for non-topic queue mode)
            if not use_topic_queue:
                i += 1

        # If batch stopped early due to no downloadable media, send a helpful tip
        try:
            if early_stop_no_media and base_url and early_stop_next_id:
                tip_html = (
                    "⛔ <b>Batch Stopped Early</b>\n"
                    "No downloadable media found in the recent checks.\n\n"
                    "👉 Try the next message link:\n"
                    f"<code>{base_url}/{early_stop_next_id}</code>"
                )
                await app.send_message(user_id, tip_html, disable_web_page_preview=True)
        except Exception:
            pass

        # Set cooldown ONLY for free users after batch, and complete batch
        try:
            if (await chk_user(message, user_id)) == 1 and not await is_user_verified(user_id):
                await set_interval(user_id, seconds=FREE_BATCH_WAIT_SECONDS)
        except Exception:
            pass
        
        # Calculate batch statistics
        total_time = time.time() - batch_start_time
        time_str = f"{total_time/60:.1f} minutes" if total_time >= 60 else f"{total_time:.1f} seconds"
        
        if processed_count > 0:
            # New HTML completion message with CTA (no Cancel on completion)
            completion_html = (
                f"✅ <b>Batch Processing Completed!</b> 🎉\n\n"
                f"📨 Successfully processed <b>{processed_count}</b> messages.\n"
                f"🔄 Need another batch? Just send <code>/batch</code>"
            )
            cta_btn = InlineKeyboardMarkup([[InlineKeyboardButton("Join AlienxSaver", url="https://t.me/AlienxSaver")]])

            # Update the pinned progress message content and keep it pinned
            completion_msg = None
            try:
                completion_msg = await pin_msg.edit_text(completion_html, reply_markup=cta_btn, disable_web_page_preview=True)
            except Exception:
                # If editing fails, send a new message
                completion_msg = await app.send_message(user_id, completion_html, reply_markup=cta_btn, disable_web_page_preview=True)
            # Ensure it is pinned for the user (both sides when applicable)
            try:
                await completion_msg.pin(both_sides=True)
            except Exception:
                pass
            
            # 🚀 IMMEDIATE CLEANUP: Clear process flag as soon as batch is completed
            try:
                users_loop.pop(user_id, None)
                process_start_times.pop(user_id, None)
                print(f"✅ Immediate cleanup: Batch process flag cleared for user {user_id} after completion")
                
                # Clean up temporary files after batch completion
                await cleanup_manager.cleanup_for_user(user_id, f"batch_completed: {processed_count} files")
            except Exception:
                pass
            
            # Also send a final confirmation to the chat with the same CTA and pin it
            try:
                final_msg = await app.send_message(message.chat.id, completion_html, reply_markup=cta_btn, disable_web_page_preview=True)
                try:
                    await final_msg.pin(both_sides=True)
                except Exception:
                    pass
            except Exception:
                pass
        else:
            # New HTML completion message for zero processed (no Cancel on completion)
            completion_html = (
                f"✅ <b>Batch Processing Completed!</b> 🎉\n\n"
                f"📨 Successfully processed <b>0</b> messages.\n"
                f"🔄 Need another batch? Just send <code>/batch</code>"
            )
            cta_btn = InlineKeyboardMarkup([[InlineKeyboardButton("Join AlienxSaver", url="https://t.me/AlienxSaver")]])
            try:
                completion_msg = await pin_msg.edit_text(completion_html, reply_markup=cta_btn, disable_web_page_preview=True)
            except Exception:
                completion_msg = await app.send_message(user_id, completion_html, reply_markup=cta_btn, disable_web_page_preview=True)
            try:
                await completion_msg.pin(both_sides=True)
            except Exception:
                pass
            
            # 🚀 IMMEDIATE CLEANUP: Clear process flag for zero processed case too
            try:
                users_loop.pop(user_id, None)
                process_start_times.pop(user_id, None)
                print(f"✅ Immediate cleanup: Batch process flag cleared for user {user_id} (zero processed)")
                
                # Clean up temporary files for zero processed batch
                await cleanup_manager.cleanup_for_user(user_id, "batch_completed: 0 files")
            except Exception:
                pass
            
            try:
                final_msg = await app.send_message(message.chat.id, completion_html, reply_markup=cta_btn, disable_web_page_preview=True)
                try:
                    await final_msg.pin(both_sides=True)
                except Exception:
                    pass
            except Exception:
                pass

    except Exception as e:
        # Silently handle batch errors - don't show to user
        print(f"Batch processing error for user {user_id}: {e}")
        
        # Clean up files after batch error
        try:
            await cleanup_manager.cleanup_for_user(user_id, f"batch_failed: {str(e)[:50]}")
        except Exception:
            pass
    finally:
        # Fallback cleanup: Only clear if not already cleared by immediate cleanup
        if user_id in users_loop:
            users_loop.pop(user_id, None)
            process_start_times.pop(user_id, None)
            print(f"🔧 Fallback cleanup: Batch process flag cleared for user {user_id} in finally block")
        try:
            if await cancel_manager.is_cancelled(user_id):
                await cancel_manager.clear(user_id)
        except Exception:
            pass

@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id

    # Check if there is an active batch process for the user
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  # Set the loop status to False
        
        # Clean up files after cancellation
        try:
            await cleanup_manager.cleanup_for_user(user_id, "cancelled")
        except Exception:
            pass
            
        await app.send_message(
            message.chat.id, 
            "Batch processing has been stopped successfully. You can start a new batch now if you want."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )

    # Always attempt to remove any waiting queue entries for this user (single or batch)
    try:
        from devgagan.core.download_queue import download_queue
        removed = await download_queue.cancel_user(user_id)
        if removed:
            try:
                await app.send_message(message.chat.id, f"🧹 Removed {removed} queued pending task(s).")
            except Exception:
                pass
    except Exception:
        pass


@app.on_message(filters.command("cleanup") & filters.private)
async def cleanup_command(_, message):
    """Manual cleanup command for administrators"""
    user_id = message.chat.id
    
    # Silent admin check - no response for non-admins
    if user_id not in OWNER_ID:
        return
        
    try:
        # Get current stats before cleanup
        stats_before = cleanup_manager.get_comprehensive_stats()
        
        # Run comprehensive cleanup
        await message.reply("🧹 Starting comprehensive cleanup...")
        
        # Clean old files
        old_files = await cleanup_manager.file_cleanup.cleanup_old_files()
        
        # Clean memory
        memory_result = await cleanup_manager.memory_cleanup.cleanup_memory(force=True)
        
        # Get stats after cleanup
        stats_after = cleanup_manager.get_comprehensive_stats()
        
        # Format results
        files_before = stats_before["file_stats"]["total_files"]
        files_after = stats_after["file_stats"]["total_files"]
        size_before = stats_before["file_stats"]["total_size_mb"]
        size_after = stats_after["file_stats"]["total_size_mb"]
        
        memory_before = stats_before["memory_stats"].get("memory_mb", 0)
        memory_after = stats_after["memory_stats"].get("memory_mb", 0)
        
        result_text = (
            f"✅ **Cleanup Complete!**\n\n"
            f"📁 **Files:**\n"
            f"• Before: {files_before} files ({size_before:.1f} MB)\n"
            f"• After: {files_after} files ({size_after:.1f} MB)\n"
            f"• Cleaned: {len(old_files)} files ({size_before - size_after:.1f} MB freed)\n\n"
            f"🧠 **Memory:**\n"
            f"• Before: {memory_before:.1f} MB\n"
            f"• After: {memory_after:.1f} MB\n"
            f"• Freed: {memory_before - memory_after:.1f} MB\n"
        )
        
        if memory_result:
            result_text += f"• Objects collected: {memory_result['objects_collected']}\n"
            
        await message.reply(result_text)
        
    except Exception as e:
        await message.reply(f"❌ Cleanup error: {str(e)}")


@app.on_message(filters.command("cleanup_stats") & filters.private)
async def cleanup_stats_command(_, message):
    """Show cleanup statistics"""
    user_id = message.chat.id
    
    # Silent admin check - no response for non-admins
    if user_id not in OWNER_ID:
        return
        
    try:
        stats = cleanup_manager.get_comprehensive_stats()
        
        file_stats = stats["file_stats"]
        memory_stats = stats["memory_stats"]
        
        stats_text = (
            f"📊 **Cleanup Statistics**\n\n"
            f"📁 **Files:**\n"
            f"• Downloads: {file_stats['downloads_count']} files ({file_stats['downloads_size_mb']:.1f} MB)\n"
            f"• Thumbnails: {file_stats['thumbnails_count']} files ({file_stats['thumbnails_size_mb']:.1f} MB)\n"
            f"• Total: {file_stats['total_files']} files ({file_stats['total_size_mb']:.1f} MB)\n"
            f"• Tracked temp files: {file_stats['tracked_temp_files']}\n"
            f"• Active downloads: {file_stats['active_downloads']}\n"
            f"• Linked thumbnails: {file_stats.get('linked_thumbnails', 0)}\n"
            f"• Videos with thumbnails: {file_stats.get('videos_with_thumbnails', 0)}\n\n"
            f"🧠 **Memory:**\n"
            f"• Current usage: {memory_stats.get('memory_mb', 0):.1f} MB ({memory_stats.get('memory_percent', 0):.1f}%)\n"
            f"• CPU usage: {memory_stats.get('cpu_percent', 0):.1f}%\n"
            f"• GC counts: {memory_stats.get('gc_counts', 'N/A')}\n\n"
            f"🕒 **Last updated:** {stats['timestamp'][:19]}"
        )
        
        await message.reply(stats_text)
        
    except Exception as e:
        await message.reply(f"❌ Stats error: {str(e)}")


@app.on_message(filters.command("emergency_cleanup") & filters.private)
async def emergency_cleanup_command(_, message):
    """Emergency cleanup command - removes ALL files"""
    user_id = message.chat.id
    
    # Silent admin check - no response for non-admins
    if user_id not in OWNER_ID:
        return
        
    try:
        await message.reply("🚨 **WARNING:** This will delete ALL downloaded files and thumbnails!\n\nSend 'CONFIRM' to proceed.")
        
        # Wait for confirmation
        confirm = await app.ask(message.chat.id, "Type CONFIRM to proceed with emergency cleanup:")
        
        if confirm.text.strip().upper() == "CONFIRM":
            await message.reply("🚨 Starting emergency cleanup...")
            
            # Run emergency cleanup
            cleaned_files = await cleanup_manager.file_cleanup.emergency_cleanup()
            
            # Force memory cleanup
            memory_result = await cleanup_manager.memory_cleanup.cleanup_memory(force=True)
            
            result_text = (
                f"🚨 **Emergency Cleanup Complete!**\n\n"
                f"📁 Files removed: {len(cleaned_files)}\n"
                f"🧠 Memory freed: {memory_result['memory_freed_mb']:.1f} MB\n"
                f"🗑️ All downloads and thumbnails cleared!"
            )
            
            await message.reply(result_text)
        else:
            await message.reply("❌ Emergency cleanup cancelled.")
            
    except Exception as e:
        await message.reply(f"❌ Emergency cleanup error: {str(e)}")
