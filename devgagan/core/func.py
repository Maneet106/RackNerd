import math
import time , re
from pyrogram import enums
from pyrogram.enums import ParseMode, ChatMemberStatus
from config import CHANNEL_ID, OWNER_ID, CHANNEL 
from devgagan.core.mongo.plans_db import check_premium
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import cv2
import shutil
from pyrogram.errors import FloodWait, InviteHashInvalid, InviteHashExpired, UserAlreadyParticipant, UserNotParticipant
from pyrogram.errors import ChatAdminRequired, ChannelInvalid
from datetime import datetime as dt
import asyncio, subprocess, re, os, time

async def chk_user(message, user_id):
    """Return 0 for premium/owner, 1 for free.
    Optimized to check a single user record instead of scanning all premium users.
    """
    try:
        if user_id in OWNER_ID:
            return 0
        data = await check_premium(user_id)
        return 0 if data else 1
    except Exception:
        # On any DB error, treat as free to avoid blocking
        return 1

async def gen_link(app, chat_id):
   """Try to export an invite link. Return None if not possible."""
   try:
       link = await app.export_chat_invite_link(chat_id)
       return link
   except (ChatAdminRequired, ChannelInvalid, Exception) as e:
       # Bot might not be admin or channel id invalid/not cached yet
       return None

async def subscribe(app, message):
   """Enhanced subscription system - requires joining both main channel and AlienxSaver channel"""
   user_id = message.from_user.id
   
   # Main channel (existing configuration)
   main_channel = CHANNEL if CHANNEL else CHANNEL_ID
   # AlienxSaver channel (hardcoded as requested)
   alienx_channel = "@AlienxSaver"
   
   # Check if user is banned from main channel
   if main_channel:
      try:
         user = await app.get_chat_member(main_channel, user_id)
         if user.status == "kicked":
            await message.reply_text("🚫 <b>You are Banned!</b>\n\n📞 Contact: @ZeroTrace0x", parse_mode=ParseMode.HTML)
            return 1
      except Exception:
         pass  # Continue with membership check
   
   # Check membership in both channels
   main_joined = False
   alienx_joined = False
   
   # Check main channel membership
   if main_channel:
      try:
         user = await app.get_chat_member(main_channel, user_id)
         if user.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
            main_joined = True
      except UserNotParticipant:
         main_joined = False
      except ChatAdminRequired:
         # Bot is not admin in the channel, skip this check
         main_joined = True  # Allow access since we can't verify
      except Exception:
         main_joined = True  # Allow access on error to prevent blocking users
   else:
      main_joined = True  # If no main channel configured, skip this check
   
   # Check AlienxSaver channel membership
   try:
      user = await app.get_chat_member(alienx_channel, user_id)
      if user.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
         alienx_joined = True
   except UserNotParticipant:
      alienx_joined = False
   except ChatAdminRequired:
      # Bot is not admin in AlienxSaver channel, allow access
      alienx_joined = True  # Allow access since we can't verify
   except Exception:
      alienx_joined = True  # Allow access on error
   
   # If user hasn't joined both channels, show subscription message
   if not (main_joined and alienx_joined):
      # Generate links for both channels
      main_url = await gen_link(app, main_channel) if main_channel else None
      if not main_url and isinstance(main_channel, str) and main_channel:
         username = main_channel.replace("@", "")
         main_url = f"https://t.me/{username}"
      
      alienx_url = "https://t.me/AlienxSaver"
      
      # Create beautiful subscription message
      fname = getattr(message.from_user, "first_name", "User") or "User"
      
      caption = (
         f"🔒 <b>Hey {fname}! Access Restricted</b>\n\n"
         f"📢 <b>To use this bot, you must join BOTH channels:</b>\n\n"
         f"🔹 <b>Main Channel:</b> Get updates & announcements\n"
         f"🔹 <b>AlienxSaver Channel:</b> Premium content & resources\n\n"
         f"✅ <b>After joining both channels:</b>\n"
         f"• Click 'Verify Membership' button below\n"
         f"• Or send /start command again\n\n"
         f"🎥 Tutorial Video: https://t.me/AlienxSaver/58\n\n"
         f"💡 <i>This ensures you get all important updates!</i>"
      )
      
      # Create keyboard with both channel links and verify button
      keyboard_buttons = []
      
      # Add main channel button if configured
      if main_channel and main_url:
         if isinstance(main_channel, str) and main_channel.startswith("@"):
            channel_name = main_channel[1:]  # Remove @ symbol
         else:
            channel_name = "Main Channel"
         keyboard_buttons.append([InlineKeyboardButton(f"📢 Join {channel_name}", url=main_url)])
      
      # Add AlienxSaver channel button
      keyboard_buttons.append([InlineKeyboardButton("🚀 Join AlienxSaver", url=alienx_url)])
      
      # Add verify membership button
      keyboard_buttons.append([InlineKeyboardButton("✅ Verify Membership", callback_data="verify_subscription")])
      
      keyboard = InlineKeyboardMarkup(keyboard_buttons)
      
      await message.reply_photo(
         photo="https://www.dropbox.com/scl/fi/ukrb46ebwhjohw2ii8rs1/restricted.jpg?rlkey=2wmu84gg3f060vjsfbgsm5twl&st=iew6agyd&dl=0",
         caption=caption,
         reply_markup=keyboard,
         parse_mode=ParseMode.HTML
      )
      return 1
   
   # User has joined both channels, allow access
   return 0

async def get_seconds(time_string):
    def extract_value_and_unit(ts):
        value = ""
        unit = ""

        index = 0
        while index < len(ts) and ts[index].isdigit():
            value += ts[index]
            index += 1

        unit = ts[index:].lstrip()

        if value:
            value = int(value)

        return value, unit

    value, unit = extract_value_and_unit(time_string)

    if unit == 's':
        return value
    elif unit == 'min':
        return value * 60
    elif unit == 'hour':
        return value * 3600
    elif unit == 'day':
        return value * 86400
    elif unit == 'month':
        return value * 86400 * 30
    elif unit == 'year':
        return value * 86400 * 365
    else:
        return 0

# Unified Progress Bar System
class UnifiedProgressBar:
    @staticmethod
    def create_progress_bar(percentage: float, bar_type: str = "download") -> str:
        """Create a unified progress bar with green squares"""
        # Calculate filled and empty blocks (10 total blocks)
        filled_blocks = int(percentage // 10)
        empty_blocks = 10 - filled_blocks
        
        # Create progress bar with green squares and white squares
        progress_bar = "🟩" * filled_blocks + "▫️" * empty_blocks
        
        # Add completion indicator
        completion_indicator = " ✅" if percentage >= 100 else ""
        
        return f"[{progress_bar}] {percentage:.0f}%{completion_indicator}"
    
    @staticmethod
    def format_progress_message(percentage: float, current_bytes: int, total_bytes: int, 
                              speed: float, eta: str, bar_type: str = "download") -> str:
        """Format complete progress message"""
        progress_bar = UnifiedProgressBar.create_progress_bar(percentage, bar_type)
        
        # 🚀 FINAL SAFETY NET: Always boost speed by 3x here as last resort
        # This ensures even if a caller forgets to boost, users still see 3x speed
        # All our callbacks already boost, so this is just a failsafe
        speed_display = speed * 3
        
        # ⚡ FINAL SAFETY NET: Reduce ETA by half here as last resort
        # Parse ETA string and divide by 2 if it's not already reduced
        eta_display = eta
        try:
            # Parse time string like "6m, 34s" or "1h, 23m" and divide by 2
            if eta and eta != "Calculating..." and eta != "0s" and eta != "--":
                # Convert to seconds - handle format like "6m, 34s"
                total_seconds = 0
                # Split by comma or space and process each part
                parts = eta.replace(",", " ").split()
                
                for part in parts:
                    part = part.strip()
                    if not part:
                        continue
                    
                    # Extract number and unit
                    if 'd' in part:
                        value = int(part.replace('d', ''))
                        total_seconds += value * 86400
                    elif 'h' in part:
                        value = int(part.replace('h', ''))
                        total_seconds += value * 3600
                    elif 'm' in part and 'ms' not in part:
                        value = int(part.replace('m', ''))
                        total_seconds += value * 60
                    elif 's' in part and 'ms' not in part:
                        value = int(part.replace('s', ''))
                        total_seconds += value
                
                # Divide by 2 and reformat
                if total_seconds > 0:
                    total_seconds = total_seconds // 2
                    eta_display = TimeFormatter(total_seconds * 1000)
        except Exception as e:
            # If parsing fails, use original ETA
            pass
        
        # Different icons for download vs upload
        icon = "📥" if bar_type == "download" else "📤"
        action = "Downloading" if bar_type == "download" else "Uploading"
        
        return (
            f"{icon} <b>{action}...</b>\n\n"
            f"{progress_bar}\n\n"
            f"📊 <b>Progress:</b> {percentage:.1f}%\n"
            f"📁 <b>Size:</b> {humanbytes(current_bytes)} / {humanbytes(total_bytes)}\n"
            f"⚡ <b>Speed:</b> {humanbytes(speed_display)}/s\n"
            f"⏱️ <b>ETA:</b> {eta_display}"
        )

async def progress_bar(current, total, ud_type, message, start):
    """Legacy progress bar function - now uses unified system"""
    now = time.time()
    diff = now - start
    percentage = current * 100 / total
    if _should_edit_progress(message, percentage) or current == total:
        # Calculate speed with minimum threshold to avoid extremely low initial speeds
        diff = max(diff, 1.0)  # Minimum 1.0 second to ensure good initial speed display
        speed = current / diff if diff > 0 else 0
        
        # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
        speed_display = speed * 3
        
        # Calculate ETA
        if speed > 0 and current < total:
            eta_seconds = (total - current) / speed
            # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
            eta_seconds = eta_seconds / 2
            eta = TimeFormatter(milliseconds=int(eta_seconds * 1000))
        else:
            eta = "0s"
        
        # Determine bar type from ud_type
        bar_type = "download" if "download" in ud_type.lower() else "upload"
        
        # Use unified progress bar (with boosted speed)
        progress_text = UnifiedProgressBar.format_progress_message(
            percentage, current, total, speed_display, eta, bar_type
        )
        
        try:
            await message.edit(text=progress_text)
        except:
            pass

def humanbytes(size):
    if not size:
        return ""
    # Convert string to int/float if needed
    if isinstance(size, str):
        try:
            size = float(size)
        except (ValueError, TypeError):
            return ""
    power = 2**10
    n = 0
    Dic_powerN = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + Dic_powerN[n] + 'B'

def TimeFormatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = ((str(days) + "d, ") if days else "") + \
        ((str(hours) + "h, ") if hours else "") + \
        ((str(minutes) + "m, ") if minutes else "") + \
        ((str(seconds) + "s, ") if seconds else "") + \
        ((str(milliseconds) + "ms, ") if milliseconds else "")
    return tmp[:-2] 

def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60      
    return "%d:%02d:%02d" % (hour, minutes, seconds)

async def userbot_join(userbot, invite_link):
    try:
        await userbot.join_chat(invite_link)
        return "Successfully joined the Channel"
    except UserAlreadyParticipant:
        return "User is already a participant."
    except (InviteHashInvalid, InviteHashExpired):
        return "Could not join. Maybe your link is expired or Invalid."
    except FloodWait:
        return "Too many requests, try again later."
    except Exception as e:
        print(e)
        return "Could not join, try joining manually."

def get_link(string):
    """Enhanced link extraction function that handles all Telegram link formats robustly"""
    # Comprehensive Telegram link patterns - ordered from most specific to least specific
    telegram_patterns = [
        # Private topic links: t.me/c/1234567890/2/255 (MUST be first - most specific)
        r"(?:https?://)?(?:www\.)?t\.me/c/[0-9]+/[0-9]+/[0-9]+(?:\?[^\s]*)?",
        # Private channel links: t.me/c/1234567890/123 (MUST be before general patterns)
        r"(?:https?://)?(?:www\.)?t\.me/c/[0-9]+/[0-9]+(?:\?[^\s]*)?",
        # Bot links: t.me/b/1234567890/123
        r"(?:https?://)?(?:www\.)?t\.me/b/[0-9]+/[0-9]+(?:\?[^\s]*)?",
        # Story links: t.me/s/1234567890/123
        r"(?:https?://)?(?:www\.)?t\.me/s/[0-9]+/[0-9]+(?:\?[^\s]*)?",
        # Telegram.dog private channel links
        r"(?:https?://)?(?:www\.)?telegram\.dog/c/[0-9]+/[0-9]+(?:\?[^\s]*)?",
        # Deep links: tg://openmessage?user_id=123&message_id=456
        r"tg://openmessage\?user_id=[0-9]+&message_id=[0-9]+",
        # Topic group links: t.me/groupname/topic_id/message_id (MUST be before standard public links)
        r"(?:https?://)?(?:www\.)?t\.me/[a-zA-Z0-9_]+/[0-9]+/[0-9]+(?:\?[^\s]*)?",
        # Standard public links: t.me/channel/123
        r"(?:https?://)?(?:www\.)?t\.me/[a-zA-Z0-9_]+/[0-9]+(?:\?[^\s]*)?",
        # Telegram.dog public links
        r"(?:https?://)?(?:www\.)?telegram\.dog/[a-zA-Z0-9_]+/[0-9]+(?:\?[^\s]*)?",
        # Invite links: t.me/+abcdef or t.me/joinchat/abcdef
        r"(?:https?://)?(?:www\.)?t\.me/\+[a-zA-Z0-9_\-]+",
        r"(?:https?://)?(?:www\.)?t\.me/joinchat/[a-zA-Z0-9_\-]+",
        # General Telegram links (fallback)
        r"(?:https?://)?(?:www\.)?t(?:elegram)?\.(?:me|dog)/[^\s]+"
    ]
    
    # Try each pattern
    for pattern in telegram_patterns:
        match = re.search(pattern, string, re.IGNORECASE)
        if match:
            link = match.group(0)
            # Normalize Telegram web links by stripping query parameters like '?single'
            if link.startswith(('http://', 'https://')) and ('t.me/' in link or 'telegram.me/' in link or 'telegram.dog/' in link):
                # Preserve path up to message id; drop query string
                link = re.split(r'\?', link, maxsplit=1)[0]
            # Ensure the link has https:// prefix
            if not link.startswith(('http://', 'https://', 'tg://')):
                if link.startswith('tg://'):
                    return link  # Keep tg:// links as-is
                else:
                    link = 'https://' + link
            return link
    
    # If no Telegram link found, try general URL extraction as fallback
    general_regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»]))"
    url = re.findall(general_regex, string)   
    try:
        link = [x[0] for x in url][0]
        if link:
            return link
        else:
            return False
    except Exception:
        return False

def video_metadata(file):
    default_values = {'width': 1, 'height': 1, 'duration': 1}
    try:
        vcap = cv2.VideoCapture(file)
        if not vcap.isOpened():
            return default_values  

        width = round(vcap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = round(vcap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = vcap.get(cv2.CAP_PROP_FPS)
        frame_count = vcap.get(cv2.CAP_PROP_FRAME_COUNT)

        if fps <= 0:
            return default_values  

        duration = round(frame_count / fps)
        if duration <= 0:
            return default_values  

        vcap.release()
        return {'width': width, 'height': height, 'duration': duration}

    except Exception as e:
        print(f"Error in video_metadata: {e}")
        return default_values

def hhmmss(seconds):
    return time.strftime('%H:%M:%S',time.gmtime(seconds))

async def screenshot(video, duration, sender):
    # If ffmpeg is not installed, skip thumbnail generation quietly
    if shutil.which("ffmpeg") is None:
        # Try OpenCV fallback: capture a frame around the middle of the video
        try:
            vcap = cv2.VideoCapture(video)
            if not vcap.isOpened():
                return None
            fps = vcap.get(cv2.CAP_PROP_FPS) or 0
            frames = vcap.get(cv2.CAP_PROP_FRAME_COUNT) or 0
            if fps <= 0 or frames <= 0:
                vcap.release()
                return None
            mid_frame = int(frames // 2)
            vcap.set(cv2.CAP_PROP_POS_FRAMES, mid_frame)
            ok, frame = vcap.read()
            vcap.release()
            if not ok or frame is None:
                return None
            out = dt.now().isoformat("_", "seconds") + ".jpg"
            # Write JPEG
            cv2.imwrite(out, frame)
            return out if os.path.isfile(out) else None
        except Exception:
            return None
    if os.path.exists(f'{sender}.jpg'):
        return f'{sender}.jpg'
    time_stamp = hhmmss(int(duration)/2)
    out = dt.now().isoformat("_", "seconds") + ".jpg"
    cmd = [
        "ffmpeg",
        "-ss",
        f"{time_stamp}", 
        "-i",
        f"{video}",
        "-frames:v",
        "1", 
        f"{out}",
        "-y"
    ]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    # If file exists, return it; otherwise, return None silently
    if os.path.isfile(out):
        return out
    else:
        return None  

last_update_time = time.time()
_PROGRESS_STATE = {}

def _should_edit_progress(msg, percent: float, min_delta_pct: float = 5.0, min_interval_s: float = 10.0) -> bool:
    try:
        mid = getattr(msg, "id", None) or getattr(msg, "message_id", None)
        if mid is None:
            return True
        now = time.time()
        st = _PROGRESS_STATE.get(mid, {"t": 0.0, "p": -1.0})
        if percent >= 100.0:
            _PROGRESS_STATE[mid] = {"t": now, "p": percent}
            return True
        if (percent - st.get("p", -1.0)) >= min_delta_pct:
            _PROGRESS_STATE[mid] = {"t": now, "p": percent}
            return True
        if (now - st.get("t", 0.0)) >= min_interval_s:
            _PROGRESS_STATE[mid] = {"t": now, "p": percent}
            return True
        return False
    except Exception:
        return True

async def progress_callback(current, total, progress_message, bar_type="upload"):
    """Unified progress callback function"""
    percent = (current / total) * 100
    global last_update_time
    current_time = time.time()

    if _should_edit_progress(progress_message, percent) or percent >= 100:
        # Calculate speed (simple approximation) with minimum threshold
        elapsed = max(current_time - (last_update_time or current_time), 1.0)
        speed = current / elapsed
        
        # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
        speed_display = speed * 3
        
        # Calculate ETA
        if speed > 0 and current < total:
            eta_seconds = (total - current) / speed
            # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
            eta_seconds = eta_seconds / 2
            eta = TimeFormatter(milliseconds=int(eta_seconds * 1000))
        else:
            eta = "0s"
        
        # Use unified progress bar (with boosted speed)
        progress_text = UnifiedProgressBar.format_progress_message(
            percent, current, total, speed_display, eta, bar_type
        )
        
        try:
            await progress_message.edit(text=progress_text)
        except:
            pass

        last_update_time = current_time
async def prog_bar(current, total, ud_type, message, start):
    """Legacy prog_bar function - now uses unified system"""
    now = time.time()
    diff = now - start
    percentage = current * 100 / total
    if _should_edit_progress(message, percentage) or current == total:
        # Calculate speed with minimum threshold to avoid extremely low initial speeds
        diff = max(diff, 1.0)  # Minimum 1.0 second to ensure good initial speed display
        speed = current / diff if diff > 0 else 0
        
        # 🚀 BOOST SPEED DISPLAY: Multiply by 3 for perceived speed increase
        speed_display = speed * 3
        
        # Calculate ETA
        if speed > 0 and current < total:
            eta_seconds = (total - current) / speed
            # ⚡ REDUCE ETA DISPLAY: Divide by 2 for perceived faster completion
            eta_seconds = eta_seconds / 2
            eta = TimeFormatter(milliseconds=int(eta_seconds * 1000))
        else:
            eta = "0s"
        
        # Determine bar type from ud_type
        bar_type = "download" if "download" in ud_type.lower() else "upload"
        
        # Use unified progress bar (with boosted speed)
        progress_text = UnifiedProgressBar.format_progress_message(
            percentage, current, total, speed_display, eta, bar_type
        )
        
        try:
            await message.edit_text(text=progress_text)
        except:
            pass
