from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import random
import requests
import string
import aiohttp
import re
from devgagan import app
from devgagan.core.func import *
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB, WEBSITE_URL, AD_API, LOG_GROUP, PREMIUM_LIMIT, FREEMIUM_LIMIT  
from pyrogram.enums import ParseMode
 
# Token database commented out
# tclient = AsyncIOMotorClient(MONGO_DB)
# tdb = tclient["telegram_bot"]
# token = tdb["tokens"]
 
 
# Token-related functions commented out
# async def create_ttl_index():
#     await token.create_index("expires_at", expireAfterSeconds=0)

# Param = {}

# async def generate_random_param(length=8):
#     """Generate a random parameter."""
#     return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# async def get_shortened_url(deep_link):
#     api_url = f"https://{WEBSITE_URL}/api?api={AD_API}&url={deep_link}"

#      
#     async with aiohttp.ClientSession() as session:
#         async with session.get(api_url) as response:
#             if response.status == 200:
#                 data = await response.json()   
#                 if data.get("status") == "success":
#                     return data.get("shortenedUrl")
#     return None

# async def is_user_verified(user_id):
#     """Check if a user has an active session."""
#     session = await token.find_one({"user_id": user_id})
#     return session is not None

async def is_user_verified(user_id):
    """Token functionality disabled - always returns False"""
    return False
 
 
@app.on_message(filters.command("start"))
async def token_handler(client, message):
    """Handle the /start command."""
    join = await subscribe(client, message)
    if join == 1:
        return
    user_id = message.chat.id
    if len(message.command) <= 1:
        # New keyboard layout:
        # 1) 1x1 Contact Admin
        # 2) 2x2 grid of channel/group/resource links (3 rows, 2 columns)
        # 3) 1x1 Upgrade to Premium (triggers the upgrade flow)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("☎️ Contact Admin", url="https://t.me/ZeroTrace0x")],
            [
                InlineKeyboardButton("📣 Broadcasting", url="https://t.me/AlienxSaver"),
                InlineKeyboardButton("💭 Chat Group", url="https://t.me/AlienxSaverchat"),
            ],
            [
                InlineKeyboardButton("🎯 Premium Courses", url="https://t.me/udemyzap"),
                InlineKeyboardButton("🔐 VIP Members", url="https://t.me/+XvxdLuhsNOEzN2I1"),
            ],
            [
                InlineKeyboardButton("📚 Secret Resources", url="https://t.me/+XLIGpgGX1hcxNTI1"),
                InlineKeyboardButton("💎 Exclusive Vault", url="https://t.me/+DyNyXnu1ceFhM2U9"),
            ],
            [InlineKeyboardButton("✨ Upgrade to Premium", callback_data="nav:open_upgrade")],
        ])

        # HTML-formatted intro (commands left as plain text to stay clickable)
        def _esc(text: str) -> str:
            try:
                return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            except Exception:
                return text

        fname = _esc(getattr(message.from_user, "first_name", None) or "there")

        caption = (
            f"<b>🎊 Hey {fname}! Welcome to Restrict Bot Saver</b>\n\n"
            f"<b>⚡ QUICK START GUIDE:</b>\n"
            f"1️⃣ Send any public post link to save instantly\n"
            f"2️⃣ Use /login for private channels access\n"
            f"3️⃣ Try /batch for bulk downloads\n"
            f"4️⃣ Use /help for all commands & features\n"
            f"5️⃣ Want premium power? Use /upgrade!\n\n"
            f"🎥 Tutorial Video: https://t.me/AlienxSaver/58\n\n"
            f"<b>🎁 FREE PLAN FEATURES:</b>\n"
            f"┣ 📥 Batch downloads (up to {FREEMIUM_LIMIT} links)\n"
            f"┣ 🔥 2GB file uploads supported\n"
            f"┣ ⚡ 3 requests per minute\n"
            f"┗ ⏱️ Standard processing with cooldowns\n\n"
            f"<b>✨ PREMIUM PLAN BENEFITS:</b>\n"
            f"┣ 🎯 <b>Massive batches</b> - Up to {PREMIUM_LIMIT} links!\n"
            f"┣ 🚀 <b>5× faster speed</b> - 15 requests/minute\n"
            f"┣ ⚡ <b>Zero cooldowns</b> - Non-stop processing\n"
            f"┗ 👑 <b>Priority processing</b> - Skip all queues\n"
            f"<b>💡 Want more power? Use /upgrade for premium!</b>\n"
            f"<b>Ready to save? Let's go! 🔥</b>"
        )


        dropbox_share_url = (
            "https://www.dropbox.com/scl/fi/ukrb46ebwhjohw2ii8rs1/"
            "restricted.jpg?rlkey=2wmu84gg3f060vjsfbgsm5twl&st=iew6agyd&dl=0"
        )

        def _to_direct_dropbox(url: str) -> str:
            try:
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

        photo_url = _to_direct_dropbox(dropbox_share_url)

        try:
            await message.reply_photo(
                photo=photo_url,
                caption=caption,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            # Fallback to previous image (do not remove old behavior)
            await message.reply_photo(
                photo="lock.jpg",
                caption=caption,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
            )
        return  

    # Handle deep-link parameters (e.g., t.me/<bot>?start=upgrade)
    param = message.command[1] if len(message.command) > 1 else None
    if param:
        pl = str(param).strip().lower()
        if pl in ("upgrade", "premium", "plan", "plans", "pro"):
            try:
                # Import inside handler to avoid circular imports at module load
                from devgagan.modules.upgrade import build_upgrade_text, get_upgrade_keyboard
                await message.reply_text(
                    build_upgrade_text(),
                    reply_markup=get_upgrade_keyboard(),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                return
            except Exception:
                # Fallback: send a button that triggers the upgrade callback
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("✨ Open Upgrade Panel", callback_data="nav:open_upgrade")]])
                await message.reply_text("Opening upgrade panel...", reply_markup=kb)
                return
