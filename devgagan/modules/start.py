from pyrogram import filters
import io
from devgagan import app
from config import OWNER_ID, PREMIUM_LIMIT, FREEMIUM_LIMIT
from devgagan.core.func import subscribe
import asyncio
from devgagan.core.func import *
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.raw.functions.bots import SetBotInfo
from pyrogram.raw.types import InputUserSelf
from devgagan.core.mongo.users_db import get_users, get_users_excluding_bots

from pyrogram.types import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import csv
import io
import datetime
import pytz
from devgagan.core.mongo import plans_db
from devgagan.core.mongo import db as userdata_db
 
@app.on_message(filters.command("set"))
async def set_commands(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
     
    await app.set_bot_commands([
        BotCommand("start", "⚡ Start the bot"),
        BotCommand("batch", "📦 Extract in bulk"),
        BotCommand("login", "🔑 Get into the bot"),
        BotCommand("upgrade", "💎 Upgrade to Premium"),
        BotCommand("logout", "🚪 Get out of the bot"),
        BotCommand("transfer", "🎁 Gift premium to others"),
        BotCommand("myplan", "⏰ Get your plan details"),
        BotCommand("terms", "📜 Terms and conditions"),
        BotCommand("help", "❓ If you're a noob, still!"),
        BotCommand("cancel", "❌ Cancel batch process")
    ])
 
    await message.reply("✅ Commands configured successfully!")
 
 
 
 
help_pages = [
    (
        "🤖 **Welcome to RestrictBotSaver!**\n\n"
        "🎯 **What can this bot do?**\n"
        "• Save media from Telegram channels & groups\n"
        "• Download videos, photos, documents & audio files\n"
        "• Process single links or bulk batches\n"
        "• Access private channels with login\n"
        "• Upload files up to 2GB in size\n\n"
        
        "📋 **How to get started:**\n\n"
        "**For Public Channels:**\n"
        "• Simply send any public post link\n"
        "• Bot will instantly download and send the media\n"
        "• Example: `https://t.me/channel/123`\n\n"
        
        "**For Private Channels:**\n"
        "• First use `/login` to authenticate\n"
        "• Enter your phone number and verification code\n"
        "• Then send private channel links normally\n\n"
        
        "**For Bulk Downloads:**\n"
        "• Use `/batch` command for Automatic Download next posts \n"
        f"• Free users: Up to {FREEMIUM_LIMIT} links per batch\n"
        f"• Premium users: Up to {PREMIUM_LIMIT} links per batch\n\n"
        
        "💡 **Ready to explore commands? →**"
    ),
    (
        "⚡ **Available Commands**\n\n"
        
        "🚀 **Basic Commands:**\n"
        "• `/start` - Start the bot and see welcome message\n"
        "• `/help` - Show this help menu (you're here!)\n"
        "• `/terms` - Read our terms and conditions\n\n"
        
        "📥 **Download Commands:**\n"
        "• `/batch` - Automatically Download multiple posts at once\n"
        f"  ┗ __Free: {FREEMIUM_LIMIT} links | Premium: {PREMIUM_LIMIT} links__\n"
        "• `/cancel` - Stop any ongoing batch process\n"
        "  ┗ __Useful when you want to start a new batch or cancel a ongoing Download__\n\n"
        
        "🔐 **Account Commands:**\n"
        "• `/login` - Login to access private channels\n"
        "  ┗ __Required for private/restricted content__\n"
        "• `/logout` - Remove your session from bot\n"
        "  ┗ __Clears your login data for privacy__\n\n"
        
        "💎 **Premium Commands:**\n"
        "• `/upgrade` - Get premium subscription\n"
        f"  ┗ __Unlock {PREMIUM_LIMIT} batch limit & faster speeds__\n"
        "• `/myplan` - Check your premium status\n"
        "  ┗ __See expiry date and remaining time__\n"
        "• `/transfer` - Gift premium to another user\n"
        "  ┗ __Share your premium with friends__\n\n"
        
        "**__Powered by RestrictBotSaver__**"
    )
]
 
 
async def send_or_edit_help_page(_, message, page_number):
    if page_number < 0 or page_number >= len(help_pages):
        return
 
     
    prev_button = InlineKeyboardButton("◀️ Previous", callback_data=f"help_prev_{page_number}")
    next_button = InlineKeyboardButton("Next ▶️", callback_data=f"help_next_{page_number}")
 
     
    buttons = []
    if page_number > 0:
        buttons.append(prev_button)
    if page_number < len(help_pages) - 1:
        buttons.append(next_button)
 
     
    # Add a Back button to close the help panel
    keyboard = InlineKeyboardMarkup([
        buttons,
        [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")]
    ])
 
     
    await message.delete()
 
     
    await message.reply(
        help_pages[page_number],
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN
    )
 
 
@app.on_message(filters.command("help"))
async def help(client, message):
    join = await subscribe(client, message)
    if join == 1:
        return
 
     
    await send_or_edit_help_page(client, message, 0)

@app.on_message(filters.command("get"))
async def get_all_users(client, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    """Owner-only: present export options and handle user data export."""
    try:
        # Get accurate user count excluding bots
        try:
            users_excluding_bots = await get_users_excluding_bots()
            real_user_count = len(users_excluding_bots)
            all_users = await get_users()
            total_count = len(all_users)
            bot_count = total_count - real_user_count
        except Exception as e:
            # Fallback to old method if new function fails
            print(f"⚠️ Bot filtering failed in /get, using fallback: {e}")
            all_users = await get_users()
            real_user_count = len(all_users)
            total_count = real_user_count
            bot_count = 0
        
        if total_count == 0:
            await message.reply_text("ℹ️ No users found in the database.")
            return

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📄 Export TXT", callback_data="export_users:txt"),
                InlineKeyboardButton("📊 Export CSV", callback_data="export_users:csv"),
            ]
        ])
        
        export_text = f"""
📊 <b>User Database Export</b>

👥 <b>Real Users:</b> <code>{real_user_count:,}</code>
🤖 <b>Bots (Filtered):</b> <code>{bot_count:,}</code>
📊 <b>Total Entries:</b> <code>{total_count:,}</code>

💡 <i>Export will only include real users (bots excluded)</i>

Select an export format:
"""
        
        await message.reply_text(
            export_text,
            reply_markup=kb,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await message.reply_text(f"❌ Failed to prepare export: {e}")


@app.on_callback_query(filters.regex(r"^export_users:(txt|csv)$"))
async def on_export_users(client, callback_query):
    # Silent admin check - no response for non-admins
    if callback_query.from_user.id not in OWNER_ID:
        return
    format_type = callback_query.data.split(":")[1]
    chat_id = callback_query.message.chat.id
    try:
        users = await get_users()
        users = sorted(set(int(u) for u in users))  # no duplicates
        count = len(users)

        # Fetch Telegram profile info in batches
        BATCH = 100
        tg_info = {}
        for i in range(0, count, BATCH):
            batch = users[i:i+BATCH]
            try:
                tg_users = await app.get_users(batch)
                if not isinstance(tg_users, list):
                    tg_users = [tg_users]
                for u in tg_users:
                    if not u:
                        continue
                    tg_info[u.id] = {
                        "username": u.username or "-",
                        "first_name": u.first_name or "-",
                        "last_name": u.last_name or "-",
                        "is_bot": getattr(u, "is_bot", False),
                    }
            except Exception:
                for uid in batch:
                    try:
                        u = await app.get_users(uid)
                        tg_info[uid] = {
                            "username": (u.username or "-") if u else "-",
                            "first_name": (u.first_name or "-") if u else "-",
                            "last_name": (u.last_name or "-") if u else "-",
                            "is_bot": getattr(u, "is_bot", False) if u else False,
                        }
                    except Exception:
                        tg_info[uid] = {"username": "-", "first_name": "-", "last_name": "-", "is_bot": False}

        # Exclude bots (username ending with 'bot' or is_bot true)
        def is_bot_username(uname: str) -> bool:
            if not uname or uname in ("-", "None"):
                return False
            return str(uname).lower().endswith("bot")

        filtered_users = []
        for uid in users:
            info = tg_info.get(uid, {})
            uname = info.get("username")
            if info.get("is_bot") or is_bot_username(uname):
                continue
            filtered_users.append(uid)

        # Build enriched rows
        ist = pytz.timezone("Asia/Kolkata")
        now_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        rows = []
        for uid in filtered_users:
            info = tg_info.get(uid, {"username": "-", "first_name": "-", "last_name": "-"})

            # PLAN and EXPIRES_AT
            plan = "Free"
            expires_at = "-"
            try:
                pdata = await plans_db.check_premium(uid)
                if pdata and pdata.get("expire_date"):
                    expiry = pdata.get("expire_date")
                    if expiry.tzinfo is None:
                        expiry = expiry.replace(tzinfo=pytz.utc)
                    expires_at = expiry.astimezone(ist).strftime("%d-%m-%Y %I:%M:%S %p")
                    plan = "Premium" if expiry > now_utc else "Free"
            except Exception:
                pass

            # SESSION_PRESENT (user-specific session in userdata_db)
            session_present = "No"
            try:
                udoc = await userdata_db.get_data(uid)
                if udoc and udoc.get("session"):
                    session_present = "Yes"
            except Exception:
                pass

            # TFA_ENABLED (not stored anywhere -> default to '-')
            tfa_enabled = "-"

            # Fields not tracked -> '-'
            joined_at = "-"
            last_active = "-"
            total_jobs = "-"
            total_downloads = "-"
            status = "active"  # default assumption

            uname = info.get("username", "-")
            uname_disp = f"@{uname}" if uname not in (None, "", "-") and not str(uname).startswith("@") else (uname or "-")

            rows.append([
                uid,
                uname_disp,
                info.get("first_name", "-"),
                info.get("last_name", "-"),
                joined_at,
                plan,
                expires_at,
                last_active,
                total_jobs,
                total_downloads,
                status,
                session_present,
                tfa_enabled,
            ])

        # Prepare files
        headers = [
            "USER_ID",
            "USERNAME",
            "FIRST_NAME",
            "LAST_NAME",
            "JOINED_AT",
            "PLAN",
            "EXPIRES_AT",
            "LAST_ACTIVE",
            "TOTAL_JOBS",
            "TOTAL_DOWNLOADS",
            "STATUS",
            "SESSION_PRESENT",
            "TFA_ENABLED",
        ]

        if format_type == "csv":
            # Use StringIO then convert to BytesIO to avoid empty file issues
            s = io.StringIO()
            writer = csv.writer(s)
            writer.writerow(headers)
            writer.writerows(rows)
            csv_bytes = ("\ufeff" + s.getvalue()).encode("utf-8")  # Prepend BOM for Excel
            bio = io.BytesIO(csv_bytes)
            bio.name = "users_export.csv"
            await app.send_document(
                chat_id,
                bio,
                caption=(
                    "<b>Users Export (CSV)</b>\n"
                    f"Total (excluding bots): <b>{len(filtered_users)}</b>\n"
                    "Columns: USER_ID, USERNAME, FIRST_NAME, LAST_NAME, JOINED_AT, PLAN, EXPIRES_AT, LAST_ACTIVE, TOTAL_JOBS, TOTAL_DOWNLOADS, STATUS, SESSION_PRESENT, TFA_ENABLED"
                ),
                parse_mode=ParseMode.HTML,
            )
        else:
            # TXT with plain ASCII table formatting
            # Compute column widths
            str_rows = [[str(c) for c in r] for r in rows]
            widths = [len(h) for h in headers]
            for r in str_rows:
                for i, c in enumerate(r):
                    widths[i] = max(widths[i], len(c))

            def sep(char_left="+", char_mid="+", char_right="+", fill="-"):
                return char_left + char_mid.join(fill * (w + 2) for w in widths) + char_right

            def fmt_row(cols):
                return "|" + "|".join(" " + cols[i].ljust(widths[i]) + " " for i in range(len(cols))) + "|"

            lines = [
                sep(),
                fmt_row(headers),
                sep(),
            ]
            for r in str_rows:
                lines.append(fmt_row(r))
            lines.append(sep())
            content = "\n".join(lines)
            bio = io.BytesIO(content.encode("utf-8"))
            bio.name = "users_export.txt"
            await app.send_document(
                chat_id,
                bio,
                caption=(
                    "<b>Users Export (TXT)</b>\n"
                    f"Total (excluding bots): <b>{len(filtered_users)}</b>\n"
                    "ASCII table inside the file."
                ),
                parse_mode=ParseMode.HTML,
            )

        await callback_query.answer("Export generated")
    except Exception as e:
        await callback_query.answer("Failed")
        await app.send_message(chat_id, f"❌ Export failed: {e}")
 
 
@app.on_callback_query(filters.regex(r"help_(prev|next)_(\d+)"))
async def on_help_navigation(client, callback_query):
    action, page_number = callback_query.data.split("_")[1], int(callback_query.data.split("_")[2])
 
    if action == "prev":
        page_number -= 1
    elif action == "next":
        page_number += 1
 
     
    await send_or_edit_help_page(client, callback_query.message, page_number)
 
     
    await callback_query.answer()
 
 
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
 
@app.on_message(filters.command("terms") & filters.private)
async def terms(client, message):
    terms_text = (
        "> 📜 **Terms, Use & Safety**\n\n"
        "We designed this bot to help users manage and back up their own content. We do not endorse or facilitate copyright infringement.\n\n"
        "• You must only use the bot with content you own, created, or have clear permission to process.\n"
        "• Do not use the bot to circumvent Telegram’s content protections (e.g., protected content, hidden history) or any platform restrictions.\n"
        "• We comply with Telegram’s Terms of Service and Bot API policies. If requested by Telegram or a valid rights holder, we may remove or restrict access to specific content.\n\n"
        "🔐 **Privacy & Data**\n"
        "• The bot processes content on-demand and does not aim to store media longer than necessary to deliver your request.\n"
        "• Minimal logs are kept for reliability, abuse-prevention, and support. We do not sell user data.\n\n"
        "💳 **Payments & Subscriptions**\n"
        "• Any subscription or payment is solely to cover operational and infrastructure costs (servers, bandwidth, maintenance).\n"
        "• Subscriptions do not grant ownership of any content, nor permission to infringe copyrights.\n"
        "• Service availability is best-effort; we may pause or limit features to comply with policies, prevent abuse, or maintain stability.\n\n"
        "🚫 **Acceptable Use & Enforcement**\n"
        "• Do not upload illegal content, spam, or attempt to bypass technical protections.\n"
        "• We may suspend or restrict usage for violations, abuse, or when required by law or platform policies.\n\n"
        "🤝 **Contact & Takedowns**\n"
        "• If you believe content processed via this bot infringes your rights, contact us with sufficient detail so we can review promptly.\n\n"
        "By using this bot, you confirm you understand and accept these terms."
    )
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔒 Privacy & Data", callback_data="see_privacy")],
            [InlineKeyboardButton("💬 Contact Support", url="https://t.me/ZeroTrace0x")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
        ]
    )
    await message.reply_text(terms_text, reply_markup=buttons, parse_mode=ParseMode.MARKDOWN)
 
 
# /plan command removed
 
 
@app.on_callback_query(filters.regex("see_terms"))
async def see_terms(client, callback_query):
    terms_text = (
        "> 📜 **Terms, Use & Safety**\n\n"
        "This bot is a utility to manage your own lawful content. Do not use it to violate copyrights, platform protections, or laws.\n\n"
        "• Use only with content you own or are authorized to process.\n"
        "• Respect Telegram rules (including protected content, fair use, and anti-spam policies).\n"
        "• We may restrict features to comply with policies or prevent abuse.\n\n"
        "🔐 Privacy: We process on-demand and try not to persist media longer than necessary to deliver. Minimal logs for reliability/abuse prevention only.\n\n"
        "💳 Subscriptions: Support operational costs only (servers/bandwidth). They do not grant rights to any copyrighted material.\n\n"
        "🚫 Violations may result in suspension. Rights holders can reach out for takedown review.\n"
    )
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔒 Privacy & Data", callback_data="see_privacy")],
            [InlineKeyboardButton("💬 Contact Support", url="https://t.me/ZeroTrace0x")],
        ]
    )
    await callback_query.message.edit_text(terms_text, reply_markup=buttons, parse_mode=ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("see_privacy"))
async def see_privacy(client, callback_query):
    privacy_text = (
        "> 🔐 **Privacy & Data**\n\n"
        "• We process your requests on-demand and aim to avoid storing files longer than needed to deliver your result.\n"
        "• We keep minimal logs for reliability, abuse prevention, and support; we do not sell user data.\n"
        "• We honor valid platform/legal takedown requests and may limit functionality to comply with policies.\n\n"
        "If you have privacy questions or need a takedown, contact support."
    )
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📃 See Terms", callback_data="see_terms")],
            [InlineKeyboardButton("💬 Contact Support", url="https://t.me/ZeroTrace0x")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")],
        ]
    )
    await callback_query.message.edit_text(privacy_text, reply_markup=buttons, parse_mode=ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("verify_subscription"))
async def verify_subscription(client, callback_query):
    """Handle subscription verification when user clicks the verify button"""
    user_id = callback_query.from_user.id
    
    # Import here to avoid circular imports
    from config import CHANNEL, CHANNEL_ID, FREEMIUM_LIMIT, PREMIUM_LIMIT
    from pyrogram.errors import UserNotParticipant, ChatAdminRequired
    from pyrogram.enums import ChatMemberStatus
    
    # Check both channels
    main_channel = CHANNEL if CHANNEL else CHANNEL_ID
    alienx_channel = "@AlienxSaver"
    
    main_joined = False
    alienx_joined = False
    
    # Check main channel membership
    if main_channel:
        try:
            user = await client.get_chat_member(main_channel, user_id)
            if user.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                main_joined = True
        except UserNotParticipant:
            main_joined = False
        except ChatAdminRequired:
            main_joined = True  # Allow access since we can't verify
        except Exception:
            main_joined = True  # Allow access on error to prevent blocking users
    else:
        main_joined = True  # If no main channel configured, skip this check
    
    # Check AlienxSaver channel membership
    try:
        user = await client.get_chat_member(alienx_channel, user_id)
        if user.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
            alienx_joined = True
    except UserNotParticipant:
        alienx_joined = False
    except ChatAdminRequired:
        alienx_joined = True  # Allow access since we can't verify
    except Exception:
        alienx_joined = True  # Allow access on error
    
    if main_joined and alienx_joined:
        # User has joined both channels - show success message and welcome
        fname = getattr(callback_query.from_user, "first_name", "User") or "User"
        
        success_text = (
            f"🎊 <b>Welcome {fname}!</b>\n\n"
            f"✅ <b>Verification Successful!</b>\n"
            f"You have successfully joined both required channels.\n\n"
            f"🎊 <b>You're all set! Here's what you can do:</b>\n\n"
            f"<b>⚡ QUICK START GUIDE:</b>\n"
            f"1️⃣ Send any public post link to save instantly\n"
            f"2️⃣ Use /login for private channels access\n"
            f"3️⃣ Try /batch for bulk downloads\n"
            f"4️⃣ Use /help for all commands & features\n"
            f"5️⃣ Want premium power? Use /upgrade!\n\n"
            f"🎥 Tutorial Video: https://t.me/AlienxSaver/58\n\n"
            f"<b>🎁 FREE FEATURES:</b>\n"
            f"┣ 📥 Batch downloads (up to {FREEMIUM_LIMIT} links)\n"
            f"┣ 🔥 2GB file uploads supported\n"
            f"┣ ⚡ 3 requests per minute\n"
            f"┗ ⏱️ Standard processing with cooldowns\n\n"
            f"<b>✨ PREMIUM UPGRADE BENEFITS:</b>\n"
            f"┣ 🎯 <b>Massive batches</b> - Up to {PREMIUM_LIMIT} links!\n"
            f"┣ 🚀 <b>5× faster</b> - 15 requests per minute\n"
            f"┣ ⚡ <b>No cooldowns</b> - Process non-stop\n"
            f"┗  👑 <b>Priority processing</b> - Skip queues\n"
            f"💡 <b>Ready to upgrade?</b> Use /upgrade for premium!\n\n"
            f"<i>Happy saving! 🎯</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("☎️ Contact Admin", url="https://t.me/ZeroTrace0x"),
                InlineKeyboardButton("✨ Upgrade Premium", callback_data="nav:open_upgrade")
            ],
            [InlineKeyboardButton("❓ Help & Commands", callback_data="show_help")]
        ])
        
        await callback_query.message.edit_text(
            success_text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        await callback_query.answer("✅ Verification successful! Welcome aboard!", show_alert=True)
        
    else:
        # User hasn't joined both channels yet
        missing_channels = []
        if not main_joined and main_channel:
            if isinstance(main_channel, str) and main_channel.startswith("@"):
                missing_channels.append(main_channel[1:])
            else:
                missing_channels.append("Main Channel")
        if not alienx_joined:
            missing_channels.append("AlienxSaver")
        
        missing_text = " and ".join(missing_channels)
        
        await callback_query.answer(
            f"❌ Please join {missing_text} first, then click verify again!",
            show_alert=True
        )

@app.on_callback_query(filters.regex("show_help"))
async def show_help_callback(client, callback_query):
    """Show help when user clicks help button"""
    await send_or_edit_help_page(client, callback_query.message, 0)
    await callback_query.answer()
 
