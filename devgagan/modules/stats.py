import time
import sys
import motor
from devgagan import app
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from config import OWNER_ID
from devgagan.core.mongo.users_db import get_users, get_users_excluding_bots, add_user, get_user
from devgagan.core.mongo.plans_db import premium_users
from devgagan.core.get_func import dashboard_manager



start_time = time.time()

@app.on_message(group=10)
async def chat_watcher_func(_, message):
    try:
        if message.from_user:
            us_in_db = await get_user(message.from_user.id)
            if not us_in_db:
                await add_user(message.from_user.id)
    except:
        pass

def time_formatter():
    minutes, seconds = divmod(int(time.time() - start_time), 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    weeks, days = divmod(days, 7)
    tmp = (
        ((str(weeks) + "w:") if weeks else "")
        + ((str(days) + "d:") if days else "")
        + ((str(hours) + "h:") if hours else "")
        + ((str(minutes) + "m:") if minutes else "")
        + ((str(seconds) + "s") if seconds else "")
    )
    if tmp != "":
        if tmp.endswith(":"):
            return tmp[:-1]
        else:
            return tmp
    else:
        return "0 s"


@app.on_message(filters.command("stats"))
async def stats(client, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    
    # Measure pure MongoDB ping (fast operations only)
    ping_start = time.time()
    all_users = await get_users()
    premium = await premium_users()
    ping = round((time.time() - ping_start) * 1000)
    
    # Get user counts (this is slow due to Telegram API calls, but don't include in ping)
    try:
        users_excluding_bots = await get_users_excluding_bots()
        user_count = len(users_excluding_bots)
        total_count = len(all_users)
        bot_count = total_count - user_count
    except Exception as e:
        # Fallback to old method if new function fails
        print(f"⚠️ Bot filtering failed, using fallback: {e}")
        user_count = len(all_users)
        total_count = user_count
        bot_count = 0
    
    stats_text = f"""
<b>📊 Bot Statistics</b> - {(await client.get_me()).mention}

🏓 <b>Ping Pong</b>: <code>{ping}ms</code>

👥 <b>Real Users</b>: <code>{user_count:,}</code>
🤖 <b>Bots Filtered</b>: <code>{bot_count:,}</code>
📊 <b>Total Entries</b>: <code>{total_count:,}</code>
📈 <b>Premium Users</b>: <code>{len(premium):,}</code>
⚙️ <b>Bot Uptime</b>: <code>{time_formatter()}</code>
    
🎨 <b>Python Version</b>: <code>{sys.version.split()[0]}</code>
📑 <b>Mongo Version</b>: <code>{motor.version}</code>

💡 <i>Statistics now exclude bot accounts for accuracy</i>
"""
    
    await message.reply_text(stats_text, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("dashboard"))
async def dashboard_command(client, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    """Enhanced admin dashboard with real-time stats"""
    try:
        # Send initial loading message
        loading_msg = await message.reply_text(
            "🔄 <b>Loading Dashboard...</b>\n\nGathering real-time statistics...",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        
        # Generate dashboard
        dashboard_text = await dashboard_manager.generate_dashboard()
        
        # Update message with dashboard and a Back button
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="nav:back_delete")]])
        await loading_msg.edit_text(
            dashboard_text,
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
        
    except Exception as e:
        await message.reply_text(
            f"❌ <b>Dashboard Error</b>: {str(e)}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
  
