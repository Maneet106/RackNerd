from time import time
from speedtest import Speedtest
import math
from datetime import datetime
import pytz
from devgagan import botStartTime
from devgagan import app
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from config import OWNER_ID

SIZE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

def get_readable_time(seconds: int) -> str:
    result = ''
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days != 0:
        result += f'{days}d'
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours != 0:
        result += f'{hours}h'
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes != 0:
        result += f'{minutes}m'
    seconds = int(seconds)
    result += f'{seconds}s'
    return result

def get_readable_file_size(size_in_bytes) -> str:
    if size_in_bytes is None:
        return '0B'
    index = 0
    while size_in_bytes >= 1024:
        size_in_bytes /= 1024
        index += 1
    try:
        return f'{round(size_in_bytes, 2)}{SIZE_UNITS[index]}'
    except IndexError:
        return 'File too large'



def speed_convert(size, byte=True):
    # Convert bits to bytes if needed
    if not byte: 
        size = size / 8
    
    # Network speeds use decimal (1000) not binary (1024)
    power = 1000
    zero = 0
    units = {0: "B/s", 1: "KB/s", 2: "MB/s", 3: "GB/s", 4: "TB/s"}
    
    while size >= power:
        size /= power
        zero += 1
    return f"{round(size, 2)} {units[zero]}"


# Admin-only speedtest command with silent handling for non-admins
@app.on_message(filters.command("speedtest"))
async def speedtest_admin_only(client, message):
    """
    Admin-only speedtest command. Non-admins get no response to save API limits.
    """
    user_id = message.from_user.id
    
    # Silent check: if not admin, do nothing (saves API limits)
    if user_id not in OWNER_ID:
        return  # Silent - no response for non-admins
    
    # Admin speedtest execution
    try:
        waiting = await message.reply_text(
            "<b>🚀 Running Network Speed Test</b>\n\n"
            "<b>⏳ Please wait, this may take a few moments...</b>\n"
            "📊 Initializing speed test servers...",
            parse_mode=ParseMode.HTML
        )
        
        # Initialize speedtest
        test = Speedtest()
        test.get_best_server()
        
        # Run download test
        await waiting.edit_text(
            "<b>🚀 Running Network Speed Test</b>\n\n"
            "<b>📥 Testing Download Speed...</b>\n"
            "📊 Measuring download bandwidth...",
            parse_mode=ParseMode.HTML
        )
        test.download()
        
        # Run upload test  
        await waiting.edit_text(
            "<b>🚀 Running Network Speed Test</b>\n\n"
            "<b>📤 Testing Upload Speed...</b>\n"
            "📊 Measuring upload bandwidth...",
            parse_mode=ParseMode.HTML
        )
        test.upload()
        
        # Generate shareable result
        test.results.share()
        result = test.results.dict()
        path = result.get('share')
        
        # Format comprehensive results with beautiful box-drawing format
        uptime = get_readable_time(time() - botStartTime)
        
        # Convert UTC timestamp to IST
        utc_time = datetime.fromisoformat(result['timestamp'].replace('Z', '+00:00'))
        ist_timezone = pytz.timezone('Asia/Kolkata')
        ist_time = utc_time.astimezone(ist_timezone)
        formatted_time = ist_time.strftime('%Y-%m-%d %H:%M:%S IST')
        
        text = (
            "╭─《 🚀 <b>SPEEDTEST RESULTS</b> 》\n"
            f"├ <b>Upload:</b> <code>{speed_convert(result['upload'], False)}</code>\n"
            f"├ <b>Download:</b> <code>{speed_convert(result['download'], False)}</code>\n"
            f"├ <b>Ping:</b> <code>{result['ping']} ms</code>\n"
            f"├ <b>Timestamp:</b> <code>{formatted_time}</code>\n"
            f"├ <b>Data Sent:</b> <code>{get_readable_file_size(int(result['bytes_sent']))}</code>\n"
            f"╰ <b>Data Received:</b> <code>{get_readable_file_size(int(result['bytes_received']))}</code>\n\n"
            "╭─《 🌐 <b>SERVER INFO</b> 》\n"
            f"├ <b>Name:</b> <code>{result['server']['name']}</code>\n"
            f"├ <b>Country:</b> <code>{result['server']['country']}, {result['server']['cc']}</code>\n"
            f"├ <b>Sponsor:</b> <code>{result['server']['sponsor']}</code>\n"
            f"├ <b>Latency:</b> <code>{result['server']['latency']} ms</code>\n"
            f"├ <b>Location:</b> <code>{result['server']['lat']}, {result['server']['lon']}</code>\n"
            f"╰ <b>Distance:</b> <code>{result['server'].get('d', 'N/A')} km</code>\n\n"
            "╭─《 👤 <b>CLIENT INFO</b> 》\n"
            f"├ <b>Location:</b> <code>{result['client']['lat']}, {result['client']['lon']}</code>\n"
            f"├ <b>Country:</b> <code>{result['client']['country']}</code>\n"
            f"├ <b>ISP:</b> <code>{result['client']['isp']}</code>\n"
            f"├ <b>ISP Rating:</b> <code>{result['client']['isprating']}</code>\n"
            f"╰ <b>Bot Uptime:</b> <code>{uptime}</code>"
        )
        
        # Create admin keyboard
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 View Chart", url=path)] if path else [],
            [InlineKeyboardButton("❌ Close", callback_data="nav:back_delete")]
        ])
        
        # Send results with image if available
        if path:
            try:
                await waiting.delete()
                await message.reply_photo(
                    path, 
                    caption=text, 
                    parse_mode=ParseMode.HTML, 
                    reply_markup=kb
                )
            except Exception:
                # Fallback to text if image fails
                await waiting.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            await waiting.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            
    except Exception as e:
        try:
            error_text = (
                "<b>❌ Speed Test Failed</b>\n\n"
                f"<b>🔍 Error Details:</b> <code>{str(e)}</code>\n\n"
                "<b>🔧 Possible Causes:</b>\n"
                "<b>•</b> Network connectivity issues\n"
                "<b>•</b> Server overload or limitations\n"
                "<b>•</b> Firewall or proxy restrictions\n\n"
                "<b>🔄 Try running the test again in a few moments.</b>"
            )
            if 'waiting' in locals():
                await waiting.edit_text(error_text, parse_mode=ParseMode.HTML)
            else:
                await message.reply_text(error_text, parse_mode=ParseMode.HTML)
        except Exception:
            pass  # Silent fail if even error message fails
