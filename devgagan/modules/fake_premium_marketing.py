"""
Fabricated Premium Marketing System
Creates realistic premium upgrade notifications to attract traffic
Designed to be completely undetectable and professional
"""

import asyncio
import random
import datetime
import pytz
from devgagan import app
from config import PREMIUM_BROADCAST, OWNER_ID, FAKE_MARKETING_MIN_INTERVAL, FAKE_MARKETING_MAX_INTERVAL
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram import filters
from pyrogram.enums import ParseMode

# Comprehensive Indian names database (weighted for authenticity)
INDIAN_NAMES = [
    # Common Hindi/North Indian names
    "Rahul", "Priya", "Amit", "Sneha", "Rohit", "Pooja", "Vikash", "Anjali", "Suresh", "Kavya",
    "Arjun", "Divya", "Rajesh", "Meera", "Sanjay", "Ritu", "Deepak", "Nisha", "Manoj", "Sunita",
    "Arun", "Shweta", "Vinod", "Rekha", "Ashok", "Geeta", "Ravi", "Sita", "Prakash", "Usha",
    "Ankit", "Preeti", "Sachin", "Kiran", "Nitin", "Seema", "Gaurav", "Asha", "Pankaj", "Lata",
    "Vishal", "Neha", "Ajay", "Sushma", "Manish", "Radha", "Sunil", "Manju", "Akash", "Poonam",
    
    # South Indian names
    "Rajesh", "Lakshmi", "Krishna", "Sita", "Venkat", "Priya", "Suresh", "Meena", "Raman", "Devi",
    "Anil", "Kamala", "Mohan", "Saraswati", "Gopal", "Shanti", "Hari", "Parvati", "Bala", "Ganga",
    "Karthik", "Vani", "Srinivas", "Padma", "Murali", "Suma", "Ramesh", "Latha", "Naresh", "Sujatha",
    
    # Bengali names
    "Sourav", "Riya", "Abhijit", "Shreya", "Subrata", "Moumita", "Debashis", "Payal", "Tapas", "Swati",
    "Arnab", "Ankita", "Suman", "Debjani", "Partha", "Ruma", "Sandip", "Mitali", "Biswajit", "Soma",
    
    # Gujarati/Marathi names
    "Kiran", "Nita", "Jayesh", "Hema", "Nilesh", "Rina", "Mahesh", "Lila", "Paresh", "Mina",
    "Hitesh", "Jaya", "Mukesh", "Sonal", "Rakesh", "Naina", "Dinesh", "Komal", "Ramesh", "Hetal",
    
    # Punjabi names
    "Harpreet", "Simran", "Jasbir", "Manpreet", "Gurpreet", "Navpreet", "Balwinder", "Kuldeep", "Amarjit", "Ranjit",
    "Sukhwinder", "Paramjit", "Gurbir", "Jaspal", "Harbir", "Surinder", "Davinder", "Rajinder", "Jatinder", "Satinder",
    
    # Modern/Urban names
    "Aryan", "Aarohi", "Vihan", "Ananya", "Reyansh", "Diya", "Aarav", "Kiara", "Vivaan", "Saanvi",
    "Aditya", "Ira", "Kabir", "Myra", "Shivansh", "Aanya", "Rudra", "Navya", "Arjun", "Tara",
    
    # Traditional names
    "Ramakrishna", "Saraswati", "Jagdish", "Durga", "Narayan", "Gayatri", "Govind", "Rukmani", "Shankar", "Tulsi",
    "Bhushan", "Vandana", "Mohan", "Savitri", "Gopal", "Kamala", "Hari", "Shobha", "Ravi", "Indira"
]

# Foreign names (fewer, as requested)
FOREIGN_NAMES = [
    # Western names
    "John", "Sarah", "Michael", "Emma", "David", "Lisa", "James", "Anna", "Robert", "Maria",
    "William", "Jennifer", "Richard", "Michelle", "Thomas", "Jessica", "Daniel", "Ashley",
    "Christopher", "Amanda", "Matthew", "Stephanie", "Anthony", "Melissa", "Mark", "Nicole",
    
    # European names
    "Alexander", "Sofia", "Nicolas", "Elena", "Andreas", "Natasha", "Viktor", "Katarina",
    "Dmitri", "Anastasia", "Ivan", "Olga", "Pavel", "Svetlana", "Mikhail", "Irina",
    
    # Asian names (non-Indian)
    "Chen", "Li", "Wang", "Zhang", "Liu", "Yang", "Huang", "Zhao", "Wu", "Zhou",
    "Takeshi", "Yuki", "Hiroshi", "Sakura", "Kenji", "Akiko", "Ryo", "Mika",
    
    # Middle Eastern names
    "Ahmed", "Fatima", "Omar", "Aisha", "Hassan", "Zara", "Ali", "Nadia", "Yusuf", "Layla"
]

# Premium plan options with realistic durations
PREMIUM_PLANS = [
    {"duration": "7 days", "display": "7 days"},
    {"duration": "30 days", "display": "30 days"},
    {"duration": "90 days", "display": "90 days"},
    {"duration": "6 months", "display": "6 months"},
    {"duration": "1 year", "display": "1 year"}
]

# Track used names to avoid repetition until all are used
used_indian_names = set()
used_foreign_names = set()

# Runtime configurable marketing intervals (in seconds)
marketing_config = {
    "min_interval": FAKE_MARKETING_MIN_INTERVAL,
    "max_interval": FAKE_MARKETING_MAX_INTERVAL,
    "enabled": True
}

def get_random_name():
    """Get a random name with 90% Indian, 10% foreign distribution"""
    global used_indian_names, used_foreign_names
    
    # Reset if all names are used
    if len(used_indian_names) >= len(INDIAN_NAMES):
        used_indian_names.clear()
    if len(used_foreign_names) >= len(FOREIGN_NAMES):
        used_foreign_names.clear()
    
    # 90% chance for Indian names, 10% for foreign
    if random.random() < 0.9:
        # Indian name
        available_indian = [name for name in INDIAN_NAMES if name not in used_indian_names]
        if not available_indian:
            used_indian_names.clear()
            available_indian = INDIAN_NAMES
        
        name = random.choice(available_indian)
        used_indian_names.add(name)
        return name
    else:
        # Foreign name
        available_foreign = [name for name in FOREIGN_NAMES if name not in used_foreign_names]
        if not available_foreign:
            used_foreign_names.clear()
            available_foreign = FOREIGN_NAMES
        
        name = random.choice(available_foreign)
        used_foreign_names.add(name)
        return name

def get_random_plan():
    """Get a random premium plan"""
    return random.choice(PREMIUM_PLANS)

def is_business_hours():
    """Check if current time is between 8 AM and 11 PM IST"""
    ist = pytz.timezone("Asia/Kolkata")
    current_time = datetime.datetime.now(ist)
    hour = current_time.hour
    return 8 <= hour <= 23

def format_premium_message(name, plan_duration):
    """Format the premium message exactly like real ones"""
    ist = pytz.timezone("Asia/Kolkata")
    current_time = datetime.datetime.now(ist)
    
    # Format joining time exactly like the real system
    joining_time = current_time.strftime("%d-%m-%Y\n⏱️ ᴊᴏɪɴɪɴɢ ᴛɪᴍᴇ : %I:%M:%S %p")
    
    # Calculate expiry date based on plan
    if "day" in plan_duration:
        days = int(plan_duration.split()[0])
        expiry_date = current_time + datetime.timedelta(days=days)
    elif "month" in plan_duration:
        months = int(plan_duration.split()[0]) if plan_duration.split()[0].isdigit() else 6
        expiry_date = current_time + datetime.timedelta(days=months * 30)
    elif "year" in plan_duration:
        years = int(plan_duration.split()[0])
        expiry_date = current_time + datetime.timedelta(days=years * 365)
    else:
        expiry_date = current_time + datetime.timedelta(days=30)  # Default
    
    expiry_str = expiry_date.strftime("%d-%m-%Y\n⏱️ ᴇxᴘɪʀʏ ᴛɪᴍᴇ : %I:%M:%S %p")
    
    # Create message identical to real premium notifications
    message_text = (
        f"👋 ʜᴇʏ {name},\n"
        "ᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴘᴜʀᴄʜᴀꜱɪɴɢ ᴘʀᴇᴍɪᴜᴍ.\nᴇɴᴊᴏʏ !! ✨🎉\n\n"
        f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : <code>{plan_duration}</code>\n"
        f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {joining_time}\n\n"
        f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str}"
    )
    
    return message_text

async def send_fake_premium_notification(force_send=False):
    """Send a fake premium notification"""
    if not PREMIUM_BROADCAST:
        print(f"❌ PREMIUM_BROADCAST not configured in .env file")
        return False
    
    if not force_send and not is_business_hours():
        return False
    
    try:
        # Get random name and plan
        name = get_random_name()
        plan = get_random_plan()
        
        # Format message exactly like real ones
        message_text = format_premium_message(name, plan["duration"])
        
        # Create upgrade button identical to real system
        upgrade_btn = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🚀 Upgrade Now", url="https://t.me/RestrictedSaverxBot?start=upgrade")]]
        )
        
        # Send to broadcast channel
        await app.send_message(
            chat_id=PREMIUM_BROADCAST,
            text=message_text,
            reply_markup=upgrade_btn,
            disable_web_page_preview=True
        )
        
        print(f"✅ Fake premium notification sent: {name} - {plan['duration']}")
        return True
        
    except Exception as e:
        print(f"❌ Error sending fake premium notification: {e}")
        return False

async def marketing_scheduler():
    """Main scheduler that runs the marketing system"""
    while True:
        try:
            if is_business_hours() and marketing_config["enabled"]:
                # Random interval based on configurable values
                interval = random.randint(marketing_config["min_interval"], marketing_config["max_interval"])
                
                await asyncio.sleep(interval)
                await send_fake_premium_notification(force_send=False)
            else:
                # Sleep until next business hour starts
                ist = pytz.timezone("Asia/Kolkata")
                current_time = datetime.datetime.now(ist)
                
                # Calculate time until 8 AM next day
                next_8am = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
                if current_time.hour >= 8:
                    next_8am += datetime.timedelta(days=1)
                
                sleep_seconds = (next_8am - current_time).total_seconds()
                print(f"💤 Sleeping until business hours: {sleep_seconds/3600:.1f} hours")
                await asyncio.sleep(sleep_seconds)
                
        except Exception as e:
            print(f"❌ Marketing scheduler error: {e}")
            await asyncio.sleep(3600)  # Sleep 1 hour on error

# Admin commands for manual control
@app.on_message(filters.command("fakestart") & filters.private)
async def start_fake_marketing(client, message):
    """Start the fake marketing system (admin only)"""
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        # Start the marketing scheduler in background
        asyncio.create_task(marketing_scheduler())
        min_hours = marketing_config["min_interval"] / 3600
        max_hours = marketing_config["max_interval"] / 3600
        status = "🟢 Enabled" if marketing_config["enabled"] else "🔴 Disabled"
        
        await message.reply_text(
            "<b>✅ Fake Premium Marketing System Started</b>\n\n"
            "<b>📊 System Details:</b>\n"
            f"<b>•</b> Indian names: <code>{len(INDIAN_NAMES)} available</code>\n"
            f"<b>•</b> Foreign names: <code>{len(FOREIGN_NAMES)} available</code>\n"
            f"<b>•</b> Premium plans: <code>{len(PREMIUM_PLANS)} options</code>\n"
            f"<b>•</b> Business hours: <code>8 AM - 11 PM IST</code>\n"
            f"<b>•</b> Status: {status}\n"
            f"<b>•</b> Interval: <code>{min_hours:.1f}-{max_hours:.1f} hours</code>\n\n"
            "<b>🎯 Marketing Strategy:</b>\n"
            "<b>•</b> 90% Indian names, 10% foreign names\n"
            "<b>•</b> Random premium plans (7d to 1y)\n"
            "<b>•</b> Identical format to real notifications\n"
            "<b>•</b> Completely undetectable system\n\n"
            "<b>⚙️ Configuration Commands:</b>\n"
            "<b>•</b> <code>/fakeconfig</code> - Configure intervals\n"
            "<b>•</b> <code>/fakefreq</code> - Quick frequency presets\n"
            "<b>•</b> <code>/fakestats</code> - View detailed statistics",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.reply_text(
            f"<b>❌ Error starting marketing system:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML
        )

@app.on_message(filters.command("faketest") & filters.private)
async def test_fake_notification(client, message):
    """Send a test fake notification (admin only)"""
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        # Check configuration first
        if not PREMIUM_BROADCAST:
            await message.reply_text(
                "<b>❌ Configuration Error!</b>\n\n"
                "<b>🔧 Issue:</b> PREMIUM_BROADCAST not configured\n"
                "<b>📝 Solution:</b> Add PREMIUM_BROADCAST=your_channel_id to your .env file\n\n"
                "<b>💡 How to get Channel ID:</b>\n"
                "<b>1.</b> Add @userinfobot to your broadcast channel\n"
                "<b>2.</b> Forward any message from the channel to @userinfobot\n"
                "<b>3.</b> Copy the channel ID (including the minus sign)\n"
                "<b>4.</b> Add it to your .env file as <code>PREMIUM_BROADCAST=-1001234567890</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Send test notification (force send regardless of business hours)
        success = await send_fake_premium_notification(force_send=True)
        
        if success:
            await message.reply_text(
                "<b>✅ Test Notification Sent Successfully!</b>\n\n"
                f"<b>📢 Broadcast Channel:</b> <code>{PREMIUM_BROADCAST}</code>\n"
                "<b>🔍 Check the broadcast channel to see the result.</b>\n\n"
                "<b>💡 If you don't see the message:</b>\n"
                "<b>•</b> Make sure the bot is admin in the broadcast channel\n"
                "<b>•</b> Verify the PREMIUM_BROADCAST ID is correct\n"
                "<b>•</b> Check bot has permission to send messages",
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply_text(
                "<b>❌ Test Notification Failed!</b>\n\n"
                f"<b>📢 Target Channel:</b> <code>{PREMIUM_BROADCAST}</code>\n\n"
                "<b>🔧 Possible Issues:</b>\n"
                "<b>•</b> Bot is not admin in the broadcast channel\n"
                "<b>•</b> Wrong PREMIUM_BROADCAST channel ID\n"
                "<b>•</b> Bot doesn't have send message permissions\n"
                "<b>•</b> Channel doesn't exist or is private\n\n"
                "<b>💡 Solution:</b>\n"
                "<b>1.</b> Add bot as admin to broadcast channel\n"
                "<b>2.</b> Give bot permission to send messages\n"
                "<b>3.</b> Verify channel ID in .env file",
                parse_mode=ParseMode.HTML
            )
            
    except Exception as e:
        await message.reply_text(
            f"<b>❌ Error Sending Test Notification</b>\n\n"
            f"<b>🔍 Error Details:</b> <code>{str(e)}</code>\n\n"
            "<b>🔧 Common Solutions:</b>\n"
            "<b>•</b> Check PREMIUM_BROADCAST configuration\n"
            "<b>•</b> Ensure bot is admin in broadcast channel\n"
            "<b>•</b> Verify bot has message sending permissions",
            parse_mode=ParseMode.HTML
        )

@app.on_message(filters.command("fakestats") & filters.private)
async def fake_marketing_stats(client, message):
    """Show fake marketing system statistics (admin only)"""
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        ist = pytz.timezone("Asia/Kolkata")
        current_time = datetime.datetime.now(ist)
        business_hours_status = "🟢 Active" if is_business_hours() else "🔴 Inactive"
        
        min_hours = marketing_config["min_interval"] / 3600
        max_hours = marketing_config["max_interval"] / 3600
        status = "🟢 Enabled" if marketing_config["enabled"] else "🔴 Disabled"
        daily_notifications = f"~{15/(min_hours+max_hours)*2:.0f}-{15/min_hours:.0f} per day"
        
        stats_text = (
            "<b>📊 Fake Premium Marketing Statistics</b>\n\n"
            f"<b>🕐 Current Time:</b> <code>{current_time.strftime('%d-%m-%Y %I:%M:%S %p')} IST</code>\n"
            f"<b>⏰ Business Hours:</b> {business_hours_status}\n"
            f"<b>🎛️ System Status:</b> {status}\n"
            f"<b>⏱️ Notification Interval:</b> <code>{min_hours:.1f}-{max_hours:.1f} hours</code>\n"
            f"<b>📈 Estimated Daily:</b> <code>{daily_notifications}</code>\n\n"
            f"<b>📝 Used Indian Names:</b> <code>{len(used_indian_names)}/{len(INDIAN_NAMES)}</code>\n"
            f"<b>🌍 Used Foreign Names:</b> <code>{len(used_foreign_names)}/{len(FOREIGN_NAMES)}</code>\n\n"
            "<b>🎯 System Configuration:</b>\n"
            f"<b>•</b> Total Indian names: <code>{len(INDIAN_NAMES)}</code>\n"
            f"<b>•</b> Total foreign names: <code>{len(FOREIGN_NAMES)}</code>\n"
            f"<b>•</b> Premium plans: <code>{len(PREMIUM_PLANS)}</code>\n"
            f"<b>•</b> Name distribution: <code>90% Indian, 10% Foreign</code>\n"
            f"<b>•</b> Active hours: <code>8 AM - 11 PM IST</code>\n"
            f"<b>•</b> Broadcast channel: <code>{PREMIUM_BROADCAST}</code>\n\n"
            "<b>🔄 Available Commands:</b>\n"
            "<b>•</b> <code>/fakestart</code> - Start marketing system\n"
            "<b>•</b> <code>/faketest</code> - Send test notification\n"
            "<b>•</b> <code>/fakeconfig</code> - Configure intervals\n"
            "<b>•</b> <code>/fakefreq</code> - Quick frequency presets\n"
            "<b>•</b> <code>/fakestats</code> - Show statistics\n"
            "<b>•</b> <code>/fakereset</code> - Reset name pools\n"
            "<b>•</b> <code>/fakediag</code> - Run system diagnostics"
        )
        
        await message.reply_text(stats_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(
            f"<b>❌ Error getting stats:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML
        )

@app.on_message(filters.command("fakereset") & filters.private)
async def reset_name_pools(client, message):
    """Reset the used name pools (admin only)"""
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        global used_indian_names, used_foreign_names
        used_indian_names.clear()
        used_foreign_names.clear()
        
        await message.reply_text(
            "<b>✅ Name Pools Reset Successfully!</b>\n\n"
            "All names are now available for selection again.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.reply_text(
            f"<b>❌ Error resetting name pools:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML
        )

@app.on_message(filters.command("fakeconfig") & filters.private)
async def fake_marketing_config(client, message):
    """Configure fake marketing intervals (admin only)"""
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        args = message.text.split()[1:]
        
        if len(args) == 0:
            # Show current configuration
            min_hours = marketing_config["min_interval"] / 3600
            max_hours = marketing_config["max_interval"] / 3600
            status = "🟢 Enabled" if marketing_config["enabled"] else "🔴 Disabled"
            
            config_text = (
                "<b>⚙️ Fake Marketing Configuration</b>\n\n"
                f"<b>📊 Current Settings:</b>\n"
                f"<b>•</b> Status: {status}\n"
                f"<b>•</b> Min Interval: <code>{min_hours:.1f} hours ({marketing_config['min_interval']} seconds)</code>\n"
                f"<b>•</b> Max Interval: <code>{max_hours:.1f} hours ({marketing_config['max_interval']} seconds)</code>\n\n"
                "<b>🔧 Commands:</b>\n"
                "<b>•</b> <code>/fakeconfig enable</code> - Enable marketing\n"
                "<b>•</b> <code>/fakeconfig disable</code> - Disable marketing\n"
                "<b>•</b> <code>/fakeconfig interval &lt;min_hours&gt; &lt;max_hours&gt;</code> - Set intervals\n"
                "<b>•</b> <code>/fakeconfig reset</code> - Reset to default values\n\n"
                "<b>📝 Examples:</b>\n"
                "<b>•</b> <code>/fakeconfig interval 1 2</code> - Set 1-2 hours\n"
                "<b>•</b> <code>/fakeconfig interval 0.5 1.5</code> - Set 30min-1.5hrs"
            )
            
            await message.reply_text(config_text, parse_mode=ParseMode.HTML)
            
        elif args[0].lower() == "enable":
            marketing_config["enabled"] = True
            await message.reply_text(
                "<b>✅ Fake Marketing Enabled!</b>\n\n"
                "Notifications will be sent during business hours.",
                parse_mode=ParseMode.HTML
            )
            
        elif args[0].lower() == "disable":
            marketing_config["enabled"] = False
            await message.reply_text(
                "<b>🔴 Fake Marketing Disabled!</b>\n\n"
                "No more notifications will be sent.",
                parse_mode=ParseMode.HTML
            )
            
        elif args[0].lower() == "interval" and len(args) == 3:
            try:
                min_hours = float(args[1])
                max_hours = float(args[2])
                
                if min_hours <= 0 or max_hours <= 0:
                    await message.reply_text(
                        "<b>❌ Error:</b> Hours must be positive numbers!",
                        parse_mode=ParseMode.HTML
                    )
                    return
                    
                if min_hours >= max_hours:
                    await message.reply_text(
                        "<b>❌ Error:</b> Min interval must be less than max interval!",
                        parse_mode=ParseMode.HTML
                    )
                    return
                
                # Convert hours to seconds
                marketing_config["min_interval"] = int(min_hours * 3600)
                marketing_config["max_interval"] = int(max_hours * 3600)
                
                await message.reply_text(
                    f"<b>✅ Intervals Updated Successfully!</b>\n\n"
                    f"<b>📊 New Settings:</b>\n"
                    f"<b>•</b> Min Interval: <code>{min_hours} hours ({marketing_config['min_interval']} seconds)</code>\n"
                    f"<b>•</b> Max Interval: <code>{max_hours} hours ({marketing_config['max_interval']} seconds)</code>\n\n"
                    f"<b>🔄 Changes take effect immediately - no restart required!</b>",
                    parse_mode=ParseMode.HTML
                )
                
            except ValueError:
                await message.reply_text(
                    "<b>❌ Error:</b> Please provide valid numbers for hours!",
                    parse_mode=ParseMode.HTML
                )
                
        elif args[0].lower() == "reset":
            marketing_config["min_interval"] = FAKE_MARKETING_MIN_INTERVAL
            marketing_config["max_interval"] = FAKE_MARKETING_MAX_INTERVAL
            marketing_config["enabled"] = True
            
            min_hours = marketing_config["min_interval"] / 3600
            max_hours = marketing_config["max_interval"] / 3600
            
            await message.reply_text(
                f"<b>🔄 Configuration Reset to Defaults!</b>\n\n"
                f"<b>📊 Default Settings:</b>\n"
                f"<b>•</b> Status: 🟢 Enabled\n"
                f"<b>•</b> Min Interval: <code>{min_hours:.1f} hours</code>\n"
                f"<b>•</b> Max Interval: <code>{max_hours:.1f} hours</code>",
                parse_mode=ParseMode.HTML
            )
            
        else:
            await message.reply_text(
                "<b>❌ Invalid Command Format!</b>\n\n"
                "Use <code>/fakeconfig</code> to see available options.",
                parse_mode=ParseMode.HTML
            )
            
    except Exception as e:
        await message.reply_text(
            f"<b>❌ Error configuring marketing:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML
        )

@app.on_message(filters.command("fakefreq") & filters.private)
async def fake_marketing_frequency(client, message):
    """Quick frequency presets (admin only)"""
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        args = message.text.split()[1:]
        
        if len(args) == 0:
            freq_text = (
                "<b>⚡ Quick Frequency Presets</b>\n\n"
                "<b>🚀 Available Presets:</b>\n"
                "<b>•</b> <code>/fakefreq high</code> - Every 30min-1hr (high activity)\n"
                "<b>•</b> <code>/fakefreq medium</code> - Every 1-2hrs (moderate activity)\n"
                "<b>•</b> <code>/fakefreq low</code> - Every 2-3hrs (low activity)\n"
                "<b>•</b> <code>/fakefreq minimal</code> - Every 4-6hrs (minimal activity)\n\n"
                "<b>📊 Current Settings:</b>\n"
                f"<b>•</b> Min: <code>{marketing_config['min_interval']/3600:.1f}hrs</code>\n"
                f"<b>•</b> Max: <code>{marketing_config['max_interval']/3600:.1f}hrs</code>\n"
                f"<b>•</b> Status: {'🟢 Enabled' if marketing_config['enabled'] else '🔴 Disabled'}"
            )
            await message.reply_text(freq_text, parse_mode=ParseMode.HTML)
            return
        
        preset = args[0].lower()
        
        if preset == "high":
            marketing_config["min_interval"] = 1800  # 30 minutes
            marketing_config["max_interval"] = 3600  # 1 hour
            freq_name = "High Activity"
            
        elif preset == "medium":
            marketing_config["min_interval"] = 3600  # 1 hour
            marketing_config["max_interval"] = 7200  # 2 hours
            freq_name = "Medium Activity"
            
        elif preset == "low":
            marketing_config["min_interval"] = 7200   # 2 hours
            marketing_config["max_interval"] = 10800  # 3 hours
            freq_name = "Low Activity"
            
        elif preset == "minimal":
            marketing_config["min_interval"] = 14400  # 4 hours
            marketing_config["max_interval"] = 21600  # 6 hours
            freq_name = "Minimal Activity"
            
        else:
            await message.reply_text(
                "<b>❌ Invalid preset!</b> Use <code>/fakefreq</code> to see available options.",
                parse_mode=ParseMode.HTML
            )
            return
        
        marketing_config["enabled"] = True
        
        min_hours = marketing_config["min_interval"] / 3600
        max_hours = marketing_config["max_interval"] / 3600
        
        await message.reply_text(
            f"<b>✅ {freq_name} Preset Applied!</b>\n\n"
            f"<b>📊 New Settings:</b>\n"
            f"<b>•</b> Frequency: Every <code>{min_hours:.1f}-{max_hours:.1f} hours</code>\n"
            f"<b>•</b> Status: 🟢 Enabled\n"
            f"<b>•</b> Daily notifications: <code>~{15/(min_hours+max_hours)*2:.0f}-{15/min_hours:.0f} per day</code>\n\n"
            f"<b>🔄 Changes active immediately!</b>",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        await message.reply_text(
            f"<b>❌ Error setting frequency:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML
        )

@app.on_message(filters.command("fakediag") & filters.private)
async def fake_marketing_diagnostics(client, message):
    """Run diagnostics for fake marketing system (admin only)"""
    if message.from_user.id not in OWNER_ID:
        return
    
    try:
        diag_text = "<b>🔍 Fake Marketing System Diagnostics</b>\n\n"
        
        # Check PREMIUM_BROADCAST configuration
        if PREMIUM_BROADCAST:
            diag_text += f"<b>✅ PREMIUM_BROADCAST:</b> <code>{PREMIUM_BROADCAST}</code>\n"
            
            # Try to get channel info
            try:
                chat_info = await app.get_chat(PREMIUM_BROADCAST)
                diag_text += f"<b>✅ Channel Found:</b> <code>{chat_info.title}</code>\n"
                diag_text += f"<b>📊 Channel Type:</b> <code>{chat_info.type}</code>\n"
                diag_text += f"<b>👥 Members:</b> <code>{chat_info.members_count or 'Unknown'}</code>\n"
                
                # Check bot permissions
                try:
                    bot_member = await app.get_chat_member(PREMIUM_BROADCAST, app.me.id)
                    diag_text += f"<b>✅ Bot Status:</b> <code>{bot_member.status}</code>\n"
                    
                    if bot_member.status in ["administrator", "creator"]:
                        diag_text += "<b>✅ Permissions:</b> Bot has admin access\n"
                    else:
                        diag_text += "<b>❌ Permissions:</b> Bot is not admin\n"
                        
                except Exception as perm_error:
                    diag_text += f"<b>❌ Permission Check Failed:</b> <code>{perm_error}</code>\n"
                    
            except Exception as chat_error:
                diag_text += f"<b>❌ Channel Access Failed:</b> <code>{chat_error}</code>\n"
                
        else:
            diag_text += "<b>❌ PREMIUM_BROADCAST:</b> Not configured\n"
        
        # Check marketing configuration
        min_hours = marketing_config["min_interval"] / 3600
        max_hours = marketing_config["max_interval"] / 3600
        status = "🟢 Enabled" if marketing_config["enabled"] else "🔴 Disabled"
        
        diag_text += f"\n<b>⚙️ Marketing Config:</b>\n"
        diag_text += f"<b>•</b> Status: {status}\n"
        diag_text += f"<b>•</b> Interval: <code>{min_hours:.1f}-{max_hours:.1f} hours</code>\n"
        
        # Check business hours
        business_status = "🟢 Active" if is_business_hours() else "🔴 Inactive"
        ist = pytz.timezone("Asia/Kolkata")
        current_time = datetime.datetime.now(ist)
        diag_text += f"<b>•</b> Business Hours: {business_status}\n"
        diag_text += f"<b>•</b> Current Time: <code>{current_time.strftime('%I:%M %p IST')}</code>\n"
        
        # Name pool status
        diag_text += f"\n<b>📝 Name Pools:</b>\n"
        diag_text += f"<b>•</b> Indian names used: <code>{len(used_indian_names)}/{len(INDIAN_NAMES)}</code>\n"
        diag_text += f"<b>•</b> Foreign names used: <code>{len(used_foreign_names)}/{len(FOREIGN_NAMES)}</code>\n"
        
        # Recommendations
        diag_text += f"\n<b>💡 Recommendations:</b>\n"
        if not PREMIUM_BROADCAST:
            diag_text += "<b>•</b> Configure PREMIUM_BROADCAST in .env file\n"
        if PREMIUM_BROADCAST:
            try:
                await app.get_chat(PREMIUM_BROADCAST)
                diag_text += "<b>•</b> Configuration looks good! ✅\n"
            except:
                diag_text += "<b>•</b> Check bot admin permissions in broadcast channel\n"
                diag_text += "<b>•</b> Verify PREMIUM_BROADCAST channel ID is correct\n"
        
        await message.reply_text(diag_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        await message.reply_text(
            f"<b>❌ Error running diagnostics:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML
        )

# Auto-start the marketing system when the module loads
async def auto_start_marketing():
    """Auto-start the marketing system"""
    try:
        print("🚀 Auto-starting Fake Premium Marketing System...")
        asyncio.create_task(marketing_scheduler())
        print("✅ Marketing system started successfully!")
    except Exception as e:
        print(f"❌ Error auto-starting marketing system: {e}")

# Schedule auto-start
asyncio.create_task(auto_start_marketing())
