from datetime import timedelta
import pytz
import datetime, time
from devgagan import app
import asyncio
from config import OWNER_ID, PREMIUM_BROADCAST
from devgagan.core.func import get_seconds
from devgagan.core.mongo import plans_db  
from pyrogram import filters 
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton



@app.on_message(filters.command("rem"))
async def remove_premium(client, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    if len(message.command) == 2:
        user_id = int(message.command[1])  
        user = await client.get_users(user_id)
        data = await plans_db.check_premium(user_id)  
        
        if data and data.get("_id"):
            await plans_db.remove_premium(user_id)
            await message.reply_text("ᴜꜱᴇʀ ʀᴇᴍᴏᴠᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ !")
            await client.send_message(
                chat_id=user_id,
                text=f"<b>ʜᴇʏ {user.mention},\n\nʏᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʜᴀs ʙᴇᴇɴ ʀᴇᴍᴏᴠᴇᴅ.\nᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴜsɪɴɢ ᴏᴜʀ sᴇʀᴠɪᴄᴇ 😊.</b>"
            )
        else:
            await message.reply_text("ᴜɴᴀʙʟᴇ ᴛᴏ ʀᴇᴍᴏᴠᴇ ᴜꜱᴇᴅ !\nᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ, ɪᴛ ᴡᴀꜷ ᴀ ᴘʀᴇᴍɪᴜᴍ ᴜꜱᴇʀ ɪᴅ ?")
    else:
        await message.reply_text("ᴜꜱᴀɢᴇ : /rem user_id") 



@app.on_message(filters.command("myplan"))
async def myplan(client, message):
    user_id = message.from_user.id
    user = message.from_user.mention
    data = await plans_db.check_premium(user_id)  
    if data and data.get("expire_date"):
        expiry = data.get("expire_date")
        expiry_ist = expiry.astimezone(pytz.timezone("Asia/Kolkata"))
        expiry_str_in_ist = expiry.astimezone(pytz.timezone("Asia/Kolkata")).strftime("%d-%m-%Y\n⏱️ ᴇxᴘɪʀʏ ᴛɪᴍᴇ : %I:%M:%S %p")            
        
        current_time = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
        time_left = expiry_ist - current_time
            
        
        days = time_left.days
        hours, remainder = divmod(time_left.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
            
        
        time_left_str = f"{days} ᴅᴀʏꜱ, {hours} ʜᴏᴜʀꜱ, {minutes} ᴍɪɴᴜᴛᴇꜱ"
        await message.reply_text(f"⚜️ ᴘʀᴇᴍɪᴜᴍ ᴜꜱᴇʀ ᴅᴀᴛᴀ :\n\n👤 ᴜꜱᴇʀ : {user}\n⚡ ᴜꜱᴇʀ ɪᴅ : <code>{user_id}</code>\n⏰ ᴛɪᴍᴇ ʟᴇꜰᴛ : {time_left_str}\n⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str_in_ist}")   
    else:
        await message.reply_text(f"ʜᴇʏ {user},\n\nʏᴏᴜ ᴅᴏ ɴᴏᴛ ʜᴀᴠᴇ ᴀɴʏ ᴀᴄᴛɪᴠᴇ ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴs")
        


@app.on_message(filters.command("check"))
async def get_premium(client, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    if len(message.command) == 2:
        user_id = int(message.command[1])
        user = await client.get_users(user_id)
        data = await plans_db.check_premium(user_id)  
        if data and data.get("expire_date"):
            expiry = data.get("expire_date") 
            expiry_ist = expiry.astimezone(pytz.timezone("Asia/Kolkata"))
            expiry_str_in_ist = expiry.astimezone(pytz.timezone("Asia/Kolkata")).strftime("%d-%m-%Y\n⏱️ ᴇxᴘɪʀʏ ᴛɪᴍᴇ : %I:%M:%S %p")            
            
            current_time = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
            time_left = expiry_ist - current_time
            
            
            days = time_left.days
            hours, remainder = divmod(time_left.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            
            time_left_str = f"{days} days, {hours} hours, {minutes} minutes"
            await message.reply_text(f"⚜️ ᴘʀᴇᴍɪᴜᴍ ᴜꜱᴇʀ ᴅᴀᴛᴀ :\n\n👤 ᴜꜱᴇʀ : {user.mention}\n⚡ ᴜꜱᴇʀ ɪᴅ : <code>{user_id}</code>\n⏰ ᴛɪᴍᴇ ʟᴇꜰᴛ : {time_left_str}\n⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str_in_ist}")
        else:
            await message.reply_text("ɴᴏ ᴀɴʏ ᴘʀᴇᴍɪᴜᴍ ᴅᴀᴛᴀ ᴏꜰ ᴛʜᴇ ᴡᴀꜷ ꜰᴏᴜɴᴅ ɪɴ ᴅᴀᴛᴀʙᴀꜱᴇ !")
    else:
        await message.reply_text("ᴜꜱᴀɢᴇ : /check user_id")


@app.on_message(filters.command("add"))
async def give_premium_cmd_handler(client, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    if len(message.command) == 4:
        time_zone = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
        current_time = time_zone.strftime("%d-%m-%Y\n⏱️ ᴊᴏɪɴɪɴɢ ᴛɪᴍᴇ : %I:%M:%S %p") 
        user_id = int(message.command[1])
        user = await client.get_users(user_id)
        time = message.command[2]+" "+message.command[3]
        seconds = await get_seconds(time)
        if seconds > 0:
            expiry_time = datetime.datetime.now() + datetime.timedelta(seconds=seconds)  
            await plans_db.add_premium(user_id, expiry_time)  
            data = await plans_db.check_premium(user_id)
            expiry = data.get("expire_date")   
            expiry_str_in_ist = expiry.astimezone(pytz.timezone("Asia/Kolkata")).strftime("%d-%m-%Y\n⏱️ ᴇxᴘɪʀʏ ᴛɪᴍᴇ : %I:%M:%S %p")         
            await message.reply_text(f"ᴘʀᴇᴍɪᴜᴍ ᴀᴅᴅᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ✅\n\n👤 ᴜꜱᴇʀ : {user.mention}\n⚡ ᴜꜱᴇʀ ɪᴅ : <code>{user_id}</code>\n⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : <code>{time}</code>\n\n⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {current_time}\n\n⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str_in_ist} \n\n<u><b>Powered by Restrict Bot Saver</b></u>", disable_web_page_preview=True)
            await client.send_message(
                chat_id=user_id,
                text=f"👋 ʜᴇʏ {user.mention},\nᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴘᴜʀᴄʜᴀꜱɪɴɢ ᴘʀᴇᴍɪᴜᴍ.\nᴇɴᴊᴏʏ !! ✨🎉\n\n⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : <code>{time}</code>\n⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {current_time}\n\n⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str_in_ist}", disable_web_page_preview=True              
            )

            # Broadcast to group: same format as DM, but plain name (no link) and no user ID
            try:
                safe_name = (user.first_name or "User").strip()

                # Make broadcast identical to the DM sent to user, but without hyperlink on name and without showing raw user ID
                broadcast_text = (
                    f"👋 ʜᴇʏ {safe_name},\n"
                    "ᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴘᴜʀᴄʜᴀꜱɪɴɢ ᴘʀᴇᴍɪᴜᴍ.\nᴇɴᴊᴏʏ !! ✨🎉\n\n"
                    f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : <code>{time}</code>\n"
                    f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {current_time}\n\n"
                    f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str_in_ist}"
                )

                # Inline button to open upgrade on the helper bot
                upgrade_btn = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🚀 Upgrade Now", url="https://t.me/RestrictedSaverxBot?start=upgrade")]]
                )

                if PREMIUM_BROADCAST:
                    await client.send_message(
                        chat_id=PREMIUM_BROADCAST,
                        text=broadcast_text,
                        reply_markup=upgrade_btn,
                        disable_web_page_preview=True
                    )
            except Exception:
                # Silently ignore broadcast errors
                pass
                    
        else:
            await message.reply_text("Invalid time format. Please use '1 day for days', '1 hour for hours', or '1 min for minutes', or '1 month for months' or '1 year for year'")
    else:
        await message.reply_text("Usage : /add user_id time (e.g., '1 day for days', '1 hour for hours', or '1 min for minutes', or '1 month for months' or '1 year for year')")


@app.on_message(filters.command("transfer"))
async def transfer_premium(client, message):
    if len(message.command) == 2:
        new_user_id = int(message.command[1])  # The user ID to whom premium is transferred
        sender_user_id = message.from_user.id  # The current premium user issuing the command
        sender_user = await client.get_users(sender_user_id)
        new_user = await client.get_users(new_user_id)
        
        # Fetch sender's premium plan details
        data = await plans_db.check_premium(sender_user_id)
        
        if data and data.get("_id"):  # Verify sender is already a premium user
            expiry = data.get("expire_date")  
            
            # Remove premium for the sender
            await plans_db.remove_premium(sender_user_id)
            
            # Add premium for the new user with the same expiry date
            await plans_db.add_premium(new_user_id, expiry)
            
            # Convert expiry date to IST format for display
            expiry_str_in_ist = expiry.astimezone(pytz.timezone("Asia/Kolkata")).strftime(
                "%d-%m-%Y\n⏱️ <b>Expiry Time:</b> %I:%M:%S %p"
            )
            time_zone = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
            current_time = time_zone.strftime("%d-%m-%Y\n⏱️ <b>Transfer Time:</b> %I:%M:%S %p")
            
            # Confirmation message to the sender
            await message.reply_text(
                f"✅ <b>Premium Plan Transferred Successfully!</b>\n\n"
                f"👤 <b>From:</b> {sender_user.mention}\n"
                f"👤 <b>To:</b> {new_user.mention}\n"
                f"⏳ <b>Expiry Date:</b> {expiry_str_in_ist}\n\n"
                f"<u>Powered by Restrict Bot Saver</u> 🚀"
            )
            
            # Notification to the new user
            await client.send_message(
                chat_id=new_user_id,
                text=(
                    f"👋 <b>Hey {new_user.mention},</b>\n\n"
                    f"🎉 <b>Your Premium Plan has been Transferred!</b>\n"
                    f"🛡️ <b>Transferred From:</b> {sender_user.mention}\n\n"
                    f"⏳ <b>Expiry Date:</b> {expiry_str_in_ist}\n"
                    f"📅 <b>Transferred On:</b> {current_time}\n\n"
                    f"<u>Enjoy the Service!</u> ✨"
                )
            )
        else:
            await message.reply_text("⚠️ <b>You are not a Premium user!</b>\n\nOnly Premium users can transfer their plans.")
    else:
        await message.reply_text("⚠️ <b>Usage:</b> /transfer user_id\n\nReplace <code>user_id</code> with the new user's ID.")


async def premium_remover():
    all_users = await plans_db.premium_users()
    removed_users = []
    not_removed_users = []

    for user_id in all_users:
        try:
            user = await app.get_users(user_id)
            chk_time = await plans_db.check_premium(user_id)

            if chk_time and chk_time.get("expire_date"):
                expiry_date = chk_time["expire_date"]

                if expiry_date <= datetime.datetime.now():
                    name = user.first_name
                    await plans_db.remove_premium(user_id)
                    await app.send_message(user_id, text=f"Hello {name}, your premium subscription has expired.")
                    print(f"{name}, your premium subscription has expired.")
                    removed_users.append(f"{name} ({user_id})")
                else:
                    name = user.first_name
                    current_time = datetime.datetime.now()
                    time_left = expiry_date - current_time

                    days = time_left.days
                    hours, remainder = divmod(time_left.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)

                    if days > 0:
                        remaining_time = f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
                    elif hours > 0:
                        remaining_time = f"{hours} hours, {minutes} minutes, {seconds} seconds"
                    elif minutes > 0:
                        remaining_time = f"{minutes} minutes, {seconds} seconds"
                    else:
                        remaining_time = f"{seconds} seconds"

                    print(f"{name} : Remaining Time : {remaining_time}")
                    not_removed_users.append(f"{name} ({user_id})")
        except:
            await plans_db.remove_premium(user_id)
            print(f"Unknown users captured : {user_id} removed")
            removed_users.append(f"Unknown ({user_id})")

    return removed_users, not_removed_users


@app.on_message(filters.command("freez"))
async def refresh_users(_, message):
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
    removed_users, not_removed_users = await premium_remover()
    # Create a summary message
    removed_text = "\n".join(removed_users) if removed_users else "No users removed."
    not_removed_text = "\n".join(not_removed_users) if not_removed_users else "No users remaining with premium."
    summary = (
        f"<b>Here is Summary...</b>\n\n"
        f"> <b>Removed Users:</b>\n{removed_text}\n\n"
        f"> <b>Not Removed Users:</b>\n{not_removed_text}"
    )
    await message.reply(summary)
    
