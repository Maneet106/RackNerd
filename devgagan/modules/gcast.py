import asyncio
import os
import traceback
from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid, 
    UserIsBot, ChatWriteForbidden, UserDeactivated, UserNotMutualContact,
    BotMethodInvalid, ChatAdminRequired, MessageIdInvalid
)
from config import OWNER_ID
from devgagan import app
from devgagan.core.mongo.users_db import get_users

async def send_msg(user_id, message):
    try:
        x = await message.copy(chat_id=user_id)
        try:
            await x.pin()
        except Exception:
            await x.pin(both_sides=True)
    except FloodWait as e:
        await asyncio.sleep(e.x)
        return send_msg(user_id, message)
    except InputUserDeactivated:
        return 400, f"{user_id} : deactivated\n"
    except UserIsBlocked:
        return 400, f"{user_id} : blocked the bot\n"
    except PeerIdInvalid:
        return 400, f"{user_id} : user id invalid\n"
    except Exception:
        return 500, f"{user_id} : {traceback.format_exc()}\n"


@app.on_message(filters.command("gcast"))
async def broadcast(client, message):
    """
    Enhanced global broadcast with comprehensive error handling and beautiful UI
    """
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
        
    if not message.reply_to_message:
        return await message.reply_text(
            "🚫 <b>No Message to Broadcast</b>\n\n"
            "📝 <b>Usage:</b> Reply to any message and use <code>/gcast</code> to copy it to all users.\n\n"
            "💡 <b>Tip:</b> This will copy the replied message to all bot users with pinning.",
            parse_mode=ParseMode.HTML
        )
    
    try:
        # Get user list
        all_users = (await get_users()) or []
        if not all_users:
            return await message.reply_text(
                "📭 <b>Empty User Database</b>\n\n"
                "❌ <b>Error:</b> No users found in the database\n"
                "🔄 <b>Solution:</b> Users need to start the bot first",
                parse_mode=ParseMode.HTML
            )
        
        total_users = len(all_users)
        
        print(f"📢 Starting global broadcast to {total_users} users")
        
        # Initialize progress message
        progress_msg = await message.reply_text(
            f"🌐 <b>Global Broadcast System</b>\n\n"
            f"📊 <b>Broadcast Analytics:</b>\n"
            f"👥 <b>Target Users:</b> <code>{total_users:,}</code>\n"
            f"📡 <b>Status:</b> <i>Initializing broadcast engine...</i>\n"
            f"⚡ <b>Mode:</b> Copy & Pin Broadcasting\n\n"
            f"🔄 <i>Please wait while we process your message...</i>",
            parse_mode=ParseMode.HTML
        )
        
        # Statistics tracking
        stats = {
            'success': 0,
            'failed': 0,
            'deactivated': 0,
            'blocked': 0,
            'invalid_peers': 0,
            'flood_waits': 0,
            'other_errors': 0
        }
        
        # Process users in batches
        batch_size = 50
        processed = 0
        
        delay_s = float(os.getenv("GCAST_DELAY_SECONDS", "0.4"))
        for i in range(0, total_users, batch_size):
            batch = all_users[i:i + batch_size]
            
            for user_id in batch:
                try:
                    # Validate user ID
                    if not user_id or not str(user_id).isdigit():
                        stats['invalid_peers'] += 1
                        stats['failed'] += 1
                        continue
                    
                    result = await send_msg(user_id, message.reply_to_message)
                    
                    if result is None:
                        stats['success'] += 1
                    else:
                        stats['failed'] += 1
                        error_code, error_msg = result
                        
                        # Categorize errors
                        if "deactivated" in error_msg.lower():
                            stats['deactivated'] += 1
                        elif "blocked" in error_msg.lower():
                            stats['blocked'] += 1
                        elif "invalid" in error_msg.lower():
                            stats['invalid_peers'] += 1
                        elif "flood" in error_msg.lower():
                            stats['flood_waits'] += 1
                        else:
                            stats['other_errors'] += 1
                    
                    processed += 1
                    
                    # Update progress every 10 users
                    if processed % 10 == 0:
                        try:
                            progress_percentage = processed/total_users*100
                            progress_bar = "█" * int(progress_percentage // 10) + "░" * (10 - int(progress_percentage // 10))
                            
                            await progress_msg.edit_text(
                                f"🌐 <b>Global Broadcasting in Progress...</b>\n\n"
                                f"📊 <b>Live Statistics:</b>\n"
                                f"🎯 <b>Progress:</b> <code>{processed:,}/{total_users:,}</code> "
                                f"(<code>{progress_percentage:.1f}%</code>)\n"
                                f"📈 <b>Progress Bar:</b> <code>[{progress_bar}]</code>\n\n"
                                f"✅ <b>Successfully Copied:</b> <code>{stats['success']:,}</code>\n"
                                f"❌ <b>Failed:</b> <code>{stats['failed']:,}</code>\n"
                                f"🚫 <b>Blocked Users:</b> <code>{stats['blocked']:,}</code>\n\n"
                                f"⚡ <i>High-speed copy & pin processing...</i>",
                                parse_mode=ParseMode.HTML
                            )
                        except:
                            pass  # Ignore edit errors
                    
                    # Small delay to prevent overwhelming
                    await asyncio.sleep(delay_s)
                    
                except Exception as e:
                    # Catch any unexpected errors to prevent crash
                    stats['failed'] += 1
                    stats['other_errors'] += 1
                    print(f"🚨 Unexpected error processing user {user_id}: {str(e)[:100]}")
                    continue
        
        # Final results
        success_rate = (stats['success'] / total_users * 100) if total_users > 0 else 0
        
        # Create success rate emoji and color
        if success_rate >= 90:
            rate_emoji = "🟢"
            rate_status = "Excellent"
        elif success_rate >= 70:
            rate_emoji = "🟡"
            rate_status = "Good"
        elif success_rate >= 50:
            rate_emoji = "🟠"
            rate_status = "Average"
        else:
            rate_emoji = "🔴"
            rate_status = "Needs Attention"
        
        # Create visual progress bar for final stats
        final_progress_bar = "█" * 10
        
        final_message = (
            f"🎉 <b>Global Broadcast Completed!</b> ✨\n\n"
            f"┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n"
            f"┃  🌐 <b>GLOBAL BROADCAST REPORT</b>    ┃\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n\n"
            f"📈 <b>Performance Overview:</b>\n"
            f"👥 <b>Total Recipients:</b> <code>{total_users:,}</code>\n"
            f"✅ <b>Successfully Copied:</b> <code>{stats['success']:,}</code>\n"
            f"❌ <b>Copy Failed:</b> <code>{stats['failed']:,}</code>\n"
            f"{rate_emoji} <b>Success Rate:</b> <code>{success_rate:.1f}%</code> <i>({rate_status})</i>\n\n"
            f"📊 <b>Progress Visualization:</b>\n"
            f"<code>[{final_progress_bar}] 100% Complete</code>\n\n"
            f"🔍 <b>Detailed Error Analysis:</b>\n"
            f"├ 🚫 <b>Blocked Users:</b> <code>{stats['blocked']:,}</code>\n"
            f"├ 💀 <b>Deactivated Accounts:</b> <code>{stats['deactivated']:,}</code>\n"
            f"├ 🔗 <b>Invalid User IDs:</b> <code>{stats['invalid_peers']:,}</code>\n"
            f"├ ⏳ <b>Rate Limit Delays:</b> <code>{stats['flood_waits']:,}</code>\n"
            f"└ ❓ <b>Unknown Errors:</b> <code>{stats['other_errors']:,}</code>\n\n"
            f"💡 <b>System Status:</b> <i>All copy operations completed successfully</i>\n"
            f"📌 <b>Pin Status:</b> <i>Messages pinned where possible</i>"
        )
        
        await progress_msg.edit_text(final_message, parse_mode=ParseMode.HTML)
        print(f"📢 Global broadcast completed: {stats['success']}/{total_users} successful")
        
    except Exception as e:
        # Ultimate fallback to prevent bot crash
        error_msg = (
            f"🚨 <b>Global Broadcast System Critical Error</b>\n\n"
            f"❌ <b>Error Details:</b>\n"
            f"<code>{str(e)[:200]}...</code>\n\n"
            f"🔧 <b>Recommended Actions:</b>\n"
            f"• Check user database integrity\n"
            f"• Verify bot permissions\n"
            f"• Try again in a few minutes\n"
            f"• Contact system administrator if issue persists\n\n"
            f"📞 <b>Support:</b> <i>Bot is still operational for other functions</i>"
        )
        
        try:
            await message.reply_text(error_msg, parse_mode=ParseMode.HTML)
        except:
            pass  # If even error message fails, just log
            
        print(f"🚨 Critical global broadcast error: {str(e)}")
        print(f"🔍 Traceback: {traceback.format_exc()}")





async def safe_forward_message(client, user_id, from_chat_id, message_id):
    """
    Safely forward a message with comprehensive error handling
    Returns: (success: bool, error_type: str, error_msg: str)
    """
    try:
        await client.forward_messages(
            chat_id=int(user_id), 
            from_chat_id=from_chat_id, 
            message_ids=message_id
        )
        return True, None, None
        
    except UserIsBot:
        return False, "bot", f"User {user_id} is a bot - cannot send to bots"
    except PeerIdInvalid:
        return False, "invalid_peer", f"Invalid peer ID: {user_id}"
    except UserIsBlocked:
        return False, "blocked", f"User {user_id} blocked the bot"
    except InputUserDeactivated:
        return False, "deactivated", f"User {user_id} account deactivated"
    except UserDeactivated:
        return False, "deactivated", f"User {user_id} account deactivated"
    except ChatWriteForbidden:
        return False, "forbidden", f"Cannot write to user {user_id}"
    except UserNotMutualContact:
        return False, "not_mutual", f"User {user_id} not mutual contact"
    except BotMethodInvalid:
        return False, "method_invalid", f"Bot method invalid for user {user_id}"
    except ChatAdminRequired:
        return False, "admin_required", f"Admin required for user {user_id}"
    except MessageIdInvalid:
        return False, "invalid_message", f"Message ID invalid for user {user_id}"
    except FloodWait as e:
        return False, "flood_wait", f"FloodWait: {e.x} seconds"
    except Exception as e:
        return False, "unknown", f"Unknown error for user {user_id}: {str(e)[:100]}"


@app.on_message(filters.command("acast"))
async def announced(client, message):
    """
    Enhanced announcement broadcast with comprehensive error handling
    """
    # Silent admin check - no response for non-admins
    if message.from_user.id not in OWNER_ID:
        return
        
    if not message.reply_to_message:
        return await message.reply_text(
            "🚫 <b>No Message to Broadcast</b>\n\n"
            "📝 <b>Usage:</b> Reply to any message and use <code>/acast</code> to forward it to all users.\n\n"
            "💡 <b>Tip:</b> This will send the replied message to all bot users as an announcement.",
            parse_mode=ParseMode.HTML
        )
    
    try:
        # Get user list
        users = await get_users() or []
        if not users:
            return await message.reply_text(
                "📭 <b>Empty User Database</b>\n\n"
                "❌ <b>Error:</b> No users found in the database\n"
                "🔄 <b>Solution:</b> Users need to start the bot first",
                parse_mode=ParseMode.HTML
            )
        
        to_send = message.reply_to_message.id
        total_users = len(users)
        
        print(f"📢 Starting broadcast to {total_users} users")
        
        # Initialize progress message
        progress_msg = await message.reply_text(
            f"🚀 <b>Announcement Broadcast System</b>\n\n"
            f"📊 <b>Broadcast Analytics:</b>\n"
            f"👥 <b>Target Users:</b> <code>{total_users:,}</code>\n"
            f"📡 <b>Status:</b> <i>Initializing broadcast engine...</i>\n"
            f"⚡ <b>Mode:</b> High-Performance Forwarding\n\n"
            f"🔄 <i>Please wait while we process your announcement...</i>",
            parse_mode=ParseMode.HTML
        )
        
        # Statistics tracking
        stats = {
            'success': 0,
            'failed': 0,
            'bots': 0,
            'invalid_peers': 0,
            'blocked': 0,
            'deactivated': 0,
            'flood_waits': 0,
            'other_errors': 0
        }
        
        # Process users in batches to avoid overwhelming
        batch_size = 50
        processed = 0
        
        delay_s = float(os.getenv("GCAST_DELAY_SECONDS", "0.4"))
        for i in range(0, total_users, batch_size):
            batch = users[i:i + batch_size]
            
            for user_id in batch:
                try:
                    # Validate user ID
                    if not user_id or not str(user_id).isdigit():
                        stats['invalid_peers'] += 1
                        continue
                    
                    success, error_type, error_msg = await safe_forward_message(
                        client, user_id, message.chat.id, to_send
                    )
                    
                    if success:
                        stats['success'] += 1
                    else:
                        stats['failed'] += 1
                        
                        # Categorize errors for statistics
                        if error_type == "bot":
                            stats['bots'] += 1
                        elif error_type == "invalid_peer":
                            stats['invalid_peers'] += 1
                        elif error_type == "blocked":
                            stats['blocked'] += 1
                        elif error_type in ["deactivated"]:
                            stats['deactivated'] += 1
                        elif error_type == "flood_wait":
                            stats['flood_waits'] += 1
                            # Extract wait time and sleep
                            try:
                                wait_time = int(error_msg.split(": ")[1].split(" ")[0])
                                print(f"⏳ FloodWait: sleeping for {wait_time} seconds")
                                await asyncio.sleep(wait_time)
                                # Retry after flood wait
                                retry_success, _, _ = await safe_forward_message(
                                    client, user_id, message.chat.id, to_send
                                )
                                if retry_success:
                                    stats['success'] += 1
                                    stats['failed'] -= 1
                            except:
                                pass
                        else:
                            stats['other_errors'] += 1
                        
                        # Log specific errors (only print, don't crash)
                        if error_type not in ["bot", "invalid_peer", "blocked", "deactivated"]:
                            print(f"⚠️ Broadcast error: {error_msg}")
                    
                    processed += 1
                    
                    # Update progress every 10 users
                    if processed % 10 == 0:
                        try:
                            progress_percentage = processed/total_users*100
                            progress_bar = "█" * int(progress_percentage // 10) + "░" * (10 - int(progress_percentage // 10))
                            
                            await progress_msg.edit_text(
                                f"📡 <b>Broadcasting in Progress...</b>\n\n"
                                f"📊 <b>Live Statistics:</b>\n"
                                f"🎯 <b>Progress:</b> <code>{processed:,}/{total_users:,}</code> "
                                f"(<code>{progress_percentage:.1f}%</code>)\n"
                                f"📈 <b>Progress Bar:</b> <code>[{progress_bar}]</code>\n\n"
                                f"✅ <b>Delivered:</b> <code>{stats['success']:,}</code>\n"
                                f"❌ <b>Failed:</b> <code>{stats['failed']:,}</code>\n"
                                f"🤖 <b>Bots Detected:</b> <code>{stats['bots']:,}</code>\n\n"
                                f"⚡ <i>High-speed processing active...</i>",
                                parse_mode=ParseMode.HTML
                            )
                        except:
                            pass  # Ignore edit errors
                    
                    # Small delay to prevent overwhelming
                    await asyncio.sleep(delay_s)
                    
                except Exception as e:
                    # Catch any unexpected errors to prevent crash
                    stats['failed'] += 1
                    stats['other_errors'] += 1
                    print(f"🚨 Unexpected error processing user {user_id}: {str(e)[:100]}")
                    continue
        
        # Final results
        success_rate = (stats['success'] / total_users * 100) if total_users > 0 else 0
        
        # Create success rate emoji and color
        if success_rate >= 90:
            rate_emoji = "🟢"
            rate_status = "Excellent"
        elif success_rate >= 70:
            rate_emoji = "🟡"
            rate_status = "Good"
        elif success_rate >= 50:
            rate_emoji = "🟠"
            rate_status = "Average"
        else:
            rate_emoji = "🔴"
            rate_status = "Needs Attention"
        
        # Create visual progress bar for final stats
        final_progress_bar = "█" * 10
        
        final_message = (
            f"🎉 <b>Announcement Broadcast Completed!</b> ✨\n\n"
            f"┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n"
            f"┃  📊 <b>BROADCAST ANALYTICS REPORT</b>  ┃\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n\n"
            f"📈 <b>Performance Overview:</b>\n"
            f"👥 <b>Total Recipients:</b> <code>{total_users:,}</code>\n"
            f"✅ <b>Successfully Delivered:</b> <code>{stats['success']:,}</code>\n"
            f"❌ <b>Delivery Failed:</b> <code>{stats['failed']:,}</code>\n"
            f"{rate_emoji} <b>Success Rate:</b> <code>{success_rate:.1f}%</code> <i>({rate_status})</i>\n\n"
            f"📊 <b>Progress Visualization:</b>\n"
            f"<code>[{final_progress_bar}] 100% Complete</code>\n\n"
            f"🔍 <b>Detailed Error Analysis:</b>\n"
            f"├ 🤖 <b>Bot Accounts:</b> <code>{stats['bots']:,}</code>\n"
            f"├ 🚫 <b>Blocked Users:</b> <code>{stats['blocked']:,}</code>\n"
            f"├ 💀 <b>Deactivated Accounts:</b> <code>{stats['deactivated']:,}</code>\n"
            f"├ 🔗 <b>Invalid User IDs:</b> <code>{stats['invalid_peers']:,}</code>\n"
            f"├ ⏳ <b>Rate Limit Delays:</b> <code>{stats['flood_waits']:,}</code>\n"
            f"└ ❓ <b>Unknown Errors:</b> <code>{stats['other_errors']:,}</code>\n\n"
            f"💡 <b>System Status:</b> <i>All operations completed successfully</i>\n"
            f"🕐 <b>Broadcast Duration:</b> <i>Processing completed</i>"
        )
        
        await progress_msg.edit_text(final_message, parse_mode=ParseMode.HTML)
        print(f"📢 Broadcast completed: {stats['success']}/{total_users} successful")
        
    except Exception as e:
        # Ultimate fallback to prevent bot crash
        error_msg = (
            f"🚨 <b>Broadcast System Critical Error</b>\n\n"
            f"❌ <b>Error Details:</b>\n"
            f"<code>{str(e)[:200]}...</code>\n\n"
            f"🔧 <b>Recommended Actions:</b>\n"
            f"• Check user database integrity\n"
            f"• Verify bot permissions\n"
            f"• Try again in a few minutes\n"
            f"• Contact system administrator if issue persists\n\n"
            f"📞 <b>Support:</b> <i>Bot is still operational for other functions</i>"
        )
        
        try:
            await message.reply_text(error_msg, parse_mode=ParseMode.HTML)
        except:
            pass  # If even error message fails, just log
            
        print(f"🚨 Critical broadcast error: {str(e)}")
        print(f"🔍 Traceback: {traceback.format_exc()}")




