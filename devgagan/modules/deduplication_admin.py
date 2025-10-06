"""
Deduplication Admin Commands Module
Provides administrative commands to manage and monitor the file deduplication system
"""

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from devgagan import app
from devgagan.core.mongo.file_hash_db import file_hash_manager
from devgagan.core.deduplication import deduplication_manager
from config import OWNER_ID

def is_owner(_, __, message: Message):
    """Filter to check if user is owner"""
    return message.from_user.id in OWNER_ID

owner_filter = filters.create(is_owner)

@app.on_message(filters.command("dedup_stats") & owner_filter)
async def deduplication_stats_command(client: Client, message: Message):
    """Show deduplication statistics"""
    try:
        # Get deduplication manager stats
        dedup_stats = deduplication_manager.get_stats()
        
        # Get file hash database stats
        hash_stats = await file_hash_manager.get_stats()
        
        # Format file sizes
        def format_bytes(bytes_val):
            if bytes_val == 0:
                return "0 B"
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if bytes_val < 1024.0:
                    return f"{bytes_val:.1f} {unit}"
                bytes_val /= 1024.0
            return f"{bytes_val:.1f} PB"
        
        stats_text = f"""📊 **File Deduplication Statistics**

🔄 **Deduplication Status:** {'✅ Enabled' if dedup_stats['enabled'] else '❌ Disabled'}

📈 **Performance Metrics:**
• Duplicates Found: {dedup_stats['duplicates_found']:,}
• Duplicates Forwarded: {dedup_stats['duplicates_forwarded']:,}
• Downloads Saved: {dedup_stats['downloads_saved']:,}
• Bandwidth Saved: {format_bytes(dedup_stats['bytes_saved'])}

💾 **Database Statistics:**
• Total Cached Files: {hash_stats.get('total_files', 0):,}
• Total Cache Size: {format_bytes(hash_stats.get('total_size_bytes', 0))}
• Average File Size: {format_bytes(hash_stats.get('average_size_bytes', 0))}
• Largest Cached File: {format_bytes(hash_stats.get('largest_file_bytes', 0))}
• Files Added (7 days): {hash_stats.get('files_last_7_days', 0):,}

💡 **Efficiency:**
• Cache Hit Rate: {(dedup_stats['duplicates_found'] / max(hash_stats.get('total_files', 1), 1) * 100):.1f}%
• Storage Efficiency: {dedup_stats['gb_saved']:.2f} GB saved"""

        await message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        await message.reply_text(f"❌ Error getting deduplication statistics: {str(e)}")

@app.on_message(filters.command("dedup_enable") & owner_filter)
async def enable_deduplication_command(client: Client, message: Message):
    """Enable file deduplication"""
    try:
        deduplication_manager.enable()
        await message.reply_text("✅ **File deduplication enabled**\n\nNew downloads will be checked for duplicates.")
    except Exception as e:
        await message.reply_text(f"❌ Error enabling deduplication: {str(e)}")

@app.on_message(filters.command("dedup_disable") & owner_filter)
async def disable_deduplication_command(client: Client, message: Message):
    """Disable file deduplication"""
    try:
        deduplication_manager.disable()
        await message.reply_text("❌ **File deduplication disabled**\n\nAll downloads will proceed normally without duplicate checking.")
    except Exception as e:
        await message.reply_text(f"❌ Error disabling deduplication: {str(e)}")

@app.on_message(filters.command("dedup_reset_stats") & owner_filter)
async def reset_deduplication_stats_command(client: Client, message: Message):
    """Reset deduplication statistics"""
    try:
        deduplication_manager.reset_stats()
        await message.reply_text("📊 **Deduplication statistics reset**\n\nAll performance counters have been cleared.")
    except Exception as e:
        await message.reply_text(f"❌ Error resetting statistics: {str(e)}")

@app.on_message(filters.command("dedup_cleanup") & owner_filter)
async def cleanup_old_hashes_command(client: Client, message: Message):
    """Clean up old file hashes"""
    try:
        # Extract days parameter if provided
        args = message.text.split()
        days_old = 90  # Default
        
        if len(args) > 1:
            try:
                days_old = int(args[1])
                if days_old < 1:
                    days_old = 90
            except ValueError:
                await message.reply_text("❌ Invalid number of days. Using default (90 days).")
        
        # Perform cleanup
        deleted_count = await file_hash_manager.cleanup_old_hashes(days_old)
        
        if deleted_count > 0:
            await message.reply_text(
                f"🧹 **Cleanup completed**\n\n"
                f"Removed {deleted_count:,} old file hash entries (older than {days_old} days)."
            )
        else:
            await message.reply_text(f"✅ No old file hashes found (older than {days_old} days).")
            
    except Exception as e:
        await message.reply_text(f"❌ Error during cleanup: {str(e)}")

@app.on_message(filters.command("dedup_search") & owner_filter)
async def search_file_hash_command(client: Client, message: Message):
    """Search for a file by hash (first 16 characters)"""
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text(
                "❌ **Usage:** `/dedup_search <hash_prefix>`\n\n"
                "Provide at least the first 8 characters of a file hash to search."
            )
            return
        
        hash_prefix = args[1].lower()
        if len(hash_prefix) < 8:
            await message.reply_text("❌ Hash prefix must be at least 8 characters long.")
            return
        
        # Search in database (this is a simplified search - in production you'd want proper indexing)
        await file_hash_manager.initialize()
        
        # Find files with matching hash prefix
        cursor = file_hash_manager.collection.find({
            "file_hash": {"$regex": f"^{hash_prefix}", "$options": "i"}
        }).limit(10)
        
        results = await cursor.to_list(10)
        
        if not results:
            await message.reply_text(f"🔍 No files found with hash prefix: `{hash_prefix}`")
            return
        
        response = f"🔍 **Search Results for:** `{hash_prefix}`\n\n"
        
        for i, result in enumerate(results, 1):
            file_name = result.get('file_name', 'Unknown')
            file_size = result.get('file_size', 0)
            log_msg_id = result.get('log_group_message_id', 'Unknown')
            created_by = result.get('created_by_user', 'Unknown')
            
            # Format file size
            if file_size > 1024*1024*1024:
                size_str = f"{file_size/(1024*1024*1024):.1f} GB"
            elif file_size > 1024*1024:
                size_str = f"{file_size/(1024*1024):.1f} MB"
            elif file_size > 1024:
                size_str = f"{file_size/1024:.1f} KB"
            else:
                size_str = f"{file_size} B"
            
            response += f"**{i}.** `{file_name}`\n"
            response += f"   📏 Size: {size_str}\n"
            response += f"   📨 LOG_GROUP ID: {log_msg_id}\n"
            response += f"   👤 User: {created_by}\n\n"
        
        if len(results) == 10:
            response += "📝 *Showing first 10 results*"
        
        await message.reply_text(response, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        await message.reply_text(f"❌ Error searching files: {str(e)}")

@app.on_message(filters.command("dedup_help") & owner_filter)
async def deduplication_help_command(client: Client, message: Message):
    """Show deduplication help"""
    help_text = """🔄 **File Deduplication Commands**

**Statistics & Monitoring:**
• `/dedup_stats` - Show detailed deduplication statistics
• `/dedup_search <hash>` - Search for files by hash prefix

**Management:**
• `/dedup_enable` - Enable file deduplication
• `/dedup_disable` - Disable file deduplication
• `/dedup_reset_stats` - Reset performance statistics

**Maintenance:**
• `/dedup_cleanup [days]` - Clean old hashes (default: 90 days)
• `/dedup_help` - Show this help message

**How it works:**
1. Before downloading, the system checks if the file already exists
2. If found, it forwards the existing file from LOG_GROUP
3. After successful downloads, file hashes are stored for future reference
4. This saves bandwidth, storage, and processing time

**Benefits:**
• Faster file delivery for duplicate requests
• Reduced server load and bandwidth usage
• Automatic cache management with TTL cleanup"""

    await message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

print("✅ Deduplication admin commands loaded")
