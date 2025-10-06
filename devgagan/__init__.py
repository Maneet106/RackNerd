import asyncio
import logging
import time
from pyrogram import Client
from pyrogram.types import Message as PyroMessage
from telethon.tl.custom.message import Message as TLMessage
from pyrogram.enums import ParseMode 
from pyrogram.errors import MessageNotModified
from config import API_ID, API_HASH, BOT_TOKEN, STRING, MONGO_DB, DEFAULT_SESSION
from telethon.sync import TelegramClient
from telethon.errors.rpcerrorlist import FloodWaitError
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram.session import Session
from pyrogram.storage import MemoryStorage
import os
from devgagan.core.download_queue import download_queue

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

logging.basicConfig(
    format="[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s",
    level=logging.INFO,
)
# Reduce Pyrogram verbosity
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection").setLevel(logging.ERROR)
# Reduce asyncio transport noise (e.g., "socket.send() raised exception")
logging.getLogger("asyncio").setLevel(logging.ERROR)
# Reduce Telethon verbosity
logging.getLogger("telethon").setLevel(logging.WARNING)

botStartTime = time.time()

# Helper to apply Pyrogram no-preview patches on any Client instance
def _apply_pyrogram_no_preview_patches(pyro_client: Client):
    _orig_send = pyro_client.send_message
    async def _send_no_preview(chat_id, text, *args, **kwargs):
        try:
            kwargs.setdefault("disable_web_page_preview", True)
        except Exception:
            kwargs = {"disable_web_page_preview": True}
        return await _orig_send(chat_id, text, *args, **kwargs)
    pyro_client.send_message = _send_no_preview  # type: ignore

    # Patch client-level edit_message_text
    _orig_edit_msg_text = pyro_client.edit_message_text
    async def _edit_text_no_preview_client(chat_id, message_id, text, *args, **kwargs):
        try:
            kwargs.setdefault("disable_web_page_preview", True)
        except Exception:
            kwargs = {"disable_web_page_preview": True}
        try:
            return await _orig_edit_msg_text(chat_id, message_id, text, *args, **kwargs)
        except MessageNotModified:
            # Swallow harmless error when content is identical
            return None
    pyro_client.edit_message_text = _edit_text_no_preview_client  # type: ignore


app = Client(
    "pyrobot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=10,  # Reduced from 50 to prevent connection overload
    parse_mode=ParseMode.HTML,
    # Allow tuning via env; default a bit higher to avoid throttling a single pipeline
    max_concurrent_transmissions=int(os.getenv("BOT_MAX_CONCURRENT_TX", "4")),
    sleep_threshold=60,  # Better flood wait handling
    no_updates=False,  # Ensure updates are handled properly
    in_memory=True  # Use in-memory storage to avoid SQLite closed DB issues
)

# --- Global setting: disable link previews by default (Pyrogram) ---
# Apply to primary bot client
_apply_pyrogram_no_preview_patches(app)

# Patch Message.edit_text as well so that edits don't create previews (object-level)
_original_edit_text = PyroMessage.edit_text

async def _edit_text_no_preview(self, text, *args, **kwargs):
    try:
        kwargs.setdefault("disable_web_page_preview", True)
    except Exception:
        kwargs = {"disable_web_page_preview": True}
    try:
        return await _original_edit_text(self, text, *args, **kwargs)
    except MessageNotModified:
        # Swallow harmless error when content is identical
        return None

PyroMessage.edit_text = _edit_text_no_preview  # type: ignore

# Patch Message.reply as well
_original_reply = PyroMessage.reply

async def _reply_no_preview(self, text, *args, **kwargs):
    try:
        kwargs.setdefault("disable_web_page_preview", True)
    except Exception:
        kwargs = {"disable_web_page_preview": True}
    return await _original_reply(self, text, *args, **kwargs)

PyroMessage.reply = _reply_no_preview  # type: ignore

# --- Global setting: disable link previews by default (Telethon) ---
# Wrap Telethon client methods to default link_preview=False
def _apply_telethon_no_preview_patches(tl_client: TelegramClient):
    _orig_send = tl_client.send_message
    async def _tl_send_no_preview(entity, message=None, *args, **kwargs):
        try:
            kwargs.setdefault("link_preview", False)
        except Exception:
            kwargs = {"link_preview": False}
        return await _orig_send(entity, message, *args, **kwargs)
    tl_client.send_message = _tl_send_no_preview  # type: ignore

    # Client-level edit_message
    _orig_edit_msg = getattr(tl_client, "edit_message", None)
    if _orig_edit_msg:
        async def _tl_edit_message_no_preview(entity, message, text=None, *args, **kwargs):
            try:
                kwargs.setdefault("link_preview", False)
            except Exception:
                kwargs = {"link_preview": False}
            return await _orig_edit_msg(entity, message, text, *args, **kwargs)
        tl_client.edit_message = _tl_edit_message_no_preview  # type: ignore

    # Patch Message.edit for Telethon Message objects
    _orig_tl_msg_edit = TLMessage.edit
    async def _tl_message_edit_no_preview(self, *args, **kwargs):
        try:
            kwargs.setdefault("link_preview", False)
        except Exception:
            kwargs = {"link_preview": False}
        return await _orig_tl_msg_edit(self, *args, **kwargs)
    TLMessage.edit = _tl_message_edit_no_preview  # type: ignore

# Initialize Telethon client without starting it at import time to avoid FloodWaits
sex = TelegramClient('sexrepo', API_ID, API_HASH)
# Apply Telethon patches on bot client
_apply_telethon_no_preview_patches(sex)
# Backward-compatibility alias for existing code importing `telethon_client`
telethon_client = sex

if STRING:
    pro = Client(
        "ggbot", 
        api_id=API_ID, 
        api_hash=API_HASH, 
        session_string=STRING,
        max_concurrent_transmissions=int(os.getenv("PRO_MAX_CONCURRENT_TX", "2")),  # Slightly higher
        sleep_threshold=60,  # Better flood wait handling
        workers=int(os.getenv("PRO_WORKERS", "2")),  # Allow a bit more parallelism for I/O
        in_memory=True,  # Avoid SQLite storage for session_string client
        no_updates=True  # Disable updates polling on user client
    )
    # Apply Pyrogram patches on pro client
    _apply_pyrogram_no_preview_patches(pro)
else:
    pro = None


if DEFAULT_SESSION:
    userrbot = Client(
        "userrbot", 
        api_id=API_ID, 
        api_hash=API_HASH, 
        session_string=DEFAULT_SESSION,
        max_concurrent_transmissions=int(os.getenv("DEFAULT_MAX_CONCURRENT_TX", "2")),
        sleep_threshold=60,  # Better flood wait handling
        workers=int(os.getenv("DEFAULT_WORKERS", "2")),
        in_memory=True,  # Avoid SQLite storage for default user session
        no_updates=True  # Disable updates polling on default user session
    )
    # Apply Pyrogram patches on default user session client
    _apply_pyrogram_no_preview_patches(userrbot)
else:
    userrbot = None

# Avoid starting a second Telethon bot session to prevent ImportBotAuthorization flood waits
# We already have a started Telethon client in `sex` used across the codebase.

# MongoDB setup
tclient = AsyncIOMotorClient(MONGO_DB)
tdb = tclient["telegram_bot"]  # Your database
token = tdb["tokens"]  # Your tokens collection

async def create_ttl_index():
    """Ensure the TTL index exists for the `tokens` collection."""
    await token.create_index("expires_at", expireAfterSeconds=0)

# Run the TTL index creation when the bot starts
async def setup_database():
    await create_ttl_index()
    print("MongoDB TTL index created.")

async def restrict_bot():
    global BOT_ID, BOT_NAME, BOT_USERNAME
    await setup_database()
    await app.start()
    getme = await app.get_me()
    BOT_ID = getme.id
    BOT_USERNAME = getme.username
    BOT_NAME = f"{getme.first_name} {getme.last_name}" if getme.last_name else getme.first_name
    
    # Start Telethon bot client with FloodWait handling
    try:
        await sex.start(bot_token=BOT_TOKEN)
    except FloodWaitError as e:
        logging.warning(f"Telethon bot FloodWait: waiting {e.seconds}s before retry (ImportBotAuthorization)")
        await asyncio.sleep(e.seconds + 1)
        await sex.start(bot_token=BOT_TOKEN)
    except Exception as e:
        logging.error(f"Failed to start Telethon bot client: {e}")
        raise

    # Initialize session pool
    from devgagan.core.session_pool import session_pool
    await session_pool.initialize()
    # Reset global free-user download queue to ensure clean state after restart
    try:
        await download_queue.reset()
        logging.info("Download queue reset at startup")
    except Exception as e:
        logging.warning(f"Failed to reset download queue at startup: {e}")
    
    if pro:
        await pro.start()
    if userrbot:
        await userrbot.start()

    # Warm up LOG_GROUP peer on all active clients to avoid PEER_ID_INVALID
    try:
        from config import LOG_GROUP
        if LOG_GROUP:
            try:
                await app.get_chat(LOG_GROUP)
            except Exception as e:
                logging.warning(f"Bot failed to warm up LOG_GROUP: {e}")
            # Warm-up on Telethon bot client as well
            try:
                await sex.get_me()
                await sex.get_entity(LOG_GROUP)
            except Exception as e:
                logging.warning(f"Telethon bot failed to warm up LOG_GROUP: {e}")

            if pro:
                try:
                    await pro.get_chat(LOG_GROUP)
                except Exception as e:
                    logging.warning(f"Pro client failed to warm up LOG_GROUP: {e}")
            if userrbot:
                try:
                    await userrbot.get_chat(LOG_GROUP)
                except Exception as e:
                    logging.warning(f"Default user session failed to warm up LOG_GROUP: {e}")

            # Pre-warm all admin pool sessions as well (premium-priority, quick tries)
            try:
                # Snapshot of known session IDs
                session_ids = list(session_pool.session_stats.keys())
                for _ in session_ids:
                    client, sid = await session_pool.request_session(is_premium=True, timeout=5.0)
                    if not client or not sid:
                        continue
                    try:
                        await client.get_chat(LOG_GROUP)
                        logging.info(f"Pre-warmed LOG_GROUP on pool session {sid}")
                    except Exception as pe:
                        logging.warning(f"Pool session {sid} failed warm-up: {pe}")
                    finally:
                        try:
                            await session_pool.release_session(sid, had_error=False)
                        except Exception:
                            pass
            except Exception as e:
                logging.warning(f"Error pre-warming pool sessions: {e}")
    except Exception:
        pass

    # Start downloads directory janitor in background (delete files older than 25 minutes)
    async def _downloads_janitor_loop():
        root = os.path.abspath(os.getcwd())
        downloads_dir = os.path.join(root, "downloads")
        max_age_seconds = 25 * 60  # 25 minutes
        while True:
            try:
                if os.path.isdir(downloads_dir):
                    now = time.time()
                    removed = 0
                    for entry in os.scandir(downloads_dir):
                        try:
                            if not entry.is_file():
                                continue
                            age = now - entry.stat().st_mtime
                            if age > max_age_seconds:
                                os.remove(entry.path)
                                removed += 1
                        except Exception as e:
                            logging.warning(f"Janitor: failed to process {entry.path}: {e}")
                    if removed:
                        logging.info(f"Janitor: removed {removed} stale file(s) from downloads/")
            except Exception as e:
                logging.warning(f"Janitor: error scanning downloads/: {e}")
            # Run every 5 minutes
            await asyncio.sleep(300)

    try:
        asyncio.create_task(_downloads_janitor_loop())
    except Exception as e:
        logging.warning(f"Failed to start downloads janitor: {e}")

loop.run_until_complete(restrict_bot())
