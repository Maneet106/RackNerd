from dotenv import load_dotenv
import logging
import os

logging.basicConfig(level=logging.INFO)

from pathlib import Path

# Explicitly load the .env file from the project root
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

logging.info(f"--- Loaded Environment Variables ---")
logging.info(f"API_ID: {os.getenv('API_ID')}")
logging.info(f"BOT_TOKEN: {'*' * 8 if os.getenv('BOT_TOKEN') else None}")
logging.info(f"OWNER_ID: {os.getenv('OWNER_ID')}")
logging.info(f"MONGO_DB: {'*' * 8 if os.getenv('MONGO_DB') else None}")
logging.info(f"LOG_GROUP: {os.getenv('LOG_GROUP')}")
logging.info(f"CHANNEL_ID: {os.getenv('CHANNEL_ID')}")
logging.info(f"STRING Session: {'Present' if os.getenv('STRING') else 'Not Present'}")
logging.info(f"------------------------------------")


import asyncio
import importlib
import gc
from pyrogram import idle
from devgagan.modules import ALL_MODULES
from devgagan.core.mongo.plans_db import check_and_remove_expired_users
from aiojobs import create_scheduler

# ----------------------------Bot-Start---------------------------- #

loop = asyncio.get_event_loop()

# Quiet down benign asyncio transport warnings like:
# [WARNING] asyncio: socket.send() raised exception.
# These often occur during transient disconnects or shutdown races and are harmless.
def _asyncio_exception_handler(loop, context):
    exc = context.get("exception")
    msg = context.get("message", "")
    # Ignore noisy transport warnings
    if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
        return
    if "socket.send() raised exception" in msg:
        return
    # Defer all other cases to default handler
    loop.default_exception_handler(context)

try:
    loop.set_exception_handler(_asyncio_exception_handler)
except Exception:
    # If setting the handler fails for any reason, proceed without it
    pass

# Function to schedule expiry checks
async def schedule_expiry_check():
    scheduler = await create_scheduler()
    while True:
        await scheduler.spawn(check_and_remove_expired_users())
        await asyncio.sleep(60)  # Check every hour
        gc.collect()

async def devggn_boot():
    for all_module in ALL_MODULES:
        importlib.import_module("devgagan.modules." + all_module)
    print("""
---------------------------------------------------
📂 Bot Deployed successfully ...
📝 Description: A Pyrogram bot for downloading files from Telegram channels or groups 
                and uploading them back to Telegram.
👨‍💻 Author: Jassal
📬 Telegram: https://t.me/ZeroTrace0x
🗓️ Created: 2025-01-11
🔄 Last Modified: 2025-01-11
🛠️ Version: 2.0.5
📜 License: MIT License
---------------------------------------------------
""")

    asyncio.create_task(schedule_expiry_check())
    print("Auto removal started ...")
    await idle()
    print("Bot stopped...")


if __name__ == "__main__":
    loop.run_until_complete(devggn_boot())

# ------------------------------------------------------------------ #
