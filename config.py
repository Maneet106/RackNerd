import os
import sys
from os import getenv

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# YouTube and Instagram download functionality removed

# Validation function for required environment variables
def validate_env_var(var_name, var_value, var_type=str):
    if var_value is None or var_value == f"your_{var_name.lower()}_here" or var_value == "":
        print(f"❌ ERROR: {var_name} is not set or contains placeholder value!")
        print(f"Please set {var_name} in your .env file with a valid value.")
        return None
    
    try:
        if var_type == int:
            return int(var_value)
        elif var_type == list:
            return list(map(int, var_value.split()))
        else:
            return var_value
    except ValueError as e:
        print(f"❌ ERROR: Invalid value for {var_name}: {e}")
        return None

# Required environment variables with validation
print("🔧 Loading configuration...")

API_ID = validate_env_var("API_ID", getenv("API_ID"), int)
API_HASH = validate_env_var("API_HASH", getenv("API_HASH"))
BOT_TOKEN = validate_env_var("BOT_TOKEN", getenv("BOT_TOKEN"))
OWNER_ID = validate_env_var("OWNER_ID", getenv("OWNER_ID"), list)
MONGO_DB = validate_env_var("MONGO_DB", getenv("MONGO_DB"))
LOG_GROUP = validate_env_var("LOG_GROUP", getenv("LOG_GROUP"), int)
CHANNEL_ID = validate_env_var("CHANNEL_ID", getenv("CHANNEL_ID"), int)

# Check if any required variables are missing
required_vars = [API_ID, API_HASH, BOT_TOKEN, OWNER_ID, MONGO_DB, LOG_GROUP, CHANNEL_ID]
if None in required_vars:
    print("\n❌ CONFIGURATION ERROR: Missing required environment variables!")
    print("\n📋 Setup Instructions:")
    print("1. Get API_ID and API_HASH from https://my.telegram.org")
    print("2. Create a bot with @BotFather and get BOT_TOKEN")
    print("3. Get your OWNER_ID from @userinfobot")
    print("4. Create a MongoDB database at https://cloud.mongodb.com")
    print("5. Create a log group and channel, add bot as admin")
    print("6. Get group/channel IDs using @userinfobot")
    print("7. Update your .env file with actual values")
    print("\n🔧 Edit the .env file and replace all placeholder values!")
    sys.exit(1)

print("✅ Required configuration loaded successfully!")

# Optional configuration variables
CHANNEL = getenv("CHANNEL")
FREEMIUM_LIMIT = int(getenv("FREEMIUM_LIMIT", "10"))
PREMIUM_LIMIT = int(getenv("PREMIUM_LIMIT", "500"))
WEBSITE_URL = getenv("WEBSITE_URL", "upshrink.com")
AD_API = getenv("AD_API")

# Configurable cooldowns for free users (in seconds)
try:
    FREE_SINGLE_WAIT_SECONDS = int(getenv("FREE_SINGLE_WAIT_SECONDS", "200"))
except Exception:
    FREE_SINGLE_WAIT_SECONDS = 200
try:
    FREE_BATCH_WAIT_SECONDS = int(getenv("FREE_BATCH_WAIT_SECONDS", "300"))
except Exception:
    FREE_BATCH_WAIT_SECONDS = 300

# Fake Premium Marketing Configuration (in seconds)
try:
    FAKE_MARKETING_MIN_INTERVAL = int(getenv("FAKE_MARKETING_MIN_INTERVAL", "7200"))  # 2 hours default
except Exception:
    FAKE_MARKETING_MIN_INTERVAL = 7200
try:
    FAKE_MARKETING_MAX_INTERVAL = int(getenv("FAKE_MARKETING_MAX_INTERVAL", "10800"))  # 3 hours default
except Exception:
    FAKE_MARKETING_MAX_INTERVAL = 10800

# Validate session strings - set to None if placeholder values
STRING_RAW = getenv("STRING")
if STRING_RAW and STRING_RAW != "your_premium_session_string_here" and STRING_RAW.strip():
    STRING = STRING_RAW
else:
    STRING = None

DEFAULT_SESSION_RAW = getenv("DEFAULT_SESSION")
if DEFAULT_SESSION_RAW and DEFAULT_SESSION_RAW != "your_default_session_string_here" and DEFAULT_SESSION_RAW.strip():
    DEFAULT_SESSION = DEFAULT_SESSION_RAW
else:
    DEFAULT_SESSION = None

# YouTube and Instagram cookies removed - bot now only supports Telegram content

print(f"🤖 Bot configured for owner: {OWNER_ID[0]}")
print(f"📊 Limits - Free: {FREEMIUM_LIMIT}, Premium: {PREMIUM_LIMIT}")
if STRING:
    print("🚀 Premium session configured for 2GB uploads")
print("🔧 Configuration complete!\n")

# Optional secure login log channel (for abuse detection)
# Use raw getenv to avoid hard validation failures when not provided
USER_LOGIN_INFO_RAW = getenv("USER_LOGIN_INFO")
try:
    USER_LOGIN_INFO = int(USER_LOGIN_INFO_RAW) if USER_LOGIN_INFO_RAW and USER_LOGIN_INFO_RAW.strip() else None
except Exception:
    USER_LOGIN_INFO = None

# Optional broadcast group/channel for premium announcements
PREMIUM_BROADCAST_RAW = getenv("PREMIUM_BROADCAST")
try:
    PREMIUM_BROADCAST = int(PREMIUM_BROADCAST_RAW) if PREMIUM_BROADCAST_RAW and PREMIUM_BROADCAST_RAW.strip() else None
except Exception:
    PREMIUM_BROADCAST = None

# Optional toggle: capture device/session info after login for abuse detection
def _to_bool(val: str) -> bool:
    if not val:
        return False
    return val.strip().lower() in ("1", "true", "yes", "on")

CAPTURE_LOGIN_DEVICE_INFO = _to_bool(getenv("CAPTURE_LOGIN_DEVICE_INFO", "false"))
