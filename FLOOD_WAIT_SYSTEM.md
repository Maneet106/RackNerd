# 🚫 Simple Flood Wait Management System

## 📋 **OVERVIEW**

A lightweight, focused flood wait system that provides admin control over user downloads without breaking existing functionality.

## 🎯 **FEATURES**

### **Admin Commands:**
- `/flood <user_id> <duration>` - Apply flood wait to user with flexible time formats
- `/unflood <user_id>` - Remove flood wait from user  
- `/checkflood <user_id>` - Check flood wait status for specific user
- `/floodcheck` - List all active flood waits with summary

### **Core Functionality:**
- **MongoDB Persistence** - Survives restarts and reboots
- **Flexible Time Parsing** - Supports 20s, 30m, 2h, 1d formats + plain seconds
- **No Time Limits** - Can set flood waits for any duration (removed 24h limit)
- **Automatic Expiry** - Flood waits expire automatically
- **Operation Cancellation** - Cancels all user operations when flood wait applied
- **Download Prevention** - Blocks both single downloads and batch operations
- **Clean Integration** - No duplicate code, minimal changes to existing system
- **Professional Formatting** - HTML formatted messages with copy-paste commands

## 📁 **FILES CREATED**

### **1. Core System:**
- `devgagan/core/simple_flood_wait.py` - Main flood wait logic
- `devgagan/modules/flood_admin.py` - Admin commands

### **2. Integration:**
- Modified `devgagan/modules/main.py` - Added flood wait checks to download functions

### **3. Testing:**
- `test_flood_wait.py` - Test script to verify functionality

## 🔧 **TECHNICAL IMPLEMENTATION**

### **MongoDB Collection:**
```
Database: flood_management
Collection: active_flood_waits

Document Structure:
{
  "user_id": 123456789,
  "applied_at": ISODate("2025-10-08T12:00:00Z"),
  "expires_at": ISODate("2025-10-08T13:00:00Z"), 
  "seconds": 3600,
  "admin_id": 987654321,
  "active": true
}
```

### **Integration Points:**
1. **Single Downloads** - Check in `single_link()` function
2. **Batch Downloads** - Check in `batch_link()` function
3. **Operation Cancellation** - Uses existing `cancel_manager`

## 🚀 **USAGE EXAMPLES**

### **Apply Flood Wait (Flexible Formats):**
```
/flood 123456789 3600    # 3600 seconds
/flood 123456789 20s     # 20 seconds  
/flood 123456789 30m     # 30 minutes
/flood 123456789 2h      # 2 hours
/flood 123456789 1d      # 1 day
/flood 123456789 7d      # 7 days (no limit!)
```
- Cancels all ongoing operations for the user
- User cannot download until flood wait expires or is removed
- Always shows duration in seconds to user

### **Remove Flood Wait:**
```
/unflood 123456789
```
- Immediately removes flood wait from user 123456789
- User can resume downloading immediately

### **Check Individual Status:**
```
/checkflood 123456789
```
- Shows current flood wait status and remaining time for specific user

### **Check All Active Flood Waits:**
```
/floodcheck
```
- Lists all users currently flood waited
- Shows remaining time for each user
- Provides unflood commands for easy copy-paste
- Includes summary statistics

## 📱 **USER EXPERIENCE**

### **When Flood Waited:**
User sees exact Telegram-style message:
```
[420 FLOOD_WAIT_X] : ⏳ A wait of 3000 seconds is required. Please try again after 3000 seconds due to Telegram's flood control.
```

### **When Not Flood Waited:**
- Downloads work normally
- No additional delays or messages
- System is completely transparent

## 🛡️ **SAFETY FEATURES**

### **Input Validation:**
- User ID must be numeric
- Seconds must be positive
- Maximum 24 hours (86400 seconds) flood wait
- Admin-only access (OWNER_ID check)

### **Automatic Cleanup:**
- Expired flood waits are automatically removed
- No manual cleanup required
- MongoDB handles persistence

### **Error Handling:**
- Graceful error handling for all operations
- Detailed logging for debugging
- Fallback to normal operation if system fails

## 🔄 **WORKFLOW**

### **Applying Flood Wait:**
1. Admin runs `/flood <user_id> <seconds>`
2. System cancels all user operations
3. Flood wait stored in MongoDB with expiry time
4. User gets flood wait message on download attempts
5. Downloads blocked until expiry or manual removal

### **Automatic Expiry:**
1. User attempts download
2. System checks MongoDB for active flood wait
3. If expired, automatically removes from database
4. Download proceeds normally

### **Manual Removal:**
1. Admin runs `/unflood <user_id>`
2. Flood wait removed from MongoDB
3. User can immediately resume downloads

## 📊 **TESTING**

Run the test script to verify functionality:
```bash
python test_flood_wait.py
```

Tests cover:
- ✅ Applying flood waits
- ✅ Checking flood wait status  
- ✅ Getting flood wait messages
- ✅ Removing flood waits
- ✅ Automatic expiry handling

## 🎯 **BENEFITS**

### **For Admins:**
- **Simple Commands** - Easy to use `/flood` and `/unflood`
- **Immediate Effect** - Operations cancelled instantly
- **Persistent** - Survives bot restarts
- **Flexible Duration** - Any duration up to 24 hours

### **For System:**
- **No Breaking Changes** - Existing code unchanged
- **Clean Integration** - Minimal code additions
- **MongoDB Persistence** - Reliable storage
- **Automatic Cleanup** - No maintenance required

### **For Users:**
- **Clear Messages** - Exact remaining time shown
- **Consistent Experience** - Same message format as Telegram
- **Fair Treatment** - Only affects flood waited users

## 🚨 **IMPORTANT NOTES**

### **Admin Access:**
- Only users in `OWNER_ID` can use flood commands
- Non-admin users get no response (silent fail)

### **Operation Cancellation:**
- All user operations cancelled when flood wait applied
- Uses existing `cancel_manager` for clean cancellation
- Prevents resource waste and conflicts

### **MongoDB Requirement:**
- Requires MongoDB connection (uses existing connection)
- Creates `flood_management.active_flood_waits` collection
- No additional configuration needed

### **Integration Safety:**
- Added checks at entry points only
- No modifications to core download logic
- Preserves all existing functionality

This system provides the exact functionality requested: simple, effective flood wait management that survives restarts and integrates cleanly with the existing codebase.
