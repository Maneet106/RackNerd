from .connection import get_collection

# Get the users collection - now using the fast users_db collection
# All 224 users have been migrated from users_db.users to users_db for better performance
db = get_collection('users', 'users_db')


async def get_users():
  user_list = []
  async for user in db.find({"user": {"$gt": 0}}):
    user_list.append(user['user'])
  return user_list


async def get_users_excluding_bots():
  """Get all users excluding bots for accurate statistics"""
  from devgagan import app
  
  user_list = []
  async for user in db.find({"user": {"$gt": 0}}):
    user_list.append(user['user'])
  
  if not user_list:
    return []
  
  # Remove duplicates and sort
  user_list = sorted(set(int(u) for u in user_list))
  
  # Fetch Telegram profile info in batches to identify bots
  BATCH = 100
  non_bot_users = []
  
  for i in range(0, len(user_list), BATCH):
    batch = user_list[i:i+BATCH]
    try:
      tg_users = await app.get_users(batch)
      if not isinstance(tg_users, list):
        tg_users = [tg_users]
      
      for u in tg_users:
        if not u:
          continue
        
        # Check if user is a bot
        is_bot = getattr(u, "is_bot", False)
        username = u.username or ""
        
        # Also check username ending with 'bot'
        is_bot_username = username.lower().endswith("bot") if username else False
        
        # Only add if not a bot
        if not is_bot and not is_bot_username:
          non_bot_users.append(u.id)
          
    except Exception:
      # Fallback: check each user individually
      for uid in batch:
        try:
          u = await app.get_users(uid)
          if u:
            is_bot = getattr(u, "is_bot", False)
            username = u.username or ""
            is_bot_username = username.lower().endswith("bot") if username else False
            
            if not is_bot and not is_bot_username:
              non_bot_users.append(uid)
        except Exception:
          # If we can't get user info, assume it's not a bot
          non_bot_users.append(uid)
  
  return sorted(set(non_bot_users))


async def get_user(user):
  users = await get_users()
  if user in users:
    return True
  else:
    return False

async def add_user(user):
  users = await get_users()
  if user in users:
    return
  else:
    await db.insert_one({"user": user})


async def del_user(user):
  users = await get_users()
  if not user in users:
    return
  else:
    await db.delete_one({"user": user})
    


