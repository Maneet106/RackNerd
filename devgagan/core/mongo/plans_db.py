import datetime
import logging
from pymongo.errors import PyMongoError, NetworkTimeout, OperationFailure
from .connection import premium_db as db
 
async def add_premium(user_id, expire_date):
    data = await check_premium(user_id)
    if data and data.get("_id"):
        await db.update_one({"_id": user_id}, {"$set": {"expire_date": expire_date}})
    else:
        await db.insert_one({"_id": user_id, "expire_date": expire_date})
 
async def remove_premium(user_id):
    await db.delete_one({"_id": user_id})
 
async def check_premium(user_id):
    return await db.find_one({"_id": user_id})
 
async def premium_users():
    id_list = []
    async for data in db.find():
        id_list.append(data["_id"])
    return id_list
 
async def check_and_remove_expired_users():
    """Scan premium users and remove expired ones without crashing the bot.

    Any Mongo connectivity errors are caught and logged as non-fatal so that
    background jobs never bring the bot down.
    """
    try:
        current_time = datetime.datetime.utcnow()
        # Project only the fields we need and avoid loading huge docs
        cursor = db.find({}, {"_id": 1, "expire_date": 1})
        async for data in cursor:
            try:
                expire_date = data.get("expire_date")
                if expire_date and expire_date < current_time:
                    await remove_premium(data["_id"])
                    logging.info(f"Removed user {data['_id']} due to expired plan.")
            except Exception as inner_err:
                logging.warning(f"Error handling premium record {data.get('_id')}: {inner_err}")
                continue
    except (NetworkTimeout, OperationFailure, PyMongoError) as e:
        logging.error(f"Premium expiry check skipped due to Mongo error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during premium expiry check: {e}")
 