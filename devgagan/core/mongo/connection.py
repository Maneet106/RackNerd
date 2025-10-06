"""
Optimized MongoDB Connection Manager for RestrictedSaverBot
- Single connection pool shared across all modules
- Aggressive timeout settings for low latency
- Connection reuse and proper error handling
- Optimized for Toronto VPS -> US East MongoDB Atlas
"""

import logging
from motor.motor_asyncio import AsyncIOMotorClient as MongoCli
from pymongo.errors import PyMongoError, NetworkTimeout, OperationFailure
from config import MONGO_DB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptimizedMongoManager:
    """Singleton MongoDB connection manager with optimized settings"""
    
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._initialize_client()
    
    def _initialize_client(self):
        """Initialize MongoDB client with optimized settings for low latency"""
        try:
            # Optimized connection settings for Toronto -> US East
            self._client = MongoCli(
                MONGO_DB,
                # Aggressive timeout settings for low latency
                serverSelectionTimeoutMS=3000,    # Reduced from 20s to 3s
                connectTimeoutMS=2000,            # Reduced from 20s to 2s  
                socketTimeoutMS=5000,             # Reduced from 20s to 5s
                
                # Connection pool optimization
                maxPoolSize=50,                   # Increased pool size
                minPoolSize=5,                    # Maintain minimum connections
                maxIdleTimeMS=30000,              # 30s idle timeout
                
                # Performance optimizations
                retryWrites=True,
                retryReads=True,
                readPreference='primaryPreferred', # Faster reads
                
                # Compression for better performance over network
                compressors='zlib',
                
                # Connection management
                heartbeatFrequencyMS=10000,       # 10s heartbeat
                
                # Write concern optimization
                w='majority',
                wtimeoutMS=5000,                  # 5s write timeout
                
                # Additional optimizations
                directConnection=False,           # Use connection pooling
                appName='RestrictedSaverBot-Optimized'
            )
            
            logger.info("✅ Optimized MongoDB client initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize MongoDB client: {e}")
            raise
    
    @property
    def client(self):
        """Get the MongoDB client instance"""
        if self._client is None:
            self._initialize_client()
        return self._client
    
    def get_database(self, db_name):
        """Get a database instance"""
        return self.client[db_name]
    
    def get_collection(self, db_name, collection_name):
        """Get a collection instance"""
        return self.client[db_name][collection_name]
    
    async def ping(self):
        """Test MongoDB connection with timing"""
        import time
        try:
            start = time.time()
            await self.client.admin.command('ping')
            end = time.time()
            latency = (end - start) * 1000
            logger.info(f"🏓 MongoDB ping: {latency:.2f}ms")
            return latency
        except Exception as e:
            logger.error(f"❌ MongoDB ping failed: {e}")
            raise
    
    async def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            logger.info("🔌 MongoDB connection closed")

# Global instance
mongo_manager = OptimizedMongoManager()

# Convenience functions for backward compatibility
def get_mongo_client():
    """Get the optimized MongoDB client"""
    return mongo_manager.client

def get_database(db_name):
    """Get a database instance"""
    return mongo_manager.get_database(db_name)

def get_collection(db_name, collection_name):
    """Get a collection instance"""
    return mongo_manager.get_collection(db_name, collection_name)

# Pre-configured database instances
premium_db = get_collection('premium', 'premium_db')
users_db = get_collection('users', 'users_db')  # Fixed: users database, users_db collection
user_data_db = get_collection('user_data', 'users_data_db')

# Export for easy imports
__all__ = [
    'mongo_manager',
    'get_mongo_client', 
    'get_database',
    'get_collection',
    'premium_db',
    'users_db', 
    'user_data_db'
]
