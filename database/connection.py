"""
Database connection management for NBO LinkedIn API.
"""
import asyncpg
import logging
from typing import Optional
from config.settings import settings

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Simple database connection manager for PostgreSQL.
    """
    
    def __init__(self):
        """Initialize the database manager."""
        self.connection_params = {
            'host': settings.DB_HOST,
            'port': settings.DB_PORT,
            'database': settings.DB_NAME,
            'user': settings.DB_USER,
            'password': settings.DB_PASSWORD
        }
        logger.info(f"Database manager initialized for {settings.DB_HOST}:{settings.DB_PORT}")
    
    async def get_connection(self):
        """
        Get a database connection.
        
        Returns:
            asyncpg.Connection: Database connection
        """
        try:
            connection = await asyncpg.connect(**self.connection_params)
            logger.debug("Database connection established successfully")
            return connection
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            connection = await self.get_connection()
            # Simple query to test connection
            result = await connection.fetchval("SELECT 1")
            await connection.close()
            
            if result == 1:
                logger.info("Database connection test successful")
                return True
            else:
                logger.error("Database connection test failed - unexpected result")
                return False
                
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    async def execute_query(self, query: str, *args):
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            
        Returns:
            Query results
        """
        connection = None
        try:
            connection = await self.get_connection()
            result = await connection.fetch(query, *args)
            return result
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            if connection:
                await connection.close()
    
    async def execute_single(self, query: str, *args):
        """
        Execute a query and return single result.
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            
        Returns:
            Single query result or None
        """
        connection = None
        try:
            connection = await self.get_connection()
            result = await connection.fetchrow(query, *args)
            return result
        except Exception as e:
            logger.error(f"Error executing single query: {e}")
            raise
        finally:
            if connection:
                await connection.close()
    
    async def execute_update(self, query: str, *args) -> bool:
        """
        Execute an UPDATE query.
        
        Args:
            query: SQL UPDATE query
            *args: Query parameters
            
        Returns:
            bool: True if successful, False otherwise
        """
        connection = None
        try:
            connection = await self.get_connection()
            result = await connection.execute(query, *args)
            logger.debug(f"Update query executed: {result}")
            return True
        except Exception as e:
            logger.error(f"Error executing update query: {e}")
            return False
        finally:
            if connection:
                await connection.close()

# Global database manager instance
db_manager = DatabaseManager()
