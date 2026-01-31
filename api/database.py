"""
Database connection and query utilities for FastAPI
"""

import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    """Database connection pool manager"""
    
    _connection_pool = None
    
    def __init__(self):
        """Initialize database connection"""
        load_dotenv()
        
        if Database._connection_pool is None:
            self._initialize_pool()
        
        self.connection = Database._connection_pool.getconn()
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
    
    @classmethod
    def _initialize_pool(cls):
        """Initialize connection pool"""
        try:
            cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB', 'telegram_warehouse'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD', 'yene1995')
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    def test_connection(self):
        """Test database connection"""
        try:
            self.cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries with query results
        """
        try:
            self.cursor.execute(query, params or ())
            
            if query.strip().upper().startswith(('SELECT', 'WITH')):
                results = self.cursor.fetchall()
                # Convert RealDictRow to regular dict
                return [dict(row) for row in results]
            else:
                self.connection.commit()
                return []
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_many(self, query: str, params_list: List[tuple]):
        """
        Execute multiple parameterized queries
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
        """
        try:
            self.cursor.executemany(query, params_list)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Batch execution failed: {e}")
            raise
    
    def close(self):
        """Close database connection and return to pool"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            Database._connection_pool.putconn(self.connection)
    
    @classmethod
    def close_all(cls):
        """Close all connections in the pool"""
        if cls._connection_pool:
            cls._connection_pool.closeall()
            logger.info("All database connections closed")

def get_db():
    """Dependency for FastAPI to get database connection"""
    db = Database()
    try:
        yield db
    finally:
        db.close()