import sqlite3
import os
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from app.core.config import settings


class DatabaseManager:
    def __init__(self):
        self.db_path = settings.SQLITE_DB_PATH
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """Ensure the database directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute a SELECT query and return results as list of dicts"""
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def execute_non_query(self, query: str, params: tuple = ()) -> int:
        """Execute INSERT, UPDATE, DELETE query and return affected rows"""
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount


# Initialize database tables
def init_db():
    """Initialize SQLite database with required tables"""
    db_manager = DatabaseManager()

    with db_manager.get_connection() as conn:
        # Users table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                full_name VARCHAR(100),
                role VARCHAR(20) DEFAULT 'user',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        """
        )

        # Conversation memory table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                session_id VARCHAR(100) NOT NULL,
                query_text TEXT NOT NULL,
                sql_query TEXT,
                result_summary TEXT,
                query_type VARCHAR(50),
                execution_time REAL,
                row_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """
        )

        # Query cache table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS query_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_hash VARCHAR(64) UNIQUE NOT NULL,
                sql_query TEXT NOT NULL,
                result_data TEXT,
                result_metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                access_count INTEGER DEFAULT 0,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Alerts configuration table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                alert_name VARCHAR(100) NOT NULL,
                metric VARCHAR(50) NOT NULL,
                threshold_value REAL NOT NULL,
                condition VARCHAR(10) NOT NULL CHECK (condition IN ('>', '<', '>=', '<=', '=', '!=')),
                notification_method VARCHAR(20) NOT NULL CHECK (notification_method IN ('email', 'slack', 'both')),
                sql_query TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                last_checked TIMESTAMP,
                last_triggered TIMESTAMP,
                trigger_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """
        )

        # Alert history table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metric_value REAL NOT NULL,
                threshold_value REAL NOT NULL,
                message TEXT,
                notification_sent BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (alert_id) REFERENCES alerts (id) ON DELETE CASCADE
            )
        """
        )

        # User sessions table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                session_id VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """
        )

        # Analytics preferences table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                preference_key VARCHAR(50) NOT NULL,
                preference_value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                UNIQUE(user_id, preference_key)
            )
        """
        )

        # Create indexes for better performance
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_memory_user_session ON conversation_memory(user_id, session_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cache_hash ON query_cache(query_hash)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cache_expires ON query_cache(expires_at)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_user ON alerts(user_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_preferences_user ON user_preferences(user_id)"
        )

        conn.commit()
        print("Database initialized successfully")


# Cache management functions
class CacheManager:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def get_cache_key(self, sql_query: str) -> str:
        """Generate cache key for SQL query"""
        return hashlib.sha256(sql_query.encode()).hexdigest()

    def get_cached_result(self, sql_query: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached query result"""
        cache_key = self.get_cache_key(sql_query)

        query = """
            SELECT result_data, result_metadata, created_at
            FROM query_cache 
            WHERE query_hash = ? AND expires_at > CURRENT_TIMESTAMP
        """

        results = self.db_manager.execute_query(query, (cache_key,))

        if results:
            # Update access count and last accessed
            update_query = """
                UPDATE query_cache 
                SET access_count = access_count + 1, last_accessed = CURRENT_TIMESTAMP
                WHERE query_hash = ?
            """
            self.db_manager.execute_non_query(update_query, (cache_key,))

            return {
                "data": results[0]["result_data"],
                "metadata": results[0]["result_metadata"],
                "cached_at": results[0]["created_at"],
            }

        return None

    def cache_result(self, sql_query: str, result_data: str, result_metadata: str):
        """Cache query result with TTL"""
        cache_key = self.get_cache_key(sql_query)
        expires_at = datetime.now() + timedelta(seconds=settings.CACHE_TTL_SECONDS)

        query = """
            INSERT OR REPLACE INTO query_cache 
            (query_hash, sql_query, result_data, result_metadata, expires_at)
            VALUES (?, ?, ?, ?, ?)
        """

        self.db_manager.execute_non_query(
            query, (cache_key, sql_query, result_data, result_metadata, expires_at)
        )

        # Clean up expired entries
        self.cleanup_expired_cache()

    def cleanup_expired_cache(self):
        """Remove expired cache entries"""
        query = "DELETE FROM query_cache WHERE expires_at <= CURRENT_TIMESTAMP"
        self.db_manager.execute_non_query(query)

        # Also limit cache size
        count_query = "SELECT COUNT(*) as count FROM query_cache"
        count_result = self.db_manager.execute_query(count_query)

        if count_result and count_result[0]["count"] > settings.MAX_CACHE_SIZE:
            # Remove oldest entries
            cleanup_query = """
                DELETE FROM query_cache 
                WHERE id IN (
                    SELECT id FROM query_cache 
                    ORDER BY last_accessed ASC 
                    LIMIT ?
                )
            """
            cleanup_count = count_result[0]["count"] - settings.MAX_CACHE_SIZE + 10
            self.db_manager.execute_non_query(cleanup_query, (cleanup_count,))


# Memory management functions
class MemoryManager:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def store_conversation(
        self,
        user_id: int,
        session_id: str,
        query_text: str,
        sql_query: str = None,
        result_summary: str = None,
        query_type: str = None,
        execution_time: float = None,
        row_count: int = None,
    ):
        """Store conversation in memory"""
        query = """
            INSERT INTO conversation_memory 
            (user_id, session_id, query_text, sql_query, result_summary, 
             query_type, execution_time, row_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.db_manager.execute_non_query(
            query,
            (
                user_id,
                session_id,
                query_text,
                sql_query,
                result_summary,
                query_type,
                execution_time,
                row_count,
            ),
        )

    def get_conversation_history(
        self, user_id: int, session_id: str = None, limit: int = 10
    ) -> List[Dict]:
        """Retrieve conversation history"""
        if session_id:
            query = """
                SELECT * FROM conversation_memory 
                WHERE user_id = ? AND session_id = ?
                ORDER BY created_at DESC LIMIT ?
            """
            params = (user_id, session_id, limit)
        else:
            query = """
                SELECT * FROM conversation_memory 
                WHERE user_id = ?
                ORDER BY created_at DESC LIMIT ?
            """
            params = (user_id, limit)

        return self.db_manager.execute_query(query, params)

    def get_recent_context(
        self, user_id: int, session_id: str, context_window: int = 5
    ) -> str:
        """Get recent conversation context for Claude API"""
        history = self.get_conversation_history(user_id, session_id, context_window)

        context_parts = []
        for entry in reversed(history):  # Chronological order
            context_parts.append(f"Q: {entry['query_text']}")
            if entry["result_summary"]:
                context_parts.append(f"A: {entry['result_summary']}")

        return
