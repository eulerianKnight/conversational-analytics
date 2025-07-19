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
        self.init_db()

    def _ensure_directory_exists(self):
        """Ensure the database directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
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

    def init_db(self):
        """Initialize SQLite database with required tables"""
        with self.get_connection() as conn:
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alerts_user ON alerts(user_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_preferences_user ON user_preferences(user_id)"
            )

            conn.commit()
            print("Database initialized successfully")


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

        return "\n".join(context_parts)


class UserManager:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def create_user(
        self,
        username: str,
        email: str,
        password_hash: str,
        full_name: str = None,
        role: str = "user",
    ) -> int:
        """Create a new user and return user ID"""
        query = """
            INSERT INTO users (username, email, password_hash, full_name, role)
            VALUES (?, ?, ?, ?, ?)
        """
        return self.db_manager.execute_non_query(
            query, (username, email, password_hash, full_name, role)
        )

    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get user by ID"""
        query = "SELECT * FROM users WHERE id = ?"
        users = self.db_manager.execute_query(query, (user_id,))
        return users[0] if users else None

    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user by username"""
        query = "SELECT * FROM users WHERE username = ?"
        users = self.db_manager.execute_query(query, (username,))
        return users[0] if users else None

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user by email"""
        query = "SELECT * FROM users WHERE email = ?"
        users = self.db_manager.execute_query(query, (email,))
        return users[0] if users else None

    def update_user(self, user_id: int, **kwargs) -> bool:
        """Update user fields"""
        if not kwargs:
            return False

        fields = []
        values = []
        for key, value in kwargs.items():
            if key in [
                "username",
                "email",
                "password_hash",
                "full_name",
                "role",
                "is_active",
            ]:
                fields.append(f"{key} = ?")
                values.append(value)

        if not fields:
            return False

        query = f"UPDATE users SET {', '.join(fields)} WHERE id = ?"
        values.append(user_id)

        rows_affected = self.db_manager.execute_non_query(query, tuple(values))
        return rows_affected > 0

    def delete_user(self, user_id: int) -> bool:
        """Delete user (soft delete by setting is_active = False)"""
        query = "UPDATE users SET is_active = FALSE WHERE id = ?"
        rows_affected = self.db_manager.execute_non_query(query, (user_id,))
        return rows_affected > 0

    def get_all_users(self, active_only: bool = True) -> List[Dict]:
        """Get all users"""
        if active_only:
            query = (
                "SELECT * FROM users WHERE is_active = TRUE ORDER BY created_at DESC"
            )
            return self.db_manager.execute_query(query)
        else:
            query = "SELECT * FROM users ORDER BY created_at DESC"
            return self.db_manager.execute_query(query)


class SessionManager:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def create_session(
        self, user_id: int, session_id: str, expires_at: datetime
    ) -> bool:
        """Create a new user session"""
        query = """
            INSERT INTO user_sessions (user_id, session_id, expires_at)
            VALUES (?, ?, ?)
        """
        rows_affected = self.db_manager.execute_non_query(
            query, (user_id, session_id, expires_at)
        )
        return rows_affected > 0

    def get_session(self, session_id: str) -> Optional[Dict]:
        """Get session by session ID"""
        query = """
            SELECT * FROM user_sessions
            WHERE session_id = ? AND is_active = TRUE AND expires_at > CURRENT_TIMESTAMP
        """
        sessions = self.db_manager.execute_query(query, (session_id,))
        return sessions[0] if sessions else None

    def invalidate_session(self, session_id: str) -> bool:
        """Invalidate a session"""
        query = "UPDATE user_sessions SET is_active = FALSE WHERE session_id = ?"
        rows_affected = self.db_manager.execute_non_query(query, (session_id,))
        return rows_affected > 0

    def invalidate_user_sessions(self, user_id: int) -> int:
        """Invalidate all sessions for a user"""
        query = "UPDATE user_sessions SET is_active = FALSE WHERE user_id = ?"
        return self.db_manager.execute_non_query(query, (user_id,))

    def cleanup_expired_sessions(self) -> int:
        """Remove expired sessions"""
        query = "DELETE FROM user_sessions WHERE expires_at <= CURRENT_TIMESTAMP"
        return self.db_manager.execute_non_query(query)


class PreferenceManager:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def set_preference(self, user_id: int, key: str, value: str) -> bool:
        """Set or update user preference"""
        query = """
            INSERT OR REPLACE INTO user_preferences (user_id, preference_key, preference_value)
            VALUES (?, ?, ?)
        """
        rows_affected = self.db_manager.execute_non_query(query, (user_id, key, value))
        return rows_affected > 0

    def get_preference(
        self, user_id: int, key: str, default_value: str = None
    ) -> Optional[str]:
        """Get user preference"""
        query = "SELECT preference_value FROM user_preferences WHERE user_id = ? AND preference_key = ?"
        results = self.db_manager.execute_query(query, (user_id, key))

        if results:
            return results[0]["preference_value"]
        return default_value

    def get_all_preferences(self, user_id: int) -> Dict[str, str]:
        """Get all preferences for a user"""
        query = "SELECT preference_key, preference_value FROM user_preferences WHERE user_id = ?"
        results = self.db_manager.execute_query(query, (user_id,))

        return {row["preference_key"]: row["preference_value"] for row in results}

    def delete_preference(self, user_id: int, key: str) -> bool:
        """Delete a user preference"""
        query = "DELETE FROM user_preferences WHERE user_id = ? AND preference_key = ?"
        rows_affected = self.db_manager.execute_non_query(query, (user_id, key))
        return rows_affected > 0


# Global instances
db_manager = DatabaseManager()
cache_manager = CacheManager()
memory_manager = MemoryManager()

# Initialize additional managers
user_manager = UserManager()
session_manager = SessionManager()
preference_manager = PreferenceManager()


def cleanup_database():
    """Perform database maintenance tasks"""
    try:
        # Clean up expired cache entries
        cache_manager.cleanup_expired_cache()

        # Clean up expired sessions
        session_manager.cleanup_expired_sessions()

        with db_manager.get_connection() as conn:
            user_ids = db_manager.execute_query(
                "SELECT DISTINCT user_id FROM conversation_memory"
            )
            for user_row in user_ids:
                user_id = user_row["user_id"]
                cleanup_query_per_user = """
                    DELETE FROM conversation_memory
                    WHERE user_id = ? AND id NOT IN (
                        SELECT id FROM conversation_memory
                        WHERE user_id = ?
                        ORDER BY created_at DESC
                        LIMIT 1000
                    )
                """
                db_manager.execute_non_query(cleanup_query_per_user, (user_id, user_id))

        print("Database cleanup completed successfully")
        return True

    except Exception as e:
        print(f"Database cleanup failed: {str(e)}")
        return False


def get_database_stats() -> Dict[str, Any]:
    """Get database statistics"""
    try:
        stats = {}

        # User statistics
        user_stats = db_manager.execute_query(
            "SELECT COUNT(*) as total, COUNT(CASE WHEN is_active THEN 1 END) as active FROM users"
        )
        stats["users"] = user_stats[0] if user_stats else {}

        # Memory statistics
        memory_stats = db_manager.execute_query(
            "SELECT COUNT(*) as total, COUNT(DISTINCT user_id) as unique_users FROM conversation_memory"
        )
        stats["conversation_memory"] = memory_stats[0] if memory_stats else {}

        # Cache statistics
        cache_stats = db_manager.execute_query(
            "SELECT COUNT(*) as total, COUNT(CASE WHEN expires_at > CURRENT_TIMESTAMP THEN 1 END) as active FROM query_cache"
        )
        stats["query_cache"] = cache_stats[0] if cache_stats else {}

        # Alert statistics
        alert_stats = db_manager.execute_query(
            "SELECT COUNT(*) as total, COUNT(CASE WHEN is_active THEN 1 END) as active FROM alerts"
        )
        stats["alerts"] = alert_stats[0] if alert_stats else {}

        return stats

    except Exception as e:
        print(f"Failed to get database stats: {str(e)}")
        return {}


__all__ = [
    "DatabaseManager",
    "CacheManager",
    "MemoryManager",
    "UserManager",
    "SessionManager",
    "PreferenceManager",
    "db_manager",
    "cache_manager",
    "memory_manager",
    "user_manager",
    "session_manager",
    "preference_manager",
    "cleanup_database",
    "get_database_stats",
]
