# backend/app/api/endpoints/queries.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import json
from datetime import datetime

from app.core.database import db_manager, cache_manager
from app.services.snowflake_service import SnowflakeService
from app.api.endpoints.auth import get_user_by_username

router = APIRouter()
security = HTTPBearer()
snowflake_service = SnowflakeService()


# Pydantic models
class SavedQuery(BaseModel):
    name: str
    sql_query: str
    description: Optional[str] = None
    tags: Optional[List[str]] = []


class SavedQueryResponse(BaseModel):
    id: int
    user_id: int
    name: str
    sql_query: str
    description: Optional[str]
    tags: List[str]
    created_at: datetime
    last_executed: Optional[datetime]
    execution_count: int


# Helper functions
async def get_current_user_from_token(credentials: HTTPAuthorizationCredentials):
    """Get current user from JWT token"""
    try:
        import jwt

        payload = jwt.decode(
            credentials.credentials,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
            )

        user = get_user_by_username(username)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )

        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired"
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


# Create saved queries table if not exists
def init_saved_queries_table():
    """Initialize saved queries table"""
    with db_manager.get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name VARCHAR(100) NOT NULL,
                sql_query TEXT NOT NULL,
                description TEXT,
                tags TEXT,  -- JSON array of tags
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_executed TIMESTAMP,
                execution_count INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_saved_queries_user ON saved_queries(user_id)"
        )
        conn.commit()


# Initialize table on import
init_saved_queries_table()


# API Endpoints
@router.post("/saved", response_model=SavedQueryResponse)
async def save_query(
    query_data: SavedQuery,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Save a query for reuse"""
    user = await get_current_user_from_token(credentials)

    # Validate SQL query
    is_valid, message = snowflake_service.validate_sql_query(query_data.sql_query)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid SQL query: {message}",
        )

    try:
        # Insert saved query
        query = """
            INSERT INTO saved_queries (user_id, name, sql_query, description, tags)
            VALUES (?, ?, ?, ?, ?)
        """
        tags_json = json.dumps(query_data.tags) if query_data.tags else "[]"

        db_manager.execute_non_query(
            query,
            (
                user["id"],
                query_data.name,
                query_data.sql_query,
                query_data.description,
                tags_json,
            ),
        )

        # Get the created query
        get_query = "SELECT * FROM saved_queries WHERE user_id = ? ORDER BY created_at DESC LIMIT 1"
        queries = db_manager.execute_query(get_query, (user["id"],))

        if queries:
            saved_query = queries[0]
            saved_query["tags"] = json.loads(saved_query.get("tags", "[]"))
            return SavedQueryResponse(**saved_query)
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to save query",
            )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save query: {str(e)}",
        )


@router.get("/saved", response_model=List[SavedQueryResponse])
async def get_saved_queries(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get all saved queries for the current user"""
    user = await get_current_user_from_token(credentials)

    query = "SELECT * FROM saved_queries WHERE user_id = ? ORDER BY created_at DESC"
    queries = db_manager.execute_query(query, (user["id"],))

    result = []
    for q in queries:
        q["tags"] = json.loads(q.get("tags", "[]"))
        result.append(SavedQueryResponse(**q))

    return result


@router.get("/saved/{query_id}", response_model=SavedQueryResponse)
async def get_saved_query(
    query_id: int, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get a specific saved query"""
    user = await get_current_user_from_token(credentials)

    query = "SELECT * FROM saved_queries WHERE id = ? AND user_id = ?"
    queries = db_manager.execute_query(query, (query_id, user["id"]))

    if not queries:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Saved query not found"
        )

    saved_query = queries[0]
    saved_query["tags"] = json.loads(saved_query.get("tags", "[]"))
    return SavedQueryResponse(**saved_query)


@router.put("/saved/{query_id}", response_model=SavedQueryResponse)
async def update_saved_query(
    query_id: int,
    query_data: SavedQuery,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Update a saved query"""
    user = await get_current_user_from_token(credentials)

    # Check if query exists and belongs to user
    check_query = "SELECT * FROM saved_queries WHERE id = ? AND user_id = ?"
    existing_queries = db_manager.execute_query(check_query, (query_id, user["id"]))

    if not existing_queries:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Saved query not found"
        )

    # Validate SQL query
    is_valid, message = snowflake_service.validate_sql_query(query_data.sql_query)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid SQL query: {message}",
        )

    try:
        # Update query
        update_query = """
            UPDATE saved_queries 
            SET name = ?, sql_query = ?, description = ?, tags = ?
            WHERE id = ? AND user_id = ?
        """
        tags_json = json.dumps(query_data.tags) if query_data.tags else "[]"

        db_manager.execute_non_query(
            update_query,
            (
                query_data.name,
                query_data.sql_query,
                query_data.description,
                tags_json,
                query_id,
                user["id"],
            ),
        )

        # Get updated query
        updated_queries = db_manager.execute_query(check_query, (query_id, user["id"]))
        saved_query = updated_queries[0]
        saved_query["tags"] = json.loads(saved_query.get("tags", "[]"))
        return SavedQueryResponse(**saved_query)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update query: {str(e)}",
        )


@router.delete("/saved/{query_id}")
async def delete_saved_query(
    query_id: int, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Delete a saved query"""
    user = await get_current_user_from_token(credentials)

    # Check if query exists and belongs to user
    check_query = "SELECT * FROM saved_queries WHERE id = ? AND user_id = ?"
    existing_queries = db_manager.execute_query(check_query, (query_id, user["id"]))

    if not existing_queries:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Saved query not found"
        )

    # Delete query
    delete_query = "DELETE FROM saved_queries WHERE id = ? AND user_id = ?"
    db_manager.execute_non_query(delete_query, (query_id, user["id"]))

    return {"message": "Saved query deleted successfully"}


@router.post("/saved/{query_id}/execute")
async def execute_saved_query(
    query_id: int,
    use_cache: bool = True,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Execute a saved query"""
    user = await get_current_user_from_token(credentials)

    # Get saved query
    query = "SELECT * FROM saved_queries WHERE id = ? AND user_id = ?"
    queries = db_manager.execute_query(query, (query_id, user["id"]))

    if not queries:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Saved query not found"
        )

    saved_query = queries[0]

    try:
        # Execute the query
        result = await snowflake_service.execute_query(
            saved_query["sql_query"], use_cache
        )

        # Update execution statistics
        update_query = """
            UPDATE saved_queries 
            SET last_executed = CURRENT_TIMESTAMP, execution_count = execution_count + 1
            WHERE id = ?
        """
        db_manager.execute_non_query(update_query, (query_id,))

        return {
            "query_id": query_id,
            "query_name": saved_query["name"],
            "sql_query": saved_query["sql_query"],
            "data": result["data"],
            "metadata": result["metadata"],
            "execution_time": result.get("execution_time", 0),
            "from_cache": result.get("from_cache", False),
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query execution failed: {str(e)}",
        )


@router.get("/cache/stats")
async def get_cache_stats(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get query cache statistics"""
    user = await get_current_user_from_token(credentials)

    try:
        stats_query = """
            SELECT 
                COUNT(*) as total_entries,
                SUM(access_count) as total_accesses,
                AVG(access_count) as avg_accesses,
                COUNT(CASE WHEN expires_at > CURRENT_TIMESTAMP THEN 1 END) as active_entries,
                COUNT(CASE WHEN expires_at <= CURRENT_TIMESTAMP THEN 1 END) as expired_entries
            FROM query_cache
        """

        stats = db_manager.execute_query(stats_query)

        return {
            "cache_stats": stats[0] if stats else {},
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get cache stats: {str(e)}",
        )


@router.delete("/cache/clear")
async def clear_query_cache(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Clear query cache (admin function)"""
    user = await get_current_user_from_token(credentials)

    if user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required"
        )

    try:
        # Clear all cache entries
        clear_query = "DELETE FROM query_cache"
        rows_affected = db_manager.execute_non_query(clear_query)

        return {
            "message": "Query cache cleared successfully",
            "entries_removed": rows_affected,
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear cache: {str(e)}",
        )


@router.get("/templates")
async def get_query_templates(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get predefined query templates"""
    user = await get_current_user_from_token(credentials)

    templates = [
        {
            "name": "Top 10 Suppliers by Revenue",
            "description": "Find the highest revenue generating suppliers",
            "sql_query": """
                SELECT 
                    s.NAME as supplier_name,
                    SUM(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) as total_revenue,
                    COUNT(DISTINCT l.ORDERKEY) as total_orders,
                    n.NAME as nation
                FROM SUPPLIER s
                JOIN LINEITEM l ON s.SUPPKEY = l.SUPPKEY
                JOIN NATION n ON s.NATIONKEY = n.NATIONKEY
                WHERE l.SHIPDATE >= DATEADD(month, -3, CURRENT_DATE)
                GROUP BY s.SUPPKEY, s.NAME, n.NAME
                ORDER BY total_revenue DESC
                LIMIT 10
            """,
            "tags": ["suppliers", "revenue", "performance"],
        },
        {
            "name": "Monthly Sales Trend",
            "description": "Analyze monthly sales trends over time",
            "sql_query": """
                SELECT 
                    DATE_TRUNC('month', l.SHIPDATE) as month,
                    SUM(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) as revenue,
                    SUM(l.QUANTITY) as quantity_sold,
                    COUNT(DISTINCT l.ORDERKEY) as orders_count
                FROM LINEITEM l
                WHERE l.SHIPDATE >= DATEADD(year, -1, CURRENT_DATE)
                GROUP BY DATE_TRUNC('month', l.SHIPDATE)
                ORDER BY month
            """,
            "tags": ["sales", "trends", "monthly"],
        },
        {
            "name": "Customer Analysis by Region",
            "description": "Analyze customer distribution and spending by region",
            "sql_query": """
                SELECT 
                    r.NAME as region,
                    COUNT(DISTINCT c.CUSTKEY) as customer_count,
                    AVG(c.ACCTBAL) as avg_account_balance,
                    COUNT(DISTINCT o.ORDERKEY) as total_orders,
                    SUM(o.TOTALPRICE) as total_revenue
                FROM REGION r
                JOIN NATION n ON r.REGIONKEY = n.REGIONKEY
                JOIN CUSTOMER c ON n.NATIONKEY = c.NATIONKEY
                LEFT JOIN ORDERS o ON c.CUSTKEY = o.CUSTKEY
                GROUP BY r.REGIONKEY, r.NAME
                ORDER BY total_revenue DESC
            """,
            "tags": ["customers", "regions", "analysis"],
        },
        {
            "name": "Inventory Analysis",
            "description": "Analyze part inventory levels and supplier availability",
            "sql_query": """
                SELECT 
                    p.NAME as part_name,
                    p.BRAND,
                    p.TYPE,
                    COUNT(DISTINCT ps.SUPPKEY) as supplier_count,
                    AVG(ps.AVAILQTY) as avg_available_qty,
                    AVG(ps.SUPPLYCOST) as avg_supply_cost,
                    p.RETAILPRICE
                FROM PART p
                JOIN PARTSUPP ps ON p.PARTKEY = ps.PARTKEY
                GROUP BY p.PARTKEY, p.NAME, p.BRAND, p.TYPE, p.RETAILPRICE
                HAVING supplier_count >= 2
                ORDER BY avg_available_qty DESC
                LIMIT 20
            """,
            "tags": ["inventory", "parts", "suppliers"],
        },
    ]

    return {
        "templates": templates,
        "count": len(templates),
        "timestamp": datetime.utcnow(),
    }
