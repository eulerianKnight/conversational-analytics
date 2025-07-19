import snowflake.connector
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
import logging
from app.core.config import settings, SCHEMA_INFO
from app.core.database import cache_manager

logger = logging.getLogger(__name__)

class SnowflakeService:
    def __init__(self):
        self.connection_params = {
            "account": settings.SNOWFLAKE_ACCOUNT,
            "user": settings.SNOWFLAKE_USER,
            "password": settings.SNOWFLAKE_PASSWORD,
            "database": settings.SNOWFLAKE_DATABASE,
            "schema": settings.SNOWFLAKE_SCHEMA,
            "warehouse": settings.SNOWFLAKE_WAREHOUSE,
            "role": settings.SNOWFLAKE_ROLE,
        }
        self.executor = ThreadPoolExecutor(max_workers=5)

    def _get_connection(self):
        """Create a new Snowflake connection"""
        try:
            conn = snowflake.connector.connect(**self.connection_params)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    async def test_connection(self) -> bool:
        """Test Snowflake connection"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor, self._test_connection_sync
            )
            return result
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

    def _test_connection_sync(self) -> bool:
        """Synchronous connection test"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception:
            return False

    async def execute_query(
        self, sql_query: str, use_cache: bool = True
    ) -> Dict[str, Any]:
        """Execute SQL query with caching and performance monitoring"""
        start_time = time.time()

        # Check cache first
        if use_cache:
            cached_result = cache_manager.get_cached_result(sql_query)
            if cached_result:
                return {
                    "data": eval(cached_result["data"]),
                    "metadata": eval(cached_result["metadata"]),
                    "cached": True,
                    "execution_time": 0,
                    "from_cache": True,
                }

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor, self._execute_query_sync, sql_query
            )

            execution_time = time.time() - start_time
            result["execution_time"] = execution_time
            result["cached"] = False
            result["from_cache"] = False

            # Cache the result
            if use_cache and result["data"]:
                cache_manager.cache_result(
                    sql_query, str(result["data"]), str(result["metadata"])
                )

            return result

        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise Exception(f"Database query failed: {str(e)}")

    def _execute_query_sync(self, sql_query: str) -> Dict[str, Any]:
        """Synchronous query execution"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Add row limit for safety
            if (
                not sql_query.upper()
                .strip()
                .startswith(("SELECT", "WITH", "SHOW", "DESCRIBE"))
            ):
                raise ValueError(
                    "Only SELECT, WITH, SHOW, and DESCRIBE queries are allowed"
                )

            # Add LIMIT if not present and it's a SELECT query
            if (
                sql_query.upper().strip().startswith("SELECT")
                and "LIMIT" not in sql_query.upper()
                and "TOP" not in sql_query.upper()
            ):
                sql_query += f" LIMIT {settings.MAX_QUERY_ROWS}"

            cursor.execute(sql_query)

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Fetch all results
            rows = cursor.fetchall()

            # Convert to list of dictionaries
            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))

            metadata = {
                "columns": columns,
                "row_count": len(data),
                "query": sql_query,
                "column_types": [desc[1].__name__ for desc in cursor.description],
            }

            return {"data": data, "metadata": metadata}

    async def get_table_info(self, table_name: str = None) -> Dict[str, Any]:
        """Get table information and schema details"""
        try:
            if table_name:
                # Get specific table info
                query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = '{settings.SNOWFLAKE_SCHEMA}' 
                    AND TABLE_NAME = '{table_name.upper()}'
                    ORDER BY ORDINAL_POSITION
                """
                result = await self.execute_query(query, use_cache=True)
                return result
            else:
                # Get all tables info
                query = f"""
                    SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{settings.SNOWFLAKE_SCHEMA}'
                    ORDER BY TABLE_NAME
                """
                result = await self.execute_query(query, use_cache=True)
                return result

        except Exception as e:
            logger.error(f"Failed to get table info: {str(e)}")
            raise

    async def get_sample_data(self, table_name: str, limit: int = 10) -> Dict[str, Any]:
        """Get sample data from a table"""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            result = await self.execute_query(query, use_cache=True)
            return result
        except Exception as e:
            logger.error(f"Failed to get sample data: {str(e)}")
            raise

    def validate_sql_query(self, sql_query: str) -> Tuple[bool, str]:
        """Validate SQL query for safety and correctness"""
        sql_upper = sql_query.upper().strip()

        # Check for forbidden operations
        forbidden_keywords = [
            "DELETE",
            "UPDATE",
            "INSERT",
            "DROP",
            "CREATE",
            "ALTER",
            "TRUNCATE",
            "GRANT",
            "REVOKE",
            "EXECUTE",
        ]

        for keyword in forbidden_keywords:
            if keyword in sql_upper:
                return False, f"Forbidden operation: {keyword}"

        # Check for required keywords
        if not any(
            keyword in sql_upper for keyword in ["SELECT", "WITH", "SHOW", "DESCRIBE"]
        ):
            return False, "Query must start with SELECT, WITH, SHOW, or DESCRIBE"

        # Basic syntax validation
        if sql_query.count("(") != sql_query.count(")"):
            return False, "Unmatched parentheses"

        if sql_query.count("'") % 2 != 0:
            return False, "Unmatched quotes"

        return True, "Valid query"

    def get_schema_context(self) -> str:
        """Get schema context for Claude API"""
        context = "Database Schema Information:\n\n"

        for table_name, table_info in SCHEMA_INFO["tables"].items():
            context += f"Table: {table_name}\n"
            context += f"Description: {table_info['description']}\n"
            context += f"Columns: {', '.join(table_info['columns'])}\n"
            context += f"Primary Key: {table_info['primary_key']}\n"

            if "foreign_keys" in table_info:
                context += f"Foreign Keys: {table_info['foreign_keys']}\n"

            context += "\n"

        context += "Relationships:\n"
        for relationship in SCHEMA_INFO["relationships"]:
            context += f"- {relationship}\n"

        context += "\nCommon Query Types:\n"
        for query_type in SCHEMA_INFO["common_queries"]:
            context += f"- {query_type}\n"

        return context

    async def analyze_query_performance(self, sql_query: str) -> Dict[str, Any]:
        """Analyze query performance and suggest optimizations"""
        try:
            # Get query plan
            explain_query = f"EXPLAIN {sql_query}"
            plan_result = await self.execute_query(explain_query, use_cache=False)

            # Simple performance analysis
            analysis = {
                "has_limit": "LIMIT" in sql_query.upper(),
                "uses_joins": "JOIN" in sql_query.upper(),
                "uses_aggregation": any(
                    func in sql_query.upper()
                    for func in ["SUM", "COUNT", "AVG", "MAX", "MIN"]
                ),
                "uses_groupby": "GROUP BY" in sql_query.upper(),
                "uses_orderby": "ORDER BY" in sql_query.upper(),
                "estimated_complexity": "medium",  # Simple heuristic
            }

            # Suggestions
            suggestions = []
            if not analysis["has_limit"]:
                suggestions.append("Consider adding LIMIT clause for large tables")

            if analysis["uses_joins"] and not analysis["has_limit"]:
                suggestions.append(
                    "JOIN operations on large tables should include LIMIT"
                )

            if "LINEITEM" in sql_query.upper() and not analysis["has_limit"]:
                suggestions.append("LINEITEM table has 6M+ rows, always use LIMIT")

            analysis["suggestions"] = suggestions
            analysis["query_plan"] = plan_result["data"]

            return analysis

        except Exception as e:
            logger.error(f"Performance analysis failed: {str(e)}")
            return {"error": str(e)}

    async def get_supplier_performance_metrics(self, days: int = 30) -> Dict[str, Any]:
        """Get supplier performance metrics for the specified period"""
        query = f"""
        SELECT 
            s.SUPPKEY,
            s.NAME as SUPPLIER_NAME,
            COUNT(DISTINCT l.ORDERKEY) as TOTAL_ORDERS,
            SUM(l.QUANTITY) as TOTAL_QUANTITY,
            SUM(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) as TOTAL_REVENUE,
            AVG(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) as AVG_ORDER_VALUE,
            AVG(DATEDIFF(day, l.SHIPDATE, l.COMMITDATE)) as AVG_DELIVERY_DELAY,
            COUNT(CASE WHEN l.SHIPDATE > l.COMMITDATE THEN 1 END) as LATE_DELIVERIES,
            s.ACCTBAL as ACCOUNT_BALANCE,
            n.NAME as NATION
        FROM SUPPLIER s
        JOIN LINEITEM l ON s.SUPPKEY = l.SUPPKEY
        JOIN NATION n ON s.NATIONKEY = n.NATIONKEY
        WHERE l.SHIPDATE >= DATEADD(day, -{days}, CURRENT_DATE)
        GROUP BY s.SUPPKEY, s.NAME, s.ACCTBAL, n.NAME
        ORDER BY TOTAL_REVENUE DESC
        LIMIT 100
        """

        return await self.execute_query(query)

    async def get_sales_forecast_data(self, months: int = 12) -> Dict[str, Any]:
        """Get historical sales data for forecasting"""
        query = f"""
        SELECT 
            DATE_TRUNC('month', l.SHIPDATE) as MONTH,
            SUM(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) as REVENUE,
            SUM(l.QUANTITY) as QUANTITY_SOLD,
            COUNT(DISTINCT l.ORDERKEY) as ORDERS_COUNT,
            COUNT(DISTINCT l.PARTKEY) as UNIQUE_PARTS,
            AVG(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) as AVG_ORDER_VALUE
        FROM LINEITEM l
        WHERE l.SHIPDATE >= DATEADD(month, -{months}, CURRENT_DATE)
        GROUP BY DATE_TRUNC('month', l.SHIPDATE)
        ORDER BY MONTH
        """

        return await self.execute_query(query)
