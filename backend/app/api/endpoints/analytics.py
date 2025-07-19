from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import jwt
import uuid
import time
import json
from datetime import datetime

from app.core.config import settings
from app.core.database import db_manager, memory_manager
from app.services.snowflake_service import SnowflakeService
from app.services.claude_service import ClaudeService
from app.api.endpoints.auth import get_user_by_username

router = APIRouter()
security = HTTPBearer()
snowflake_service = SnowflakeService()
claude_service = ClaudeService()


# Pydantic models
class QueryRequest(BaseModel):
    query: str
    session_id: Optional[str] = None
    use_cache: bool = True


class QueryResponse(BaseModel):
    query_id: str
    original_query: str
    sql_query: str
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    insights: str
    chart_recommendation: Dict[str, Any]
    follow_up_suggestions: List[str]
    execution_time: float
    from_cache: bool
    timestamp: datetime


class SupplierMetricsRequest(BaseModel):
    days: int = 30
    top_n: int = 20


class ForecastRequest(BaseModel):
    months: int = 12
    forecast_periods: int = 6


# Helper functions
async def get_current_user_from_token(credentials: HTTPAuthorizationCredentials):
    try:
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


# API Endpoints
@router.post("/query", response_model=QueryResponse)
async def execute_natural_language_query(
    request: QueryRequest, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Convert natural language to SQL and execute query"""
    user = await get_current_user_from_token(credentials)
    start_time = time.time()

    # Generate session ID if not provided
    session_id = request.session_id or str(uuid.uuid4())

    try:
        # Get conversation context
        context = memory_manager.get_recent_context(user["id"], session_id)

        # Convert text to SQL using Claude
        sql_result = await claude_service.convert_text_to_sql(request.query, context)

        if sql_result.get("error"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to generate SQL: {sql_result['error']}",
            )

        sql_query = sql_result["sql_query"]

        # Execute SQL query
        query_result = await snowflake_service.execute_query(
            sql_query, request.use_cache
        )

        # Generate insights
        insights = await claude_service.generate_insights(query_result, request.query)

        # Get chart recommendations
        chart_recommendation = await claude_service.generate_chart_recommendation(
            query_result, request.query
        )

        # Get follow-up suggestions
        follow_up_suggestions = await claude_service.suggest_follow_up_queries(
            request.query, query_result
        )

        total_execution_time = time.time() - start_time
        query_id = str(uuid.uuid4())

        # Store in memory
        memory_manager.store_conversation(
            user_id=user["id"],
            session_id=session_id,
            query_text=request.query,
            sql_query=sql_query,
            result_summary=insights[:500],  # Store truncated summary
            query_type=sql_result.get("query_type", "general"),
            execution_time=total_execution_time,
            row_count=len(query_result["data"]),
        )

        response = QueryResponse(
            query_id=query_id,
            original_query=request.query,
            sql_query=sql_query,
            data=query_result["data"],
            metadata=query_result["metadata"],
            insights=insights,
            chart_recommendation=chart_recommendation,
            follow_up_suggestions=follow_up_suggestions,
            execution_time=total_execution_time,
            from_cache=query_result.get("from_cache", False),
            timestamp=datetime.utcnow(),
        )

        return response

    except Exception as e:
        # Store failed query in memory for learning
        memory_manager.store_conversation(
            user_id=user["id"],
            session_id=session_id,
            query_text=request.query,
            result_summary=f"Error: {str(e)}",
            query_type="error",
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query execution failed: {str(e)}",
        )


@router.get("/supplier-performance")
async def get_supplier_performance(
    days: int = 30, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get supplier performance metrics"""
    user = await get_current_user_from_token(credentials)

    try:
        result = await snowflake_service.get_supplier_performance_metrics(days)

        # Generate insights for supplier performance
        insights = await claude_service.generate_insights(
            result, f"Supplier performance analysis for the last {days} days"
        )

        return {
            "data": result["data"],
            "metadata": result["metadata"],
            "insights": insights,
            "period_days": days,
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get supplier performance: {str(e)}",
        )


@router.get("/sales-forecast")
async def get_sales_forecast_data(
    months: int = 12, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get historical sales data for forecasting"""
    user = await get_current_user_from_token(credentials)

    try:
        result = await snowflake_service.get_sales_forecast_data(months)

        # Generate insights for sales data
        insights = await claude_service.generate_insights(
            result,
            f"Historical sales data for the last {months} months for forecasting",
        )

        return {
            "data": result["data"],
            "metadata": result["metadata"],
            "insights": insights,
            "period_months": months,
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get sales forecast data: {str(e)}",
        )


@router.get("/schema")
async def get_database_schema(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get database schema information"""
    user = await get_current_user_from_token(credentials)

    try:
        schema_info = snowflake_service.get_schema_context()
        table_info = await snowflake_service.get_table_info()

        return {
            "schema_context": schema_info,
            "tables": table_info["data"],
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schema: {str(e)}",
        )


@router.get("/table/{table_name}")
async def get_table_details(
    table_name: str, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get detailed information about a specific table"""
    user = await get_current_user_from_token(credentials)

    try:
        table_info = await snowflake_service.get_table_info(table_name)
        sample_data = await snowflake_service.get_sample_data(table_name, 5)

        return {
            "table_name": table_name,
            "columns": table_info["data"],
            "sample_data": sample_data["data"],
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get table details: {str(e)}",
        )


@router.get("/history")
async def get_query_history(
    session_id: Optional[str] = None,
    limit: int = 20,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get user's query history"""
    user = await get_current_user_from_token(credentials)

    try:
        history = memory_manager.get_conversation_history(user["id"], session_id, limit)

        return {
            "history": history,
            "count": len(history),
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get query history: {str(e)}",
        )


@router.post("/validate-sql")
async def validate_sql_query(
    sql_query: str, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Validate SQL query without executing it"""
    user = await get_current_user_from_token(credentials)

    try:
        is_valid, message = snowflake_service.validate_sql_query(sql_query)

        # Get performance analysis if valid
        performance_analysis = None
        if is_valid:
            performance_analysis = await snowflake_service.analyze_query_performance(
                sql_query
            )

        return {
            "is_valid": is_valid,
            "message": message,
            "performance_analysis": performance_analysis,
            "timestamp": datetime.utcnow(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to validate query: {str(e)}",
        )


@router.get("/dashboard")
async def get_dashboard_data(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get dashboard summary data"""
    user = await get_current_user_from_token(credentials)

    try:
        # Get key metrics for dashboard
        queries = [
            ("total_orders", "SELECT COUNT(*) as count FROM ORDERS"),
            ("total_revenue", "SELECT SUM(TOTALPRICE) as revenue FROM ORDERS"),
            (
                "active_suppliers",
                "SELECT COUNT(DISTINCT SUPPKEY) as count FROM LINEITEM WHERE SHIPDATE >= DATEADD(month, -1, CURRENT_DATE)",
            ),
            (
                "top_customers",
                "SELECT COUNT(DISTINCT CUSTKEY) as count FROM ORDERS WHERE ORDERDATE >= DATEADD(month, -1, CURRENT_DATE)",
            ),
        ]

        dashboard_data = {}
        for metric_name, query in queries:
            try:
                result = await snowflake_service.execute_query(query)
                dashboard_data[metric_name] = (
                    result["data"][0] if result["data"] else {}
                )
            except Exception as e:
                dashboard_data[metric_name] = {"error": str(e)}

        return {"metrics": dashboard_data, "timestamp": datetime.utcnow()}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get dashboard data: {str(e)}",
        )
