# backend/main.py
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import asynccontextmanager
import uvicorn
import os
from datetime import datetime, timedelta
import jwt
from passlib.context import CryptContext

from app.core.config import settings
from app.core.database import init_db
from app.api.endpoints import auth, analytics, queries, alerts
from app.services.snowflake_service import SnowflakeService
from app.services.claude_service import ClaudeService

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    print("Application started successfully!")
    yield
    # Shutdown
    print("Application shutting down...")

app = FastAPI(
    title="Agentic Analytics API",
    description="AI-powered analytics with Text-to-SQL and visualizations",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/auth", tags=["authentication"])
app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
app.include_router(queries.router, prefix="/queries", tags=["queries"])
app.include_router(alerts.router, prefix="/alerts", tags=["alerts"])

# Global services
snowflake_service = SnowflakeService()
claude_service = ClaudeService()

@app.get("/")
async def root():
    return {
        "message": "Agentic Analytics API", 
        "status": "active",
        "timestamp": datetime.now()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connections
        sf_status = await snowflake_service.test_connection()
        claude_status = claude_service.test_connection()
        
        return {
            "status": "healthy",
            "services": {
                "snowflake": sf_status,
                "claude": claude_status,
                "api": "active"
            },
            "timestamp": datetime.utcnow()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Health check failed: {str(e)}"
        )

# JWT token verification
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(
            credentials.credentials, 
            settings.SECRET_KEY, 
            algorithms=[settings.ALGORITHM]
        )
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials"
            )
        return username
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )