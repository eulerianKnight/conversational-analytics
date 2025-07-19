from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from datetime import datetime, timedelta
import jwt
import uuid
from typing import Optional
from app.core.config import settings
from app.core.database import db_manager

router = APIRouter()
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# Pydantic models
class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    full_name: Optional[str] = None


class UserLogin(BaseModel):
    username: str
    password: str


class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    full_name: Optional[str]
    role: str
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime]


class Token(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    user: UserResponse


# Helper functions
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM
    )
    return encoded_jwt


def get_user_by_username(username: str) -> Optional[dict]:
    query = "SELECT * FROM users WHERE username = ?"
    users = db_manager.execute_query(query, (username,))
    return users[0] if users else None


def get_user_by_email(email: str) -> Optional[dict]:
    query = "SELECT * FROM users WHERE email = ?"
    users = db_manager.execute_query(query, (email,))
    return users[0] if users else None


def create_user(user_data: UserCreate) -> dict:
    hashed_password = get_password_hash(user_data.password)

    query = """
        INSERT INTO users (username, email, password_hash, full_name)
        VALUES (?, ?, ?, ?)
    """

    db_manager.execute_non_query(
        query,
        (user_data.username, user_data.email, hashed_password, user_data.full_name),
    )

    return get_user_by_username(user_data.username)


def authenticate_user(username: str, password: str) -> Optional[dict]:
    user = get_user_by_username(username)
    if not user:
        return None
    if not verify_password(password, user["password_hash"]):
        return None
    return user


def update_last_login(user_id: int):
    query = "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?"
    db_manager.execute_non_query(query, (user_id,))


# API Endpoints
@router.post("/register", response_model=UserResponse)
async def register(user_data: UserCreate):
    # Check if username exists
    if get_user_by_username(user_data.username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered",
        )

    # Check if email exists
    if get_user_by_email(user_data.email):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered"
        )

    # Create user
    try:
        user = create_user(user_data)
        return UserResponse(**user)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create user: {str(e)}",
        )


@router.post("/login", response_model=Token)
async def login(user_credentials: UserLogin):
    user = authenticate_user(user_credentials.username, user_credentials.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user"
        )

    # Update last login
    update_last_login(user["id"])

    # Create access token
    access_token = create_access_token(data={"sub": user["username"]})

    # Create session
    session_id = str(uuid.uuid4())
    session_query = """
        INSERT INTO user_sessions (user_id, session_id, expires_at)
        VALUES (?, ?, ?)
    """
    expires_at = datetime.utcnow() + timedelta(
        minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
    )
    db_manager.execute_non_query(session_query, (user["id"], session_id, expires_at))

    return Token(
        access_token=access_token,
        token_type="bearer",
        expires_in=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        user=UserResponse(**user),
    )


@router.get("/me", response_model=UserResponse)
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
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
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired"
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )

    user = get_user_by_username(username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    return UserResponse(**user)


@router.post("/logout")
async def logout(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        username: str = payload.get("sub")
        user = get_user_by_username(username)

        if user:
            # Deactivate all sessions for this user
            query = "UPDATE user_sessions SET is_active = FALSE WHERE user_id = ?"
            db_manager.execute_non_query(query, (user["id"],))

        return {"message": "Successfully logged out"}

    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


@router.get("/users", response_model=list[UserResponse])
async def list_users(credentials: HTTPAuthorizationCredentials = Depends(security)):
    # Verify admin access
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        username: str = payload.get("sub")
        current_user = get_user_by_username(username)

        if not current_user or current_user["role"] != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required"
            )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )

    query = "SELECT * FROM users ORDER BY created_at DESC"
    users = db_manager.execute_query(query)
    return [UserResponse(**user) for user in users]
