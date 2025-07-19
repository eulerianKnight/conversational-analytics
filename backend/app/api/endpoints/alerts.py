from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import smtplib
import requests
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import asyncio

from app.core.config import settings
from app.core.database import DatabaseManager

# Initialize database manager
db_manager = DatabaseManager()
from app.services.snowflake_service import SnowflakeService
from app.api.endpoints.auth import get_user_by_username

router = APIRouter()
security = HTTPBearer()
snowflake_service = SnowflakeService()


# Pydantic models
class AlertCreate(BaseModel):
    alert_name: str
    metric: str
    threshold_value: float
    condition: str  # '>', '<', '>=', '<=', '=', '!='
    notification_method: str  # 'email', 'slack', 'both'
    sql_query: str


class AlertResponse(BaseModel):
    id: int
    user_id: int
    alert_name: str
    metric: str
    threshold_value: float
    condition: str
    notification_method: str
    sql_query: str
    is_active: bool
    last_checked: Optional[datetime]
    last_triggered: Optional[datetime]
    trigger_count: int
    created_at: datetime


class AlertUpdate(BaseModel):
    alert_name: Optional[str] = None
    threshold_value: Optional[float] = None
    condition: Optional[str] = None
    notification_method: Optional[str] = None
    is_active: Optional[bool] = None


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


def send_email_notification(to_email: str, subject: str, body: str) -> bool:
    """Send email notification"""
    try:
        if not all(
            [settings.SMTP_SERVER, settings.SMTP_USERNAME, settings.SMTP_PASSWORD]
        ):
            return False

        msg = MIMEMultipart()
        msg["From"] = settings.SMTP_USERNAME
        msg["To"] = to_email
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "html"))

        server = smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT)
        server.starttls()
        server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        text = msg.as_string()
        server.sendmail(settings.SMTP_USERNAME, to_email, text)
        server.quit()

        return True
    except Exception as e:
        print(f"Email notification failed: {str(e)}")
        return False


def send_slack_notification(message: str) -> bool:
    """Send Slack notification"""
    try:
        if not settings.SLACK_WEBHOOK_URL:
            return False

        payload = {
            "text": message,
            "username": "Analytics Alert Bot",
            "icon_emoji": ":warning:",
        }

        response = requests.post(
            settings.SLACK_WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        return response.status_code == 200
    except Exception as e:
        print(f"Slack notification failed: {str(e)}")
        return False


async def check_alert_condition(alert: Dict[str, Any]) -> tuple[bool, float]:
    """Check if alert condition is met"""
    try:
        # Execute the alert's SQL query
        result = await snowflake_service.execute_query(alert["sql_query"])

        if not result["data"]:
            return False, 0

        # Extract the metric value (assume first row, first numeric column)
        data = result["data"][0]
        metric_value = None

        # Try to find the metric value
        for key, value in data.items():
            if isinstance(value, (int, float)):
                metric_value = float(value)
                break

        if metric_value is None:
            return False, 0

        # Check condition
        threshold = alert["threshold_value"]
        condition = alert["condition"]

        condition_met = False
        if condition == ">":
            condition_met = metric_value > threshold
        elif condition == "<":
            condition_met = metric_value < threshold
        elif condition == ">=":
            condition_met = metric_value >= threshold
        elif condition == "<=":
            condition_met = metric_value <= threshold
        elif condition == "=":
            condition_met = metric_value == threshold
        elif condition == "!=":
            condition_met = metric_value != threshold

        return condition_met, metric_value

    except Exception as e:
        print(f"Alert condition check failed: {str(e)}")
        return False, 0


async def process_triggered_alert(
    alert: Dict[str, Any], metric_value: float, user: Dict[str, Any]
):
    """Process a triggered alert by sending notifications"""
    try:
        # Create notification message
        message = f"""
        🚨 **Alert Triggered: {alert['alert_name']}**
        
        **Metric:** {alert['metric']}
        **Current Value:** {metric_value}
        **Threshold:** {alert['condition']} {alert['threshold_value']}
        **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        Query: {alert['sql_query'][:100]}...
        """

        # Send notifications
        notification_sent = False

        if alert["notification_method"] in ["email", "both"]:
            email_sent = send_email_notification(
                user["email"],
                f"Alert: {alert['alert_name']}",
                message.replace("\n", "<br>"),
            )
            notification_sent = notification_sent or email_sent

        if alert["notification_method"] in ["slack", "both"]:
            slack_sent = send_slack_notification(message)
            notification_sent = notification_sent or slack_sent

        # Record alert history
        history_query = """
            INSERT INTO alert_history (alert_id, metric_value, threshold_value, message, notification_sent)
            VALUES (?, ?, ?, ?, ?)
        """
        db_manager.execute_non_query(
            history_query,
            (
                alert["id"],
                metric_value,
                alert["threshold_value"],
                message,
                notification_sent,
            ),
        )

        # Update alert trigger info
        update_query = """
            UPDATE alerts 
            SET last_triggered = CURRENT_TIMESTAMP, trigger_count = trigger_count + 1
            WHERE id = ?
        """
        db_manager.execute_non_query(update_query, (alert["id"],))

        return notification_sent

    except Exception as e:
        print(f"Alert processing failed: {str(e)}")
        return False


# API Endpoints
@router.post("/", response_model=AlertResponse)
async def create_alert(
    alert_data: AlertCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Create a new alert"""
    user = await get_current_user_from_token(credentials)

    # Validate condition
    valid_conditions = [">", "<", ">=", "<=", "=", "!="]
    if alert_data.condition not in valid_conditions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid condition. Must be one of: {valid_conditions}",
        )

    # Validate notification method
    valid_methods = ["email", "slack", "both"]
    if alert_data.notification_method not in valid_methods:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid notification method. Must be one of: {valid_methods}",
        )

    # Validate SQL query
    is_valid, message = snowflake_service.validate_sql_query(alert_data.sql_query)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid SQL query: {message}",
        )

    try:
        # Insert alert
        query = """
            INSERT INTO alerts 
            (user_id, alert_name, metric, threshold_value, condition, notification_method, sql_query)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        db_manager.execute_non_query(
            query,
            (
                user["id"],
                alert_data.alert_name,
                alert_data.metric,
                alert_data.threshold_value,
                alert_data.condition,
                alert_data.notification_method,
                alert_data.sql_query,
            ),
        )

        # Get the created alert
        get_query = (
            "SELECT * FROM alerts WHERE user_id = ? ORDER BY created_at DESC LIMIT 1"
        )
        alerts = db_manager.execute_query(get_query, (user["id"],))

        if alerts:
            return AlertResponse(**alerts[0])
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create alert",
            )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create alert: {str(e)}",
        )


@router.get("/", response_model=List[AlertResponse])
async def get_user_alerts(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get all alerts for the current user"""
    user = await get_current_user_from_token(credentials)

    query = "SELECT * FROM alerts WHERE user_id = ? ORDER BY created_at DESC"
    alerts = db_manager.execute_query(query, (user["id"],))

    return [AlertResponse(**alert) for alert in alerts]


@router.get("/{alert_id}", response_model=AlertResponse)
async def get_alert(
    alert_id: int, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get a specific alert"""
    user = await get_current_user_from_token(credentials)

    query = "SELECT * FROM alerts WHERE id = ? AND user_id = ?"
    alerts = db_manager.execute_query(query, (alert_id, user["id"]))

    if not alerts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found"
        )

    return AlertResponse(**alerts[0])


@router.put("/{alert_id}", response_model=AlertResponse)
async def update_alert(
    alert_id: int,
    alert_update: AlertUpdate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Update an existing alert"""
    user = await get_current_user_from_token(credentials)

    # Check if alert exists and belongs to user
    query = "SELECT * FROM alerts WHERE id = ? AND user_id = ?"
    alerts = db_manager.execute_query(query, (alert_id, user["id"]))

    if not alerts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found"
        )

    # Build update query dynamically
    update_fields = []
    update_values = []

    if alert_update.alert_name is not None:
        update_fields.append("alert_name = ?")
        update_values.append(alert_update.alert_name)

    if alert_update.threshold_value is not None:
        update_fields.append("threshold_value = ?")
        update_values.append(alert_update.threshold_value)

    if alert_update.condition is not None:
        valid_conditions = [">", "<", ">=", "<=", "=", "!="]
        if alert_update.condition not in valid_conditions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid condition. Must be one of: {valid_conditions}",
            )
        update_fields.append("condition = ?")
        update_values.append(alert_update.condition)

    if alert_update.notification_method is not None:
        valid_methods = ["email", "slack", "both"]
        if alert_update.notification_method not in valid_methods:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid notification method. Must be one of: {valid_methods}",
            )
        update_fields.append("notification_method = ?")
        update_values.append(alert_update.notification_method)

    if alert_update.is_active is not None:
        update_fields.append("is_active = ?")
        update_values.append(alert_update.is_active)

    if not update_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update"
        )

    # Execute update
    update_query = (
        f"UPDATE alerts SET {', '.join(update_fields)} WHERE id = ? AND user_id = ?"
    )
    update_values.extend([alert_id, user["id"]])

    db_manager.execute_non_query(update_query, tuple(update_values))

    # Get updated alert
    updated_alerts = db_manager.execute_query(query, (alert_id, user["id"]))
    return AlertResponse(**updated_alerts[0])


@router.delete("/{alert_id}")
async def delete_alert(
    alert_id: int, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Delete an alert"""
    user = await get_current_user_from_token(credentials)

    # Check if alert exists and belongs to user
    query = "SELECT * FROM alerts WHERE id = ? AND user_id = ?"
    alerts = db_manager.execute_query(query, (alert_id, user["id"]))

    if not alerts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found"
        )

    # Delete alert (cascade will handle history)
    delete_query = "DELETE FROM alerts WHERE id = ? AND user_id = ?"
    db_manager.execute_non_query(delete_query, (alert_id, user["id"]))

    return {"message": "Alert deleted successfully"}


@router.post("/{alert_id}/test")
async def test_alert(
    alert_id: int, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Test an alert by checking its condition"""
    user = await get_current_user_from_token(credentials)

    # Get alert
    query = "SELECT * FROM alerts WHERE id = ? AND user_id = ?"
    alerts = db_manager.execute_query(query, (alert_id, user["id"]))

    if not alerts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found"
        )

    alert = alerts[0]

    try:
        # Check alert condition
        condition_met, metric_value = await check_alert_condition(alert)

        result = {
            "alert_id": alert_id,
            "condition_met": condition_met,
            "metric_value": metric_value,
            "threshold_value": alert["threshold_value"],
            "condition": alert["condition"],
            "timestamp": datetime.now(),
        }

        if condition_met:
            # Send test notification
            notification_sent = await process_triggered_alert(alert, metric_value, user)
            result["notification_sent"] = notification_sent

        return result

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Alert test failed: {str(e)}",
        )


@router.get("/{alert_id}/history")
async def get_alert_history(
    alert_id: int,
    limit: int = 50,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get alert trigger history"""
    user = await get_current_user_from_token(credentials)

    # Check if alert belongs to user
    alert_query = "SELECT * FROM alerts WHERE id = ? AND user_id = ?"
    alerts = db_manager.execute_query(alert_query, (alert_id, user["id"]))

    if not alerts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found"
        )

    # Get history
    history_query = """
        SELECT * FROM alert_history 
        WHERE alert_id = ? 
        ORDER BY triggered_at DESC 
        LIMIT ?
    """
    history = db_manager.execute_query(history_query, (alert_id, limit))

    return {"alert_id": alert_id, "history": history, "count": len(history)}


@router.post("/check-all")
async def check_all_alerts(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Check all active alerts (admin or system function)"""
    user = await get_current_user_from_token(credentials)

    # Get all active alerts
    query = "SELECT * FROM alerts WHERE is_active = TRUE"
    alerts = db_manager.execute_query(query)

    results = []

    for alert in alerts:
        try:
            # Update last_checked
            update_query = (
                "UPDATE alerts SET last_checked = CURRENT_TIMESTAMP WHERE id = ?"
            )
            db_manager.execute_non_query(update_query, (alert["id"],))

            # Check condition
            condition_met, metric_value = await check_alert_condition(alert)

            if condition_met:
                # Get alert owner
                user_query = "SELECT * FROM users WHERE id = ?"
                alert_users = db_manager.execute_query(user_query, (alert["user_id"],))

                if alert_users:
                    alert_user = alert_users[0]
                    notification_sent = await process_triggered_alert(
                        alert, metric_value, alert_user
                    )

                    results.append(
                        {
                            "alert_id": alert["id"],
                            "alert_name": alert["alert_name"],
                            "triggered": True,
                            "metric_value": metric_value,
                            "notification_sent": notification_sent,
                        }
                    )
            else:
                results.append(
                    {
                        "alert_id": alert["id"],
                        "alert_name": alert["alert_name"],
                        "triggered": False,
                        "metric_value": metric_value,
                    }
                )

        except Exception as e:
            results.append(
                {
                    "alert_id": alert["id"],
                    "alert_name": alert["alert_name"],
                    "error": str(e),
                }
            )

    return {
        "checked_count": len(alerts),
        "triggered_count": len([r for r in results if r.get("triggered", False)]),
        "results": results,
        "timestamp": datetime.now(),
    }
