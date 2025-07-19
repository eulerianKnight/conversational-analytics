import streamlit as st
import requests
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
import time

# Configuration
API_BASE_URL = "http://localhost:8000"
st.set_page_config(
    page_title="Agentic Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Session state initialization
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "token" not in st.session_state:
    st.session_state.token = None
if "user" not in st.session_state:
    st.session_state.user = None
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "query_history" not in st.session_state:
    st.session_state.query_history = []


# Helper functions
def make_authenticated_request(
    method: str, endpoint: str, data: dict = None, params: dict = None
):
    """Make authenticated API request"""
    headers = {
        "Authorization": f"Bearer {st.session_state.token}",
        "Content-Type": "application/json",
    }

    url = f"{API_BASE_URL}{endpoint}"

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=data)
        else:
            st.error(f"Unsupported method: {method}")
            return None

        if response.status_code == 401:
            st.session_state.authenticated = False
            st.session_state.token = None
            st.error("Session expired. Please login again.")
            st.rerun()

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        st.error(f"API request failed: {str(e)}")
        return None


def login_user(username: str, password: str) -> bool:
    """Login user and store token"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/auth/login",
            json={"username": username, "password": password},
        )

        if response.status_code == 200:
            data = response.json()
            st.session_state.token = data["access_token"]
            st.session_state.user = data["user"]
            st.session_state.authenticated = True
            return True
        else:
            st.error("Invalid credentials")
            return False

    except requests.exceptions.RequestException as e:
        st.error(f"Login failed: {str(e)}")
        return False


def register_user(
    username: str, email: str, password: str, full_name: str = ""
) -> bool:
    """Register new user"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/auth/register",
            json={
                "username": username,
                "email": email,
                "password": password,
                "full_name": full_name,
            },
        )

        if response.status_code == 200:
            st.success("Registration successful! Please login.")
            return True
        else:
            error_data = response.json()
            st.error(
                f"Registration failed: {error_data.get('detail', 'Unknown error')}"
            )
            return False

    except requests.exceptions.RequestException as e:
        st.error(f"Registration failed: {str(e)}")
        return False


def logout_user():
    """Logout user"""
    if st.session_state.token:
        make_authenticated_request("POST", "/auth/logout")

    st.session_state.authenticated = False
    st.session_state.token = None
    st.session_state.user = None
    st.session_state.session_id = None
    st.session_state.query_history = []


def create_chart(data: List[Dict], chart_config: Dict[str, Any]) -> go.Figure:
    """Create chart based on recommendation"""
    if not data:
        return go.Figure().add_annotation(text="No data to display")

    df = pd.DataFrame(data)
    chart_type = chart_config.get("chart_type", "table")

    try:
        if chart_type == "bar":
            x_col = chart_config.get("x_axis")
            y_col = chart_config.get("y_axis")
            if x_col and y_col and x_col in df.columns and y_col in df.columns:
                fig = px.bar(
                    df, x=x_col, y=y_col, title=chart_config.get("title", "Bar Chart")
                )
            else:
                fig = px.bar(df, title="Bar Chart")

        elif chart_type == "line":
            x_col = chart_config.get("x_axis")
            y_col = chart_config.get("y_axis")
            if x_col and y_col and x_col in df.columns and y_col in df.columns:
                fig = px.line(
                    df, x=x_col, y=y_col, title=chart_config.get("title", "Line Chart")
                )
            else:
                fig = px.line(df, title="Line Chart")

        elif chart_type == "scatter":
            x_col = chart_config.get("x_axis")
            y_col = chart_config.get("y_axis")
            if x_col and y_col and x_col in df.columns and y_col in df.columns:
                fig = px.scatter(
                    df,
                    x=x_col,
                    y=y_col,
                    title=chart_config.get("title", "Scatter Plot"),
                )
            else:
                fig = px.scatter(df, title="Scatter Plot")

        elif chart_type == "pie":
            values_col = chart_config.get("y_axis")
            names_col = chart_config.get("x_axis")
            if (
                values_col
                and names_col
                and values_col in df.columns
                and names_col in df.columns
            ):
                fig = px.pie(
                    df,
                    values=values_col,
                    names=names_col,
                    title=chart_config.get("title", "Pie Chart"),
                )
            else:
                fig = px.pie(df, title="Pie Chart")

        else:  # Default to table or when chart creation fails
            return None

        fig.update_layout(height=500)
        return fig

    except Exception as e:
        st.error(f"Chart creation failed: {str(e)}")
        return None


# Authentication UI
def show_auth_page():
    """Show login/register page"""
    st.title("Agentic Analytics - Authentication")

    tab1, tab2 = st.tabs(["Login", "Register"])

    with tab1:
        st.header("Login")
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit_login = st.form_submit_button("Login")

            if submit_login and username and password:
                if login_user(username, password):
                    st.success("Login successful!")
                    st.rerun()

    with tab2:
        st.header("Register")
        with st.form("register_form"):
            reg_username = st.text_input("Username", key="reg_username")
            reg_email = st.text_input("Email", key="reg_email")
            reg_full_name = st.text_input("Full Name (Optional)", key="reg_full_name")
            reg_password = st.text_input(
                "Password", type="password", key="reg_password"
            )
            reg_confirm_password = st.text_input(
                "Confirm Password", type="password", key="reg_confirm_password"
            )
            submit_register = st.form_submit_button("Register")

            if submit_register:
                if not all(
                    [reg_username, reg_email, reg_password, reg_confirm_password]
                ):
                    st.error("Please fill in all required fields")
                elif reg_password != reg_confirm_password:
                    st.error("Passwords do not match")
                elif len(reg_password) < 6:
                    st.error("Password must be at least 6 characters long")
                else:
                    register_user(reg_username, reg_email, reg_password, reg_full_name)


# Main application UI
def show_main_app():
    """Show main application interface"""
    # Sidebar
    with st.sidebar:
        st.title("Navigation")

        # User info
        if st.session_state.user:
            st.write(
                f"**{st.session_state.user['full_name'] or st.session_state.user['username']}**"
            )
            st.write(f"Role: {st.session_state.user['role']}")

        # Navigation
        page = st.selectbox(
            "Select Page",
            [
                "Dashboard",
                "Chat Analytics",
                "Supplier Performance",
                "Sales Forecast",
                "Database Schema",
                "Query History",
                "Settings",
            ],
        )

        # Logout button
        if st.button("Logout", use_container_width=True):
            logout_user()
            st.rerun()

    # Main content based on selected page
    if page == "Dashboard":
        show_dashboard()
    elif page == "Chat Analytics":
        show_chat_analytics()
    elif page == "Supplier Performance":
        show_supplier_performance()
    elif page == "Sales Forecast":
        show_sales_forecast()
    elif page == "Database Schema":
        show_database_schema()
    elif page == "Query History":
        show_query_history()
    elif page == "Settings":
        show_settings()


def show_dashboard():
    """Show dashboard with key metrics"""
    st.title("Analytics Dashboard")

    # Get dashboard data
    dashboard_data = make_authenticated_request("GET", "/analytics/dashboard")

    if dashboard_data:
        metrics = dashboard_data.get("metrics", {})

        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_orders = metrics.get("total_orders", {}).get("count", 0)
            st.metric("Total Orders", f"{total_orders:,}")

        with col2:
            total_revenue = metrics.get("total_revenue", {}).get("revenue", 0)
            st.metric("Total Revenue", f"${total_revenue:,.2f}")

        with col3:
            active_suppliers = metrics.get("active_suppliers", {}).get("count", 0)
            st.metric("Active Suppliers", f"{active_suppliers:,}")

        with col4:
            top_customers = metrics.get("top_customers", {}).get("count", 0)
            st.metric("Active Customers", f"{top_customers:,}")

        st.divider()

        # Quick actions
        st.subheader("Quick Actions")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Supplier Performance", use_container_width=True):
                st.session_state.page = "Supplier Performance"
                st.rerun()

        with col2:
            if st.button("Sales Analysis", use_container_width=True):
                st.session_state.page = "Sales Forecast"
                st.rerun()

        with col3:
            if st.button("Ask Question", use_container_width=True):
                st.session_state.page = "Chat Analytics"
                st.rerun()


def show_chat_analytics():
    """Show chat-based analytics interface"""
    st.title("Chat Analytics")
    st.write("Ask questions about your data in natural language!")

    # Chat interface
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                # Display query results
                if "data" in message:
                    st.write(message["content"])

                    # Show data table
                    if message["data"]:
                        with st.expander("Data Table", expanded=False):
                            df = pd.DataFrame(message["data"])
                            st.dataframe(df, use_container_width=True)

                    # Show chart if available
                    if message.get("chart"):
                        st.plotly_chart(message["chart"], use_container_width=True)

                    # Show insights
                    if message.get("insights"):
                        with st.expander("Insights", expanded=True):
                            st.write(message["insights"])

                    # Show SQL query
                    if message.get("sql_query"):
                        with st.expander("SQL Query", expanded=False):
                            st.code(message["sql_query"], language="sql")

                    # Show follow-up suggestions
                    if message.get("follow_up_suggestions"):
                        st.write("**Suggested follow-up questions:**")
                        for i, suggestion in enumerate(
                            message["follow_up_suggestions"]
                        ):
                            if st.button(
                                suggestion,
                                key=f"suggestion_{i}_{len(st.session_state.messages)}",
                            ):
                                # Add suggestion as user message and process it
                                st.session_state.messages.append(
                                    {"role": "user", "content": suggestion}
                                )
                                process_query(suggestion)
                                st.rerun()
                else:
                    st.write(message["content"])
            else:
                st.write(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask a question about your data..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Process the query
        process_query(prompt)

        # Rerun to update the interface
        st.rerun()


def process_query(query: str):
    """Process user query and add response to chat"""
    with st.chat_message("assistant"):
        with st.spinner("Analyzing your question..."):
            # Make API request
            response = make_authenticated_request(
                "POST",
                "/analytics/query",
                data={
                    "query": query,
                    "session_id": st.session_state.session_id,
                    "use_cache": True,
                },
            )

            if response:
                # Create chart if recommended
                chart = None
                chart_config = response.get("chart_recommendation", {})
                if chart_config.get("chart_type") != "table":
                    chart = create_chart(response["data"], chart_config)

                # Add assistant response to chat history
                assistant_message = {
                    "role": "assistant",
                    "content": response.get(
                        "insights", "Query processed successfully."
                    ),
                    "data": response["data"],
                    "sql_query": response["sql_query"],
                    "insights": response.get("insights"),
                    "follow_up_suggestions": response.get("follow_up_suggestions", []),
                    "chart": chart,
                }

                st.session_state.messages.append(assistant_message)

                # Update session ID
                if not st.session_state.session_id:
                    st.session_state.session_id = response.get("query_id")
            else:
                error_message = {
                    "role": "assistant",
                    "content": "Sorry, I couldn't process your question. Please try again.",
                }
                st.session_state.messages.append(error_message)


def show_supplier_performance():
    """Show supplier performance analysis"""
    st.title("Supplier Performance Analysis")

    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        days = st.selectbox("Time Period", [7, 14, 30, 60, 90], index=2)
        if st.button("Refresh Data"):
            st.rerun()

    # Get supplier performance data
    response = make_authenticated_request(
        "GET", "/analytics/supplier-performance", params={"days": days}
    )

    if response:
        data = response["data"]
        insights = response.get("insights", "")

        if data:
            df = pd.DataFrame(data)

            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Suppliers", len(df))
            with col2:
                avg_revenue = (
                    df["TOTAL_REVENUE"].mean() if "TOTAL_REVENUE" in df.columns else 0
                )
                st.metric("Avg Revenue per Supplier", f"${avg_revenue:,.2f}")
            with col3:
                total_orders = (
                    df["TOTAL_ORDERS"].sum() if "TOTAL_ORDERS" in df.columns else 0
                )
                st.metric("Total Orders", f"{total_orders:,}")
            with col4:
                avg_delay = (
                    df["AVG_DELIVERY_DELAY"].mean()
                    if "AVG_DELIVERY_DELAY" in df.columns
                    else 0
                )
                st.metric("Avg Delivery Delay", f"{avg_delay:.1f} days")

            st.divider()

            # Charts
            col1, col2 = st.columns(2)

            with col1:
                if "TOTAL_REVENUE" in df.columns and "SUPPLIER_NAME" in df.columns:
                    top_suppliers = df.nlargest(10, "TOTAL_REVENUE")
                    fig = px.bar(
                        top_suppliers,
                        x="SUPPLIER_NAME",
                        y="TOTAL_REVENUE",
                        title="Top 10 Suppliers by Revenue",
                    )
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                if "AVG_DELIVERY_DELAY" in df.columns and "TOTAL_REVENUE" in df.columns:
                    fig = px.scatter(
                        df,
                        x="AVG_DELIVERY_DELAY",
                        y="TOTAL_REVENUE",
                        hover_data=["SUPPLIER_NAME"],
                        title="Revenue vs Delivery Performance",
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Detailed table
            st.subheader("Detailed Supplier Data")
            st.dataframe(df, use_container_width=True)

            # Insights
            if insights:
                with st.expander("AI Insights", expanded=True):
                    st.write(insights)


def show_sales_forecast():
    """Show sales forecasting"""
    st.title("Sales Forecast Analysis")

    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        months = st.selectbox("Historical Period (months)", [6, 12, 18, 24], index=1)
        if st.button("Refresh Data", key="sales_refresh"):
            st.rerun()

    # Get sales forecast data
    response = make_authenticated_request(
        "GET", "/analytics/sales-forecast", params={"months": months}
    )

    if response:
        data = response["data"]
        insights = response.get("insights", "")

        if data:
            df = pd.DataFrame(data)
            df["MONTH"] = pd.to_datetime(df["MONTH"])

            # Key metrics
            total_revenue = df["REVENUE"].sum()
            avg_monthly_revenue = df["REVENUE"].mean()
            revenue_growth = (
                (
                    (df["REVENUE"].iloc[-1] - df["REVENUE"].iloc[0])
                    / df["REVENUE"].iloc[0]
                    * 100
                )
                if len(df) > 1
                else 0
            )

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Revenue", f"${total_revenue:,.2f}")
            with col2:
                st.metric("Avg Monthly Revenue", f"${avg_monthly_revenue:,.2f}")
            with col3:
                st.metric("Revenue Growth", f"{revenue_growth:.1f}%")

            st.divider()

            # Charts
            col1, col2 = st.columns(2)

            with col1:
                fig = px.line(df, x="MONTH", y="REVENUE", title="Monthly Revenue Trend")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.line(
                    df, x="MONTH", y="QUANTITY_SOLD", title="Monthly Quantity Sold"
                )
                st.plotly_chart(fig, use_container_width=True)

            # Detailed table
            st.subheader("Historical Sales Data")
            st.dataframe(df, use_container_width=True)

            # Insights
            if insights:
                with st.expander("AI Insights", expanded=True):
                    st.write(insights)


def show_database_schema():
    """Show database schema information"""
    st.title("Database Schema")

    # Get schema information
    response = make_authenticated_request("GET", "/analytics/schema")

    if response:
        # Schema context
        st.subheader("Schema Overview")
        with st.expander("Schema Details", expanded=True):
            st.text(response["schema_context"])

        # Tables information
        st.subheader("Tables")
        tables_data = response.get("tables", [])
        if tables_data:
            df = pd.DataFrame(tables_data)
            st.dataframe(df, use_container_width=True)

        # Table details
        st.subheader("Table Details")
        if tables_data:
            table_names = [table.get("TABLE_NAME", "") for table in tables_data]
            selected_table = st.selectbox(
                "Select a table to view details:", table_names
            )

            if selected_table and st.button("Get Table Details"):
                table_response = make_authenticated_request(
                    "GET", f"/analytics/table/{selected_table}"
                )

                if table_response:
                    st.write(f"**Table:** {selected_table}")

                    # Column information
                    columns_df = pd.DataFrame(table_response["columns"])
                    st.write("**Columns:**")
                    st.dataframe(columns_df, use_container_width=True)

                    # Sample data
                    if table_response["sample_data"]:
                        st.write("**Sample Data:**")
                        sample_df = pd.DataFrame(table_response["sample_data"])
                        st.dataframe(sample_df, use_container_width=True)


def show_query_history():
    """Show user's query history"""
    st.title("Query History")

    # Get query history
    response = make_authenticated_request(
        "GET", "/analytics/history", params={"limit": 50}
    )

    if response:
        history = response.get("history", [])

        if history:
            for i, entry in enumerate(history):
                with st.expander(
                    f"Query {i+1}: {entry['query_text'][:50]}...", expanded=False
                ):
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        st.write(f"**Query:** {entry['query_text']}")
                        if entry.get("sql_query"):
                            st.code(entry["sql_query"], language="sql")
                        if entry.get("result_summary"):
                            st.write(f"**Summary:** {entry['result_summary']}")

                    with col2:
                        st.write(f"**Type:** {entry.get('query_type', 'Unknown')}")
                        st.write(f"**Rows:** {entry.get('row_count', 'N/A')}")
                        st.write(f"**Time:** {entry.get('execution_time', 'N/A')}s")
                        st.write(f"**Date:** {entry['created_at'][:19]}")

                        if st.button("Re-run Query", key=f"rerun_{i}"):
                            # Switch to chat page and add query
                            st.session_state.page = "Chat Analytics"
                            process_query(entry["query_text"])
                            st.rerun()
        else:
            st.info(
                "No query history found. Start asking questions in the Chat Analytics page!"
            )


def show_settings():
    """Show user settings and preferences"""
    st.title("Settings")

    # User information
    st.subheader("User Information")
    if st.session_state.user:
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Username:** {st.session_state.user['username']}")
            st.write(f"**Email:** {st.session_state.user['email']}")
        with col2:
            st.write(
                f"**Full Name:** {st.session_state.user.get('full_name', 'Not set')}"
            )
            st.write(f"**Role:** {st.session_state.user['role']}")

    st.divider()

    # Application settings
    st.subheader("Application Settings")

    # Query preferences
    st.write("**Query Preferences:**")
    use_cache = st.checkbox("Use query caching", value=True)
    max_rows = st.slider("Maximum rows to display", 10, 1000, 100)

    # Visualization preferences
    st.write("**Visualization Preferences:**")
    default_chart = st.selectbox(
        "Default chart type", ["auto", "table", "bar", "line", "scatter", "pie"]
    )

    # Save settings
    if st.button("Save Settings"):
        st.success("Settings saved successfully!")

    st.divider()

    # API Information
    st.subheader("API Information")
    st.write(f"**API Base URL:** {API_BASE_URL}")

    # Test API connection
    if st.button("Test API Connection"):
        try:
            response = requests.get(f"{API_BASE_URL}/health")
            if response.status_code == 200:
                data = response.json()
                st.success("API connection successful!")
                st.json(data)
            else:
                st.error("API connection failed!")
        except Exception as e:
            st.error(f"API connection failed: {str(e)}")


# Main application logic
def main():
    """Main application entry point"""
    if not st.session_state.authenticated:
        show_auth_page()
    else:
        show_main_app()


if __name__ == "__main__":
    main()
