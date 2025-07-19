# backend/app/core/config.py
from pydantic import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    # API Configuration
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-super-secret-key-change-this")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24 hours
    
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    SNOWFLAKE_USER: str = os.getenv("SNOWFLAKE_USER", "")
    SNOWFLAKE_PASSWORD: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    SNOWFLAKE_DATABASE: str = os.getenv("SNOWFLAKE_DATABASE", "")
    SNOWFLAKE_SCHEMA: str = os.getenv("SNOWFLAKE_SCHEMA", "")
    SNOWFLAKE_WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    SNOWFLAKE_ROLE: str = os.getenv("SNOWFLAKE_ROLE", "")
    
    # Claude API Configuration
    CLAUDE_API_KEY: str = os.getenv("CLAUDE_API_KEY", "")
    CLAUDE_MODEL: str = "claude-3-sonnet-20240229"
    
    # SQLite Configuration
    SQLITE_DB_PATH: str = "data/analytics.db"
    
    # Cache Configuration
    CACHE_TTL_SECONDS: int = 3600  # 1 hour
    MAX_CACHE_SIZE: int = 1000
    
    # Analytics Configuration
    MAX_QUERY_ROWS: int = 100000  # Limit for safety
    QUERY_TIMEOUT_SECONDS: int = 300  # 5 minutes
    
    # Alert Configuration
    SMTP_SERVER: str = os.getenv("SMTP_SERVER", "")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME: str = os.getenv("SMTP_USERNAME", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")
    
    # Performance Configuration
    CONNECTION_POOL_SIZE: int = 10
    MAX_OVERFLOW: int = 20
    POOL_TIMEOUT: int = 30
    POOL_RECYCLE: int = 3600
    
    class Config:
        env_file = ".env"

# Global settings instance
settings = Settings()

# Schema mapping for the supply chain database
SCHEMA_INFO = {
    "tables": {
        "PART": {
            "columns": ["PARTKEY", "NAME", "MFGR", "BRAND", "TYPE", "SIZE", "CONTAINER", "RETAILPRICE", "COMMENT"],
            "primary_key": "PARTKEY",
            "description": "Parts catalog with specifications and pricing"
        },
        "SUPPLIER": {
            "columns": ["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "COMMENT"],
            "primary_key": "SUPPKEY",
            "foreign_keys": {"NATIONKEY": "NATION.NATIONKEY"},
            "description": "Supplier information and contact details"
        },
        "PARTSUPP": {
            "columns": ["PARTKEY", "SUPPKEY", "AVAILQTY", "SUPPLYCOST", "COMMENT"],
            "primary_key": ["PARTKEY", "SUPPKEY"],
            "foreign_keys": {"PARTKEY": "PART.PARTKEY", "SUPPKEY": "SUPPLIER.SUPPKEY"},
            "description": "Part-supplier relationships with availability and costs"
        },
        "CUSTOMER": {
            "columns": ["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "MKTSEGMENT", "COMMENT"],
            "primary_key": "CUSTKEY",
            "foreign_keys": {"NATIONKEY": "NATION.NATIONKEY"},
            "description": "Customer information and market segmentation"
        },
        "ORDERS": {
            "columns": ["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE", "ORDERPRIORITY", "CLERK", "SHIPPRIORITY", "COMMENT"],
            "primary_key": "ORDERKEY",
            "foreign_keys": {"CUSTKEY": "CUSTOMER.CUSTKEY"},
            "description": "Order header information"
        },
        "LINEITEM": {
            "columns": ["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT", "SHIPMODE", "COMMENT"],
            "primary_key": ["ORDERKEY", "LINENUMBER"],
            "foreign_keys": {
                "ORDERKEY": "ORDERS.ORDERKEY",
                "PARTKEY": "PART.PARTKEY",
                "SUPPKEY": "SUPPLIER.SUPPKEY"
            },
            "description": "Detailed line items for each order (6M+ rows)"
        },
        "NATION": {
            "columns": ["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"],
            "primary_key": "NATIONKEY",
            "foreign_keys": {"REGIONKEY": "REGION.REGIONKEY"},
            "description": "Nation/country reference data"
        },
        "REGION": {
            "columns": ["REGIONKEY", "NAME", "COMMENT"],
            "primary_key": "REGIONKEY",
            "description": "Geographic regions"
        }
    },
    "relationships": [
        "SUPPLIER -> NATION (via NATIONKEY)",
        "CUSTOMER -> NATION (via NATIONKEY)",
        "NATION -> REGION (via REGIONKEY)",
        "ORDERS -> CUSTOMER (via CUSTKEY)",
        "LINEITEM -> ORDERS (via ORDERKEY)",
        "LINEITEM -> PART (via PARTKEY)",
        "LINEITEM -> SUPPLIER (via SUPPKEY)",
        "PARTSUPP -> PART (via PARTKEY)",
        "PARTSUPP -> SUPPLIER (via SUPPKEY)"
    ],
    "common_queries": [
        "Supplier performance analysis",
        "Sales forecasting by region",
        "Top customers by revenue",
        "Part demand analysis",
        "Order fulfillment metrics",
        "Geographic sales distribution",
        "Seasonal trends analysis",
        "Supply chain efficiency metrics"
    ]
}