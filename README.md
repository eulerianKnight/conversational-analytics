# Conversational Analytics with Snowflake

**A comprehensive analytics platform that combines natural language processing with SQL querying, featuring supplier performance monitoring, sales forecasting, and intelligent alerting.**

## Features

- **Natural Language to SQL**: Convert plain English questions into SQL queries using Claude AI
- **Interactive Chat Interface**: Streamlit-based chat for conversational analytics
- **Supplier Performance Monitoring**: Track supplier metrics, delivery times, and performance KPIs
- **Sales Forecasting**: Historical analysis and trend prediction
- **Smart Alerting**: Configurable alerts with email and Slack notifications
- **Query Memory**: Context-aware conversations with query history
- **Visualization Engine**: Automatic chart generation using Plotly
- **User Management**: Authentication and role-based access control
- **Query Caching**: Performance optimization for large datasets

## Architecture

```

```

## Technology Stack

- **Backend**: FastAPI, Python 3.11+
- **Frontend**: Streamlit
- **Database**: Snowflake (primary), SQLite (metadata)
- **AI**: Claude API (Anthropic)
- **Visualization**: Plotly
- **Authentication**: JWT tokens
- **Containerization**: Docker & Docker Compose
- **Alerts**: SMTP email, Slack webhooks

## Prerequisites

- Docker and Docker Compose
- Snowflake account with database access
- Claude API key from Anthropic
- Email account for SMTP (optional)
- Slack webhook URL (optional)

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd agentic-analytics
   ```

2. **Setup environment**
   ```bash
   make setup
   # Edit .env file with your configuration
   ```

3. **Build and start services**
   ```bash
   make build
   make up
   ```

4. **Access the application**
   - Frontend: http://localhost:8501
   - Backend API: http://localhost:8000
   - API Documentation: http://localhost:8000/docs

## Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```env
# Security
SECRET_KEY=your-super-secret-key

# Snowflake
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=your-role

# Claude API
CLAUDE_API_KEY=your-claude-api-key

# Email (optional)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-app-password

# Slack (optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK
```

## Database Schema

The solution works with a supply chain database containing:

- **PART**: Product catalog
- **SUPPLIER**: Supplier information
- **CUSTOMER**: Customer data
- **ORDERS**: Order headers
- **LINEITEM**: Order details (6M+ rows)
- **NATION**: Country reference
- **REGION**: Geographic regions
- **PARTSUPP**: Part-supplier relationships

## Key Use Cases

### Supplier Performance Monitoring
- Track delivery times and delays
- Monitor supplier reliability
- Analyze cost efficiency
- Regional performance comparison

### Sales Forecasting
- Historical trend analysis
- Seasonal pattern detection
- Revenue predictions
- Demand forecasting

### Interactive Analytics
- Natural language queries
- Conversational data exploration
- Automatic visualization
- Context-aware follow-ups

## 🔧 Development

### Local Development

1. **Backend development**
   ```bash
   make dev-backend
   ```

2. **Frontend development**
   ```bash
   make dev-frontend
   ```

3. **Install dependencies**
   ```bash
   make install-dev
   ```

### Testing

```bash
make test
make lint
make format
```

### Docker Commands

```bash
make build          # Build containers
make up             # Start services
make down           # Stop services
make logs           # View logs
make restart        # Restart services
make clean          # Clean up containers
```

## 📚 API Documentation

The FastAPI backend provides comprehensive API documentation:

- **Interactive docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Main Endpoints

- `POST /auth/login` - User authentication
- `POST /analytics/query` - Natural language query
- `GET /analytics/supplier-performance` - Supplier metrics
- `GET /analytics/sales-forecast` - Sales data
- `POST /alerts/` - Create alerts
- `GET /analytics/schema` - Database schema

## 🔔 Alert System

Configure intelligent alerts for:

- Revenue thresholds
- Supplier performance issues
- Inventory levels
- Customer behavior changes
- System anomalies

Notifications via:
- Email (SMTP)
- Slack webhooks
- Dashboard alerts

## 🔒 Security Features

- JWT-based authentication
- Password hashing (bcrypt)
- SQL injection prevention
- Rate limiting
- Input validation
- Role-based access control

## 📈 Performance Optimization

- Query result caching
- Connection pooling
- Async operations
- Pagination for large datasets
- Background processing
- Index optimization

## 🚀 Deployment

### Production Deployment

1. **Configure production environment**
2. **Use production-grade secrets management**
3. **Setup SSL/TLS certificates**
4. **Configure monitoring and logging**
5. **Setup backup strategies**

### Scaling Considerations

- Load balancing for multiple instances
- Redis for distributed caching
- Database connection pooling
- Horizontal scaling with Docker Swarm/Kubernetes

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For questions and support:

1. Check the documentation
2. Review existing issues
3. Create a new issue with detailed information
4. Contact the development team

## 🔄 Roadmap

- [ ] Advanced ML models for forecasting
- [ ] Real-time data streaming
- [ ] Mobile app support
- [ ] Advanced visualization options
- [ ] Integration with BI tools
- [ ] Multi-tenant support
- [ ] Advanced analytics featuresuvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]