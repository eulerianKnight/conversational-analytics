import anthropic
import json
import re
from typing import Dict, List, Any, Optional, Tuple
import logging
from app.core.config import settings, SCHEMA_INFO
from app.services.snowflake_service import SnowflakeService

logger = logging.getLogger(__name__)


class ClaudeService:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=settings.CLAUDE_API_KEY)
        self.model = settings.CLAUDE_MODEL
        self.snowflake_service = SnowflakeService()

    def test_connection(self) -> bool:
        """Test Claude API connection"""
        try:
            # Simple test message
            response = self.client.messages.create(
                model=self.model,
                max_tokens=10,
                messages=[{"role": "user", "content": "Hello"}],
            )
            return bool(response.content)
        except Exception as e:
            logger.error(f"Claude API test failed: {str(e)}")
            return False

    def _build_system_prompt(self) -> str:
        """Build comprehensive system prompt for text-to-SQL conversion"""
        schema_context = self.snowflake_service.get_schema_context()

        system_prompt = f"""You are an expert SQL analyst specializing in supply chain and business analytics. Your task is to convert natural language queries into precise SQL queries for a Snowflake database.

{schema_context}

IMPORTANT GUIDELINES:
1. Always use proper table and column names exactly as defined in the schema
2. Include appropriate JOINs when querying multiple tables
3. Add LIMIT clauses for safety, especially with LINEITEM table (6M+ rows)
4. Use proper date functions and formatting for Snowflake
5. Include meaningful column aliases for better readability
6. Consider performance implications of queries
7. Only generate SELECT, WITH, SHOW, or DESCRIBE statements
8. Use appropriate aggregation functions when summarizing data
9. Include proper WHERE clauses for filtering
10. Use CASE statements for conditional logic when needed

RESPONSE FORMAT:
Always respond with a JSON object containing:
{{
    "sql_query": "the SQL query",
    "explanation": "brief explanation of what the query does",
    "query_type": "type of analysis (e.g., 'supplier_performance', 'sales_analysis', 'inventory_check')",
    "estimated_rows": "estimated number of rows returned",
    "performance_notes": "any performance considerations or optimizations"
}}

EXAMPLES:
User: "Show me top 10 suppliers by revenue last month"
Response: {{
    "sql_query": "SELECT s.NAME as supplier_name, SUM(l.EXTENDEDPRICE * (1 - l.DISCOUNT)) as total_revenue FROM SUPPLIER s JOIN LINEITEM l ON s.SUPPKEY = l.SUPPKEY WHERE l.SHIPDATE >= DATEADD(month, -1, CURRENT_DATE) GROUP BY s.SUPPKEY, s.NAME ORDER BY total_revenue DESC LIMIT 10",
    "explanation": "Retrieves top 10 suppliers by total revenue in the last month, joining SUPPLIER and LINEITEM tables",
    "query_type": "supplier_performance",
    "estimated_rows": "10",
    "performance_notes": "Uses date filter to limit LINEITEM scan, includes LIMIT for safety"
}}
"""
        return system_prompt

    async def convert_text_to_sql(
        self, user_query: str, context: str = ""
    ) -> Dict[str, Any]:
        """Convert natural language to SQL query"""
        try:
            system_prompt = self._build_system_prompt()

            # Build user message with context
            user_message = user_query
            if context:
                user_message = f"Previous conversation context:\n{context}\n\nCurrent query: {user_query}"

            response = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            # Parse the JSON response
            response_text = response.content[0].text

            # Extract JSON from response if wrapped in markdown
            json_match = re.search(r"```json\n(.*?)\n```", response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON object directly
                json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    json_str = response_text

            try:
                result = json.loads(json_str)
            except json.JSONDecodeError:
                # Fallback: extract SQL query manually
                sql_match = re.search(
                    r"SELECT.*?(?=\n\n|\n$|$)", response_text, re.DOTALL | re.IGNORECASE
                )
                if sql_match:
                    result = {
                        "sql_query": sql_match.group(0).strip(),
                        "explanation": "SQL query extracted from response",
                        "query_type": "general",
                        "estimated_rows": "unknown",
                        "performance_notes": "Manual extraction - review performance",
                    }
                else:
                    raise ValueError("Could not extract SQL query from response")

            # Validate the SQL query
            is_valid, validation_message = self.snowflake_service.validate_sql_query(
                result["sql_query"]
            )
            if not is_valid:
                raise ValueError(f"Invalid SQL query: {validation_message}")

            result["validation_status"] = "valid"
            result["claude_response"] = response_text

            return result

        except Exception as e:
            logger.error(f"Text-to-SQL conversion failed: {str(e)}")
            return {
                "error": str(e),
                "sql_query": None,
                "explanation": f"Failed to convert query: {str(e)}",
                "validation_status": "error",
            }

    async def generate_insights(
        self, query_result: Dict[str, Any], original_query: str
    ) -> str:
        """Generate insights and summary from query results"""
        try:
            # Prepare data summary for Claude
            data = query_result.get("data", [])
            metadata = query_result.get("metadata", {})

            if not data:
                return "No data found for the given query."

            # Create a summary of the data
            data_summary = {
                "row_count": len(data),
                "columns": metadata.get("columns", []),
                "sample_data": data[:5] if len(data) > 5 else data,
                "execution_time": query_result.get("execution_time", 0),
            }

            system_prompt = """You are a business analytics expert. Analyze the query results and provide actionable insights. Focus on:
1. Key findings and trends
2. Business implications
3. Recommendations for action
4. Notable patterns or anomalies

Keep the response concise but informative, suitable for business stakeholders."""

            user_message = f"""
Original Query: {original_query}

Query Results Summary:
- Rows returned: {data_summary['row_count']}
- Columns: {', '.join(data_summary['columns'])}
- Execution time: {data_summary['execution_time']:.2f} seconds

Sample Data (first 5 rows):
{json.dumps(data_summary['sample_data'], indent=2, default=str)}

Please provide insights and analysis of these results.
"""

            response = self.client.messages.create(
                model=self.model,
                max_tokens=800,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            return response.content[0].text

        except Exception as e:
            logger.error(f"Insight generation failed: {str(e)}")
            return f"Could not generate insights: {str(e)}"

    async def suggest_follow_up_queries(
        self, original_query: str, query_result: Dict[str, Any]
    ) -> List[str]:
        """Suggest follow-up queries based on current results"""
        try:
            data = query_result.get("data", [])
            metadata = query_result.get("metadata", {})

            if not data:
                return [
                    "Modify your query to include different filters or time periods"
                ]

            system_prompt = """You are a business analyst. Based on the original query and results, suggest 3-5 relevant follow-up questions that would provide additional insights. Focus on:
1. Drill-down analysis
2. Comparative analysis
3. Time-based trends
4. Related metrics
5. Root cause analysis

Return only the questions, one per line, without numbering or bullets."""

            user_message = f"""
Original Query: {original_query}
Number of results: {len(data)}
Columns in results: {', '.join(metadata.get('columns', []))}

Sample data: {json.dumps(data[:3], indent=2, default=str)}

Suggest follow-up questions for deeper analysis.
"""

            response = self.client.messages.create(
                model=self.model,
                max_tokens=400,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            suggestions = response.content[0].text.strip().split("\n")
            return [s.strip() for s in suggestions if s.strip()]

        except Exception as e:
            logger.error(f"Follow-up suggestion failed: {str(e)}")
            return ["Explore related data by modifying your query"]

    async def generate_chart_recommendation(
        self, query_result: Dict[str, Any], original_query: str
    ) -> Dict[str, Any]:
        """Recommend appropriate chart type and configuration"""
        try:
            data = query_result.get("data", [])
            metadata = query_result.get("metadata", {})

            if not data:
                return {"chart_type": "table", "reason": "No data to visualize"}

            columns = metadata.get("columns", [])
            sample_data = data[:5] if len(data) > 5 else data

            system_prompt = """You are a data visualization expert. Based on the query results, recommend the most appropriate chart type and configuration.

Consider:
1. Data types (numerical, categorical, date/time)
2. Number of dimensions
3. Data volume
4. Business context
5. Clarity of visualization

Respond with a JSON object:
{
    "chart_type": "bar|line|pie|scatter|heatmap|table",
    "x_axis": "column_name",
    "y_axis": "column_name",
    "color_by": "column_name or null",
    "reason": "explanation for chart choice",
    "title": "suggested chart title",
    "additional_config": {}
}"""

            user_message = f"""
Original Query: {original_query}
Columns: {columns}
Sample Data: {json.dumps(sample_data, indent=2, default=str)}
Total Rows: {len(data)}

Recommend the best visualization for this data.
"""

            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            # Parse JSON response
            response_text = response.content[0].text
            json_match = re.search(r"\{.*\}", response_text, re.DOTALL)

            if json_match:
                try:
                    return json.loads(json_match.group(0))
                except json.JSONDecodeError:
                    pass

            # Fallback to simple heuristics
            return self._fallback_chart_recommendation(columns, data)

        except Exception as e:
            logger.error(f"Chart recommendation failed: {str(e)}")
            return self._fallback_chart_recommendation(
                metadata.get("columns", []), data
            )

    def _fallback_chart_recommendation(
        self, columns: List[str], data: List[Dict]
    ) -> Dict[str, Any]:
        """Fallback chart recommendation using simple heuristics"""
        if not data or not columns:
            return {"chart_type": "table", "reason": "Insufficient data"}

        numeric_columns = []
        categorical_columns = []
        date_columns = []

        # Analyze column types based on sample data
        sample = data[0] if data else {}
        for col in columns:
            value = sample.get(col)
            if isinstance(value, (int, float)):
                numeric_columns.append(col)
            elif isinstance(value, str) and any(
                date_indicator in col.lower()
                for date_indicator in ["date", "time", "month", "year"]
            ):
                date_columns.append(col)
            else:
                categorical_columns.append(col)

        # Simple chart selection logic
        if len(numeric_columns) >= 2:
            return {
                "chart_type": "scatter",
                "x_axis": numeric_columns[0],
                "y_axis": numeric_columns[1],
                "reason": "Two numeric columns suitable for scatter plot",
            }
        elif len(numeric_columns) == 1 and len(categorical_columns) >= 1:
            return {
                "chart_type": "bar",
                "x_axis": categorical_columns[0],
                "y_axis": numeric_columns[0],
                "reason": "Categorical and numeric data suitable for bar chart",
            }
        elif date_columns and numeric_columns:
            return {
                "chart_type": "line",
                "x_axis": date_columns[0],
                "y_axis": numeric_columns[0],
                "reason": "Time series data suitable for line chart",
            }
        else:
            return {
                "chart_type": "table",
                "reason": "Data structure best suited for tabular display",
            }
