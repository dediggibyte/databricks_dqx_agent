"""
AI Analysis service for analyzing DQ rules using Model Serving.
"""
import json
import re
import time
from typing import Dict, Any, List

from ..config import Config
from .databricks import databricks_service


class AIAnalysisService:
    """Service for AI-powered DQ rules analysis."""

    @staticmethod
    def analyze_rules(
        rules: List[Dict],
        table_name: str,
        user_prompt: str
    ) -> Dict[str, Any]:
        """
        Analyze DQ rules using AI and provide summary with reasoning.
        Uses ai_query() via SQL Statement Execution API.
        """
        try:
            ws = databricks_service.client
            warehouse_id = databricks_service.get_sql_warehouse_id()

            if not warehouse_id:
                raise Exception("No SQL warehouse available")

            # Build the analysis prompt
            rules_json = json.dumps(rules, indent=2)
            rules_escaped = rules_json.replace("'", "''").replace("\\", "\\\\")
            table_escaped = table_name.replace("'", "''")
            prompt_escaped = user_prompt.replace("'", "''")

            analysis_prompt = f"""You are a Data Quality expert. Analyze the following DQ rules \
generated for table '{table_escaped}'.

User's original requirement: {prompt_escaped}

Generated DQ Rules:
{rules_escaped}

Analyze each rule and provide a JSON response with this EXACT structure:
{{
    "summary": "2-3 sentence summary of what these rules check",
    "rule_analysis": [
        {{
            "rule_function": "the check function name from the rule (e.g., is_not_null, is_in_range)",
            "column": "the column name this rule applies to (from arguments.col_name or arguments.col_names)",
            "explanation": "what this rule checks",
            "importance": "why this rule is important for data quality",
            "criticality": "error or warn"
        }}
    ],
    "coverage_assessment": "how well do these rules cover the user's requirements",
    "recommendations": ["additional rule suggestion 1", "additional rule suggestion 2"],
    "overall_quality_score": 8
}}

IMPORTANT: For each rule in rule_analysis, extract the rule_function from check.function \
and the column from check.arguments.col_name or check.arguments.col_names[0]. \
Return ONLY valid JSON."""

            # Escape for SQL string
            prompt_sql_escaped = analysis_prompt.replace("'", "''")

            # Call ai_query via SQL Statement Execution
            sql = f"""
            SELECT ai_query(
                '{Config.MODEL_SERVING_ENDPOINT}',
                '{prompt_sql_escaped}'
            ) as analysis
            """

            # Start async execution
            response = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="0s"
            )

            # Poll for results
            statement_id = response.statement_id
            max_wait = 120
            poll_interval = 2
            elapsed = 0

            while elapsed < max_wait:
                status = ws.statement_execution.get_statement(statement_id)
                state = status.status.state.value if status.status and status.status.state else None

                if state == "SUCCEEDED":
                    response = status
                    break
                elif state in ["FAILED", "CANCELED", "CLOSED"]:
                    error_msg = status.status.error.message if status.status.error else "Unknown error"
                    raise Exception(f"Query failed: {error_msg}")

                time.sleep(poll_interval)
                elapsed += poll_interval

            if elapsed >= max_wait:
                raise Exception("AI analysis timed out after 120 seconds")

            # Parse response
            if response.result and response.result.data_array and len(response.result.data_array) > 0:
                content = response.result.data_array[0][0]

                try:
                    json_match = re.search(r'\{[\s\S]*\}', content)
                    if json_match:
                        analysis = json.loads(json_match.group())
                        return {"success": True, "analysis": analysis}
                except json.JSONDecodeError:
                    pass

                return {"success": True, "analysis": {"summary": content, "raw_response": True}}

            return {"success": False, "error": "No response from ai_query"}

        except Exception as e:
            print(f"Error analyzing rules with AI: {e}")
            return {"success": False, "error": str(e)}
