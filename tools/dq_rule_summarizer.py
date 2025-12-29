"""
AgentBricks Tool: DQ Rule Summarizer

This tool is designed to be used with Databricks AgentBricks framework.
It analyzes generated DQ rules and provides:
1. A human-readable summary of the rules
2. The actual JSON rule definition for editing
3. Recommendations for rule improvements
"""
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import dspy


@dataclass
class DQRuleSummary:
    """Data class for DQ rule summary output."""
    summary: str
    rules_json: List[Dict[str, Any]]
    recommendations: List[str]
    affected_columns: List[str]
    criticality_breakdown: Dict[str, int]


class DQRuleSummarizerSignature(dspy.Signature):
    """Signature for DQ rule summarization using DSPy."""

    rules_json: str = dspy.InputField(desc="JSON string of DQ rules")
    table_name: str = dspy.InputField(desc="Name of the table being validated")
    column_info: str = dspy.InputField(desc="Information about table columns")

    summary: str = dspy.OutputField(
        desc="Clear, concise summary of what the DQ rules check for"
    )
    recommendations: str = dspy.OutputField(
        desc="JSON array of recommendations for improving the rules"
    )


class DQRuleSummarizer:
    """
    AgentBricks-compatible tool for summarizing DQ rules.

    This tool can be registered with AgentBricks to provide
    intelligent summarization of generated data quality rules.
    """

    def __init__(self, model_endpoint: str = None):
        """
        Initialize the DQ Rule Summarizer.

        Args:
            model_endpoint: Databricks model serving endpoint name
        """
        self.model_endpoint = model_endpoint or "databricks-meta-llama-3-1-70b-instruct"
        self._summarizer = None

    def _init_dspy(self):
        """Initialize DSPy with Databricks LLM."""
        if self._summarizer is None:
            from databricks.sdk import WorkspaceClient

            ws = WorkspaceClient()
            lm = dspy.LM(
                model=f"databricks/{self.model_endpoint}",
                api_base=f"{ws.config.host}/serving-endpoints",
                api_key=ws.config.token
            )
            dspy.configure(lm=lm)
            self._summarizer = dspy.ChainOfThought(DQRuleSummarizerSignature)

    def analyze_rules(self, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze DQ rules and extract metadata.

        Args:
            rules: List of DQ rule definitions

        Returns:
            Dictionary with analysis results
        """
        affected_columns = set()
        criticality_breakdown = {"error": 0, "warn": 0, "info": 0}
        check_types = {}

        for rule in rules:
            # Extract affected columns
            check = rule.get("check", {})
            args = check.get("arguments", {})
            if "col_name" in args:
                affected_columns.add(args["col_name"])
            if "col_names" in args:
                affected_columns.update(args["col_names"])

            # Count criticality levels
            criticality = rule.get("criticality", "warn")
            if criticality in criticality_breakdown:
                criticality_breakdown[criticality] += 1

            # Track check types
            check_function = check.get("function", "unknown")
            check_types[check_function] = check_types.get(check_function, 0) + 1

        return {
            "affected_columns": list(affected_columns),
            "criticality_breakdown": criticality_breakdown,
            "check_types": check_types,
            "total_rules": len(rules)
        }

    def summarize(
        self,
        rules: List[Dict[str, Any]],
        table_name: str,
        column_info: Optional[Dict[str, Any]] = None,
        use_llm: bool = True
    ) -> DQRuleSummary:
        """
        Generate a comprehensive summary of DQ rules.

        Args:
            rules: List of DQ rule definitions
            table_name: Name of the table being validated
            column_info: Optional information about table columns
            use_llm: Whether to use LLM for summary generation

        Returns:
            DQRuleSummary with detailed analysis
        """
        # Analyze rules
        analysis = self.analyze_rules(rules)

        if use_llm:
            # Use LLM for intelligent summarization
            self._init_dspy()

            result = self._summarizer(
                rules_json=json.dumps(rules, indent=2),
                table_name=table_name,
                column_info=json.dumps(column_info or {})
            )

            summary = result.summary

            try:
                recommendations = json.loads(result.recommendations)
            except json.JSONDecodeError:
                recommendations = [result.recommendations]
        else:
            # Generate basic summary without LLM
            summary = self._generate_basic_summary(rules, table_name, analysis)
            recommendations = self._generate_basic_recommendations(rules, analysis)

        return DQRuleSummary(
            summary=summary,
            rules_json=rules,
            recommendations=recommendations,
            affected_columns=analysis["affected_columns"],
            criticality_breakdown=analysis["criticality_breakdown"]
        )

    def _generate_basic_summary(
        self,
        rules: List[Dict[str, Any]],
        table_name: str,
        analysis: Dict[str, Any]
    ) -> str:
        """Generate a basic summary without LLM."""
        summary_parts = [
            f"## Data Quality Rules for `{table_name}`\n",
            f"**Total Rules:** {analysis['total_rules']}\n",
            f"**Affected Columns:** {', '.join(analysis['affected_columns']) or 'None specified'}\n",
            "\n### Criticality Breakdown:\n"
        ]

        for level, count in analysis["criticality_breakdown"].items():
            if count > 0:
                summary_parts.append(f"- **{level.upper()}:** {count} rule(s)\n")

        summary_parts.append("\n### Check Types:\n")
        for check_type, count in analysis["check_types"].items():
            summary_parts.append(f"- `{check_type}`: {count} rule(s)\n")

        summary_parts.append("\n### Rule Details:\n")
        for i, rule in enumerate(rules, 1):
            name = rule.get("name", f"Rule {i}")
            criticality = rule.get("criticality", "warn")
            check_fn = rule.get("check", {}).get("function", "unknown")
            summary_parts.append(
                f"{i}. **{name}** [{criticality}] - `{check_fn}`\n"
            )

        return "".join(summary_parts)

    def _generate_basic_recommendations(
        self,
        rules: List[Dict[str, Any]],
        analysis: Dict[str, Any]
    ) -> List[str]:
        """Generate basic recommendations without LLM."""
        recommendations = []

        # Check for missing criticality levels
        if analysis["criticality_breakdown"]["error"] == 0:
            recommendations.append(
                "Consider adding error-level rules for critical data integrity checks"
            )

        # Check for low rule count
        if analysis["total_rules"] < 3:
            recommendations.append(
                "Consider adding more rules to ensure comprehensive data quality coverage"
            )

        # Check for column coverage
        if len(analysis["affected_columns"]) < 3:
            recommendations.append(
                "Consider adding rules for additional columns to improve coverage"
            )

        # Check for null checks
        null_checks = ["is_not_null", "is_not_null_and_not_empty"]
        has_null_check = any(
            rule.get("check", {}).get("function") in null_checks
            for rule in rules
        )
        if not has_null_check:
            recommendations.append(
                "Consider adding null value checks for important columns"
            )

        return recommendations

    def to_dict(self, summary: DQRuleSummary) -> Dict[str, Any]:
        """Convert DQRuleSummary to dictionary for JSON serialization."""
        return {
            "summary": summary.summary,
            "rules": summary.rules_json,
            "recommendations": summary.recommendations,
            "affected_columns": summary.affected_columns,
            "criticality_breakdown": summary.criticality_breakdown
        }


# AgentBricks Tool Definition
def create_agentbricks_tool() -> Dict[str, Any]:
    """
    Create the AgentBricks tool definition for DQ Rule Summarizer.

    Returns:
        Tool definition dictionary compatible with AgentBricks
    """
    return {
        "name": "dq_rule_summarizer",
        "description": (
            "Analyzes generated data quality rules and provides a comprehensive "
            "summary including: human-readable explanation of what each rule checks, "
            "the JSON rule definition for editing, recommendations for improvements, "
            "and metadata about affected columns and criticality levels."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rules": {
                    "type": "array",
                    "description": "Array of DQ rule definitions to summarize",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "criticality": {"type": "string", "enum": ["error", "warn", "info"]},
                            "check": {
                                "type": "object",
                                "properties": {
                                    "function": {"type": "string"},
                                    "arguments": {"type": "object"}
                                }
                            },
                            "filter": {"type": ["string", "null"]}
                        }
                    }
                },
                "table_name": {
                    "type": "string",
                    "description": "Name of the table the rules apply to"
                },
                "column_info": {
                    "type": "object",
                    "description": "Optional column metadata from profiling"
                },
                "use_llm": {
                    "type": "boolean",
                    "description": "Whether to use LLM for intelligent summarization",
                    "default": True
                }
            },
            "required": ["rules", "table_name"]
        }
    }


def execute_tool(
    rules: List[Dict[str, Any]],
    table_name: str,
    column_info: Optional[Dict[str, Any]] = None,
    use_llm: bool = True
) -> Dict[str, Any]:
    """
    Execute the DQ Rule Summarizer tool.

    This function is the entry point for AgentBricks tool execution.

    Args:
        rules: List of DQ rule definitions
        table_name: Name of the table
        column_info: Optional column metadata
        use_llm: Whether to use LLM for summarization

    Returns:
        Dictionary with summary results
    """
    summarizer = DQRuleSummarizer()
    summary = summarizer.summarize(
        rules=rules,
        table_name=table_name,
        column_info=column_info,
        use_llm=use_llm
    )
    return summarizer.to_dict(summary)


# Example usage and testing
if __name__ == "__main__":
    # Example rules for testing
    example_rules = [
        {
            "name": "valid_email_check",
            "criticality": "error",
            "check": {
                "function": "matches_regex",
                "arguments": {
                    "col_name": "email",
                    "regex": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
                }
            },
            "filter": None
        },
        {
            "name": "positive_amount",
            "criticality": "warn",
            "check": {
                "function": "is_greater_than",
                "arguments": {
                    "col_name": "amount",
                    "limit": 0
                }
            },
            "filter": "status = 'active'"
        },
        {
            "name": "not_null_customer_id",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {
                    "col_name": "customer_id"
                }
            },
            "filter": None
        }
    ]

    # Test without LLM
    result = execute_tool(
        rules=example_rules,
        table_name="main.sales.transactions",
        use_llm=False
    )

    print("Summary:")
    print(result["summary"])
    print("\nRecommendations:")
    for rec in result["recommendations"]:
        print(f"- {rec}")
