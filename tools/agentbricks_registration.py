"""
AgentBricks Tool Registration

This module registers the DQ Rule Summarizer tool with Databricks AgentBricks.
Use this to make the tool available in your AI agent workflows.
"""
from typing import Dict, Any, List
import json
import logging

from .dq_rule_summarizer import (
    DQRuleSummarizer,
    create_agentbricks_tool,
    execute_tool
)

logger = logging.getLogger(__name__)


class DQRuleSummarizerTool:
    """
    AgentBricks-compatible wrapper for the DQ Rule Summarizer.

    This class provides the standard interface expected by AgentBricks
    for tool registration and execution.
    """

    def __init__(self):
        """Initialize the tool."""
        self.name = "dq_rule_summarizer"
        self.description = (
            "Analyzes generated data quality rules and provides a comprehensive "
            "summary. Returns: human-readable explanation of rules, JSON rule "
            "definition for editing, improvement recommendations, and metadata."
        )
        self._summarizer = DQRuleSummarizer()

    @property
    def tool_definition(self) -> Dict[str, Any]:
        """Get the tool definition for AgentBricks registration."""
        return create_agentbricks_tool()

    def __call__(
        self,
        rules: List[Dict[str, Any]],
        table_name: str,
        column_info: Dict[str, Any] = None,
        use_llm: bool = True
    ) -> Dict[str, Any]:
        """
        Execute the tool.

        Args:
            rules: List of DQ rule definitions
            table_name: Name of the table
            column_info: Optional column metadata
            use_llm: Whether to use LLM for summarization

        Returns:
            Dictionary with summary results
        """
        return execute_tool(
            rules=rules,
            table_name=table_name,
            column_info=column_info,
            use_llm=use_llm
        )

    def to_langchain_tool(self):
        """
        Convert to a LangChain-compatible tool.

        Returns:
            LangChain Tool object
        """
        try:
            from langchain.tools import Tool

            return Tool(
                name=self.name,
                description=self.description,
                func=lambda x: self(
                    rules=x.get("rules", []),
                    table_name=x.get("table_name", ""),
                    column_info=x.get("column_info"),
                    use_llm=x.get("use_llm", True)
                )
            )
        except ImportError:
            logger.warning("LangChain not installed, cannot create LangChain tool")
            return None


def register_with_agentbricks(agent_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Register the DQ Rule Summarizer tool with an AgentBricks agent.

    Args:
        agent_config: The agent configuration dictionary

    Returns:
        Updated agent configuration with the tool registered

    Example:
        ```python
        from tools.agentbricks_registration import register_with_agentbricks

        agent_config = {
            "name": "dq_agent",
            "tools": []
        }

        updated_config = register_with_agentbricks(agent_config)
        ```
    """
    tool = DQRuleSummarizerTool()

    if "tools" not in agent_config:
        agent_config["tools"] = []

    agent_config["tools"].append(tool.tool_definition)

    return agent_config


def create_mlflow_model_tool():
    """
    Create an MLflow-compatible tool for deployment.

    Returns:
        Dictionary with MLflow model configuration
    """
    return {
        "name": "dq_rule_summarizer",
        "type": "function",
        "function": {
            "name": "dq_rule_summarizer",
            "description": (
                "Analyzes data quality rules and provides summaries, "
                "recommendations, and editable JSON definitions."
            ),
            "parameters": create_agentbricks_tool()["parameters"]
        }
    }


# Example usage for Databricks AI Playground
PLAYGROUND_TOOL_CONFIG = {
    "tools": [
        {
            "type": "function",
            "function": {
                "name": "dq_rule_summarizer",
                "description": (
                    "Summarizes data quality rules. Input: rules (array of rule "
                    "objects), table_name (string). Output: summary, recommendations, "
                    "and formatted rules for editing."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "rules": {
                            "type": "array",
                            "description": "Array of DQ rule definitions"
                        },
                        "table_name": {
                            "type": "string",
                            "description": "Name of the table"
                        }
                    },
                    "required": ["rules", "table_name"]
                }
            }
        }
    ]
}
