"""
Spark utilities for data operations.
"""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def get_spark_session():
    """Get or create a Spark session."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise


def get_sample_data(
    table_name: str,
    limit: int = 100,
    catalog: str = None,
    schema: str = None
) -> List[Dict[str, Any]]:
    """
    Get sample data from a table.

    Args:
        table_name: Name of the table
        limit: Maximum number of rows to return
        catalog: Optional catalog name
        schema: Optional schema name

    Returns:
        List of dictionaries representing rows
    """
    spark = get_spark_session()

    # Build full table name
    if catalog and schema:
        full_table_name = f"{catalog}.{schema}.{table_name}"
    elif schema:
        full_table_name = f"{schema}.{table_name}"
    else:
        full_table_name = table_name

    try:
        df = spark.table(full_table_name).limit(limit)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        logger.error(f"Error getting sample data from {full_table_name}: {e}")
        return []


def get_table_schema(
    table_name: str,
    catalog: str = None,
    schema: str = None
) -> List[Dict[str, str]]:
    """
    Get the schema of a table.

    Args:
        table_name: Name of the table
        catalog: Optional catalog name
        schema: Optional schema name

    Returns:
        List of dictionaries with column name and type
    """
    spark = get_spark_session()

    # Build full table name
    if catalog and schema:
        full_table_name = f"{catalog}.{schema}.{table_name}"
    elif schema:
        full_table_name = f"{schema}.{table_name}"
    else:
        full_table_name = table_name

    try:
        df = spark.table(full_table_name)
        return [
            {"name": field.name, "type": str(field.dataType)}
            for field in df.schema.fields
        ]
    except Exception as e:
        logger.error(f"Error getting schema for {full_table_name}: {e}")
        return []


def get_table_stats(
    table_name: str,
    catalog: str = None,
    schema: str = None
) -> Dict[str, Any]:
    """
    Get basic statistics for a table.

    Args:
        table_name: Name of the table
        catalog: Optional catalog name
        schema: Optional schema name

    Returns:
        Dictionary with table statistics
    """
    spark = get_spark_session()

    # Build full table name
    if catalog and schema:
        full_table_name = f"{catalog}.{schema}.{table_name}"
    elif schema:
        full_table_name = f"{schema}.{table_name}"
    else:
        full_table_name = table_name

    try:
        df = spark.table(full_table_name)
        row_count = df.count()
        col_count = len(df.columns)

        return {
            "table_name": full_table_name,
            "row_count": row_count,
            "column_count": col_count,
            "columns": df.columns
        }
    except Exception as e:
        logger.error(f"Error getting stats for {full_table_name}: {e}")
        return {}
