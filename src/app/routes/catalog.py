"""
Unity Catalog routes for browsing catalogs, schemas, and tables.
"""
from flask import Blueprint, jsonify, request

from ..services.databricks import databricks_service
from ..config import Config

catalog_bp = Blueprint('catalog', __name__, url_prefix='/api')


@catalog_bp.route('/debug')
def debug_info():
    """API: Debug endpoint to troubleshoot configuration."""
    import os
    user_token = request.headers.get('x-forwarded-access-token')
    warehouse_id = Config.SQL_WAREHOUSE_ID
    return jsonify({
        "user_token_present": bool(user_token),
        "user_token_length": len(user_token) if user_token else 0,
        "sql_warehouse_id": warehouse_id,
        "sql_http_path": f"/sql/1.0/warehouses/{warehouse_id}" if warehouse_id else None,
        "databricks_host": Config.DATABRICKS_HOST or os.getenv("DATABRICKS_HOST"),
        "has_databricks_token": bool(Config.DATABRICKS_TOKEN),
    })


@catalog_bp.route('/catalogs')
def get_catalogs():
    """API: Get list of catalogs."""
    try:
        catalogs = databricks_service.get_catalogs()
        print(f"[DEBUG] get_catalogs returning: {catalogs}")
        return jsonify(catalogs)
    except Exception as e:
        print(f"[ERROR] get_catalogs failed: {e}")
        return jsonify({"error": str(e)}), 500


@catalog_bp.route('/schemas/<catalog>')
def get_schemas(catalog):
    """API: Get schemas for a catalog."""
    return jsonify(databricks_service.get_schemas(catalog))


@catalog_bp.route('/tables/<catalog>/<schema>')
def get_tables(catalog, schema):
    """API: Get tables for a schema."""
    return jsonify(databricks_service.get_tables(catalog, schema))


@catalog_bp.route('/sample/<catalog>/<schema>/<table>')
def get_sample(catalog, schema, table):
    """API: Get sample data from a table."""
    full_table_name = f"{catalog}.{schema}.{table}"
    return jsonify(databricks_service.get_table_sample(
        full_table_name,
        Config.SAMPLE_DATA_LIMIT
    ))
