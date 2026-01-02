"""
Unity Catalog routes for browsing catalogs, schemas, and tables.
"""
from flask import Blueprint, jsonify

from ..services.databricks import databricks_service
from ..config import Config

catalog_bp = Blueprint('catalog', __name__, url_prefix='/api')


@catalog_bp.route('/catalogs')
def get_catalogs():
    """API: Get list of catalogs."""
    return jsonify(databricks_service.get_catalogs())


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
