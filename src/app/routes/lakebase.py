"""
Lakebase routes for database status and operations.
"""
from flask import Blueprint, jsonify

from ..services.lakebase import LakebaseService

lakebase_bp = Blueprint('lakebase', __name__, url_prefix='/api/lakebase')


@lakebase_bp.route('/status')
def get_status():
    """API: Check Lakebase connection status."""
    result = LakebaseService.check_connection()
    return jsonify(result)
