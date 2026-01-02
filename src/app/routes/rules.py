"""
DQ Rules routes for generation, analysis, and management.
"""
from flask import Blueprint, jsonify, request

from ..services.databricks import databricks_service
from ..services.ai import AIAnalysisService
from ..services.lakebase import LakebaseService

rules_bp = Blueprint('rules', __name__, url_prefix='/api')


@rules_bp.route('/generate', methods=['POST'])
def generate():
    """API: Trigger DQ rule generation job."""
    data = request.json
    table_name = data.get('table_name')
    user_prompt = data.get('user_prompt')
    sample_limit = data.get('sample_limit')  # Optional: if not provided, uses all rows

    if not table_name or not user_prompt:
        return jsonify({"error": "Missing table_name or user_prompt"}), 400

    result = databricks_service.trigger_dq_job(table_name, user_prompt, sample_limit)
    return jsonify(result)


@rules_bp.route('/status/<run_id>')
def get_status(run_id):
    """API: Get job run status."""
    return jsonify(databricks_service.get_job_status(int(run_id)))


@rules_bp.route('/analyze', methods=['POST'])
def analyze():
    """API: Analyze DQ rules using AI."""
    data = request.json
    rules = data.get('rules', [])
    table_name = data.get('table_name', '')
    user_prompt = data.get('user_prompt', '')

    if not rules:
        return jsonify({"success": False, "error": "No rules provided"}), 400

    result = AIAnalysisService.analyze_rules(rules, table_name, user_prompt)
    return jsonify(result)


@rules_bp.route('/confirm', methods=['POST'])
def confirm():
    """API: Confirm and save DQ rules to Lakebase."""
    data = request.json
    rules = data.get('rules', [])
    table_name = data.get('table_name', '')
    user_prompt = data.get('user_prompt', '')
    ai_summary = data.get('ai_summary')
    metadata = data.get('metadata')

    if not rules:
        return jsonify({"success": False, "error": "No rules provided"}), 400

    if not table_name:
        return jsonify({"success": False, "error": "No table name provided"}), 400

    result = LakebaseService.save_rules(
        table_name=table_name,
        rules=rules,
        user_prompt=user_prompt,
        ai_summary=ai_summary,
        metadata=metadata
    )
    return jsonify(result)


@rules_bp.route('/history/<path:table_name>')
def get_history(table_name):
    """API: Get DQ rules history for a table."""
    limit = request.args.get('limit', 10, type=int)
    result = LakebaseService.get_history(table_name, limit)
    return jsonify(result)


# ============================================================
# Validation Endpoints
# ============================================================

@rules_bp.route('/validate', methods=['POST'])
def validate():
    """API: Trigger DQ rule validation job."""
    data = request.json
    table_name = data.get('table_name')
    rules = data.get('rules', [])

    if not table_name:
        return jsonify({"error": "Missing table_name"}), 400

    if not rules:
        return jsonify({"error": "Missing rules"}), 400

    result = databricks_service.trigger_validation_job(table_name, rules)
    return jsonify(result)


@rules_bp.route('/validate/status/<run_id>')
def get_validation_status(run_id):
    """API: Get validation job run status."""
    return jsonify(databricks_service.get_job_status(int(run_id)))
