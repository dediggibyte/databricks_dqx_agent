"""
DQ Rule Generator - Flask Application Package
=============================================
A Databricks App for generating DQ rules using AI assistance.
"""
from flask import Flask, render_template, jsonify
from datetime import datetime

from .config import Config


def create_app() -> Flask:
    """Application factory for creating Flask app instance."""
    app = Flask(__name__, template_folder='../templates')
    app.secret_key = Config.SECRET_KEY

    # Register blueprints
    from .routes import catalog_bp, rules_bp, lakebase_bp
    app.register_blueprint(catalog_bp)
    app.register_blueprint(rules_bp)
    app.register_blueprint(lakebase_bp)

    # Register main routes
    @app.route('/')
    def index():
        """Main page."""
        return render_template('index.html', job_id=Config.DQ_GENERATION_JOB_ID)

    @app.route('/health')
    def health():
        """Health check endpoint."""
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat()
        })

    return app
