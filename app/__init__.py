"""
DQ Rule Generator - Flask Application Package
=============================================
A Databricks App for generating DQ rules using AI assistance.
"""
from flask import Flask, render_template, jsonify, redirect, url_for
from datetime import datetime

from .config import Config


def create_app() -> Flask:
    """Application factory for creating Flask app instance."""
    app = Flask(
        __name__,
        template_folder='../templates',
        static_folder='../static'
    )
    app.secret_key = Config.SECRET_KEY

    # Register blueprints
    from .routes import catalog_bp, rules_bp, lakebase_bp
    app.register_blueprint(catalog_bp)
    app.register_blueprint(rules_bp)
    app.register_blueprint(lakebase_bp)

    # Register main routes
    @app.route('/')
    def index():
        """Redirect to generator page."""
        return redirect(url_for('generator'))

    @app.route('/generator')
    def generator():
        """DQ Rule Generator page."""
        return render_template(
            'generator.html',
            active_tab='generator',
            job_id=Config.DQ_GENERATION_JOB_ID
        )

    @app.route('/validator')
    def validator():
        """DQ Rule Validator page."""
        return render_template(
            'validator.html',
            active_tab='validator',
            job_id=Config.DQ_VALIDATION_JOB_ID
        )

    @app.route('/health')
    def health():
        """Health check endpoint."""
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat()
        })

    return app
