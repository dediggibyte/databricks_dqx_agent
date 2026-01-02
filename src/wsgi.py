"""
Data Quality Rule Generator - WSGI Entry Point
===============================================
A Databricks App for generating DQ rules using AI assistance.

This file serves as the WSGI entry point for the Flask application.
All business logic is organized in the app/ package.

Usage:
    python wsgi.py             # Run locally
    gunicorn wsgi:app          # Production deployment

Directory Structure:
    app/
    ├── __init__.py           # Flask app factory
    ├── config.py             # Configuration management
    ├── routes/               # API endpoints
    │   ├── catalog.py        # Unity Catalog routes
    │   ├── rules.py          # DQ Rules routes
    │   └── lakebase.py       # Lakebase routes
    └── services/             # Business logic
        ├── databricks.py     # Databricks SDK service
        ├── lakebase.py       # Lakebase service
        └── ai.py             # AI analysis service
"""
from app import create_app

# Create the Flask application
app = create_app()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
