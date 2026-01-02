"""
Routes module - Flask API endpoints.
"""
from flask import Blueprint

from .catalog import catalog_bp
from .rules import rules_bp
from .lakebase import lakebase_bp

__all__ = ["catalog_bp", "rules_bp", "lakebase_bp"]
