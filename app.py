"""Compatibility shim: Use `main.py` as the single app entry point.

This module provides `create_app()` for tests and any callers that expect an
app factory (like `from app import create_app`) while delegating the actual
application implementation to `main.app`.
"""

from main import app as main_app
from utils.module_loader import load_modules
from models import db


def create_app():
    """Return the application instance from `main`.

    This will load blueprints once and ensure the database tables exist so
    test-suite and other callers continue to work.
    """
    app = main_app

    # Load modules once to avoid duplicate registrations
    if not getattr(app, '_modules_loaded', False):
        load_modules(app, blueprint_dir='blueprints')
        app._modules_loaded = True

    with app.app_context():
        db.create_all()

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)
