# file: webapp/__init__.py
from flask import Flask, session, redirect, url_for
import os

def create_app():
    app = Flask(__name__)
    app.config.from_mapping(SECRET_KEY=os.urandom(24))

    # Đăng ký Blueprints
    from .routes import auth, main, admin
    app.register_blueprint(auth.auth_bp)
    app.register_blueprint(main.main_bp)
    app.register_blueprint(admin.admin_bp)

    @app.route('/')
    def index():
        if 'user_id' in session:
            return redirect(url_for('main.report_page'))
        return redirect(url_for('auth.login'))

    return app