# file: webapp/__init__.py
from flask import Flask
import os
from datetime import datetime # <<< THÊM DÒNG NÀY ĐỂ IMPORT datetime

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    try: os.makedirs(app.instance_path)
    except OSError: pass
    
    app.config.from_mapping(SECRET_KEY='chuoi_bi_mat_cua_ban_o_day_thay_doi_sau_nay')
    app.config['REPORT_FOLDER'] = os.path.join(app.instance_path, 'reports')
    os.makedirs(app.config['REPORT_FOLDER'], exist_ok=True)
    
    # THÊM ĐOẠN CODE NÀY ĐỂ INJECT BIẾN 'now' VÀO MỌI TEMPLATE
    # Đặt nó sau khi 'app' được tạo, và trước khi 'app' được trả về.
    @app.context_processor
    def inject_now():
        return {'now': datetime.now()}
    
    with app.app_context():
        from .routes import auth, main, admin
        app.register_blueprint(auth.auth_bp)
        app.register_blueprint(main.main_bp)
        app.register_blueprint(admin.admin_bp)
    
    @app.route('/')
    def index(): 
        from flask import session, redirect, url_for
        return redirect(url_for('main.report_page')) if 'user_id' in session else redirect(url_for('auth.login'))
    
    return app