# file: webapp/__init__.py
from flask import Flask
import os
from datetime import datetime
from flask_caching import Cache # <<< THÊM DÒNG NÀY

# <<< THÊM CÁC DÒNG NÀY ĐỂ KHỞI TẠO CACHE
# Sử dụng 'SimpleCache' cho môi trường phát triển. 
# Khi triển khai thực tế, nên cân nhắc dùng 'RedisCache' hoặc 'MemcachedCache'.
cache = Cache(config={
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300 # Thời gian cache mặc định là 300 giây (5 phút)
})

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    try: os.makedirs(app.instance_path)
    except OSError: pass
    
    app.config.from_mapping(SECRET_KEY='chuoi_bi_mat_cua_ban_o_day_thay_doi_sau_nay')
    app.config['REPORT_FOLDER'] = os.path.join(app.instance_path, 'reports')
    os.makedirs(app.config['REPORT_FOLDER'], exist_ok=True)
    
    cache.init_app(app) # <<< THÊM DÒNG NÀY ĐỂ KẾT NỐI CACHE VỚI APP

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