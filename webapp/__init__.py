# file: webapp/__init__.py

import os
from datetime import datetime
from flask import Flask, session, redirect, url_for
from flask_caching import Cache
from dotenv import load_dotenv

# =================================================================
# === THAY ĐỔI 1: Tải biến môi trường ngay từ đầu ===
# =================================================================
load_dotenv()

# Khởi tạo Cache
cache = Cache(config={
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
})

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    
    # --- Cấu hình ứng dụng ---
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # =================================================================
    # === THAY ĐỔI 2: Đọc cấu hình từ biến môi trường ===
    # =================================================================
    
    # Lấy SECRET_KEY từ biến môi trường, nếu không có thì dùng giá trị mặc định (chỉ cho dev)
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev_secret_key_thay_the_sau')
    
    # Lấy DATABASE_URL từ biến môi trường. Nếu không có, ứng dụng sẽ báo lỗi.
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("Lỗi cấu hình: Biến môi trường DATABASE_URL chưa được thiết lập. Vui lòng kiểm tra file .env")
    
    # In ra để xác nhận trong quá trình phát triển
    print(f"--- Ứng dụng đang khởi tạo với CSDL: {database_url} ---")
    
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # Tắt tính năng không cần thiết để tiết kiệm tài nguyên
    
    # Cấu hình thư mục báo cáo
    app.config['REPORT_FOLDER'] = os.path.join(app.instance_path, 'reports')
    os.makedirs(app.config['REPORT_FOLDER'], exist_ok=True)
    
    # Kết nối Cache với app
    cache.init_app(app)

    # Inject biến 'now' vào mọi template
    @app.context_processor
    def inject_now():
        return {'now': datetime.now()}
    
    # Đăng ký các blueprint
    with app.app_context():
        from .routes import auth, main, admin
        app.register_blueprint(auth.auth_bp)
        app.register_blueprint(main.main_bp)
        app.register_blueprint(admin.admin_bp)
    
    # Route gốc để điều hướng
    @app.route('/')
    def index():
        if 'user_id' in session:
            return redirect(url_for('main.dashboard_page')) # Điều hướng đến dashboard nếu đã đăng nhập
        return redirect(url_for('auth.login'))
    
    return app