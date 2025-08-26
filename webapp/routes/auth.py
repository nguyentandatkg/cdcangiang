# file: routes/auth.py (Phiên bản đã sửa lỗi và cải tiến bảo mật)

from flask import Blueprint, render_template, request, flash, redirect, url_for, session
from sqlalchemy.orm import joinedload
# SỬA LỖI: Import các hàm hash an toàn từ Werkzeug
from werkzeug.security import check_password_hash

# Tái sử dụng các module lõi
from webapp.core.database_setup import NguoiDung
from webapp.core.database_utils import get_db_session

# Tạo một Blueprint tên là 'auth'
auth_bp = Blueprint('auth', __name__)


def check_login(username, password):
    """
    Kiểm tra thông tin đăng nhập của người dùng so với cơ sở dữ liệu một cách an toàn.
    """
    if not username or not password:
        return None

    db = get_db_session()
    try:
        user = db.query(NguoiDung).options(
            joinedload(NguoiDung.don_vi)
        ).filter_by(ten_dang_nhap=username).first()

        # CẢI TIẾN BẢO MẬT: Dùng check_password_hash thay vì so sánh sha256 thủ công
        if user and check_password_hash(user.mat_khau_hashed, password):
            return user
    finally:
        db.close()

    return None


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Xử lý trang và logic đăng nhập."""
    if 'user_id' in session:
        return redirect(url_for('main.report_page'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        user = check_login(username, password)

        if user:
            session.clear()
            session['user_id'] = user.id
            session['username'] = user.ten_dang_nhap
            session['role'] = user.quyen_han
            
            if user.don_vi:
                session['don_vi_id'] = user.don_vi_id
                session['don_vi_ten'] = user.don_vi.ten_don_vi
                session['don_vi_cap'] = user.don_vi.cap_don_vi
            elif user.quyen_han == 'admin':
                session['don_vi_id'] = None
                session['don_vi_ten'] = 'Quản trị viên'
                session['don_vi_cap'] = 'Tỉnh'

            # SỬA LỖI: Truyền chuỗi trực tiếp vào flash
            flash({'message': 'Đăng nhập thành công!'}, 'success')
            return redirect(url_for('main.report_page'))
        else:
            # SỬA LỖI: Truyền chuỗi trực tiếp vào flash
            flash({'message': 'Tên đăng nhập hoặc mật khẩu không chính xác.'}, 'danger')

    return render_template('login.html')


@auth_bp.route('/logout')
def logout():
    """Xử lý đăng xuất người dùng."""
    session.clear()
    # SỬA LỖI: Truyền chuỗi trực tiếp vào flash
    flash({'message': 'Bạn đã đăng xuất.'}, 'info')
    return redirect(url_for('auth.login'))