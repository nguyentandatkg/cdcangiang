import io
from math import ceil

from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file

from webapp.core.database_setup import DonViHanhChinh
from webapp.core.database_utils import get_db_session
from webapp.core.admin_utils import (
    get_all_don_vi, add_new_don_vi, get_don_vi_by_id, update_don_vi, delete_don_vi,
    get_users_list, add_new_user, get_user_by_id, update_user, reset_user_password,
    delete_user, export_users_to_excel_bytes
)

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


@admin_bp.before_request
def require_admin_login():
    """
    Middleware kiểm tra quyền admin trước khi truy cập bất kỳ route nào trong blueprint này.
    """
    if session.get('role') != 'admin':
        flash({'message': 'Bạn không có quyền truy cập khu vực quản trị.'}, 'danger')
        return redirect(url_for('main.report_page'))


@admin_bp.route('/')
def dashboard():
    """Trang chính của khu vực quản trị."""
    return render_template('admin/dashboard.html', title='Bảng điều khiển Admin')


# --- ROUTES QUẢN LÝ ĐƠN VỊ ---

@admin_bp.route('/don_vi', methods=['GET', 'POST'])
def manage_don_vi():
    """Trang quản lý Đơn vị Hành chính (Thêm mới và Liệt kê)."""
    if request.method == 'POST':
        ten_don_vi = request.form.get('ten_don_vi')
        cap_don_vi = request.form.get('cap_don_vi')
        parent_id = request.form.get('parent_id')
        parent_id = int(parent_id) if parent_id and parent_id.isdigit() else None

        result = add_new_don_vi(ten_don_vi, cap_don_vi, parent_id)
        flash({'message': result['message']}, 'success' if result['success'] else 'danger')
        return redirect(url_for('admin.manage_don_vi'))

    page = request.args.get('page', 1, type=int)
    cap_don_vi_filter = request.args.get('cap_don_vi', 'Tất cả')
    filters = {'cap_don_vi': cap_don_vi_filter}
    PER_PAGE = 20

    don_vi_paginated, total_items = get_all_don_vi(page=page, per_page=PER_PAGE, filters=filters)
    pagination = {
        'page': page,
        'per_page': PER_PAGE,
        'total_items': total_items,
        'total_pages': ceil(total_items / PER_PAGE)
    }

    full_don_vi_list, _ = get_all_don_vi(page=1, per_page=10000)
    don_vi_data_for_js = [dv.to_dict() for dv in full_don_vi_list]

    return render_template(
        'admin/don_vi.html',
        title='Quản lý Đơn vị',
        don_vi_list=don_vi_paginated,
        full_don_vi_list=full_don_vi_list,
        don_vi_data_for_js=don_vi_data_for_js,
        pagination=pagination,
        filters=filters
    )


@admin_bp.route('/don_vi/edit/<int:don_vi_id>', methods=['GET', 'POST'])
def edit_don_vi(don_vi_id):
    """Trang sửa thông tin một đơn vị."""
    unit = get_don_vi_by_id(don_vi_id)
    if not unit:
        # SỬA LỖI: Flash một dictionary
        flash({'message': "Không tìm thấy đơn vị."}, "danger")
        return redirect(url_for('admin.manage_don_vi'))

    if request.method == 'POST':
        data = {
            'ten_don_vi': request.form.get('ten_don_vi'),
            'parent_id': int(request.form.get('parent_id')) if request.form.get('parent_id') else None
        }
        result = update_don_vi(don_vi_id, data)
        # SỬA LỖI: Flash một dictionary
        flash({'message': result['message']}, 'success' if result['success'] else 'danger')
        return redirect(url_for('admin.manage_don_vi'))

    full_don_vi_list, _ = get_all_don_vi(page=1, per_page=10000)
    don_vi_data_for_js = [dv.to_dict() for dv in full_don_vi_list]

    return render_template(
        'admin/edit_don_vi.html',
        title='Sửa Đơn vị',
        unit=unit,
        all_units_js=don_vi_data_for_js
    )


@admin_bp.route('/don_vi/delete/<int:don_vi_id>', methods=['POST'])
def delete_don_vi_action(don_vi_id):
    """Xử lý hành động xóa một đơn vị."""
    result = delete_don_vi(don_vi_id)
    # SỬA LỖI: Flash một dictionary
    flash({'message': result['message']}, 'success' if result['success'] else 'danger')
    return redirect(url_for('admin.manage_don_vi'))


# --- ROUTES QUẢN LÝ NGƯỜI DÙNG ---

@admin_bp.route('/users', methods=['GET', 'POST'])
def manage_users():
    """Trang quản lý Người dùng (Thêm mới và Liệt kê)."""
    if request.method == 'POST':
        ten_dang_nhap = request.form.get('ten_dang_nhap')
        mat_khau = request.form.get('mat_khau')
        don_vi_id = int(request.form.get('don_vi_id'))

        db = get_db_session()
        don_vi = db.query(DonViHanhChinh).get(don_vi_id)
        db.close()

        quyen_han_map = {'Tỉnh': 'tinh', 'Khu vực': 'khuvuc', 'Xã': 'xa'}
        quyen_han = quyen_han_map.get(don_vi.cap_don_vi, 'xa') if don_vi else 'xa'

        result = add_new_user(ten_dang_nhap, mat_khau, quyen_han, don_vi_id)
        flash({'message': result['message']}, 'success' if result['success'] else 'danger')
        return redirect(url_for('admin.manage_users'))

    page = request.args.get('page', 1, type=int)
    PER_PAGE = 20
    
    # NÂNG CẤP: Lấy các tham số lọc từ URL
    filters = {
        'ten_dang_nhap': request.args.get('ten_dang_nhap', '').strip(),
        'quyen_han': request.args.get('quyen_han', '').strip()
    }

    users_paginated, total_items = get_users_list(page=page, per_page=PER_PAGE, filters=filters)
    pagination = {
        'page': page,
        'per_page': PER_PAGE,
        'total_items': total_items,
        'total_pages': ceil(total_items / PER_PAGE)
    }

    don_vi_list_for_dropdown, _ = get_all_don_vi(page=1, per_page=10000)

    return render_template(
        'admin/users.html',
        title='Quản lý Người dùng',
        users_list=users_paginated,
        don_vi_list=don_vi_list_for_dropdown,
        pagination=pagination,
        filters=filters # Truyền filters vào template để giữ giá trị trên form
    )


@admin_bp.route('/users/edit/<int:user_id>', methods=['GET', 'POST'])
def edit_user(user_id):
    """Trang sửa thông tin người dùng và đặt lại mật khẩu."""
    user = get_user_by_id(user_id)
    if not user:
        # SỬA LỖI: Flash một dictionary
        flash({'message': "Không tìm thấy người dùng."}, "danger")
        return redirect(url_for('admin.manage_users'))

    if request.method == 'POST':
        if 'reset_password' in request.form:
            new_password = request.form.get('new_password')
            result = reset_user_password(user_id, new_password)
        else:
            don_vi_id = int(request.form.get('don_vi_id'))
            db = get_db_session()
            don_vi = db.query(DonViHanhChinh).get(don_vi_id)
            db.close()

            quyen_han_map = {'Tỉnh': 'tinh', 'Khu vực': 'khuvuc', 'Xã': 'xa'}
            quyen_han = quyen_han_map.get(don_vi.cap_don_vi, 'xa') if don_vi else 'xa'

            data = {'don_vi_id': don_vi_id, 'quyen_han': quyen_han}
            result = update_user(user_id, data)

        # SỬA LỖI: Flash một dictionary
        flash({'message': result['message']}, 'success' if result['success'] else 'danger')
        return redirect(url_for('admin.manage_users'))

    don_vi_list, _ = get_all_don_vi(page=1, per_page=10000)
    return render_template('admin/edit_user.html', title='Sửa Người dùng', user=user, don_vi_list=don_vi_list)


@admin_bp.route('/users/delete/<int:user_id>', methods=['POST'])
def delete_user_action(user_id):
    """Xử lý hành động xóa một người dùng."""
    if user_id == session.get('user_id'):
        # SỬA LỖI: Flash một dictionary
        flash({'message': "Bạn không thể tự xóa chính mình."}, "warning")
        return redirect(url_for('admin.manage_users'))

    result = delete_user(user_id)
    # SỬA LỖI: Flash một dictionary
    flash({'message': result['message']}, 'success' if result['success'] else 'danger')
    return redirect(url_for('admin.manage_users'))


@admin_bp.route('/users/export')
def export_users():
    """Tạo và gửi file Excel chứa danh sách người dùng."""
    try:
        excel_bytes = export_users_to_excel_bytes()

        if excel_bytes:
            return send_file(
                io.BytesIO(excel_bytes),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='danh_sach_tai_khoan.xlsx'
            )
        else:
            # SỬA LỖI: Flash một dictionary
            flash({'message': "Không có người dùng nào để xuất ra file."}, "info")
            return redirect(url_for('admin.manage_users'))
    except Exception as e:
        # SỬA LỖI: Flash một dictionary
        flash({'message': f"Có lỗi xảy ra khi xuất file: {e}"}, "danger")
        return redirect(url_for('admin.manage_users'))