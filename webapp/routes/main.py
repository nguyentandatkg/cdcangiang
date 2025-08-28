# webapp/routes/main.py (Phiên bản đồng bộ - Đã sửa lỗi và cập nhật quyền)

# --- Các import gốc của bạn ---
from flask import (Blueprint, render_template, session, redirect, url_for, 
                   request, flash, send_file, current_app, g)
from datetime import datetime, timedelta, date
import os
import traceback
import uuid
from functools import wraps
from sqlalchemy.orm import joinedload
from math import ceil
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename # <-- Giữ lại import này
from flask import jsonify # Nhớ thêm jsonify vào import
from sqlalchemy import func
# --- Imports từ project của bạn ---
from webapp.core.database_utils import get_db_session
from webapp.core.database_setup import DonViHanhChinh, CaBenh, O_Dich, NguoiDung
from webapp.core.week_calendar import WeekCalendar
from webapp.core.report_generator import (
    generate_benh_truyen_nhiem_report, generate_sxh_report, 
    generate_odich_sxh_report, generate_odich_tcm_report,
    generate_benh_truyen_nhiem_report_monthly, generate_sxh_report_monthly, generate_all_reports_zip, generate_custom_btn_report,
    generate_cases_export
)
# === THAY ĐỔI: Import trực tiếp hàm logic, không qua task nữa ===
from webapp.core.data_importer import import_data_from_excel
	
from webapp.core.dashboard_utils import (
    create_cases_by_week_chart, get_top_diseases, 
    create_top_diseases_chart, create_disease_pie_chart
)
from webapp.core.admin_utils import (
    get_cases_by_user_scope, update_case, delete_case, add_new_case,
    get_odich_by_user_scope, add_new_odich, delete_odich, get_odich_by_id, update_odich,
    get_all_don_vi, get_unassigned_cases, link_cases_to_odich, unlink_case_from_odich
)
from webapp.core.utils import get_all_child_xa_ids
from webapp.core.forms import ChangePasswordForm
from webapp import cache # <<< THÊM DÒNG NÀY

main_bp = Blueprint('main', __name__)

# ==============================================================================
# DECORATORS VÀ HÀM TRỢ GIÚP (CẢI TIẾN CHÍNH)
# ==============================================================================
# ĐỊNH NGHĨA CÁC LỰA CHỌN Ở BÊN NGOÀI ĐỂ DỄ QUẢN LÝ
CASE_OPTIONS = {
    'chan_doan_chinh': [
    'Dịch hạch',
    'Cúm A(H5N1)',
    'Cúm A(H7N9)',
    'Viêm đường hô hấp Trung đông (MERS-CoV)',
    'Ê-bô-la (Ebolla)',
    'Lát-sa (Lassa)',
    'Mác-bớt (Marburg)',
    'Sốt Tây sông Nin',
    'Sốt Vàng',
    'Than',
    'Bệnh truyền nhiễm nguy hiểm và Bệnh chưa rõ tác nhân gây bệnh',

    # Nhóm B
    'Bạch hầu',
    'Bệnh do liên cầu lợn ở người',
    'COVID-19',
    'Dại',
    'Ho gà',
    'Lao phổi',
    'Liệt mềm cấp nghi bại liệt',
    'Rubella (Rubeon)',
    'Sởi',
    'Sốt rét',
    'Sốt xuất huyết Dengue',
    'Tả',
    'Tay - chân - miệng',
    'Thương hàn',
    'Thủy đậu',
    'Uốn ván sơ sinh',
    'Uốn ván khác',
    'Viêm gan vi rút A',
    'Viêm gan vi rút B',
    'Viêm gan vi rút C',
    'Viêm gan vi rút khác',
    'Viêm màng não do não mô cầu',
    'Viêm não Nhật bản',
    'Viêm não vi rút khác',
    'Xoắn khuẩn vàng da (Leptospira)',
    'Zika',
    'Chikungunya',
    
    # Nhóm C
    'Bệnh do vi rút Adeno',
    'Cúm',
    'Lỵ amíp',
    'Lỵ trực trùng',
    'Quai bị',
    'Tiêu chảy',
    
    # Các mục khác
    'Thay đổi chẩn đoán- Bệnh không thuộc danh mục',
    'Đã điều tra nhưng không có ca bệnh trên địa bàn'
    ],
    'phan_do_benh': {
        'Sốt xuất huyết Dengue': [
            'Sốt xuất huyết Dengue',
            'Sốt xuất huyết Dengue có dấu hiệu cảnh báo',
            'Sốt xuất huyết Dengue nặng'
        ],
        'Tay - chân - miệng': [
            'Độ 1',
            'Độ 2a',
            'Độ 2b',
            'Độ 3',
            'Độ 4'
        ]
        # Thêm các bệnh khác nếu cần
    },
    'tinh_trang_hien_nay': [
        'Điều trị ngoại trú',
        'Điều trị nội trú',
        'Ra viện',
        'Tử vong',
        'Chuyển viện',
        'Tình trạng khác'
    ]
}

@main_bp.before_request
def before_request_handler():
    """
    Hàm này chạy trước MỌI request trong blueprint này.
    1. Yêu cầu đăng nhập.
    2. Mở session DB và lưu vào `g.db`.
    3. Lấy thông tin đơn vị của user và lưu vào `g.user_don_vi`.
    """
    # Kiểm tra đăng nhập trước
    if 'user_id' not in session and request.endpoint != 'auth.login':
        flash('Vui lòng đăng nhập để truy cập trang này.', 'warning')
        return redirect(url_for('auth.login'))

    # Mở session DB cho mỗi request và lưu vào global context `g`
    g.db = get_db_session()

    if 'user_id' in session:
        # Lấy thông tin đơn vị của người dùng và lưu vào `g` để tái sử dụng
        # Sử dụng g.db thay vì get_db_session() lần nữa
        if session.get('role') == 'admin':
            g.user_don_vi = g.db.query(DonViHanhChinh).options(
                joinedload(DonViHanhChinh.children).joinedload(DonViHanhChinh.children)
            ).filter_by(cap_don_vi='Tỉnh').first()
        else:
            g.user_don_vi = g.db.query(DonViHanhChinh).options(
                joinedload(DonViHanhChinh.children).joinedload(DonViHanhChinh.children)
            ).get(session.get('don_vi_id'))
        
        if not g.user_don_vi:
            # Nếu không tìm thấy đơn vị, xóa session và yêu cầu đăng nhập lại
            session.pop('user_id', None)
            flash("Lỗi: Không tìm thấy thông tin đơn vị của bạn. Vui lòng đăng nhập lại.", "danger")
            return redirect(url_for('auth.login'))

@main_bp.teardown_request
def teardown_request_handler(exception=None):
    """Hàm này chạy sau MỌI request, đảm bảo session DB luôn được đóng."""
    # Sử dụng g.pop để lấy và xóa giá trị, tránh lỗi nếu g không có 'db'
    db = g.pop('db', None)
    if db is not None:
        db.close()

# ==============================================================================
# CÁC ROUTE ĐÃ ĐƯỢC TỐI ƯU HÓA
# ==============================================================================

@main_bp.route('/')
def home_page():
    return redirect(url_for('main.dashboard_page'))

@main_bp.route('/dashboard')
@cache.cached(timeout=300, query_string=True) # <<< THÊM DECORATOR NÀY
def dashboard_page():
    user_don_vi = g.user_don_vi
    db = g.db

    # --- XỬ LÝ CÁC BỘ LỌC TỪ URL ---
    time_range = request.args.get('time_range', '30d')
    disease_filter = request.args.get('disease', 'Tất cả')
    khu_vuc_id = request.args.get('khu_vuc_id')
    xa_id = request.args.get('xa_id')

    # --- XỬ LÝ DỮ LIỆU CHO BIỂU ĐỒ TOP BỆNH ---
    end_date = date.today()
    if time_range == '7d':
        start_date = end_date - timedelta(days=6)
        time_range_text = "7 ngày qua"
    elif time_range == 'this_year':
        start_date = date(end_date.year, 1, 1)
        time_range_text = f"Năm {end_date.year}"
    else:  # Mặc định '30d'
        start_date = end_date - timedelta(days=29)
        time_range_text = "30 ngày qua"

    top_diseases_df = get_top_diseases(user_don_vi, start_date, end_date, khu_vuc_id=khu_vuc_id, xa_id=xa_id)
    
    # --- LẤY DỮ LIỆU CHO DROPDOWN LỌC ---
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    diseases_in_scope = db.query(CaBenh.chan_doan_chinh).filter(CaBenh.xa_id.in_(xa_ids_to_query)).distinct().order_by(CaBenh.chan_doan_chinh).all()
    disease_list = [d[0] for d in diseases_in_scope if d[0]]

    khu_vuc_list = []
    xa_list = []
    if session.get('role') == 'admin':
        khu_vuc_list = db.query(DonViHanhChinh).filter_by(cap_don_vi='Khu vực').order_by(DonViHanhChinh.ten_don_vi).all()
        xa_list = db.query(DonViHanhChinh).filter_by(cap_don_vi='Xã').order_by(DonViHanhChinh.ten_don_vi).all()
    
    filter_data = {'khu_vuc_list': khu_vuc_list, 'xa_list': xa_list}

    # --- TẠO CÁC BIỂU ĐỒ ---
    cases_by_week_chart_json = create_cases_by_week_chart(user_don_vi, disease_filter, khu_vuc_id=khu_vuc_id, xa_id=xa_id)
    top_diseases_chart_json = create_top_diseases_chart(top_diseases_df, time_range_text)
    disease_pie_chart_json = create_disease_pie_chart(top_diseases_df, time_range_text)

    return render_template(
        'dashboard.html',
        title='Bảng điều khiển',
        cases_by_week_chart_json=cases_by_week_chart_json,
        top_diseases_chart_json=top_diseases_chart_json,
        disease_pie_chart_json=disease_pie_chart_json,
        top_diseases_data=top_diseases_df.to_dict('records'),
        active_time_range=time_range,
        disease_list=disease_list,
        active_disease=disease_filter,
        filter_data=filter_data
    )

@main_bp.route('/report', methods=['GET', 'POST'])
def report_page():
    user_don_vi = g.user_don_vi
    db = g.db

    if request.method == 'POST':
        year = int(request.form.get('year'))
        week_number = int(request.form.get('week_number'))
        month_number = int(request.form.get('month_number'))
        report_template = request.form.get('report_template')

        # Cập nhật report_handlers với các lựa chọn mới
        report_handlers = {
            "Báo cáo Bệnh truyền nhiễm tổng hợp": {
                "func": generate_benh_truyen_nhiem_report,
                "args": (db, WeekCalendar(year), week_number, user_don_vi),
                "name": f"BaoCao_BTN_{user_don_vi.ten_don_vi}_{year}_Tuan{week_number}.xlsx"
            },
            "Báo cáo Sốt Xuất Huyết": {
                "func": generate_sxh_report,
                "args": (db, WeekCalendar(year), week_number, user_don_vi),
                "name": f"BaoCao_SXH_{user_don_vi.ten_don_vi}_{year}_Tuan{week_number}.xlsx"
            },
            "Báo cáo Ổ dịch SXH": {
                "func": generate_odich_sxh_report,
                "args": (db, WeekCalendar(year), week_number, user_don_vi),
                "name": f"BaoCao_ODich_SXH_{user_don_vi.ten_don_vi}_{year}_Tuan{week_number}.xlsx"
            },
            "Báo cáo Ổ dịch TCM": {
                "func": generate_odich_tcm_report,
                "args": (db, WeekCalendar(year), week_number, user_don_vi),
                "name": f"BaoCao_ODich_TCM_{user_don_vi.ten_don_vi}_{year}_Tuan{week_number}.xlsx"
            },
            "Báo cáo BTN theo tháng": {
                "func": generate_benh_truyen_nhiem_report_monthly,
                "args": (db, year, month_number, user_don_vi),
                "name": f"BaoCao_BTN_Thang_{user_don_vi.ten_don_vi}_{year}_Thang{month_number}.xlsx"
            },
            "Báo cáo SXH theo tháng": {
                "func": generate_sxh_report_monthly,
                "args": (db, year, month_number, user_don_vi),
                "name": f"BaoCao_SXH_Thang_{user_don_vi.ten_don_vi}_{year}_Thang{month_number}.xlsx"
            },
            # --- TÍNH NĂNG MỚI ---
            "Tất cả báo cáo (Tuần)": {
                "func": generate_all_reports_zip,
                "args": (db, user_don_vi, year, 'week', week_number),
                "name": f"TatCaBaoCao_{user_don_vi.ten_don_vi}_{year}_Tuan{week_number}.zip"
            },
            "Tất cả báo cáo (Tháng)": {
                "func": generate_all_reports_zip,
                "args": (db, user_don_vi, year, 'month', month_number),
                "name": f"TatCaBaoCao_{user_don_vi.ten_don_vi}_{year}_Thang{month_number}.zip"
            }
        }

        handler = report_handlers.get(report_template)
        if handler:
            # Thay đổi tên file từ .xlsx thành .zip nếu cần
            file_extension = ".zip" if "zip" in handler["name"] else ".xlsx"
            random_filename = f"{uuid.uuid4()}{file_extension}"
            filepath = os.path.join(current_app.config['REPORT_FOLDER'], random_filename)
            
            try:
                handler["func"](*handler["args"], filepath)
                session['last_report'] = {'filename': random_filename, 'display_name': handler["name"]}
                flash({'message': "Tạo báo cáo thành công!"}, "success")
            except Exception as e:
                current_app.logger.error(f"Lỗi tạo báo cáo: {e}\n{traceback.format_exc()}")
                flash({'message': f'Đã có lỗi xảy ra khi tạo báo cáo: {e}'}, 'danger')
        else:
            flash({'message': 'Không thể tạo báo cáo, vui lòng chọn mẫu báo cáo.'}, 'warning')

        return redirect(url_for('main.report_page'))

    # Logic cho GET request
    report_info = session.pop('last_report', None)
    now = datetime.now()

    # Bổ sung: Lấy dữ liệu cho form Báo cáo Tùy chỉnh
    custom_report_donvi_options = {}
    user_cap = user_don_vi.cap_don_vi
    user_role = session.get('role')
    
    # Chỉ admin, tỉnh và khu vực mới thấy tùy chọn này
    if user_role == 'admin' or user_cap in ['Tỉnh', 'Khu vực']:
        if user_role == 'admin' or user_cap == 'Tỉnh':
            khu_vuc_list = db.query(DonViHanhChinh).filter_by(cap_don_vi='Khu vực').order_by(DonViHanhChinh.ten_don_vi).all()
            xa_list = db.query(DonViHanhChinh).filter_by(cap_don_vi='Xã').order_by(DonViHanhChinh.ten_don_vi).all()
            custom_report_donvi_options['Khu vực'] = khu_vuc_list
            custom_report_donvi_options['Xã'] = xa_list
        elif user_cap == 'Khu vực':
            # Khu vực chỉ thấy các xã của mình
            xa_list = [c for c in user_don_vi.children if c.cap_don_vi == 'Xã']
            custom_report_donvi_options['Xã'] = sorted(xa_list, key=lambda x: x.ten_don_vi)
            
    return render_template(
        'report.html',
        title='Tạo Báo cáo',
        current_year=now.year,
        selected_year=now.year,
        selected_month=now.month,
        selected_week=now.isocalendar()[1],
        report_info=report_info,
        custom_report_donvi_options=custom_report_donvi_options # <-- Truyền dữ liệu mới
    )

@main_bp.route('/download_report/<filename>/<display_name>')
def download_report(filename, display_name):
    filepath = os.path.join(current_app.config['REPORT_FOLDER'], filename)
    try:
        return send_file(filepath, as_attachment=True, download_name=display_name)
    except FileNotFoundError:
        flash("Không tìm thấy file báo cáo hoặc file đã quá hạn. Vui lòng tạo lại.", "danger")
        return redirect(url_for('main.report_page'))


# ==============================================================================
# ROUTE MỚI CHO BÁO CÁO TÙY CHỈNH
# ==============================================================================
@main_bp.route('/report/custom-btn', methods=['POST'])
def custom_btn_report_action():
    user_don_vi = g.user_don_vi
    db = g.db
    
    try:
        # 1. Lấy và xác thực dữ liệu từ form
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')
        selected_ids_str = request.form.getlist('don_vi_ids') # .getlist() cho multi-select

        if not all([start_date_str, end_date_str, selected_ids_str]):
            flash({'message': 'Vui lòng điền đầy đủ thông tin: ngày bắt đầu, ngày kết thúc và chọn ít nhất một đơn vị.'}, 'warning')
            return redirect(url_for('main.report_page'))

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        selected_don_vi_ids = [int(id_str) for id_str in selected_ids_str]

        # 2. Chuẩn bị file để lưu
        display_name = f"BaoCao_BTN_TuyChinh_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.xlsx"
        random_filename = f"{uuid.uuid4()}.xlsx"
        filepath = os.path.join(current_app.config['REPORT_FOLDER'], random_filename)

        # 3. Gọi hàm xử lý chính
        generate_custom_btn_report(
            db_session=db,
            user_don_vi=user_don_vi,
            start_date=start_date,
            end_date=end_date,
            selected_don_vi_ids=selected_don_vi_ids,
            filepath=filepath
        )

        # 4. Trả kết quả về cho người dùng (theo pattern đã có)
        session['last_report'] = {'filename': random_filename, 'display_name': display_name}
        flash({'message': "Tạo báo cáo tùy chỉnh thành công!"}, "success")

    except (ValueError, PermissionError) as e:
        # Bắt các lỗi đã định nghĩa trong report_generator
        current_app.logger.warning(f"Lỗi tạo báo cáo tùy chỉnh: {e}")
        flash({'message': f'Lỗi: {e}'}, 'danger')
    except Exception as e:
        # Bắt các lỗi không lường trước
        current_app.logger.error(f"Lỗi không xác định khi tạo báo cáo tùy chỉnh: {e}\n{traceback.format_exc()}")
        flash({'message': 'Đã có lỗi không xác định xảy ra khi tạo báo cáo.'}, 'danger')

    return redirect(url_for('main.report_page'))

# ==============================================================================
# ROUTE IMPORT ĐÃ ĐƯỢC CẬP NHẬT
# ==============================================================================

@main_bp.route('/import', methods=['GET', 'POST'])
def import_page():
    if session.get('role') not in ['admin', 'khuvuc']:
        flash('Bạn không có quyền truy cập chức năng này.', 'warning')
        return redirect(url_for('main.report_page'))

    if request.method == 'POST':
        if 'excel_file' not in request.files or not request.files['excel_file'].filename:
            flash({'message': 'Không có file nào được chọn.'}, 'danger')
            return redirect(request.url)

        file = request.files['excel_file']
        
        if file and file.filename.endswith('.xlsx'):
            # Vẫn cần lưu file tạm thời để hàm import có thể đọc
            filename = secure_filename(file.filename)
            temp_dir = os.path.join(os.path.dirname(current_app.root_path), 'tmp')
            os.makedirs(temp_dir, exist_ok=True)
            filepath = os.path.join(temp_dir, filename)
            
            try:
                file.save(filepath)
                
                # Tham số user_xa_id không còn phù hợp khi Khu vực import
                # Hàm import_data_from_excel sẽ cần tự xác định xã từ file Excel
                result = import_data_from_excel(filepath=filepath, user_xa_id=None)

                # Hiển thị thông báo dựa trên kết quả trả về
                if result.get('success'):
                    # Flash một dictionary chứa thông báo và danh sách lỗi
                    flash({'message': result.get('message'), 'errors': result.get('errors', [])}, 'success')
                else:
                    flash({'message': result.get('message')}, 'danger')

            except Exception as e:
                current_app.logger.error(f"Lỗi không xác định khi import: {e}\n{traceback.format_exc()}")
                flash({'message': f'Đã có lỗi không xác định xảy ra: {e}'}, 'danger')
            finally:
                # Rất QUAN TRỌNG: Luôn xóa file tạm sau khi xử lý xong
                if os.path.exists(filepath):
                    os.remove(filepath)
        else:
            flash({'message': 'Chỉ chấp nhận file định dạng .xlsx'}, 'warning')
        
        # Sau khi xử lý xong, redirect lại chính trang import để hiển thị thông báo
        return redirect(url_for('main.import_page'))

    # Nếu là GET request, chỉ hiển thị trang
    return render_template('import.html', title='Import Dữ liệu')

# --- CÁC ROUTE QUẢN LÝ CA BỆNH ---

@main_bp.route('/cases', methods=['GET'])
def cases_page():
    user_don_vi = g.user_don_vi
    db = g.db

    page = request.args.get('page', 1, type=int)
    PER_PAGE = 20

    # Lấy tất cả các tham số lọc từ URL
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')
    report_start_date_str = request.args.get('report_start_date', '')
    report_end_date_str = request.args.get('report_end_date', '')
    chan_doan = request.args.get('chan_doan', '')
    ho_ten = request.args.get('ho_ten', '')
    khu_vuc_id_str = request.args.get('khu_vuc_id', '')
    xa_id_str = request.args.get('xa_id', '')
    dia_chi_ap = request.args.get('dia_chi_ap', '')

    filter_data = {'khu_vuc_list': [], 'xa_list': [], 'ap_list': [], 'chan_doan_list': []}

    # Chuyển đổi và tạo dictionary filters để truy vấn
    filters = {
        'chan_doan': chan_doan,
        'ho_ten': ho_ten,
        'khu_vuc_id': int(khu_vuc_id_str) if khu_vuc_id_str.isdigit() else None,
        'xa_id': int(xa_id_str) if xa_id_str.isdigit() else None,
        'dia_chi_ap': dia_chi_ap,
    }
    try:
        if start_date_str: filters['start_date'] = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        if end_date_str: filters['end_date'] = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        if report_start_date_str: filters['report_start_date'] = datetime.strptime(report_start_date_str, '%Y-%m-%d').date()
        if report_end_date_str: filters['report_end_date'] = datetime.strptime(report_end_date_str, '%Y-%m-%d').date()
    except ValueError:
        flash("Định dạng ngày không hợp lệ. Vui lòng chọn lại.", "warning")
        filters.pop('start_date', None); filters.pop('end_date', None)
        filters.pop('report_start_date', None); filters.pop('report_end_date', None)

    # Lấy dữ liệu cho các dropdown lọc động dựa trên quyền người dùng
    user_cap = user_don_vi.cap_don_vi

    if session.get('role') == 'admin' or user_cap == 'Tỉnh':
        filter_data['khu_vuc_list'] = db.query(DonViHanhChinh).filter_by(cap_don_vi='Khu vực').order_by(DonViHanhChinh.ten_don_vi).all()
        filter_data['xa_list'] = db.query(DonViHanhChinh).filter_by(cap_don_vi='Xã').order_by(DonViHanhChinh.ten_don_vi).all()
    
    elif user_cap == 'Khu vực':
        filter_data['xa_list'] = db.query(DonViHanhChinh).filter_by(parent_id=user_don_vi.id, cap_don_vi='Xã').order_by(DonViHanhChinh.ten_don_vi).all()
    
    elif user_cap == 'Xã':
        filter_data['ap_list'] = db.query(DonViHanhChinh).filter_by(parent_id=user_don_vi.id, cap_don_vi='Ấp').order_by(DonViHanhChinh.ten_don_vi).all()

    # Lấy danh sách chẩn đoán duy nhất
    xa_ids_in_scope = get_all_child_xa_ids(user_don_vi)
    if xa_ids_in_scope:
        chan_doan_query = db.query(CaBenh.chan_doan_chinh).distinct().filter(
            CaBenh.xa_id.in_(xa_ids_in_scope),
            CaBenh.chan_doan_chinh.isnot(None)
        ).order_by(CaBenh.chan_doan_chinh).all()
        filter_data['chan_doan_list'] = [item[0] for item in chan_doan_query]

    # Thực hiện truy vấn chính
    cases_paginated, total_cases = get_cases_by_user_scope(user_don_vi, filters, page=page, per_page=PER_PAGE)

    pagination = {
        'page': page, 'per_page': PER_PAGE, 'total_items': total_cases,
        'total_pages': ceil(total_cases / PER_PAGE)
    }

    # Dùng dictionary chứa các chuỗi gốc để điền lại form
    filters_for_template = {
        'start_date': start_date_str, 'end_date': end_date_str,
        'report_start_date': report_start_date_str, 'report_end_date': report_end_date_str,
        'chan_doan': chan_doan, 'ho_ten': ho_ten,
        'khu_vuc_id': khu_vuc_id_str, 'xa_id': xa_id_str, 'dia_chi_ap': dia_chi_ap
    }

    # THAY ĐỔI QUAN TRỌNG: Lấy chuỗi query string từ request hiện tại
    # request.query_string là dạng bytes, cần decode thành utf-8
    current_query_string = request.query_string.decode('utf-8')

    return render_template(
        'cases.html',
        title='Quản lý Ca bệnh',
        cases=cases_paginated,
        pagination=pagination,
        filters=filters_for_template,
        filter_data=filter_data,
        current_query_string=current_query_string # <-- TRUYỀN BIẾN MỚI NÀY VÀO TEMPLATE
    )

@main_bp.route('/cases/export', methods=['GET'])
def export_cases():
    """
    Xuất danh sách ca bệnh ra file Excel.
    Chỉ cho phép admin và user cấp khu vực truy cập.
    """
    if session.get('role') not in ['admin', 'khuvuc']:
        return jsonify({'error': 'Không có quyền truy cập'}), 403

    db = g.db
    user_don_vi = g.user_don_vi

    # Lấy tất cả các tham số lọc từ URL
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')
    chan_doan = request.args.get('chan_doan', '')
    ho_ten = request.args.get('ho_ten', '')
    khu_vuc_id_str = request.args.get('khu_vuc_id', '')
    xa_id_str = request.args.get('xa_id', '')
    dia_chi_ap = request.args.get('dia_chi_ap', '')

    # Chuyển đổi và tạo dictionary filters để truy vấn
    filters = {
        'chan_doan': chan_doan,
        'ho_ten': ho_ten,
        'khu_vuc_id': int(khu_vuc_id_str) if khu_vuc_id_str.isdigit() else None,
        'xa_id': int(xa_id_str) if xa_id_str.isdigit() else None,
        'dia_chi_ap': dia_chi_ap,
    }
    try:
        if start_date_str: filters['start_date'] = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        if end_date_str: filters['end_date'] = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        filters.pop('start_date', None)
        filters.pop('end_date', None)

    # Lấy danh sách ca bệnh theo quyền của người dùng
    cases, _ = get_cases_by_user_scope(user_don_vi, filters, page=1, per_page=10000)

    # Tạo file Excel và gửi về cho người dùng
    try:
        output = generate_cases_export(cases)
        response = send_file(
            output,
            as_attachment=True,
            download_name=f'danh_sach_ca_benh_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
        return response
    except Exception as e:
        current_app.logger.error(f"Lỗi xuất Excel: {e}")
        return jsonify({'error': 'Đã có lỗi xảy ra khi xuất file Excel.'}), 500

@main_bp.route('/cases/new', methods=['GET', 'POST'])
def new_case_page():
    if session.get('role') not in ['admin', 'khuvuc']:
        flash("Bạn không có quyền thêm mới ca bệnh.", "warning")
        return redirect(url_for('main.cases_page'))

    db = g.db
    
    if request.method == 'POST':
        ## <<< SỬA LỖI 1: Sửa logic lấy xa_id khi tạo ca bệnh mới
        # Cả admin và khu vực đều phải chọn xã từ form
        if session.get('role') in ['admin', 'khuvuc']:
            xa_id = int(request.form.get('xa_id'))
        else:
            # Fallback an toàn, mặc dù trường hợp này không nên xảy ra do đã check quyền ở đầu hàm
            flash({'message': 'Lỗi phân quyền khi tạo ca bệnh.'}, 'danger')
            return redirect(url_for('main.cases_page'))

        o_dich_id_str = request.form.get('o_dich_id')

        case_data = {
            "ma_so_benh_nhan": request.form.get('ma_so_benh_nhan'),
            "ho_ten": request.form.get('ho_ten'),
            "ngay_sinh": datetime.strptime(request.form.get('ngay_sinh'), '%Y-%m-%d').date() if request.form.get('ngay_sinh') else None,
            "gioi_tinh": request.form.get('gioi_tinh'),
            "dia_chi_ap": request.form.get('dia_chi_ap'),
            "ngay_khoi_phat": datetime.strptime(request.form.get('ngay_khoi_phat'), '%Y-%m-%d').date(),
            "chan_doan_chinh": request.form.get('chan_doan_chinh'),
            "phan_do_benh": request.form.get('phan_do_benh'),
            "tinh_trang_hien_nay": request.form.get('tinh_trang_hien_nay'),
            "xa_id": xa_id,
            "o_dich_id": int(o_dich_id_str) if o_dich_id_str and o_dich_id_str.isdigit() else None
        }

        result = add_new_case(case_data)
        if result['success']:
            flash({'message': result['message']}, 'success')
            # THAY ĐỔI QUAN TRỌNG: Chuyển hướng về trang danh sách với bộ lọc được giữ lại
            query_string_from_form = request.form.get('query_string', '')
            return redirect(url_for('main.cases_page') + '?' + query_string_from_form)
        else:
            flash({'message': result['message']}, 'danger')

    # --- Logic cho GET request ---
    # Lấy danh sách cần thiết cho các dropdown trong form
    all_don_vi_list, _ = get_all_don_vi(page=1, per_page=10000)

    # Chuẩn bị dữ liệu cho JavaScript
    xa_list_js = [dv.to_dict() for dv in all_don_vi_list if dv.cap_don_vi == 'Xã']
    ap_list_js = [dv.to_dict() for dv in all_don_vi_list if dv.cap_don_vi == 'Ấp']

    # Lấy tất cả ổ dịch để lọc bằng JS
    all_odich = db.query(O_Dich).all()
    odich_list_js = [
        {'id': od.id, 'loai_benh': od.loai_benh, 'xa_id': od.xa_id, 'display': f"#{od.id} - {od.loai_benh} ({od.ngay_phat_hien.strftime('%d/%m/%Y')})"}
        for od in all_odich
    ]

    # THAY ĐỔI QUAN TRỌNG: Lấy chuỗi query string từ URL
    current_query_string = request.query_string.decode('utf-8')

    return render_template(
        'new_case.html',
        title='Thêm Ca bệnh mới',
        xa_list_js=xa_list_js,
        ap_list_js=ap_list_js,
        odich_list_js=odich_list_js,
        current_query_string=current_query_string, # <-- Truyền vào template
        case_options=CASE_OPTIONS
    )

@main_bp.route('/cases/edit/<int:case_id>', methods=['GET', 'POST'])
def edit_case_page(case_id):
    db = g.db
    
    case = db.query(CaBenh).options(joinedload(CaBenh.don_vi).joinedload(DonViHanhChinh.children)).filter_by(id=case_id).first()
    is_admin = session.get('role') == 'admin'
    user_can_edit = False
    if is_admin:
        user_can_edit = True
    elif session.get('role') == 'khuvuc' and case:
        # Lấy tất cả ID xã con của đơn vị khu vực đang đăng nhập
        child_xa_ids = get_all_child_xa_ids(g.user_don_vi)
        if case.xa_id in child_xa_ids:
            user_can_edit = True

    if not user_can_edit:
        flash({'message': 'Không tìm thấy ca bệnh hoặc bạn không có quyền truy cập.'}, 'danger')
        return redirect(url_for('main.cases_page'))

    if request.method == 'POST':
        o_dich_id = request.form.get('o_dich_id')
        new_data = {
            'ho_ten': request.form.get('ho_ten'),
            'ngay_sinh': datetime.strptime(request.form.get('ngay_sinh'), '%Y-%m-%d').date() if request.form.get('ngay_sinh') else None,
            'dia_chi_ap': request.form.get('dia_chi_ap'),
            'ngay_khoi_phat': datetime.strptime(request.form.get('ngay_khoi_phat'), '%Y-%m-%d').date() if request.form.get('ngay_khoi_phat') else None,
            'chan_doan_chinh': request.form.get('chan_doan_chinh'),
            'tinh_trang_hien_nay': request.form.get('tinh_trang_hien_nay'),
            'o_dich_id': int(o_dich_id) if o_dich_id and o_dich_id.isdigit() else None
        }
        result = update_case(case_id, new_data)
        if result['success']:
            flash({'message': result['message']}, 'success')
        else:
            flash({'message': result['message']}, 'danger')
        
        # THAY ĐỔI QUAN TRỌNG: Chuyển hướng về trang danh sách với bộ lọc được giữ lại
        query_string_from_form = request.form.get('query_string', '')
        return redirect(url_for('main.cases_page') + '?' + query_string_from_form)

    # --- Logic cho GET request ---
    ap_list = [u.ten_don_vi for u in case.don_vi.children if u.cap_don_vi == 'Ấp']
    loai_benh_map = {'Sốt xuất huyết Dengue': 'SXH', 'Tay - chân - miệng': 'TCM'}
    loai_benh_od = loai_benh_map.get(case.chan_doan_chinh)
    odich_list = []
    if loai_benh_od:
        odich_list = db.query(O_Dich).filter_by(xa_id=case.xa_id, loai_benh=loai_benh_od).order_by(O_Dich.ngay_phat_hien.desc()).all()
    
    # THAY ĐỔI QUAN TRỌNG: Lấy chuỗi query string từ URL
    current_query_string = request.query_string.decode('utf-8')
        
    return render_template(
        'edit_case.html',
        title='Sửa Ca bệnh',
        case=case,
        ap_list=ap_list,
        odich_list=odich_list,
        current_query_string=current_query_string, # <-- Truyền vào template
        case_options=CASE_OPTIONS
    )


@main_bp.route('/cases/delete/<int:case_id>', methods=['POST'])
def delete_case_action(case_id):
    db = g.db
    
    case = db.query(CaBenh).filter_by(id=case_id).first()
    ## <<< SỬA LỖI 2: Thêm định nghĩa cho biến is_admin
    is_admin = session.get('role') == 'admin'
    user_can_delete = False
    if is_admin:
        user_can_delete = True
    elif session.get('role') == 'khuvuc' and case:
        child_xa_ids = get_all_child_xa_ids(g.user_don_vi)
        if case.xa_id in child_xa_ids:
            user_can_delete = True

    if not user_can_delete:
        flash({'message': 'Không tìm thấy ca bệnh hoặc bạn không có quyền truy cập.'}, 'danger')
        return redirect(url_for('main.cases_page'))

    result = delete_case(case_id)
    if result['success']:
        flash({'message': 'Đã xóa ca bệnh thành công.'}, 'success')
    else:
        flash({'message': f"Xóa ca bệnh thất bại. Lỗi: {result.get('message', 'Không rõ')}"}, 'danger')

    return redirect(url_for('main.cases_page'))


@main_bp.route('/cases/view/<int:case_id>', methods=['GET'])
def view_case_page(case_id):
    """Hiển thị thông tin chi tiết của một ca bệnh."""
    db = g.db
    user_don_vi = g.user_don_vi
    
    case = db.query(CaBenh).options(
        joinedload(CaBenh.don_vi),
        joinedload(CaBenh.o_dich)
    ).filter_by(id=case_id).first()

    if not case:
        flash({'message': "Không tìm thấy ca bệnh."}, "danger")
        return redirect(url_for('main.cases_page'))

    # Phân quyền xem
    user_can_view = False
    if session.get('role') == 'admin':
        user_can_view = True
    else:
        xa_ids_in_scope = get_all_child_xa_ids(user_don_vi)
        if case.xa_id in xa_ids_in_scope:
            user_can_view = True

    if not user_can_view:
        flash({'message': "Bạn không có quyền xem thông tin ca bệnh này."}, "warning")
        return redirect(url_for('main.cases_page'))

    # THAY ĐỔI QUAN TRỌNG: Lấy chuỗi query string từ URL
    current_query_string = request.query_string.decode('utf-8')

    return render_template(
        'view_case.html', 
        title='Chi tiết Ca bệnh', 
        case=case,
        current_query_string=current_query_string # <-- Truyền vào template
    )

# --- CÁC ROUTE QUẢN LÝ Ổ DỊCH ---

@main_bp.route('/odich', methods=['GET'])
def manage_odich_page():
    allowed_roles = ['admin', 'xa', 'khuvuc', 'tinh']
    if session.get('role') not in allowed_roles:
        flash({'message': 'Bạn không có quyền truy cập chức năng này.'}, 'warning')
        return redirect(url_for('main.report_page'))

    db = g.db
    user_don_vi = g.user_don_vi
    
    # --- BƯỚC 1: XÁC ĐỊNH PHẠM VI DỮ LIỆU BAN ĐẦU CỦA USER ---
    xa_ids_in_scope = get_all_child_xa_ids(user_don_vi)
    query = db.query(O_Dich).options(joinedload(O_Dich.don_vi)).filter(O_Dich.xa_id.in_(xa_ids_in_scope))

    # --- BƯỚC 2: ĐỌC VÀ ÁP DỤNG CÁC BỘ LỌC TỪ URL ---
    page = request.args.get('page', 1, type=int)
    PER_PAGE = 20
    
    # Lọc theo loại bệnh
    loai_benh_filter = request.args.get('loai_benh')
    if loai_benh_filter:
        query = query.filter(O_Dich.loai_benh == loai_benh_filter)

    # Lọc theo trạng thái xử lý
    trang_thai_filter = request.args.get('trang_thai')
    if trang_thai_filter == 'daxuly':
        query = query.filter(O_Dich.ngay_xu_ly.isnot(None))
    elif trang_thai_filter == 'chuaxuly':
        query = query.filter(O_Dich.ngay_xu_ly.is_(None))

    # Lọc theo xã (chỉ dành cho admin)
    xa_id_filter = request.args.get('xa_id')
    if xa_id_filter and session.get('role') == 'admin':
        query = query.filter(O_Dich.xa_id == int(xa_id_filter))

    # Lọc theo khoảng ngày phát hiện
    start_date_filter = request.args.get('start_date')
    if start_date_filter:
        query = query.filter(O_Dich.ngay_phat_hien >= start_date_filter)
    
    end_date_filter = request.args.get('end_date')
    if end_date_filter:
        query = query.filter(O_Dich.ngay_phat_hien <= end_date_filter)

    # --- BƯỚC 3: THỰC HIỆN PHÂN TRANG THỦ CÔNG ---
    total_items = query.count()

    query = query.order_by(O_Dich.ngay_phat_hien.desc()).limit(PER_PAGE).offset((page - 1) * PER_PAGE)
    
    odich_list = query.all()

    pagination = {
        'page': page,
        'per_page': PER_PAGE,
        'total_items': total_items,
        'total_pages': ceil(total_items / PER_PAGE)
    }

    # --- BƯỚC 4: LẤY DỮ LIỆU CHO DROPDOWN LỌC (CHO ADMIN) ---
    all_xa_list = []
    if session.get('role') == 'admin':
        all_xa_list = db.query(DonViHanhChinh).filter_by(cap_don_vi='Xã').order_by(DonViHanhChinh.ten_don_vi).all()

    return render_template(
        'odich.html',
        title='Quản lý Ổ dịch',
        odich_list=odich_list,
        pagination=pagination,
        all_xa_list=all_xa_list
    )


@main_bp.route('/odich/new', methods=['GET', 'POST'])
def new_odich_page():
    if session.get('role') not in ['admin', 'khuvuc']:
        flash({'message': "Bạn không có quyền tạo ổ dịch."}, "warning")
        return redirect(url_for('main.manage_odich_page'))

    case_id_from_url = request.args.get('from_case_id', type=int)
    prefill_data = {}
    case = None
    all_xa_list = None
    db = g.db

    if request.method == 'POST':
        case_id_from_form = request.form.get('case_id_from_url')
        if case_id_from_form:
            case_id_from_form = int(case_id_from_form)
            
        xa_id_str = request.form.get('xa_id')

        # --- VALIDATION ---
        validation_error = False
        if not xa_id_str or not xa_id_str.isdigit():
            flash({'message': "Lỗi: Vui lòng chọn một xã hợp lệ."}, "danger")
            validation_error = True

        if not request.form.get('ngay_phat_hien'):
            flash({'message': "Lỗi: Ngày phát hiện là trường bắt buộc."}, "danger")
            validation_error = True

        if validation_error:
            if case_id_from_form:
                case = db.query(CaBenh).options(joinedload(CaBenh.don_vi)).get(case_id_from_form)
            if session.get('role') == 'admin':
                all_xa_list = db.query(DonViHanhChinh).filter_by(cap_don_vi='Xã').order_by(DonViHanhChinh.ten_don_vi).all()

            return render_template(
                'new_odich.html',
                title='Khai báo Ổ dịch mới',
                all_xa_list=all_xa_list,
                prefill_data=request.form,
                case=case,
                date=date
            )

        # --- PROCESS FORM DATA ---
        xa_id = int(xa_id_str)
        data = {
            'loai_benh': request.form.get('loai_benh'),
            'ngay_phat_hien': datetime.strptime(request.form.get('ngay_phat_hien'), '%Y-%m-%d').date(),
            'ngay_xu_ly': datetime.strptime(request.form.get('ngay_xu_ly'), '%Y-%m-%d').date() if request.form.get('ngay_xu_ly') else None,
            'dia_diem_xu_ly': request.form.get('dia_diem_xu_ly'), # Lấy đúng tên trường
            'xa_id': xa_id,
            'noi_phat_hien_tcm': request.form.get('noi_phat_hien_tcm'),
            'tieu_chi_2ca_7ngay': 'tieu_chi_2ca_7ngay' in request.form,
            'tieu_chi_sxh_nang': 'tieu_chi_sxh_nang' in request.form,
            'tieu_chi_xet_nghiem': 'tieu_chi_xet_nghiem' in request.form,
            'tieu_chi_tu_vong': 'tieu_chi_tu_vong' in request.form,
            'loai_xet_nghiem_sxh': request.form.get('loai_xet_nghiem_sxh')
        }
        
        if case_id_from_form:
            case = db.query(CaBenh).get(case_id_from_form)
            if case:
                # Ưu tiên lấy thông tin từ ca bệnh
                data['dia_diem_xu_ly'] = f"{case.dia_chi_chi_tiet or ''}, {case.dia_chi_ap or ''}"
                data['dia_chi_ap'] = case.dia_chi_ap
                data['so_ca_mac_trong_od'] = 1 # Đặt số ca ban đầu là 1
        

        result = add_new_odich(data)

        if result.get('success'):
            flash({'message': result['message']}, 'success')
            new_odich_id = result.get('new_id')
            
            # Liên kết ca bệnh chỉ điểm SAU KHI đã tạo ổ dịch thành công
            if new_odich_id and case_id_from_form:
                link_cases_to_odich(new_odich_id, [case_id_from_form])
                flash({'message': "Đã tự động liên kết ca bệnh chỉ điểm vào ổ dịch."}, "info")
            
            if new_odich_id:
                return redirect(url_for('main.view_odich_page', odich_id=new_odich_id))
            return redirect(url_for('main.manage_odich_page'))
        else:
            flash({'message': result['message']}, 'danger')

    # --- Handle GET request ---
    if case_id_from_url:
        case = db.query(CaBenh).options(joinedload(CaBenh.don_vi)).get(case_id_from_url)
        if not case:
            flash({'message': "Không tìm thấy ca bệnh chỉ điểm."}, "danger")
            return redirect(url_for('main.cases_page'))

        loai_benh_map = {'Sốt xuất huyết Dengue': 'SXH', 'Tay - chân - miệng': 'TCM'}
        dia_diem_goi_y = f"Ổ dịch tại {case.dia_chi_chi_tiet or ''}, {case.don_vi.ten_don_vi if case.don_vi else ''} (khởi phát từ BN {case.ho_ten})"
        prefill_data = {
            'loai_benh': loai_benh_map.get(case.chan_doan_chinh, ''),
            'ngay_phat_hien': date.today(),
            'xa_ten': case.don_vi.ten_don_vi,
            'xa_id': case.xa_id,
            'dia_diem_xu_ly': dia_diem_goi_y
        }

    if session.get('role') == 'admin':
        all_xa_list = db.query(DonViHanhChinh).filter_by(cap_don_vi='Xã').order_by(DonViHanhChinh.ten_don_vi).all()

    return render_template('new_odich.html', title='Khai báo Ổ dịch mới', all_xa_list=all_xa_list, prefill_data=prefill_data, case=case, date=date)


@main_bp.route('/odich/view/<int:odich_id>', methods=['GET', 'POST'])
def view_odich_page(odich_id):
    db = g.db
    
    odich = get_odich_by_id(odich_id)
    if not odich:
        flash({'message': "Không tìm thấy ổ dịch."}, "warning")
        return redirect(url_for('main.manage_odich_page'))

    is_admin = session.get('role') == 'admin'
    user_don_vi_id = session.get('don_vi_id')
    
    is_manager = False
    if session.get('role') == 'khuvuc' and odich:
        child_xa_ids = get_all_child_xa_ids(g.user_don_vi)
        if odich.xa_id in child_xa_ids:
            is_manager = True
        
    if not (is_admin or is_manager): # <-- THAY ĐỔI Ở ĐÂY
        flash({'message': "Bạn không có quyền xem hoặc sửa ổ dịch này."}, "warning")
        return redirect(url_for('main.manage_odich_page'))

    if request.method == 'POST':
        # Logic POST để cập nhật ổ dịch (giữ nguyên như phiên bản đã sửa lỗi trước)
        data = {
            'loai_benh': odich.loai_benh, 
            'ngay_phat_hien': datetime.strptime(request.form.get('ngay_phat_hien'), '%Y-%m-%d').date(),
            'ngay_xu_ly': datetime.strptime(request.form.get('ngay_xu_ly'), '%Y-%m-%d').date() if request.form.get('ngay_xu_ly') else None,
            'dia_diem_xu_ly': request.form.get('dia_diem_xu_ly')
        }
        if odich.loai_benh == 'TCM':
            data['noi_phat_hien_tcm'] = request.form.get('noi_phat_hien_tcm')
        elif odich.loai_benh == 'SXH':
            data['tieu_chi_2ca_7ngay'] = 'tieu_chi_2ca_7ngay' in request.form
            data['tieu_chi_sxh_nang'] = 'tieu_chi_sxh_nang' in request.form
            data['tieu_chi_xet_nghiem'] = 'tieu_chi_xet_nghiem' in request.form
            data['tieu_chi_tu_vong'] = 'tieu_chi_tu_vong' in request.form
            data['loai_xet_nghiem_sxh'] = request.form.get('loai_xet_nghiem_sxh')
        if is_admin and request.form.get('xa_id'):
            data['xa_id'] = int(request.form.get('xa_id'))
        result = update_odich(odich_id, data)
        if result['success']:
            flash({'message': 'Cập nhật ổ dịch thành công.'}, 'success')
        else:
            flash({'message': f"Lỗi khi cập nhật: {result.get('message', 'Không rõ')}"}, 'danger')
        return redirect(url_for('main.view_odich_page', odich_id=odich_id))

    # --- LOGIC GET REQUEST (ĐÃ NÂNG CẤP) ---
    
    # 1. Xác định "mốc thời gian" chính xác
    moc_ngay = odich.ngay_phat_hien # Mặc định là ngày phát hiện ổ dịch
    
    # Nếu ổ dịch đã có ca bệnh liên quan
    if odich.ca_benh_lien_quan:
        # Tìm ngày khởi phát sớm nhất trong số các ca bệnh đó
        # Lọc ra các ca có ngày khởi phát hợp lệ trước khi tìm min
        valid_cases = [case for case in odich.ca_benh_lien_quan if case.ngay_khoi_phat]
        if valid_cases:
            ca_chi_diem = min(valid_cases, key=lambda case: case.ngay_khoi_phat)
            moc_ngay = ca_chi_diem.ngay_khoi_phat

    # 2. Tính toán khoảng thời gian gợi ý dựa trên "mốc thời gian"
    # Khoảng thời gian ủ bệnh và lây truyền của SXH/TCM thường trong vòng 14-21 ngày
    # Lấy rộng ra một chút để không bỏ sót
    ngay_bat_dau_goi_y = moc_ngay - timedelta(days=7)
    ngay_ket_thuc_goi_y = moc_ngay + timedelta(days=14) # Mở rộng khoảng thời gian sau

    # 3. Lấy danh sách ca bệnh chưa được gán trong khoảng thời gian đó
    unassigned_cases = get_unassigned_cases(
        xa_id=odich.xa_id, 
        loai_benh=odich.loai_benh, 
        start_date=ngay_bat_dau_goi_y, 
        end_date=ngay_ket_thuc_goi_y
    )
    
    all_xa_list = None
    if is_admin:
        all_xa_list, _ = get_all_don_vi(page=1, per_page=1000, filters={'cap_don_vi': 'Xã'})

    return render_template(
        'view_odich.html', 
        title='Chi tiết Ổ dịch', 
        odich=odich, 
        unassigned_cases=unassigned_cases, 
        all_xa_list=all_xa_list
    )


@main_bp.route('/odich/link_cases/<int:odich_id>', methods=['POST'])
def link_cases_to_odich_action(odich_id):
    case_ids = request.form.getlist('case_ids')
    if not case_ids:
        flash({'message': "Chưa chọn ca bệnh nào để thêm."}, "warning")
    else:
        case_ids_int = [int(cid) for cid in case_ids]
        result = link_cases_to_odich(odich_id, case_ids_int)
        if result['success']:
            flash({'message': result['message']}, 'success')
        else:
            flash({'message': result['message']}, 'danger')
    return redirect(url_for('main.view_odich_page', odich_id=odich_id))


@main_bp.route('/odich/unlink_case/<int:case_id>', methods=['POST'])
def unlink_case_from_odich_action(case_id):
    db = g.db
    
    case = db.query(CaBenh).get(case_id)
    odich_id = case.o_dich_id if case else None

    result = unlink_case_from_odich(case_id)
    if result['success']:
        flash({'message': 'Đã gỡ ca bệnh khỏi ổ dịch.'}, 'success')
    else:
        flash({'message': f"Gỡ ca bệnh thất bại. Lỗi: {result.get('message', 'Không rõ')}"}, 'danger')

    if odich_id:
        return redirect(url_for('main.view_odich_page', odich_id=odich_id))
    return redirect(url_for('main.manage_odich_page'))


@main_bp.route('/odich/delete/<int:odich_id>', methods=['POST'])
def delete_odich_action(odich_id):
    result = delete_odich(odich_id, session.get('role'), session.get('don_vi_id'))
    if result['success']:
        flash({'message': 'Đã xóa ổ dịch thành công.'}, 'success')
    else:
        flash({'message': f"Xóa ổ dịch thất bại. Lỗi: {result.get('message', 'Không rõ')}"}, 'danger')

    return redirect(url_for('main.manage_odich_page'))


@main_bp.route('/profile', methods=['GET', 'POST'])
def profile_page():
    db = g.db
    
    user_id = session.get('user_id')
    user = db.query(NguoiDung).options(joinedload(NguoiDung.don_vi)).filter_by(id=user_id).first()

    if not user:
        flash("Không tìm thấy thông tin người dùng. Vui lòng đăng nhập lại.", "danger")
        return redirect(url_for('auth.login'))

    form = ChangePasswordForm()

    if form.validate_on_submit():
        if check_password_hash(user.mat_khau_hashed, form.current_password.data):
            user.mat_khau_hashed = generate_password_hash(form.new_password.data)
            db.commit()
            flash({'message': "Đổi mật khẩu thành công!"}, "success")
            return redirect(url_for('main.profile_page'))
        else:
            flash({'message': "Mật khẩu hiện tại không đúng. Vui lòng thử lại."}, "danger")

    return render_template('profile.html', title="Thông tin tài khoản", user=user, form=form)

@main_bp.route('/api/search_cases')
def api_search_cases():
    """
    API endpoint để tìm kiếm ca bệnh và trả về kết quả JSON.
    Chỉ cho phép admin và user cấp khu vực truy cập.
    """
    ## <<< SỬA LỖI 3: Cập nhật quyền và logic cho API
    if session.get('role') not in ['admin', 'khuvuc']:
        return jsonify({'error': 'Không có quyền truy cập'}), 403

    db = g.db
    user_don_vi = g.user_don_vi

    search_term = request.args.get('term', '').strip()
    loai_benh = request.args.get('loai_benh', '')
    
    query = db.query(CaBenh).options(joinedload(CaBenh.don_vi))

    # Lọc theo phạm vi của khu vực
    if session.get('role') == 'khuvuc':
        child_xa_ids = get_all_child_xa_ids(user_don_vi)
        query = query.filter(CaBenh.xa_id.in_(child_xa_ids))

    if search_term:
        search_term_lower = search_term.lower()
        query = query.filter(
            (func.lower(CaBenh.ho_ten).like(f'%{search_term_lower}%')) | 
            (CaBenh.ma_so_benh_nhan.ilike(f'%{search_term}%'))
        )
    
    benh_map_reverse = {
        'SXH': 'Sốt xuất huyết Dengue',
        'TCM': 'Tay - chân - miệng'
    }
    if loai_benh in benh_map_reverse:
        query = query.filter(CaBenh.chan_doan_chinh == benh_map_reverse[loai_benh])

    query = query.filter(CaBenh.o_dich_id.is_(None))
    cases = query.order_by(CaBenh.ngay_khoi_phat.desc()).limit(50).all()

    results = [
        {
            'id': case.id,
            'ho_ten': case.ho_ten,
            'ma_so_benh_nhan': case.ma_so_benh_nhan,
            'ngay_khoi_phat': case.ngay_khoi_phat.strftime('%d/%m/%Y') if case.ngay_khoi_phat else '',
            'chan_doan_chinh': case.chan_doan_chinh,
            'xa_id': case.xa_id,
            'xa_ten': case.don_vi.ten_don_vi if case.don_vi else '',
            'dia_chi_ap': case.dia_chi_ap or '',
            'dia_chi_chi_tiet': case.dia_chi_chi_tiet or ''
        } for case in cases
    ]
    
    return jsonify(results)