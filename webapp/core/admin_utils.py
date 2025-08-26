# file: core/admin_utils.py (Phiên bản đã nâng cấp bảo mật)

from sqlalchemy import cast, Date
import pandas as pd
import io
# SỬA LỖI: Bỏ import hashlib không còn dùng
from sqlalchemy.orm import joinedload, subqueryload
# SỬA LỖI: Import các hàm hash an toàn từ Werkzeug
from werkzeug.security import generate_password_hash

from .database_utils import get_db_session
from .database_setup import DonViHanhChinh, NguoiDung, CaBenh, O_Dich
from .utils import get_all_child_xa_ids

# --- CÁC HÀM QUẢN LÝ ĐƠN VỊ HÀNH CHÍNH ---
# (Không có thay đổi trong phần này)
def get_don_vi_by_id(don_vi_id: int):
    db = get_db_session()
    try: return db.query(DonViHanhChinh).get(don_vi_id)
    finally: db.close()

def get_all_don_vi(page: int = 1, per_page: int = 20, filters: dict = None):
    db = get_db_session()
    try:
        query = db.query(DonViHanhChinh).options(joinedload(DonViHanhChinh.parent))
        if filters and filters.get('cap_don_vi') and filters['cap_don_vi'] != 'Tất cả':
            query = query.filter(DonViHanhChinh.cap_don_vi == filters['cap_don_vi'])
        total_items = query.count()
        don_vi_list = query.order_by(DonViHanhChinh.ten_don_vi).limit(per_page).offset((page - 1) * per_page).all()
        return don_vi_list, total_items
    finally: db.close()

def add_new_don_vi(ten_don_vi: str, cap_don_vi: str, parent_id: int = None):
    if not ten_don_vi or not ten_don_vi.strip() or not cap_don_vi:
        return {"success": False, "message": "Tên và Cấp đơn vị không được để trống."}
    ten_don_vi = ten_don_vi.strip()
    db = get_db_session()
    try:
        existing_don_vi = db.query(DonViHanhChinh).filter_by(ten_don_vi=ten_don_vi, parent_id=parent_id).first()
        if existing_don_vi:
            return {"success": False, "message": f"Đơn vị '{ten_don_vi}' đã tồn tại trong đơn vị cha được chọn."}
        new_don_vi = DonViHanhChinh(ten_don_vi=ten_don_vi, cap_don_vi=cap_don_vi, parent_id=parent_id)
        db.add(new_don_vi)
        db.commit()
        return {"success": True, "message": f"Đã thêm thành công '{cap_don_vi}: {ten_don_vi}'."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def update_don_vi(don_vi_id: int, data: dict):
    db = get_db_session()
    try:
        unit = db.query(DonViHanhChinh).get(don_vi_id)
        if not unit: return {"success": False, "message": "Không tìm thấy đơn vị."}
        new_name = data.get('ten_don_vi', unit.ten_don_vi)
        new_parent_id = data.get('parent_id', unit.parent_id)
        if new_name != unit.ten_don_vi or new_parent_id != unit.parent_id:
            existing = db.query(DonViHanhChinh).filter_by(ten_don_vi=new_name, parent_id=new_parent_id).first()
            if existing: return {"success": False, "message": f"Tên đơn vị '{new_name}' đã tồn tại trong đơn vị cha này."}
        unit.ten_don_vi = new_name
        unit.parent_id = new_parent_id
        db.commit()
        return {"success": True, "message": "Cập nhật đơn vị thành công."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def delete_don_vi(don_vi_id: int):
    db = get_db_session()
    try:
        unit = db.query(DonViHanhChinh).options(joinedload(DonViHanhChinh.children)).get(don_vi_id)
        if not unit: return {"success": False, "message": "Không tìm thấy đơn vị."}
        if unit.children: return {"success": False, "message": "Không thể xóa đơn vị vì vẫn còn các đơn vị con."}
        if unit.nguoi_dung or unit.ca_benh: return {"success": False, "message": "Không thể xóa đơn vị vì đang có người dùng hoặc ca bệnh được gán."}
        db.delete(unit)
        db.commit()
        return {"success": True, "message": "Đã xóa đơn vị."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

# --- CÁC HÀM QUẢN LÝ NGƯỜI DÙNG ---

def get_user_by_id(user_id: int):
    db = get_db_session()
    try: return db.query(NguoiDung).options(joinedload(NguoiDung.don_vi)).get(user_id)
    finally: db.close()

def get_users_list(page: int = 1, per_page: int = 20):
    db = get_db_session()
    try:
        query = db.query(NguoiDung).options(joinedload(NguoiDung.don_vi))
        total_items = query.count()
        users = query.order_by(NguoiDung.ten_dang_nhap).limit(per_page).offset((page - 1) * per_page).all()
        return users, total_items
    finally: db.close()

def add_new_user(ten_dang_nhap, mat_khau, quyen_han, don_vi_id):
    if not all([ten_dang_nhap, mat_khau, quyen_han, don_vi_id]):
        return {"success": False, "message": "Vui lòng điền đầy đủ thông tin."}
    db = get_db_session()
    try:
        existing_user = db.query(NguoiDung).filter_by(ten_dang_nhap=ten_dang_nhap).first()
        if existing_user: return {"success": False, "message": "Tên đăng nhập đã tồn tại."}
        
        # CẢI TIẾN BẢO MẬT: Sử dụng generate_password_hash
        hashed_password = generate_password_hash(mat_khau)
        
        new_user = NguoiDung(ten_dang_nhap=ten_dang_nhap, mat_khau_hashed=hashed_password, quyen_han=quyen_han, don_vi_id=don_vi_id)
        db.add(new_user)
        db.commit()
        return {"success": True, "message": f"Đã tạo thành công tài khoản '{ten_dang_nhap}'."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL khi tạo người dùng: {e}"}
    finally: db.close()

def update_user(user_id: int, data: dict):
    db = get_db_session()
    try:
        user = db.query(NguoiDung).get(user_id)
        if not user: return {"success": False, "message": "Không tìm thấy người dùng."}
        user.don_vi_id = data.get('don_vi_id', user.don_vi_id)
        user.quyen_han = data.get('quyen_han', user.quyen_han)
        db.commit()
        return {"success": True, "message": "Cập nhật người dùng thành công."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def reset_user_password(user_id: int, new_password: str):
    if not new_password or len(new_password) < 6:
        return {"success": False, "message": "Mật khẩu mới phải có ít nhất 6 ký tự."}

    db = get_db_session()
    try:
        user = db.query(NguoiDung).get(user_id)
        if not user: return {"success": False, "message": "Không tìm thấy người dùng."}

        # CẢI TIẾN BẢO MẬT: Sử dụng generate_password_hash
        user.mat_khau_hashed = generate_password_hash(new_password)
        
        db.commit()
        return {"success": True, "message": f"Đã đặt lại mật khẩu cho '{user.ten_dang_nhap}'."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def delete_user(user_id: int):
    db = get_db_session()
    try:
        user = db.query(NguoiDung).get(user_id)
        if not user: return {"success": False, "message": "Không tìm thấy người dùng."}
        if user.quyen_han == 'admin' and db.query(NguoiDung).filter_by(quyen_han='admin').count() <= 1:
            return {"success": False, "message": "Không thể xóa tài khoản Admin cuối cùng."}
        db.delete(user)
        db.commit()
        return {"success": True, "message": f"Đã xóa người dùng '{user.ten_dang_nhap}'."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

# --- CÁC HÀM QUẢN LÝ CA BỆNH ---
# (Không có thay đổi trong phần này)
def get_cases_by_user_scope(_user_don_vi, filters: dict = None, page: int = 1, per_page: int = 20):
    db = get_db_session()
    try:
        user_don_vi_attached = db.query(DonViHanhChinh).get(_user_don_vi.id)
        if not user_don_vi_attached: return [], 0
        xa_ids_to_query = get_all_child_xa_ids(user_don_vi_attached)
        if not xa_ids_to_query: return [], 0
        query = db.query(CaBenh).options(joinedload(CaBenh.don_vi), joinedload(CaBenh.o_dich)).filter(CaBenh.xa_id.in_(xa_ids_to_query))
        if filters:
            if filters.get("start_date"): query = query.filter(CaBenh.ngay_khoi_phat >= filters["start_date"])
            if filters.get("end_date"): query = query.filter(CaBenh.ngay_khoi_phat <= filters["end_date"])
            if filters.get("report_start_date"): query = query.filter(cast(CaBenh.ngay_import, Date) >= filters["report_start_date"])
            if filters.get("report_end_date"): query = query.filter(cast(CaBenh.ngay_import, Date) <= filters["report_end_date"])
            if filters.get("chan_doan"): query = query.filter(CaBenh.chan_doan_chinh.ilike(f"%{filters['chan_doan']}%"))
            if filters.get("ho_ten"): query = query.filter(CaBenh.ho_ten.ilike(f"%{filters['ho_ten']}%"))
            if filters.get("dia_chi_ap"): query = query.filter(CaBenh.dia_chi_ap.ilike(f"%{filters['dia_chi_ap']}%"))
            if filters.get("xa_id"): query = query.filter(CaBenh.xa_id == filters["xa_id"])
            elif filters.get("khu_vuc_id"):
                kv = db.query(DonViHanhChinh).options(joinedload(DonViHanhChinh.children)).get(filters["khu_vuc_id"])
                if kv:
                    child_xa_ids = [xa.id for xa in kv.children if xa.cap_don_vi == 'Xã']
                    if child_xa_ids: query = query.filter(CaBenh.xa_id.in_(child_xa_ids))
        total_items = query.count()
        query = query.order_by(CaBenh.ngay_khoi_phat.desc(), CaBenh.id.desc())
        cases_for_page = query.limit(per_page).offset((page - 1) * per_page).all()
        return cases_for_page, total_items
    finally: db.close()

def update_case(case_id: int, new_data: dict):
    db = get_db_session()
    try:
        case_to_update = db.query(CaBenh).filter(CaBenh.id == case_id).first()
        if not case_to_update: return {"success": False, "message": "Không tìm thấy ca bệnh."}
        for key, value in new_data.items():
            if hasattr(case_to_update, key): setattr(case_to_update, key, value)
        db.commit()
        return {"success": True, "message": "Cập nhật ca bệnh thành công."}
    finally: db.close()

def delete_case(case_id: int):
    db = get_db_session()
    try:
        case_to_delete = db.query(CaBenh).filter(CaBenh.id == case_id).first()
        if not case_to_delete: return {"success": False, "message": "Không tìm thấy ca bệnh."}
        db.delete(case_to_delete)
        db.commit()
        return {"success": True, "message": "Đã xóa ca bệnh."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def add_new_case(case_data: dict):
    db = get_db_session()
    try:
        required_fields = ['ma_so_benh_nhan', 'ho_ten', 'ngay_khoi_phat', 'chan_doan_chinh', 'xa_id']
        if not all(field in case_data and case_data[field] for field in required_fields):
            return {"success": False, "message": "Vui lòng điền đầy đủ các trường bắt buộc (*)."}
        existing_case = db.query(CaBenh).filter_by(ma_so_benh_nhan=case_data['ma_so_benh_nhan']).first()
        if existing_case: return {"success": False, "message": "Mã số bệnh nhân đã tồn tài."}
        new_case = CaBenh(**case_data)
        db.add(new_case)
        db.commit()
        return {"success": True, "message": f"Đã thêm thành công ca bệnh: {case_data['ho_ten']}"}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

# --- CÁC HÀM QUẢN LÝ Ổ DỊCH ---
# (Không có thay đổi trong phần này)
def get_odich_by_user_scope(user_don_vi: DonViHanhChinh, filters: dict = None, page: int = 1, per_page: int = 20):
    db = get_db_session()
    try:
        xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
        if not xa_ids_to_query: return [], 0
        query = db.query(O_Dich).options(joinedload(O_Dich.don_vi), subqueryload(O_Dich.ca_benh_lien_quan)).filter(O_Dich.xa_id.in_(xa_ids_to_query))
        if filters and filters.get('loai_benh'): query = query.filter(O_Dich.loai_benh == filters['loai_benh'])
        total_items = query.count()
        odich_for_page = query.order_by(O_Dich.ngay_phat_hien.desc()).limit(per_page).offset((page - 1) * per_page).all()
        return odich_for_page, total_items
    finally: db.close()

def get_odich_by_id(odich_id: int):
    db = get_db_session()
    try: return db.query(O_Dich).options(joinedload(O_Dich.don_vi), subqueryload(O_Dich.ca_benh_lien_quan)).get(odich_id)
    finally: db.close()

def add_new_odich(data: dict):
    if not data.get('loai_benh') or not data.get('ngay_phat_hien') or not data.get('xa_id'):
        return {"success": False, "message": "Loại bệnh, ngày phát hiện và xã là bắt buộc."}
    db = get_db_session()
    try:
        new_od = O_Dich(**data)
        db.add(new_od)
        db.flush()
        new_id = new_od.id
        db.commit()
        return {"success": True, "message": "Đã thêm ổ dịch thành công.", "new_id": new_id}
    except Exception as e:
        db.rollback(); return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def update_odich(odich_id: int, data: dict):
    db = get_db_session()
    try:
        odich_to_update = db.query(O_Dich).filter(O_Dich.id == odich_id).first()
        if not odich_to_update: return {"success": False, "message": "Không tìm thấy ổ dịch."}
        for key, value in data.items(): setattr(odich_to_update, key, value)
        db.commit()
        return {"success": True, "message": "Cập nhật ổ dịch thành công."}
    except Exception as e:
        db.rollback(); return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def delete_odich(odich_id: int, user_role: str, user_don_vi_id: int):
    db = get_db_session()
    try:
        odich = db.query(O_Dich).get(odich_id)
        if not odich: return {"success": False, "message": "Không tìm thấy ổ dịch."}
        if user_role == 'xa' and odich.xa_id != user_don_vi_id:
            return {"success": False, "message": "Bạn không có quyền xóa ổ dịch này."}
        db.delete(odich); db.commit()
        return {"success": True, "message": "Đã xóa ổ dịch."}
    except Exception as e:
        db.rollback(); return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def get_unassigned_cases(xa_id: int, loai_benh: str, start_date=None, end_date=None):
    db = get_db_session()
    try:
        loai_benh_map = {'SXH': 'Sốt xuất huyết Dengue', 'TCM': 'Tay - chân - miệng'}
        chan_doan_chinh = loai_benh_map.get(loai_benh)
        if not chan_doan_chinh: return []
        query = db.query(CaBenh).filter(CaBenh.xa_id == xa_id, CaBenh.chan_doan_chinh == chan_doan_chinh, CaBenh.o_dich_id == None)
        if start_date: query = query.filter(CaBenh.ngay_khoi_phat >= start_date)
        if end_date: query = query.filter(CaBenh.ngay_khoi_phat <= end_date)
        return query.order_by(CaBenh.ngay_khoi_phat.desc()).all()
    finally: db.close()

def link_cases_to_odich(odich_id: int, case_ids: list):
    db = get_db_session()
    try:
        db.query(CaBenh).filter(CaBenh.id.in_(case_ids)).update({"o_dich_id": odich_id}, synchronize_session=False)
        db.commit()
        return {"success": True, "message": f"Đã thêm {len(case_ids)} ca bệnh vào ổ dịch."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def unlink_case_from_odich(case_id: int):
    db = get_db_session()
    try:
        db.query(CaBenh).filter(CaBenh.id == case_id).update({"o_dich_id": None}, synchronize_session=False)
        db.commit()
        return {"success": True, "message": "Đã gỡ ca bệnh khỏi ổ dịch."}
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"Lỗi CSDL: {e}"}
    finally: db.close()

def export_users_to_excel_bytes():
    db = get_db_session()
    try:
        all_users = db.query(NguoiDung).options(joinedload(NguoiDung.don_vi)).order_by(NguoiDung.ten_dang_nhap).all()
        if not all_users: return None
        users_data = []
        for user in all_users:
            users_data.append({'ID': user.id, 'Tên đăng nhập': user.ten_dang_nhap, 'Quyền hạn': user.quyen_han, 'Tên Đơn vị': user.don_vi.ten_don_vi if user.don_vi else 'N/A', 'Cấp Đơn vị': user.don_vi.cap_don_vi if user.don_vi else 'N/A'})
        df = pd.DataFrame(users_data)
        output_buffer = io.BytesIO()
        with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='DanhSachNguoiDung')
            worksheet = writer.sheets['DanhSachNguoiDung']
            for idx, col in enumerate(df):
                series = df[col]
                max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
                worksheet.set_column(idx, idx, max_len)
        return output_buffer.getvalue()
    finally: db.close()