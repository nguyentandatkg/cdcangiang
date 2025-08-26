# webapp/core/data_importer.py

import pandas as pd
from .database_setup import CaBenh, DonViHanhChinh
from .database_utils import get_db_session
import traceback

# Helper function để chuyển đổi an toàn từ Timestamp của Pandas sang date của Python
def to_py_date(pd_timestamp):
    if pd.notna(pd_timestamp):
        return pd_timestamp.date()
    return None

COLUMN_MAP = {
    'Mã số': 'ma_so_benh_nhan', 'Họ tên': 'ho_ten', 'Ngày sinh': 'ngay_sinh',
    'Giới tính': 'gioi_tinh', 'Nơi ở hiện nay': 'dia_chi_chi_tiet', 'Xã': 'ten_xa',
    'Ấp': 'dia_chi_ap', 'Ngày khởi phát': 'ngay_khoi_phat', 'Ngày nhập viện/khám': 'ngay_nhap_vien',
    'Ngày ra viện/chuyển viện/tử vong': 'ngay_ra_vien', 'Chẩn đoán chính': 'chan_doan_chinh',
    'Phân độ bệnh': 'phan_do_benh', 'Tình trạng hiện nay': 'tinh_trang_hien_nay'
}

def import_data_from_excel(filepath: str, user_xa_id: int = None):
    try:
        df = pd.read_excel(filepath, dtype={'Mã số': str})
        excel_cols = list(COLUMN_MAP.keys())
        missing_cols = [col for col in excel_cols if col not in df.columns]
        if missing_cols:
            if 'Ấp' in missing_cols and len(missing_cols) == 1:
                df['Ấp'] = None
            else:
                return {"success": False, "message": f"Lỗi: Các cột sau không tồn tại: {', '.join(missing_cols)}"}
        df = df[excel_cols]
        df.rename(columns=COLUMN_MAP, inplace=True)
    except Exception as e:
        return {"success": False, "message": f"Lỗi khi đọc file Excel: {e}"}

    df = df.astype(object).where(pd.notna(df), None)
    date_columns = ['ngay_sinh', 'ngay_khoi_phat', 'ngay_nhap_vien', 'ngay_ra_vien']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

    db_session = get_db_session()
    new_cases_count = 0
    skipped_cases_count = 0
    error_log = []

    try:
        existing_cases_query = db_session.query(CaBenh.ma_so_benh_nhan, CaBenh.ngay_khoi_phat, CaBenh.chan_doan_chinh).all()
        existing_cases_set = {(row.ma_so_benh_nhan, row.ngay_khoi_phat, str(row.chan_doan_chinh or '').strip()) for row in existing_cases_query}
        xa_map = {xa.ten_don_vi: xa.id for xa in db_session.query(DonViHanhChinh).filter_by(cap_don_vi='Xã').all()}

        for index, row in df.iterrows():
            row_num = index + 2
            ma_so = str(row.get('ma_so_benh_nhan') or '').strip()
            
            if not ma_so:
                error_log.append({'row': row_num, 'error': 'Thiếu Mã số bệnh nhân.'})
                skipped_cases_count += 1
                continue

            ngay_khoi_phat_hien_tai = to_py_date(row.get('ngay_khoi_phat'))
            chan_doan_hien_tai = str(row.get('chan_doan_chinh') or '').strip()
            current_case_tuple = (ma_so, ngay_khoi_phat_hien_tai, chan_doan_hien_tai)

            if current_case_tuple in existing_cases_set:
                skipped_cases_count += 1
                continue
                
            ten_xa_trong_file = str(row.get('ten_xa') or '').strip()
            if not ten_xa_trong_file:
                error_log.append({'row': row_num, 'error': 'Thiếu tên xã.'})
                skipped_cases_count += 1
                continue
                
            xa_id_cua_ca_benh = xa_map.get(ten_xa_trong_file)
            if xa_id_cua_ca_benh is None:
                error_log.append({'row': row_num, 'error': f"Tên xã '{ten_xa_trong_file}' không tồn tại."})
                skipped_cases_count += 1
                continue
                
            if user_xa_id is not None and xa_id_cua_ca_benh != user_xa_id:
                error_log.append({'row': row_num, 'error': 'Bạn chỉ có quyền import dữ liệu cho xã của mình.'})
                skipped_cases_count += 1
                continue
            
            new_case = CaBenh(
                xa_id=xa_id_cua_ca_benh, ma_so_benh_nhan=ma_so, ho_ten=row.get('ho_ten'),
                dia_chi_ap=row.get('dia_chi_ap'), ngay_sinh=to_py_date(row.get('ngay_sinh')),
                ngay_khoi_phat=ngay_khoi_phat_hien_tai, ngay_nhap_vien=to_py_date(row.get('ngay_nhap_vien')),
                ngay_ra_vien=to_py_date(row.get('ngay_ra_vien')), gioi_tinh=row.get('gioi_tinh'),
                dia_chi_chi_tiet=row.get('dia_chi_chi_tiet'), chan_doan_chinh=row.get('chan_doan_chinh'),
                phan_do_benh=row.get('phan_do_benh'), tinh_trang_hien_nay=row.get('tinh_trang_hien_nay')
            )
            db_session.add(new_case)
            existing_cases_set.add(current_case_tuple) 
            new_cases_count += 1

        db_session.commit()
    except Exception as e:
        db_session.rollback()
        traceback.print_exc()
        return {"success": False, "message": f"Lỗi khi xử lý hoặc lưu vào CSDL: {e}", "errors": []}
    finally:
        db_session.close()

    message = f"Hoàn thành! Đã thêm {new_cases_count} ca mới, bỏ qua {skipped_cases_count} ca trùng lặp hoặc có lỗi."
    
    return {"success": True, "message": message, "errors": error_log}