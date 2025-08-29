# webapp/core/data_importer.py

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session
from .database_setup import CaBenh, DonViHanhChinh
from .database_utils import get_db_session
import traceback

# Helper function này không thay đổi
def to_py_date(pd_timestamp):
    if pd.notna(pd_timestamp):
        return pd_timestamp.date() if hasattr(pd_timestamp, 'date') else pd_timestamp
    return None

COLUMN_MAP = {
    'Mã số': 'ma_so_benh_nhan', 'Họ tên': 'ho_ten', 'Ngày sinh': 'ngay_sinh',
    'Giới tính': 'gioi_tinh', 'Nơi ở hiện nay': 'dia_chi_chi_tiet', 'Xã': 'ten_xa',
    'Ấp': 'dia_chi_ap', 'Ngày khởi phát': 'ngay_khoi_phat', 'Ngày nhập viện/khám': 'ngay_nhap_vien',
    'Ngày ra viện/chuyển viện/tử vong': 'ngay_ra_vien', 'Chẩn đoán chính': 'chan_doan_chinh',
    'Phân độ bệnh': 'phan_do_benh', 'Tình trạng hiện nay': 'tinh_trang_hien_nay'
}

def import_data_from_excel(filepath: str, user_xa_id: int = None):
    """
    Nhập dữ liệu ca bệnh từ file Excel, tối ưu hóa đặc biệt cho PostgreSQL.
    Cải tiến:
    1. Chuẩn bị dữ liệu bằng Pandas (vectorization).
    2. Chia batch 5000 record để kiểm tra trùng lặp với DB bằng VALUES + LEFT JOIN.
    3. Sử dụng session.bulk_insert_mappings để chèn dữ liệu hàng loạt một cách hiệu quả.
    """
    # ======================================================================
    # BƯỚC 1: ĐỌC VÀ CHUẨN BỊ DỮ LIỆU
    # ======================================================================
    try:
        df = pd.read_excel(filepath, dtype={'Mã số': str})

        # Kiểm tra cột
        excel_cols_set = set(COLUMN_MAP.keys())
        missing_cols = excel_cols_set - set(df.columns)
        if missing_cols:
            if 'Ấp' in missing_cols and len(missing_cols) == 1:
                df['Ấp'] = None
            else:
                return {"success": False, "message": f"Lỗi: Các cột sau không tồn tại: {', '.join(missing_cols)}"}

        df = df[list(COLUMN_MAP.keys())]
        df.rename(columns=COLUMN_MAP, inplace=True)

        # Vector hóa làm sạch và chuyển đổi
        str_cols = ['ma_so_benh_nhan', 'ho_ten', 'ten_xa', 'chan_doan_chinh']
        for col in str_cols:
            df[col] = df[col].astype(str).str.strip()

        date_cols = ['ngay_sinh', 'ngay_khoi_phat', 'ngay_nhap_vien', 'ngay_ra_vien']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.date

        # Thay thế NaN/NaT bằng None
        df.replace({pd.NaT: None, np.nan: None, 'nan': None}, inplace=True)
        # Bỏ các dòng không có mã số hoặc ngày khởi phát
        df.dropna(subset=['ma_so_benh_nhan', 'ngay_khoi_phat'], inplace=True)
        # Loại bỏ trùng lặp trong chính file Excel
        df.drop_duplicates(subset=['ma_so_benh_nhan', 'ngay_khoi_phat', 'chan_doan_chinh'], inplace=True)

        if df.empty:
            return {"success": True, "message": "Hoàn thành! Không có dữ liệu hợp lệ để import.", "errors": []}

    except Exception as e:
        return {"success": False, "message": f"Lỗi khi đọc và chuẩn bị file Excel: {e}"}

    db_session = get_db_session()
    try:
        # ======================================================================
        # BƯỚC 2: KIỂM TRA QUYỀN & MAP `xa_id`
        # ======================================================================
        xa_map_query = db_session.query(DonViHanhChinh.ten_don_vi, DonViHanhChinh.id).filter_by(cap_don_vi='Xã').all()
        xa_map = {name: id for name, id in xa_map_query}
        df['xa_id'] = df['ten_xa'].map(xa_map)

        error_log = []
        original_row_count = len(df)

        # Lọc ra các dòng có xã không hợp lệ
        invalid_xa_df = df[df['xa_id'].isnull()]
        for index, row in invalid_xa_df.iterrows():
            error_log.append({'row': index + 2, 'error': f"Tên xã '{row['ten_xa']}' không hợp lệ."})
        df.dropna(subset=['xa_id'], inplace=True)
        df['xa_id'] = df['xa_id'].astype(int)

        # Lọc ra các dòng vi phạm quyền user (nếu có)
        if user_xa_id is not None:
            unauthorized_df = df[df['xa_id'] != user_xa_id]
            for index, row in unauthorized_df.iterrows():
                error_log.append({'row': index + 2, 'error': 'Bạn chỉ có quyền import dữ liệu cho xã của mình.'})
            df = df[df['xa_id'] == user_xa_id]

        if df.empty:
            skipped_cases_count = original_row_count
            message = f"Hoàn thành! Đã thêm 0 ca mới, bỏ qua {skipped_cases_count} ca do lỗi hoặc không có quyền."
            return {"success": True, "message": message, "errors": error_log}

        # ======================================================================
        # BƯỚC 3: KIỂM TRA TRÙNG LẶP VỚI DB BẰNG BATCH QUERY
        # ======================================================================
        BATCH_SIZE = 5000
        keys_to_check = df[['ma_so_benh_nhan', 'ngay_khoi_phat', 'chan_doan_chinh']].to_dict('records')
        non_existing_keys = set()

        for batch_start in range(0, len(keys_to_check), BATCH_SIZE):
            batch = keys_to_check[batch_start: batch_start + BATCH_SIZE]

            # Xây dựng danh sách values cho batch
            values_sql = ", ".join([
                f"(:ma_so_{i+1}, :ngay_kp_{i+1}, :chan_doan_{i+1})"
                for i in range(len(batch))
            ])

            sql = text(f"""
                SELECT v.ma_so, v.ngay_kp, v.chan_doan
                FROM (VALUES {values_sql})
                AS v (ma_so, ngay_kp, chan_doan)
                LEFT JOIN ca_benh cb ON 
                    cb.ma_so_benh_nhan = v.ma_so AND 
                    cb.ngay_khoi_phat = v.ngay_kp AND
                    cb.chan_doan_chinh = v.chan_doan
                WHERE cb.id IS NULL
            """)

            # Tạo dictionary tham số cho batch
            params = {}
            for i, key_dict in enumerate(batch):
                params[f"ma_so_{i+1}"] = key_dict['ma_so_benh_nhan']
                params[f"ngay_kp_{i+1}"] = key_dict['ngay_khoi_phat']
                params[f"chan_doan_{i+1}"] = key_dict['chan_doan_chinh']

            # Thực thi query và gom các ca chưa tồn tại
            result = db_session.execute(sql, params)
            for row in result:
                non_existing_keys.add((row.ma_so, row.ngay_kp, row.chan_doan))

        # ======================================================================
        # BƯỚC 4: LỌC DỮ LIỆU MỚI & INSERT
        # ======================================================================
        df['_key'] = list(zip(df['ma_so_benh_nhan'], df['ngay_khoi_phat'], df['chan_doan_chinh']))
        df_new_cases = df[df['_key'].isin(non_existing_keys)]

        if not df_new_cases.empty:
            # Chuẩn bị dữ liệu cho bulk insert
            columns_for_db = [col for col in COLUMN_MAP.values() if col != 'ten_xa'] + ['xa_id']
            new_cases_dict = df_new_cases[columns_for_db].to_dict('records')

            # Insert hàng loạt
            db_session.bulk_insert_mappings(CaBenh, new_cases_dict)

        db_session.commit()

        new_cases_count = len(df_new_cases)
        skipped_cases_count = original_row_count - new_cases_count
        message = f"Hoàn thành! Đã thêm {new_cases_count} ca mới, bỏ qua {skipped_cases_count} ca trùng lặp hoặc có lỗi."

        return {"success": True, "message": message, "errors": error_log}

    except Exception as e:
        db_session.rollback()
        traceback.print_exc()
        return {"success": False, "message": f"Lỗi nghiêm trọng khi xử lý dữ liệu: {e}", "errors": []}
    finally:
        db_session.close()
