# file: export_all_users.py

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy.orm import joinedload
from webapp.core.database_utils import get_db_session
from webapp.core.database_setup import NguoiDung

OUTPUT_FILE = "danh_sach_TOAN_BO_tai_khoan.xlsx"

def export_users_to_excel():
    print("--- Bắt đầu xuất toàn bộ danh sách người dùng ---")
    db = get_db_session()
    try:
        # Lấy tất cả user và thông tin đơn vị liên quan
        all_users = db.query(NguoiDung).options(joinedload(NguoiDung.don_vi)).order_by(NguoiDung.ten_dang_nhap).all()
        
        if not all_users:
            print("Không có người dùng nào trong CSDL.")
            return

        print(f"Tìm thấy {len(all_users)} tài khoản.")
        
        users_data = []
        for user in all_users:
            users_data.append({
                'ID': user.id,
                'Tên đăng nhập': user.ten_dang_nhap,
                'Quyền hạn': user.quyen_han,
                'Tên Đơn vị': user.don_vi.ten_don_vi if user.don_vi else 'N/A',
                'Cấp Đơn vị': user.don_vi.cap_don_vi if user.don_vi else 'N/A'
                # Chúng ta không thể lấy lại mật khẩu gốc
            })

        df = pd.DataFrame(users_data)
        df.to_excel(OUTPUT_FILE, index=False)
        
        print(f"\n--- XUẤT FILE THÀNH CÔNG! ---")
        print(f"Vui lòng kiểm tra file: '{OUTPUT_FILE}'")

    finally:
        db.close()

if __name__ == '__main__':
    export_users_to_excel()