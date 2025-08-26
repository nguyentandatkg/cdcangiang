# file: create_bulk_users.py

# [SỬA LỖI QUAN TRỌNG] Import hàm hash tương thích với Flask
from werkzeug.security import generate_password_hash
from unidecode import unidecode
import re
import pandas as pd

# Cần thiết lập sys.path
import os
import sys
# Thêm đường dẫn tới thư mục gốc của dự án, không phải thư mục hiện tại
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# ---

from webapp.core.database_utils import get_db_session
from webapp.core.database_setup import DonViHanhChinh, NguoiDung

# --- PHẦN CẤU HÌNH ---
DEFAULT_PASSWORD = "123456"
USERNAME_SUFFIX = "_91"
TINH_USERNAME = f"tinh_an_giang{USERNAME_SUFFIX}" 
OUTPUT_EXCEL_FILE = "danh_sach_tai_khoan_moi.xlsx"

def normalize_name(name: str) -> str:
    """Chuyển đổi tên đơn vị thành chuỗi không dấu, viết thường, nối bằng gạch dưới."""
    no_accent_name = unidecode(name)
    lower_name = no_accent_name.lower()
    normalized = re.sub(r'[^a-z0-9]+', '_', lower_name).strip('_')
    return normalized

def create_bulk_users():
    """Tự động tạo tài khoản và xuất ra file Excel."""
    print("--- Bắt đầu quá trình tạo tài khoản hàng loạt ---")
    
    db = get_db_session()
    
    try:
        # [Cải tiến nhỏ] Thêm 'Thị xã' vào danh sách cấp đơn vị
        don_vi_list = db.query(DonViHanhChinh).filter(
            DonViHanhChinh.cap_don_vi.in_(['Tỉnh', 'Khu vực', 'Xã', 'Phường', 'Thị xã'])
        ).all()
        
        if not don_vi_list:
            print("Không tìm thấy đơn vị nào cần tạo tài khoản trong CSDL.")
            return

        print(f"Tìm thấy {len(don_vi_list)} đơn vị cần kiểm tra.")

        existing_usernames = {user[0] for user in db.query(NguoiDung.ten_dang_nhap).all()}
        
        users_to_add = []
        created_users_info = [] 
        
        # [SỬA LỖI QUAN TRỌNG] Sử dụng hàm hash của Werkzeug
        # Hàm này tạo ra hash có salt, tương thích với check_password_hash
        hashed_password = generate_password_hash(DEFAULT_PASSWORD)
        
        for don_vi in don_vi_list:
            base_name = normalize_name(don_vi.ten_don_vi)
            username = ""; quyen_han = ""

            if don_vi.cap_don_vi == 'Tỉnh':
                username = TINH_USERNAME; quyen_han = 'tinh'
            elif don_vi.cap_don_vi == 'Khu vực':
                base_name = base_name.replace("ttyt_khu_vuc_", "").replace("ttyt_", "").replace("khu_vuc_", "")
                username = f"kv_{base_name}{USERNAME_SUFFIX}"; quyen_han = 'khuvuc'
            # [Cải tiến nhỏ] Gộp các trường hợp tương tự
            elif don_vi.cap_don_vi in ['Xã', 'Phường', 'Thị xã']:
                base_name = base_name.replace("xa_", "").replace("phuong_", "").replace("thi_xa_", "")
                username = f"{base_name}{USERNAME_SUFFIX}"; quyen_han = 'xa'
            else:
                continue

            if username in existing_usernames:
                print(f"- Bỏ qua: Tài khoản '{username}' cho '{don_vi.ten_don_vi}' đã tồn tại.")
                continue
                
            print(f"+ Chuẩn bị tạo tài khoản '{username}' cho '{don_vi.ten_don_vi}'...")
            
            new_user = NguoiDung(
                ten_dang_nhap=username, 
                mat_khau_hashed=hashed_password, 
                quyen_han=quyen_han, 
                don_vi_id=don_vi.id
            )
            users_to_add.append(new_user)
            
            created_users_info.append({
                'Tên Đơn vị': don_vi.ten_don_vi,
                'Cấp Đơn vị': don_vi.cap_don_vi,
                'Tên đăng nhập': username,
                'Mật khẩu': DEFAULT_PASSWORD
            })
            existing_usernames.add(username)

        if not users_to_add:
            print("\nKhông có tài khoản mới nào cần tạo.")
            return

        print(f"\nChuẩn bị thêm {len(users_to_add)} tài khoản mới vào CSDL...")
        
        db.add_all(users_to_add)
        db.commit()
        
        print("\n--- HOÀN THÀNH TẠO TÀI KHOẢN! ---")
        print(f"Đã tạo thành công {len(users_to_add)} tài khoản mới.")
        print(f"Mật khẩu mặc định cho tất cả tài khoản là: '{DEFAULT_PASSWORD}'")
        
        print("\nĐang xuất danh sách tài khoản mới ra file Excel...")
        df = pd.DataFrame(created_users_info)
        df.to_excel(OUTPUT_EXCEL_FILE, index=False)
        print(f"--- XUẤT FILE THÀNH CÔNG! ---")
        print(f"Vui lòng kiểm tra file: '{OUTPUT_EXCEL_FILE}' trong thư mục dự án.")

    except Exception as e:
        db.rollback()
        print(f"\n!!! Đã có lỗi xảy ra. Hoàn tác tất cả thay đổi. Lỗi: {e}")
    finally:
        db.close()

if __name__ == '__main__':
    # Để chạy script này, mở terminal, kích hoạt venv và chạy:
    # python -m webapp.scripts.create_bulk_users 
    # (Giả sử bạn đặt file này trong webapp/scripts/)
    create_bulk_users()