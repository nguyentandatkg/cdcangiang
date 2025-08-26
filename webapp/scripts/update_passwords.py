# file: webapp/scripts/update_passwords.py

from werkzeug.security import generate_password_hash
import os
import sys

# Thêm đường dẫn tới thư mục gốc của dự án
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from webapp.core.database_utils import get_db_session
from webapp.core.database_setup import NguoiDung

# Mật khẩu mặc định bạn muốn đặt lại cho tất cả tài khoản
DEFAULT_PASSWORD = "123456"

def update_all_user_passwords():
    """Cập nhật mật khẩu cho tất cả người dùng về giá trị mặc định."""
    print("--- Bắt đầu quá trình cập nhật mật khẩu ---")
    db = get_db_session()
    try:
        users = db.query(NguoiDung).all()
        if not users:
            print("Không tìm thấy người dùng nào trong CSDL.")
            return

        print(f"Tìm thấy {len(users)} người dùng. Chuẩn bị cập nhật...")
        
        # Hash mật khẩu mới bằng phương thức đúng
        new_hashed_password = generate_password_hash(DEFAULT_PASSWORD)

        for user in users:
            user.mat_khau_hashed = new_hashed_password
            print(f"- Đã cập nhật mật khẩu cho: {user.ten_dang_nhap}")

        db.commit()
        print("\n--- HOÀN THÀNH ---")
        print(f"Đã cập nhật thành công mật khẩu cho {len(users)} người dùng.")
        print(f"Mật khẩu mới cho tất cả là: '{DEFAULT_PASSWORD}'")

    except Exception as e:
        db.rollback()
        print(f"\n!!! Đã có lỗi xảy ra. Hoàn tác tất cả thay đổi. Lỗi: {e}")
    finally:
        db.close()

if __name__ == '__main__':
    update_all_user_passwords()