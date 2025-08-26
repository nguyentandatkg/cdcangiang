# file: create_user.py
import getpass
from werkzeug.security import generate_password_hash
from webapp.core.database_setup import NguoiDung
from webapp.core.database_utils import get_db_session

def main():
    """Script để tạo người dùng mới hoặc cập nhật mật khẩu với hash an toàn."""
    username = input("Nhập tên đăng nhập (username) của người dùng cần tạo/cập nhật: ")
    password = getpass.getpass("Nhập mật khẩu mới: ")
    password_confirm = getpass.getpass("Xác nhận mật khẩu mới: ")

    if password != password_confirm:
        print("Lỗi: Mật khẩu không khớp!")
        return

    # Hash mật khẩu theo phương pháp an toàn mới
    hashed_password = generate_password_hash(password)

    db = get_db_session()
    try:
        # Tìm xem người dùng đã tồn tại chưa
        user = db.query(NguoiDung).filter_by(ten_dang_nhap=username).first()

        if user:
            # Nếu đã tồn tại, chỉ cập nhật mật khẩu
            user.mat_khau_hashed = hashed_password
            print(f"Đã cập nhật mật khẩu thành công cho người dùng '{username}'.")
        else:
            # Nếu chưa, tạo người dùng mới (bạn có thể thêm các trường khác nếu cần)
            quyen_han = input("Nhập quyền hạn (admin, xa, khuvuc, tinh): ")
            new_user = NguoiDung(
                ten_dang_nhap=username,
                mat_khau_hashed=hashed_password,
                quyen_han=quyen_han
            )
            db.add(new_user)
            print(f"Đã tạo người dùng mới '{username}' thành công.")
        
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Đã có lỗi xảy ra: {e}")
    finally:
        db.close()

if __name__ == '__main__':
    main()