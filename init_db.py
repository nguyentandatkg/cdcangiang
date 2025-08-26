# file: init_db.py

import sys
import os

# Thêm thư mục gốc của dự án vào Python Path
# Điều này giúp Python tìm thấy các module trong `webapp`
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from webapp.core.database_setup import Base
from webapp.core.database_utils import engine

def initialize_database():
    """
    Hàm này sẽ xóa (nếu có) và tạo lại tất cả các bảng trong CSDL
    dựa trên các Model đã được định nghĩa trong database_setup.py.
    """
    print("Đang khởi tạo cơ sở dữ liệu...")
    
    # CẢNH BÁO: Dòng này sẽ xóa tất cả dữ liệu hiện có!
    # Chỉ chạy khi bạn chắc chắn muốn làm mới CSDL.
    # Base.metadata.drop_all(bind=engine)
    # print("Đã xóa các bảng cũ (nếu có).")

    # Tạo tất cả các bảng mới dựa trên các Model
    Base.metadata.create_all(bind=engine)
    
    print("Đã tạo thành công tất cả các bảng.")
    print("Cơ sở dữ liệu đã sẵn sàng để sử dụng!")

if __name__ == "__main__":
    # Hỏi người dùng để xác nhận, tránh xóa nhầm dữ liệu
    confirm = input(
        "BẠN CÓ CHẮC CHẮN MUỐN KHỞI TẠO CSDL KHÔNG? \n"
        "Hành động này có thể tạo các bảng mới. (nhập 'yes' để tiếp tục): "
    )
    if confirm.lower() == 'yes':
        initialize_database()
    else:
        print("Hủy bỏ thao tác.")