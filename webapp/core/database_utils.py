# file: webapp/core/database_utils.py (PHIÊN BẢN ĐÃ SỬA LỖI HOÀN CHỈNH)

import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env
load_dotenv()

# Lấy chuỗi kết nối từ biến môi trường
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("Không tìm thấy biến môi trường DATABASE_URL. Vui lòng kiểm tra file .env của bạn.")

# =========================================================================
# SỬA LỖI 1: TĂNG TIMEOUT ĐỂ TRÁNH LỖI "DATABASE IS LOCKED"
# =========================================================================
# Chỉ áp dụng connect_args nếu đây là CSDL SQLite
connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args = {"timeout": 15}
# =========================================================================

# Tạo engine kết nối tới CSDL với connect_args đã được cấu hình
engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args
)

# =========================================================================
# SỬA LỖI 2: XỬ LÝ TÌM KIẾM UNICODE (TIẾNG VIỆT) CHO SQLITE
# =========================================================================
# Chỉ đăng ký hàm lower nếu đây là CSDL SQLite
if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        # Đăng ký hàm "lower" để SQLite có thể xử lý Unicode đúng cách
        dbapi_connection.create_function("lower", 1, lambda s: s.lower() if isinstance(s, str) else s)
# =========================================================================


# Tạo một "nhà máy" sản xuất session (Session factory)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db_session():
    """
    Hàm tiện ích để tạo và trả về một session CSDL mới.
    """
    return SessionLocal()