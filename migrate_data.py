# file: migrate_data.py
import pandas as pd
from sqlalchemy import create_engine
import sys

# ==================== CẤU HÌNH ====================
# 1. Cấu hình CSDL nguồn (SQLite)
SQLITE_URI = 'sqlite:///instance/mydatabase.db' # <<< CẬP NHẬT ĐÚNG ĐƯỜNG DẪN FILE .db

# 2. Cấu hình CSDL đích (PostgreSQL)
POSTGRES_URI = 'postgresql://cdcangiang:LCCaV5eZf8VwShvbvDpehzgDB8mX3H8a@dpg-d2o6hs95pdvs739iflkg-a.singapore-postgres.render.com/cdcangiangdb' # <<< CẬP NHẬT THÔNG TIN CỦA BẠN

# 3. Liệt kê TẤT CẢ các bảng bạn muốn di chuyển theo đúng thứ tự (bảng cha trước, bảng con sau)
# Ví dụ: DonViHanhChinh phải có trước NguoiDung, CaBenh, O_Dich
TABLES_TO_MIGRATE = [
    'don_vi_hanh_chinh',
    'nguoi_dung',
    'o_dich',
    'ca_benh'
    # Thêm các bảng khác của bạn vào đây nếu có...
]
# ==================================================

def migrate():
    try:
        # Tạo kết nối đến 2 CSDL
        sqlite_engine = create_engine(SQLITE_URI)
        postgres_engine = create_engine(POSTGRES_URI)
        print("✅ Đã kết nối tới CSDL nguồn (SQLite) và đích (PostgreSQL).")
    except Exception as e:
        print(f"❌ Lỗi kết nối CSDL: {e}")
        sys.exit(1)

    # Chạy lại script khởi tạo CSDL để tạo các bảng rỗng trên PostgreSQL
    # Giả sử bạn có file init_db.py
    try:
        from webapp.core.database_setup import Base
        print("🔄 Bắt đầu tạo các bảng trên PostgreSQL...")
        Base.metadata.create_all(bind=postgres_engine)
        print("✅ Đã tạo các bảng thành công.")
    except Exception as e:
        print(f"❌ Lỗi khi tạo bảng trên PostgreSQL: {e}")
        print("   Hãy chắc chắn rằng các model trong database_setup.py của bạn đã đúng.")
        sys.exit(1)

    # Bắt đầu di chuyển dữ liệu
    with postgres_engine.connect() as pg_conn:
        for table_name in TABLES_TO_MIGRATE:
            print(f"\n🚚 Bắt đầu di chuyển bảng: '{table_name}'...")
            try:
                # Đọc dữ liệu từ SQLite vào DataFrame của Pandas
                df = pd.read_sql_table(table_name, sqlite_engine)
                print(f"   - Đã đọc {len(df)} dòng từ SQLite.")
                
                # Ghi DataFrame vào PostgreSQL
                # if_exists='append' sẽ thêm dữ liệu vào bảng đã tồn tại
                # index=False để không ghi cột index của DataFrame vào CSDL
                df.to_sql(table_name, pg_conn, if_exists='append', index=False)
                print(f"   - Đã ghi {len(df)} dòng vào PostgreSQL.")

                # !!! BƯỚC CỰC KỲ QUAN TRỌNG: Cập nhật sequence cho cột ID tự tăng
                # PostgreSQL sử dụng "sequences" để quản lý ID tự tăng. Sau khi chèn dữ liệu thủ công,
                # sequence này không tự cập nhật. Chúng ta phải cập nhật nó bằng tay.
                if 'id' in df.columns:
                    max_id = df['id'].max()
                    if pd.notna(max_id):
                        # Dùng transaction để đảm bảo an toàn
                        trans = pg_conn.begin()
                        try:
                            sequence_name = f"{table_name}_id_seq" # Tên sequence mặc định của SQLAlchemy
                            query = f"SELECT setval('{sequence_name}', {int(max_id)}, true);"
                            pg_conn.execute(query)
                            trans.commit()
                            print(f"   - Đã cập nhật sequence '{sequence_name}' lên giá trị {max_id}.")
                        except Exception as seq_e:
                            trans.rollback()
                            print(f"   - ⚠️ Cảnh báo: Không thể cập nhật sequence cho bảng '{table_name}'. Lỗi: {seq_e}")
                            print(f"   -   Bạn có thể cần chạy lệnh SQL sau thủ công: SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), max(id)) FROM {table_name};")
                            
            except Exception as e:
                print(f"   - ❌ LỖI khi di chuyển bảng '{table_name}': {e}")
                print("   - Dừng quá trình di chuyển.")
                sys.exit(1)
                
    print("\n🎉 Di chuyển dữ liệu hoàn tất!")

if __name__ == '__main__':
    migrate()