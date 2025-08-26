# file: import_admin_units.py
import pandas as pd
from webapp.core.database_utils import get_db_session, DATABASE_URL
from webapp.core.database_setup import DonViHanhChinh, Base
from sqlalchemy import create_engine

def import_administrative_units(filepath: str):
    """
    Đọc file Excel chứa cây đơn vị hành chính và import vào CSDL.
    Hàm này sẽ xóa sạch dữ liệu cũ trong bảng DonViHanhChinh trước khi import.
    """
    try:
        df = pd.read_excel(filepath)
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file '{filepath}'")
        return
    except Exception as e:
        print(f"Lỗi khi đọc file Excel: {e}")
        return

    db_session = get_db_session()
    
    # CẢNH BÁO: Xóa tất cả dữ liệu cũ để import lại từ đầu
    # Điều này cũng sẽ xóa các liên kết với người dùng và ca bệnh nếu có.
    # Chỉ nên chạy khi thiết lập hệ thống lần đầu.
    print("CẢNH BÁO: Sẽ xóa toàn bộ dữ liệu đơn vị hành chính cũ!")
    confirm = input("Bạn có chắc chắn muốn tiếp tục? (nhập 'yes' để xác nhận): ")
    if confirm.lower() != 'yes':
        print("Hủy bỏ thao tác.")
        db_session.close()
        return
        
    try:
        # Xóa theo thứ tự để không vi phạm khóa ngoại (nếu có)
        db_session.query(DonViHanhChinh).delete()
        db_session.commit()
        print("Đã xóa dữ liệu đơn vị hành chính cũ.")
        
        # Dùng một dictionary để tra cứu ID của các đơn vị đã được thêm vào
        unit_map = {}

        # Sắp xếp theo cấp đơn vị để đảm bảo đơn vị cha luôn được thêm vào trước
        df['cap_don_vi'] = pd.Categorical(df['cap_don_vi'], categories=['Tỉnh', 'Khu vực', 'Xã', 'Ấp'], ordered=True)
        df = df.sort_values('cap_don_vi')
        
        print("Bắt đầu import...")
        for index, row in df.iterrows():
            ten_don_vi = row['ten_don_vi'].strip()
            cap_don_vi = row['cap_don_vi'].strip()
            ten_don_vi_cha = row.get('ten_don_vi_cha')
            
            parent_id = None
            if pd.notna(ten_don_vi_cha):
                ten_don_vi_cha = ten_don_vi_cha.strip()
                if ten_don_vi_cha in unit_map:
                    parent_id = unit_map[ten_don_vi_cha]
                else:
                    print(f"Lỗi: Không tìm thấy đơn vị cha '{ten_don_vi_cha}' cho đơn vị '{ten_don_vi}'. Bỏ qua.")
                    continue
            
            # Tạo đối tượng mới và thêm vào session
            new_unit = DonViHanhChinh(
                ten_don_vi=ten_don_vi,
                cap_don_vi=cap_don_vi,
                parent_id=parent_id
            )
            db_session.add(new_unit)
            
            # Commit ngay để lấy được ID của đơn vị vừa thêm
            db_session.commit()
            
            # Lưu lại ID vào map để các đơn vị con có thể tra cứu
            unit_map[ten_don_vi] = new_unit.id
            print(f"Đã thêm: {cap_don_vi} - {ten_don_vi}")

        print("\nIMPORT THÀNH CÔNG!")
        
    except Exception as e:
        db_session.rollback()
        print(f"\nĐã có lỗi xảy ra. Hoàn tác tất cả thay đổi. Lỗi: {e}")
    finally:
        db_session.close()

if __name__ == "__main__":
    # Đảm bảo các bảng đã được tạo
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    
    # Đường dẫn đến file Excel của bạn
    EXCEL_FILE_PATH = 'danh_muc_hanh_chinh.xlsx'
    import_administrative_units(EXCEL_FILE_PATH)