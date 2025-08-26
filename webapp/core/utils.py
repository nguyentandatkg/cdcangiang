# file: core/utils.py

from .database_setup import DonViHanhChinh

def get_all_child_xa_ids(user_don_vi: DonViHanhChinh):
    """
    Lấy ID của tất cả các xã thuộc quyền quản lý của đơn vị người dùng.
    Hàm này độc lập và có thể được sử dụng ở nhiều nơi.
    """
    if not user_don_vi:
        return []
        
    if user_don_vi.cap_don_vi == 'Xã':
        return [user_don_vi.id]
    
    xa_ids = []
    # Cần đảm bảo user_don_vi.children đã được tải sẵn (eager loaded)
    children_level_1 = user_don_vi.children
    
    for unit_l1 in children_level_1:
        if unit_l1.cap_don_vi == 'Xã':
            xa_ids.append(unit_l1.id)
        elif unit_l1.cap_don_vi == 'Khu vực':
            for unit_l2 in unit_l1.children:
                if unit_l2.cap_don_vi == 'Xã':
                    xa_ids.append(unit_l2.id)
    return xa_ids