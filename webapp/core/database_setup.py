# file: core/database_setup.py (Phiên bản đã cập nhật hoàn chỉnh)

from sqlalchemy import (create_engine, Column, Integer, String, Date, 
                        ForeignKey, Text, Boolean, UniqueConstraint)
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from datetime import date

Base = declarative_base()

class DonViHanhChinh(Base):
    __tablename__ = 'don_vi_hanh_chinh'
    id = Column(Integer, primary_key=True)
    ten_don_vi = Column(String(250), nullable=False)
    cap_don_vi = Column(String(50), nullable=False, index=True)
    parent_id = Column(Integer, ForeignKey('don_vi_hanh_chinh.id'))
    
    children = relationship("DonViHanhChinh", back_populates="parent", cascade="all, delete-orphan")
    parent = relationship("DonViHanhChinh", back_populates="children", remote_side=[id])
    
    # Các relationship trỏ tới DonViHanhChinh
    nguoi_dung = relationship("NguoiDung", back_populates="don_vi")
    ca_benh = relationship("CaBenh", back_populates="don_vi")
    o_dich = relationship("O_Dich", back_populates="don_vi")

    def to_dict(self):
        return {
            'id': self.id, 
            'ten_don_vi': self.ten_don_vi, 
            'cap_don_vi': self.cap_don_vi,
            'parent_id': self.parent_id
        }

class NguoiDung(Base):
    __tablename__ = 'nguoi_dung'
    id = Column(Integer, primary_key=True)
    ten_dang_nhap = Column(String(100), nullable=False, unique=True)
    email = Column(String(120), unique=True, nullable=True)
    mat_khau_hashed = Column(String(250), nullable=False)
    quyen_han = Column(String(50), default='xa') 
    don_vi_id = Column(Integer, ForeignKey('don_vi_hanh_chinh.id'))
    
    don_vi = relationship("DonViHanhChinh", back_populates="nguoi_dung")

class O_Dich(Base):
    __tablename__ = 'o_dich'
    id = Column(Integer, primary_key=True)
    loai_benh = Column(String(50), nullable=False, index=True)
    ngay_phat_hien = Column(Date, nullable=False, index=True)
    ngay_xu_ly = Column(Date, index=True)
    dia_diem_xu_ly = Column(Text)
    
    # === THAY ĐỔI 1: THÊM CỘT dia_chi_ap VÀO BẢNG O_DICH ===
    dia_chi_ap = Column(String(250))
    # =======================================================

    xa_id = Column(Integer, ForeignKey('don_vi_hanh_chinh.id'), nullable=False)
    don_vi = relationship("DonViHanhChinh", back_populates="o_dich")
    ca_benh_lien_quan = relationship("CaBenh", back_populates="o_dich")
    
    # Các trường thông tin bổ sung
    noi_phat_hien_tcm = Column(String(50), nullable=True)
    tieu_chi_2ca_7ngay = Column(Boolean, default=False)
    tieu_chi_sxh_nang = Column(Boolean, default=False)
    tieu_chi_xet_nghiem = Column(Boolean, default=False)
    tieu_chi_tu_vong = Column(Boolean, default=False)
    loai_xet_nghiem_sxh = Column(String(100), nullable=True)
    so_ca_mac_trong_od = Column(Integer, default=0)

class CaBenh(Base):
    __tablename__ = 'ca_benh'
    id = Column(Integer, primary_key=True)
    
    # === THAY ĐỔI 2: Bỏ unique=True ở đây ===
    ma_so_benh_nhan = Column(String(100), nullable=False, index=True)
    
    ho_ten = Column(String(250))
    ngay_sinh = Column(Date)
    gioi_tinh = Column(String(10))
    dia_chi_chi_tiet = Column(Text)
    dia_chi_ap = Column(String(250))
    
    # Thêm index=True vào các cột sẽ dùng cho ràng buộc UNIQUE
    ngay_khoi_phat = Column(Date, index=True)
    chan_doan_chinh = Column(String(250), index=True)

    ngay_nhap_vien = Column(Date)
    ngay_ra_vien = Column(Date)
    phan_do_benh = Column(String(250))
    tinh_trang_hien_nay = Column(String(100))
    ngay_import = Column(Date, default=date.today)
    
    xa_id = Column(Integer, ForeignKey('don_vi_hanh_chinh.id'), nullable=False)
    don_vi = relationship("DonViHanhChinh", back_populates="ca_benh")
    
    o_dich_id = Column(Integer, ForeignKey('o_dich.id'), nullable=True)
    o_dich = relationship("O_Dich", back_populates="ca_benh_lien_quan")

    # === THAY ĐỔI 3: Thêm ràng buộc UNIQUE kết hợp ở cấp độ bảng ===
    __table_args__ = (
        UniqueConstraint('ma_so_benh_nhan', 'ngay_khoi_phat', 'chan_doan_chinh', name='_ma_so_ngay_khoi_phat_chan_doan_uc'),
    )

def create_db():
    engine = create_engine('sqlite:///app.db') 
    print("Đang tạo các bảng trong CSDL...")
    Base.metadata.create_all(engine)
    print("Tạo CSDL và các bảng thành công!")

if __name__ == "__main__":
    create_db()