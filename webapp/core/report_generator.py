# file: report_generator.py (Phiên bản hoàn chỉnh cuối cùng - Đã cập nhật)

import pandas as pd
import calendar
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
import os
import uuid
import shutil
import zipfile
import re

# Giả định các import này từ project của bạn là chính xác
# QUAN TRỌNG: Đảm bảo model O_Dich có relationship tên là 'ca_benh_lien_quan'
# trỏ đến danh sách các CaBenh thuộc ổ dịch đó.
from .database_setup import CaBenh, DonViHanhChinh, O_Dich
from .week_calendar import WeekCalendar
from .utils import get_all_child_xa_ids

# ==============================================================================
# 1. HẰNG SỐ VÀ CẤU HÌNH
# ==============================================================================

LIST_BENH_TRUYEN_NHIEM = [
    'Tả', 'Thương hàn', 'Sốt xuất huyết Dengue', 'Viêm não Vi rút',
    'Tay - chân - miệng', 'Viêm màng não do Não mô cầu', 'Sởi',
    'Viêm gan cấp tính', 'Đậu mùa khỉ', 'BTN nguy hiểm mới'
]

# ==============================================================================
# 2. CÁC HÀM TRỢ GIÚP (HELPER FUNCTIONS)
# ==============================================================================

def _get_formatted_unit_name(user_don_vi: DonViHanhChinh) -> str:
    """
    Tạo tên đơn vị được định dạng chuẩn để hiển thị trên header báo cáo.
    - Tỉnh: Tên đơn vị cấp tỉnh.
    - Khu vực: TRUNG TÂM Y TẾ [TÊN] (bỏ các tiền tố như 'Khu vực', 'TTYT')
    - Xã: TRẠM Y TẾ [TÊN XÃ]
    """
    cap_don_vi = user_don_vi.cap_don_vi
    ten_don_vi = user_don_vi.ten_don_vi

    if cap_don_vi == 'Tỉnh':
        return ten_don_vi.upper()
    
    elif cap_don_vi == 'Khu vực':
        # Danh sách các tiền tố cần xóa, xếp từ DÀI NHẤT đến NGẮN NHẤT.
        prefixes_to_remove = [
            "TTYT Khu vực", "Trung tâm y tế Khu vực", "Khu vực TTYT", "TTYT", "Khu vực"
        ]
        ten_sach = ten_don_vi
        for prefix in prefixes_to_remove:
            if ten_sach.lower().startswith(prefix.lower()):
                ten_sach = ten_sach[len(prefix):].strip()
                break # Thoát sau khi tìm thấy và xử lý
        return f"TRUNG TÂM Y TẾ {ten_sach}".upper()

    elif cap_don_vi == 'Xã':
        return f"TRẠM Y TẾ {ten_don_vi}".upper()
    
    return ten_don_vi.upper()

def _get_reporting_units(db_session: Session, user_don_vi: DonViHanhChinh):
    """Xác định các đơn vị con cần báo cáo và cột để nhóm dữ liệu."""
    reporting_units = []
    group_by_col = None
    child_level_map = {'Xã': 'Ấp', 'Khu vực': 'Xã'}

    if user_don_vi.cap_don_vi in child_level_map:
        child_level = child_level_map[user_don_vi.cap_don_vi]
        reporting_units = sorted(
            [c for c in user_don_vi.children if c.cap_don_vi == child_level],
            key=lambda x: x.ten_don_vi
        )
        group_by_col = 'dia_chi_ap' if child_level == 'Ấp' else 'xa_id'
    elif user_don_vi.cap_don_vi == 'Tỉnh':
        reporting_units = db_session.query(DonViHanhChinh).filter(
            DonViHanhChinh.cap_don_vi == 'Xã'
        ).order_by(DonViHanhChinh.ten_don_vi).all()
        group_by_col = 'xa_id'

    return reporting_units, group_by_col

def _create_excel_formats(workbook):
    """Tạo và trả về một từ điển chứa các định dạng cell cho Excel."""
    base_font_14 = {'font_name': 'Times new Roman', 'font_size': 14}
    base_font_13 = {'font_name': 'Times new Roman', 'font_size': 13}
    base_font_12 = {'font_name': 'Times new Roman', 'font_size': 12}

    return {
        'title': workbook.add_format({**base_font_14, 'bold': True, 'align': 'center', 'valign': 'vcenter'}),
        'header': workbook.add_format({**base_font_14, 'bold': True, 'text_wrap': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'fg_color': '#DDEBF7'}),
        'cell': workbook.add_format({**base_font_14, 'align': 'center', 'border': 1}),
        'row_header': workbook.add_format({**base_font_14, 'align': 'left', 'valign': 'vcenter', 'border': 1}),
        'org_header': workbook.add_format({**base_font_14, 'bold': True, 'align': 'center'}),
        'org_header_0': workbook.add_format({**base_font_14, 'align': 'center'}),
        'total_header': workbook.add_format({**base_font_14, 'align': 'center', 'border': 1, 'fg_color': '#DDEBF7', 'bold': True}),
        'italic': workbook.add_format({**base_font_14, 'italic': True, 'align': 'center'}),
        'ghichu': workbook.add_format({**base_font_13, 'italic': True, 'align': 'left'}),
        'noinhan': workbook.add_format({**base_font_12, 'bold': True, 'italic': True, 'align': 'left'}),
        'nhanxet': workbook.add_format({**base_font_13, 'bold': True, 'italic': True, 'align': 'left'}), 
        'tieungu': workbook.add_format({**base_font_14, 'bold': True, 'align': 'center'}),
        'sxh_title': workbook.add_format({**base_font_13, 'bold': True, 'align': 'center', 'valign': 'vcenter'}),
        'sxh_header': workbook.add_format({**base_font_13, 'bold': True, 'text_wrap': True, 'align': 'center', 'valign': 'vcenter', 'border': 1}),
        'sxh_cell': workbook.add_format({**base_font_13, 'align': 'center', 'border': 1}),
        'sxh_cell_left': workbook.add_format({**base_font_13, 'align': 'left', 'border': 1}),
        'sxh_org_header_bold': workbook.add_format({**base_font_13, 'bold': True, 'align': 'center'}),
        'sxh_org_header': workbook.add_format({**base_font_13, 'align': 'center'}),
        'sxh_italic': workbook.add_format({**base_font_13, 'italic': True, 'align': 'center'}),
        'sxh_noi_nhan': workbook.add_format({**base_font_13, 'bold': True, 'italic': True, 'align': 'left'}),
        'sxh_tieungu': workbook.add_format({**base_font_13, 'bold': True, 'align': 'center'}),
        'od_cell_left_wrap': workbook.add_format({**base_font_13, 'align': 'left', 'valign': 'top', 'border': 1, 'text_wrap': True}),
        # *** MỚI: Định dạng cho các ô được gộp trong sheet chi tiết ổ dịch ***
        'od_cell_top_left_wrap_merged': workbook.add_format({**base_font_13, 'align': 'left', 'valign': 'top', 'border': 1, 'text_wrap': True}),
        'od_cell_top_center_merged': workbook.add_format({**base_font_13, 'align': 'center', 'valign': 'top', 'border': 1}),
    }

def _draw_standard_header(
    worksheet, formats, user_don_vi, report_title, period_subtitle, date_range_subtitle, last_col_letter
):
    """Vẽ phần tiêu đề chuẩn cho tất cả các báo cáo."""
    so_hieu_map = {'Tỉnh': 'Số    /BC-KSBT', 'Khu vực': 'Số    /BC-TTYT', 'Xã': 'Số    /BC-TYT'}
    formatted_unit_name = _get_formatted_unit_name(user_don_vi)
    
    ten_don_vi_cap_tren = "SỞ Y TẾ AN GIANG"
    if user_don_vi.parent and user_don_vi.cap_don_vi != 'Tỉnh':
        ten_don_vi_cap_tren = user_don_vi.parent.ten_don_vi.upper()

    worksheet.merge_range('A1:E1', ten_don_vi_cap_tren, formats['sxh_org_header'])
    worksheet.merge_range('A2:E2', formatted_unit_name, formats['sxh_org_header_bold'])
    worksheet.merge_range('A4:E4', so_hieu_map.get(user_don_vi.cap_don_vi, '/BC'), formats['sxh_org_header'])

    right_header_start_col = 'G'
    worksheet.merge_range(f'{right_header_start_col}1:{last_col_letter}1', 'CỘNG HOÀ XÃ HỘI CHỦ NGHĨA VIỆT NAM', formats['sxh_org_header_bold'])
    worksheet.merge_range(f'{right_header_start_col}2:{last_col_letter}2', 'Độc lập - Tự do - Hạnh phúc', formats['sxh_tieungu'])

    worksheet.merge_range(f'A6:{last_col_letter}6', report_title, formats['sxh_title'])
    worksheet.merge_range(f'A7:{last_col_letter}7', period_subtitle, formats['sxh_title'])
    if date_range_subtitle:
        worksheet.merge_range(f'A8:{last_col_letter}8', date_range_subtitle, formats['sxh_italic'])

def _draw_standard_footer(
    worksheet, formats, user_don_vi, end_of_period_dt, last_row_idx, last_col_idx
):
    """Vẽ phần chân trang và chữ ký chuẩn cho tất cả các báo cáo."""
    chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC', 'Xã': 'TRƯỞNG TRẠM'}
    
    date_line_row = last_row_idx + 2
    title_line_row = date_line_row + 1
    recipient_line_row = title_line_row + 6

    sign_col_start = max(1, last_col_idx - 5)
    sign_col_start_letter = chr(ord('A') + sign_col_start)
    last_col_letter = chr(ord('A') + last_col_idx)

    worksheet.merge_range(
        f'{sign_col_start_letter}{date_line_row}:{last_col_letter}{date_line_row}',
        f"{user_don_vi.ten_don_vi}, ngày {end_of_period_dt.day} tháng {end_of_period_dt.month} năm {end_of_period_dt.year}",
        formats['sxh_italic']
    )
    worksheet.merge_range(f'{sign_col_start_letter}{title_line_row}:{last_col_letter}{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['sxh_org_header_bold'])

    worksheet.merge_range(f'A{title_line_row}:D{title_line_row}', 'NGƯỜI BÁO CÁO', formats['sxh_org_header_bold'])
    worksheet.merge_range(f'A{recipient_line_row}:D{recipient_line_row}', 'Nơi nhận:', formats['sxh_noi_nhan'])

def _draw_details_sheet(writer: pd.ExcelWriter, sheet_name: str, data_objects: list, column_map: dict, title: str, formats: dict):
    """
    Vẽ một sheet mới chứa danh sách chi tiết các đối tượng (CaBenh hoặc O_Dich).
    """
    if not data_objects:
        return

    records = []
    for obj in data_objects:
        record = {}
        for attr, header in column_map.items():
            value = obj
            try:
                for part in attr.split('.'):
                    value = getattr(value, part, None)
                    if value is None: break
                record[header] = value
            except AttributeError:
                record[header] = None
        records.append(record)

    df_details = pd.DataFrame(records)
    df_details.to_excel(writer, sheet_name=sheet_name, index=False, startrow=2)
    
    worksheet = writer.sheets[sheet_name]
    num_cols = len(df_details.columns)
    if num_cols > 0:
        last_col_letter = chr(ord('A') + num_cols - 1)
        worksheet.merge_range(f'A1:{last_col_letter}1', title, formats['title'])

    for idx, col in enumerate(df_details.columns):
        series = df_details[col]
        max_len = max((
            series.astype(str).map(len).max(),
            len(str(series.name))
        )) + 2
        worksheet.set_column(idx, idx, max_len)

# *** HÀM MỚI: Vẽ sheet chi tiết ca bệnh theo ổ dịch ***
def _draw_outbreak_cases_details_sheet(
    writer: pd.ExcelWriter, sheet_name: str, outbreaks_in_period: list, 
    column_map: dict, title: str, formats: dict, disease_type: str
):
    """
    Vẽ sheet chi tiết danh sách ca bệnh theo từng ổ dịch, có gộp ô.
    """
    if not outbreaks_in_period:
        return

    worksheet = writer.book.add_worksheet(sheet_name)
    
    # --- 1. Vẽ tiêu đề và header ---
    headers = ["STT", "Thông tin ổ dịch"] + list(column_map.values())
    num_cols = len(headers)
    last_col_letter = chr(ord('A') + num_cols - 1)
    worksheet.merge_range(f'A1:{last_col_letter}1', title, formats['title'])
    
    for col_idx, header in enumerate(headers):
        worksheet.write(2, col_idx, header, formats['sxh_header'])

    # --- 2. Ghi dữ liệu ---
    current_row = 3
    stt = 1
    for outbreak in outbreaks_in_period:
        # Giả định model O_Dich có relationship 'ca_benh_lien_quan'
        cases = outbreak.ca_benh_lien_quan
        if not cases:
            continue
        
        num_cases = len(cases)
        
        # --- Tạo chuỗi thông tin ổ dịch ---
        outbreak_info = f"Địa điểm: {outbreak.dia_diem_xu_ly or 'Chưa xác định'}\n" \
                        f"Phát hiện: {outbreak.ngay_phat_hien.strftime('%d/%m/%Y') if outbreak.ngay_phat_hien else 'N/A'}"
        if disease_type == 'TCM':
            type_text = f"Ổ dịch {outbreak.noi_phat_hien_tcm}" if outbreak.noi_phat_hien_tcm else "Ổ dịch"
            outbreak_info = f"{type_text}\n{outbreak_info}"

        # --- Gộp ô cho STT và Thông tin ổ dịch ---
        if num_cases > 1:
            worksheet.merge_range(current_row, 0, current_row + num_cases - 1, 0, stt, formats['od_cell_top_center_merged'])
            worksheet.merge_range(current_row, 1, current_row + num_cases - 1, 1, outbreak_info, formats['od_cell_top_left_wrap_merged'])
        else:
            worksheet.write(current_row, 0, stt, formats['sxh_cell'])
            worksheet.write(current_row, 1, outbreak_info, formats['od_cell_left_wrap'])

        # --- Ghi thông tin từng ca bệnh ---
        for case in cases:
            col_offset = 2
            for attr in column_map.keys():
                value = case
                try:
                    for part in attr.split('.'):
                        value = getattr(value, part, None)
                        if value is None: break
                except AttributeError:
                    value = None
                
                # Định dạng ngày tháng nếu có
                if isinstance(value, (date, datetime)):
                    value = value.strftime('%d/%m/%Y')
                
                worksheet.write(current_row, col_offset, value, formats['sxh_cell_left'])
                col_offset += 1
            current_row += 1
        
        stt += 1

    # --- 3. Điều chỉnh độ rộng cột ---
    worksheet.set_column('A:A', 5)   # STT
    worksheet.set_column('B:B', 35)  # Thông tin ổ dịch
    worksheet.set_column('C:C', 25)  # Họ tên
    worksheet.set_column('D:D', 12)  # Ngày sinh
    worksheet.set_column('E:G', 20)  # Địa chỉ
    worksheet.set_column('H:I', 15)  # Ngày KP, Tình trạng

# ==============================================================================
# 3. BÁO CÁO BỆNH TRUYỀN NHIỄM
# ==============================================================================

# ... (Toàn bộ code của phần 3 không thay đổi) ...
def _generate_btn_analysis_data(
    db_session: Session, user_don_vi: DonViHanhChinh,
    current_period: tuple, prev_period: tuple,
    df_raw: pd.DataFrame,
    period_type: str
):
    """Hàm "não bộ" cho báo cáo BTN: Truy vấn và tính toán dữ liệu cho nhận xét."""
    analysis = {}
    
    df_current = df_raw[df_raw['ngay_khoi_phat'].between(pd.to_datetime(current_period[0]), pd.to_datetime(current_period[1]))]
    df_prev = df_raw[df_raw['ngay_khoi_phat'].between(pd.to_datetime(prev_period[0]), pd.to_datetime(prev_period[1]))] if prev_period else pd.DataFrame()

    analysis['total_ts'] = len(df_current)
    analysis['deaths_ts'] = len(df_current[df_current['tinh_trang_hien_nay'] == 'Tử vong'])

    df_bs = df_raw[
        (df_raw['ngay_import'] >= pd.to_datetime(current_period[0])) &
        (df_raw['ngay_import'] < pd.to_datetime(current_period[1]) + timedelta(days=1)) &
        (df_raw['ngay_khoi_phat'] < pd.to_datetime(current_period[0]))
    ]
    analysis['total_bs'] = len(df_bs)
    
    if not df_bs.empty:
        df_bs_copy = df_bs.copy()
        
        if period_type == 'week':
            df_bs_copy['original_period'] = df_bs_copy['ngay_khoi_phat'].dt.isocalendar().week
            df_bs_copy['original_year'] = df_bs_copy['ngay_khoi_phat'].dt.isocalendar().year
            group_by_cols = ['chan_doan_chinh', 'original_year', 'original_period']
        else: # month
            df_bs_copy['original_period'] = df_bs_copy['ngay_khoi_phat'].dt.month
            df_bs_copy['original_year'] = df_bs_copy['ngay_khoi_phat'].dt.year
            group_by_cols = ['chan_doan_chinh', 'original_year', 'original_period']

        bs_details_grouped = df_bs_copy.groupby(group_by_cols).size().reset_index(name='count')
        bs_details_grouped = bs_details_grouped.sort_values(by='count', ascending=False)
        analysis['bs_details'] = bs_details_grouped.to_dict('records')
    else:
        analysis['bs_details'] = []

    analysis['top_diseases'] = df_current['chan_doan_chinh'].value_counts().head(3).to_dict()
    analysis['total_prev'] = len(df_prev)
    
    df_this_period_all = pd.concat([df_current, df_bs])
    if not df_this_period_all.empty:
        top_locations = df_this_period_all['don_vi_ten'].value_counts().head(1)
        if not top_locations.empty:
            top_loc_name = top_locations.index[0]
            mode_result = df_this_period_all[df_this_period_all['don_vi_ten'] == top_loc_name]['chan_doan_chinh'].mode()
            top_disease = mode_result[0] if not mode_result.empty else "Không xác định"
            analysis['top_location'] = {
                'name': top_loc_name,
                'count': int(top_locations.iloc[0]),
                'disease': top_disease
            }
        else:
            analysis['top_location'] = None
    else:
        analysis['top_location'] = None
        
    return analysis

def _generate_btn_comments(analysis: dict, period_type: str, period_number: int, prev_period_number: int, user_don_vi: DonViHanhChinh):
    if not analysis: return ["- Không có dữ liệu để tạo nhận xét."]
    comments = []
    period_type_lower = period_type.lower()
    display_don_vi_ten = user_don_vi.ten_don_vi
    if user_don_vi.cap_don_vi == 'Tỉnh': display_don_vi_ten = "tỉnh An Giang"
    elif user_don_vi.cap_don_vi == 'Khu vực': display_don_vi_ten = user_don_vi.ten_don_vi.replace("Trung tâm Y tế", "").strip()
    comments.append(f"- Trong {period_type_lower} {period_number}, toàn {display_don_vi_ten} ghi nhận {analysis['total_ts']} ca mắc mới và {analysis['deaths_ts']} ca tử vong. Ngoài ra, đã ghi nhận bổ sung {analysis['total_bs']} ca mắc từ các kỳ trước.")
    if analysis['top_diseases']: comments.append(f"- 03 bệnh có số ca mắc mới cao nhất trong kỳ là: {', '.join([f'{name} ({count} ca)' for name, count in analysis['top_diseases'].items()])}.")
    diff_vs_prev = analysis['total_ts'] - analysis['total_prev']
    comparison_text = "ổn định"
    if diff_vs_prev > 0: comparison_text = f"tăng ({analysis['total_ts']} so với {analysis['total_prev']} ca)"
    elif diff_vs_prev < 0: comparison_text = f"giảm ({analysis['total_ts']} so với {analysis['total_prev']} ca)"
    comments.append(f"- So với {period_type_lower} trước, số ca mắc mới thực tế có xu hướng {comparison_text}.")
    if analysis['total_bs'] > 0: comments.append(f"- Việc ghi nhận {analysis['total_bs']} ca báo cáo bổ sung cho thấy có sự chậm trễ trong công tác giám sát, báo cáo tại tuyến dưới (xem sheet 'ChiTiet_CaBoSung' để có danh sách đầy đủ).")
    if analysis['top_location']: comments.append(f"- {analysis['top_location']['name']} là địa phương có tổng số ca ghi nhận (cả mới và bổ sung) cao nhất trong kỳ với {analysis['top_location']['count']} ca, chủ yếu là bệnh {analysis['top_location']['disease']}.")
    return comments

def _generate_benh_truyen_nhiem_report_base(db_session: Session, user_don_vi: DonViHanhChinh, filepath: str, year: int, period_type: str, period_number: int):
    # --- PHẦN 1: TÍNH TOÁN THỜI GIAN ---
    if period_type == 'week':
        calendar_obj = WeekCalendar(year)
        current_details = calendar_obj.get_week_details(period_number)
        if current_details is None: raise ValueError(f"Không tìm thấy tuần {period_number}.")
        prev_details = calendar_obj.get_week_details(period_number - 1)
        start_of_year_dt, end_of_period_dt_obj = calendar_obj.get_ytd_range(period_number)
        start_of_period_dt = current_details['ngay_bat_dau']
        end_of_period_dt = end_of_period_dt_obj.date()
        analysis_periods = {"current_period": (start_of_period_dt.date(), end_of_period_dt), "prev_period": (prev_details['ngay_bat_dau'].date(), prev_details['ngay_ket_thuc'].date()) if prev_details is not None else None}
        comment_details = {"period_type": "Tuần", "period_number": period_number, "prev_period_number": period_number - 1, "user_don_vi": user_don_vi}
        period_name = f"Tuần {period_number} năm {year}"
        period_label, note_text = "TS", "Ghi chú: TS: Tổng số ca mắc trong tuần; BS: Bổ sung ca mắc; CD: Số ca mắc cộng dồn"
    elif period_type == 'month':
        _, num_days = calendar.monthrange(year, period_number)
        start_of_period_dt, end_of_period_dt_obj = datetime(year, period_number, 1), datetime(year, period_number, num_days)
        start_of_year_dt = datetime(year, 1, 1)
        end_of_period_dt = end_of_period_dt_obj.date()
        prev_month = period_number - 1 if period_number > 1 else 12
        prev_month_year = year if period_number > 1 else year - 1
        _, prev_month_num_days = calendar.monthrange(prev_month_year, prev_month)
        start_of_prev_month, end_of_prev_month = date(prev_month_year, prev_month, 1), date(prev_month_year, prev_month, prev_month_num_days)
        analysis_periods = {"current_period": (start_of_period_dt.date(), end_of_period_dt), "prev_period": (start_of_prev_month, end_of_prev_month)}
        comment_details = {"period_type": "Tháng", "period_number": period_number, "prev_period_number": prev_month, "user_don_vi": user_don_vi}
        period_name = f"Tháng {period_number} năm {year}"
        period_label, note_text = "TM", "Ghi chú: TM: Tổng số ca mắc trong tháng; BS: Bổ sung ca mắc; CD: Số ca mắc cộng dồn"
    else: raise ValueError("Loại kỳ báo cáo không hợp lệ.")
    
    # --- PHẦN 2: LẤY VÀ XỬ LÝ DỮ LIỆU ---
    list_cases_for_details_sheet = []
    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    results, totals = {}, {benh: {'mac_p': 0, 'chet_p': 0, 'mac_bs': 0, 'chet_bs': 0, 'mac_cd': 0, 'chet_cd': 0} for benh in LIST_BENH_TRUYEN_NHIEM}
    df_raw_for_analysis = pd.DataFrame()

    if reporting_units and group_by_col:
        xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
        if xa_ids_to_query:
            query_start_date = start_of_year_dt.date()
            if analysis_periods.get('prev_period') and analysis_periods['prev_period']: query_start_date = min(start_of_year_dt.date(), analysis_periods['prev_period'][0])
            query = db_session.query(CaBenh).options(joinedload(CaBenh.don_vi)).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.ngay_khoi_phat >= query_start_date)
            all_cases = query.all()

            start_dt = start_of_period_dt.date() if isinstance(start_of_period_dt, datetime) else start_of_period_dt
            end_dt = end_of_period_dt
            cases_in_period = [c for c in all_cases if c.ngay_khoi_phat and start_dt <= c.ngay_khoi_phat <= end_dt]
            
            cases_supplementary = []
            for c in all_cases:
                if c.ngay_import and c.ngay_khoi_phat:
                    ngay_import_date = c.ngay_import.date() if isinstance(c.ngay_import, datetime) else c.ngay_import
                    if start_dt <= ngay_import_date <= end_dt and c.ngay_khoi_phat < start_dt:
                        cases_supplementary.append(c)

            list_cases_for_details_sheet.extend(cases_in_period); list_cases_for_details_sheet.extend(cases_supplementary)
            
            data_list = [{'ngay_khoi_phat': c.ngay_khoi_phat, 'chan_doan_chinh': c.chan_doan_chinh, 'tinh_trang_hien_nay': c.tinh_trang_hien_nay, 'xa_id': c.xa_id, 'dia_chi_ap': c.dia_chi_ap, 'ngay_import': c.ngay_import, 'don_vi_ten': c.don_vi.ten_don_vi if c.don_vi else ''} for c in all_cases]
            df_raw = pd.DataFrame(data_list)
            if df_raw.empty: df_raw = pd.DataFrame(columns=['ngay_khoi_phat', 'chan_doan_chinh', 'tinh_trang_hien_nay', 'xa_id', 'dia_chi_ap', 'ngay_import', 'don_vi_ten'])
            else: df_raw['ngay_khoi_phat'], df_raw['ngay_import'] = pd.to_datetime(df_raw['ngay_khoi_phat'], errors='coerce'), pd.to_datetime(df_raw['ngay_import'], errors='coerce')
            
            df_raw_for_analysis = df_raw.copy()
            df_raw = df_raw[df_raw['ngay_khoi_phat'] <= pd.to_datetime(end_of_period_dt)]
            for unit in reporting_units:
                unit_id_or_name = unit.id if group_by_col == 'xa_id' else unit.ten_don_vi
                df_unit_raw = df_raw[df_raw[group_by_col] == unit_id_or_name]
                unit_results = {}
                for benh in LIST_BENH_TRUYEN_NHIEM:
                    df_benh_raw = df_unit_raw[df_unit_raw['chan_doan_chinh'] == benh]
                    df_benh_period = df_benh_raw[df_benh_raw['ngay_khoi_phat'] >= pd.to_datetime(start_dt)]
                    df_import_in_period = df_benh_raw[(df_benh_raw['ngay_import'] >= pd.to_datetime(start_dt)) & (df_benh_raw['ngay_import'] <= datetime.combine(end_dt, datetime.max.time()))]
                    df_benh_bosung = df_import_in_period[df_import_in_period['ngay_khoi_phat'] < pd.to_datetime(start_dt)]
                    unit_results[benh] = {'mac_p': len(df_benh_period), 'chet_p': len(df_benh_period[df_benh_period['tinh_trang_hien_nay'] == 'Tử vong']), 'mac_bs': len(df_benh_bosung), 'chet_bs': len(df_benh_bosung[df_benh_bosung['tinh_trang_hien_nay'] == 'Tử vong']), 'mac_cd': len(df_benh_raw), 'chet_cd': len(df_benh_raw[df_benh_raw['tinh_trang_hien_nay'] == 'Tử vong'])}
                    for key in totals[benh]: totals[benh][key] += unit_results[benh][key]
                results[unit.id] = unit_results
    
    # --- PHẦN 3: TẠO NHẬN XÉT ---
    comments = []
    analysis_data = None
    if not df_raw_for_analysis.empty:
        analysis_data = _generate_btn_analysis_data(db_session, user_don_vi, df_raw=df_raw_for_analysis, period_type=period_type, **analysis_periods)
        comments = _generate_btn_comments(analysis_data, **comment_details)

    # --- PHẦN 4: VẼ EXCEL ---
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # --- Sheet 1: Báo cáo chính ---
        worksheet = workbook.add_worksheet('BaoCaoBTN_TongHop')
        formats = _create_excel_formats(workbook)
        last_col_idx = 1 + 1 + (len(LIST_BENH_TRUYEN_NHIEM) * 2) - 1
        _draw_standard_header(worksheet, formats, user_don_vi, 'BÁO CÁO BỆNH TRUYỀN NHIỄM', period_name, f"Từ ngày {start_of_period_dt.strftime('%d/%m/%Y')} đến ngày {end_of_period_dt.strftime('%d/%m/%Y')}", chr(ord('A') + last_col_idx))
        
        header_start_row = 9
        worksheet.merge_range(header_start_row, 0, header_start_row + 1, 0, 'Địa phương', formats['header']); worksheet.merge_range(header_start_row, 1, header_start_row + 1, 1, 'Loại dịch', formats['header'])
        for i, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
            start_col = 2 + i * 2
            worksheet.set_column(start_col, start_col + 1, max(10, len(benh) // 2))
            worksheet.merge_range(header_start_row, start_col, header_start_row, start_col + 1, benh, formats['header'])
            worksheet.write(header_start_row + 1, start_col, 'Mắc', formats['header']); worksheet.write(header_start_row + 1, start_col + 1, 'Chết', formats['header'])
        worksheet.set_column('A:A', 22); worksheet.set_column('B:B', 15); worksheet.set_row(9, 50)
        current_row = header_start_row + 2
        for unit in reporting_units:
            for i, label in enumerate([period_label, 'BS', 'CD']): worksheet.write(current_row + i, 1, label, formats['cell'])
            worksheet.merge_range(current_row, 0, current_row + 2, 0, unit.ten_don_vi, formats['row_header'])
            for j, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
                start_col = 2 + j * 2
                res = results.get(unit.id, {}).get(benh, {})
                worksheet.write(current_row, start_col, res.get('mac_p', 0), formats['cell']); worksheet.write(current_row, start_col + 1, res.get('chet_p', 0), formats['cell'])
                worksheet.write(current_row + 1, start_col, res.get('mac_bs', 0), formats['cell']); worksheet.write(current_row + 1, start_col + 1, res.get('chet_bs', 0), formats['cell'])
                worksheet.write(current_row + 2, start_col, res.get('mac_cd', 0), formats['cell']); worksheet.write(current_row + 2, start_col + 1, res.get('chet_cd', 0), formats['cell'])
            current_row += 3
        worksheet.merge_range(current_row, 0, current_row + 2, 0, 'TỔNG CỘNG', formats['header'])
        for i, label in enumerate([period_label, 'BS', 'CD']): worksheet.write(current_row + i, 1, label, formats['total_header'])
        for j, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
            start_col = 2 + j * 2
            worksheet.write(current_row, start_col, totals[benh]['mac_p'], formats['header']); worksheet.write(current_row, start_col + 1, totals[benh]['chet_p'], formats['header'])
            worksheet.write(current_row + 1, start_col, totals[benh]['mac_bs'], formats['header']); worksheet.write(current_row + 1, start_col + 1, totals[benh]['chet_bs'], formats['header'])
            worksheet.write(current_row + 2, start_col, totals[benh]['mac_cd'], formats['header']); worksheet.write(current_row + 2, start_col + 1, totals[benh]['chet_cd'], formats['header'])
        footer_base_row = current_row + 2
        if comments:
            worksheet.write(footer_base_row + 1, 0, note_text, formats['ghichu'])
            worksheet.write(footer_base_row + 2, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row = footer_base_row + 3
            comment_format = workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, last_col_idx, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)
        _draw_standard_footer(worksheet, formats, user_don_vi, end_of_period_dt, footer_base_row, last_col_idx)

        # --- Sheet 2: Chi tiết TẤT CẢ ca bệnh (mới + bổ sung) ---
        cabenh_column_map = {'ho_ten': 'Họ và tên', 'ngay_sinh': 'Ngày sinh', 'don_vi.ten_don_vi': 'Xã/Phường', 'dia_chi_ap': 'Ấp/Khu vực', 'dia_chi_chi_tiet': 'Địa chỉ chi tiết', 'chan_doan_chinh': 'Chẩn đoán', 'ngay_khoi_phat': 'Ngày khởi phát', 'tinh_trang_hien_nay': 'Tình trạng'}
        _draw_details_sheet(writer, 'ChiTiet_CaBenh', list_cases_for_details_sheet, cabenh_column_map, f"DANH SÁCH CA BỆNH GHI NHẬN TRONG {period_name.upper()}", formats)

        # --- Sheet 3: Chi tiết ca bổ sung ---
        if analysis_data and analysis_data.get('bs_details'):
            worksheet_bs = workbook.add_worksheet('ChiTiet_CaBoSung')
            df_bs_details = pd.DataFrame(analysis_data['bs_details'])
            
            period_col_name = 'Tuần khởi phát' if period_type == 'week' else 'Tháng khởi phát'
            df_bs_details.rename(columns={
                'chan_doan_chinh': 'Tên bệnh',
                'original_year': 'Năm khởi phát',
                'original_period': period_col_name,
                'count': 'Số ca bổ sung'
            }, inplace=True)

            worksheet_bs.merge_range('A1:D1', f"Chi tiết các ca bệnh bổ sung được ghi nhận trong {period_name.lower()}", formats['title'])
            df_bs_details.to_excel(writer, sheet_name='ChiTiet_CaBoSung', startrow=2, index=False)
            
            for idx, col in enumerate(df_bs_details.columns):
                series = df_bs_details[col]
                max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
                worksheet_bs.set_column(idx, idx, max_len)

def generate_benh_truyen_nhiem_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    _generate_benh_truyen_nhiem_report_base(db_session, user_don_vi, filepath, year=calendar_obj.year, period_type='week', period_number=week_number)

def generate_benh_truyen_nhiem_report_monthly(db_session: Session, year: int, month: int, user_don_vi: DonViHanhChinh, filepath: str):
    _generate_benh_truyen_nhiem_report_base(db_session, user_don_vi, filepath, year=year, period_type='month', period_number=month)

# ==============================================================================
# 4. BÁO CÁO SỐT XUẤT HUYẾT
# ==============================================================================

# ... (Toàn bộ code của phần 4 không thay đổi) ...
def _generate_sxh_analysis_data(db_session: Session, user_don_vi: DonViHanhChinh, current_period: tuple, prev_period: tuple, cumulative_this_year: tuple, cumulative_last_year: tuple):
    data = {}
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return None
    cases_this_period = db_session.query(CaBenh).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.chan_doan_chinh.like('%Sốt xuất huyết%'), CaBenh.ngay_khoi_phat.between(current_period[0], current_period[1])).all()
    data['total_this_period'] = len(cases_this_period)
    data['warning_this_period'] = sum(1 for c in cases_this_period if c.phan_do_benh != 'Sốt xuất huyết Dengue nặng')
    data['severe_this_period'] = sum(1 for c in cases_this_period if c.phan_do_benh == 'Sốt xuất huyết Dengue nặng')
    data['deaths_this_period'] = sum(1 for c in cases_this_period if c.tinh_trang_hien_nay == 'Tử vong')
    data['total_prev_period'] = db_session.query(CaBenh).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.chan_doan_chinh.like('%Sốt xuất huyết%'), CaBenh.ngay_khoi_phat.between(prev_period[0], prev_period[1])).count() if prev_period else 0
    data['cumulative_this_year'] = db_session.query(CaBenh).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.chan_doan_chinh.like('%Sốt xuất huyết%'), CaBenh.ngay_khoi_phat.between(cumulative_this_year[0], cumulative_this_year[1])).count()
    data['cumulative_last_year'] = db_session.query(CaBenh).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.chan_doan_chinh.like('%Sốt xuất huyết%'), CaBenh.ngay_khoi_phat.between(cumulative_last_year[0], cumulative_last_year[1])).count()
    top_locations = db_session.query(DonViHanhChinh.ten_don_vi, func.count(CaBenh.id).label('so_ca')).join(CaBenh, CaBenh.xa_id == DonViHanhChinh.id).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.chan_doan_chinh.like('%Sốt xuất huyết%'), CaBenh.ngay_khoi_phat.between(current_period[0], current_period[1])).group_by(DonViHanhChinh.ten_don_vi).order_by(func.count(CaBenh.id).desc()).all()
    if top_locations: data['top_locations_this_period'] = {"locations": [loc.ten_don_vi for loc in top_locations if loc.so_ca == top_locations[0].so_ca], "count": top_locations[0].so_ca}
    else: data['top_locations_this_period'] = None
    return data

def _generate_sxh_comments(data: dict, period_type: str, period_number: int, prev_period_number: int, year: int, end_of_period_dt: date):
    if not data: return ["- Không có đủ dữ liệu để tạo nhận xét."]
    comments = []
    period_type_lower = period_type.lower()
    comments.append(f"- Trong {period_type_lower} {period_number} ghi nhận {data['total_this_period']} ca mắc, trong đó SXHD và SXHD có dấu hiệu cảnh báo {data['warning_this_period']} ca, SXHD nặng {data['severe_this_period']} ca, tử vong {data['deaths_this_period']} ca.")
    diff_vs_prev = data['total_this_period'] - data['total_prev_period']
    comparison_text = "bằng"
    if diff_vs_prev > 0: comparison_text = f"tăng {diff_vs_prev} ca"
    elif diff_vs_prev < 0: comparison_text = f"giảm {abs(diff_vs_prev)} ca"
    comments.append(f"- Số mắc SXHD trong {period_type_lower} thứ {period_number} là {data['total_this_period']} ca {comparison_text} so với {period_type_lower} {prev_period_number}/{year} ({data['total_prev_period']} ca).")
    diff_vs_last_year = data['cumulative_this_year'] - data['cumulative_last_year']
    comparison_text_y = "bằng"
    if diff_vs_last_year > 0: comparison_text_y = f"tăng {diff_vs_last_year} ca"
    elif diff_vs_last_year < 0: comparison_text_y = f"giảm {abs(diff_vs_last_year)} ca"
    comments.append(f"- Số mắc SXHD tính đến ngày {end_of_period_dt.strftime('%d/%m/%Y')} là {data['cumulative_this_year']} ca {comparison_text_y} so với cùng kỳ năm {year - 1} ({data['cumulative_last_year']} ca).")
    if data['top_locations_this_period'] and data['top_locations_this_period']['count'] > 0: comments.append(f"- Địa phương có số mắc SXHD cao nhất trong {period_type_lower} là: {', '.join(data['top_locations_this_period']['locations'])} ({data['top_locations_this_period']['count']} ca).")
    else: comments.append(f"- Trong {period_type_lower} không ghi nhận ca mắc SXHD nào.")
    return comments

def _generate_sxh_report_base(db_session: Session, start_of_year_dt: date, end_of_period_dt: date, start_of_period_dt: date, user_don_vi: DonViHanhChinh, filepath: str, period_name: str, year: int, analysis_periods: dict = None, comment_details: dict = None):
    list_cases_for_details_sheet = []
    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    results = {}
    if reporting_units and group_by_col:
        xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
        if xa_ids_to_query:
            query = db_session.query(CaBenh).options(joinedload(CaBenh.don_vi)).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.chan_doan_chinh.like('%Sốt xuất huyết%'), CaBenh.ngay_khoi_phat >= start_of_year_dt, CaBenh.ngay_khoi_phat <= end_of_period_dt)
            all_sxh_cases = query.all()
            
            list_cases_for_details_sheet = [c for c in all_sxh_cases if c.ngay_khoi_phat and start_of_period_dt <= c.ngay_khoi_phat <= end_of_period_dt]
            
            data_list = [{'ngay_khoi_phat': c.ngay_khoi_phat, 'ngay_sinh': c.ngay_sinh, 'phan_do_benh': c.phan_do_benh, 'tinh_trang_hien_nay': c.tinh_trang_hien_nay, 'xa_id': c.xa_id, 'dia_chi_ap': c.dia_chi_ap} for c in all_sxh_cases]
            df_raw = pd.DataFrame(data_list)
            if df_raw.empty: df_raw = pd.DataFrame(columns=['ngay_khoi_phat', 'ngay_sinh', 'phan_do_benh', 'tinh_trang_hien_nay', 'xa_id', 'dia_chi_ap', 'tuoi'])
            else:
                df_raw['ngay_khoi_phat'], df_raw['ngay_sinh'] = pd.to_datetime(df_raw['ngay_khoi_phat'], errors='coerce'), pd.to_datetime(df_raw['ngay_sinh'], errors='coerce')
                df_raw.dropna(subset=['ngay_khoi_phat', 'ngay_sinh'], inplace=True)
                df_raw['tuoi'] = (df_raw['ngay_khoi_phat'] - df_raw['ngay_sinh']).dt.days / 365.25
            df_period = df_raw[df_raw['ngay_khoi_phat'] >= pd.to_datetime(start_of_period_dt)].copy()
            for unit in reporting_units:
                unit_id_or_name = unit.id if group_by_col == 'xa_id' else unit.ten_don_vi
                df_unit_raw, df_unit_period = df_raw[df_raw[group_by_col] == unit_id_or_name], df_period[df_period[group_by_col] == unit_id_or_name]
                results[unit.id] = {'mac_cb_p': len(df_unit_period[df_unit_period['phan_do_benh'] != 'Sốt xuất huyết Dengue nặng']), 'mac_cb_p_15t': len(df_unit_period[(df_unit_period['phan_do_benh'] != 'Sốt xuất huyết Dengue nặng') & (df_unit_period['tuoi'] <= 15)]), 'mac_cb_cd': len(df_unit_raw[df_unit_raw['phan_do_benh'] != 'Sốt xuất huyết Dengue nặng']), 'mac_nang_p': len(df_unit_period[df_unit_period['phan_do_benh'] == 'Sốt xuất huyết Dengue nặng']), 'mac_nang_p_15t': len(df_unit_period[(df_unit_period['phan_do_benh'] == 'Sốt xuất huyết Dengue nặng') & (df_unit_period['tuoi'] <= 15)]), 'mac_nang_cd': len(df_unit_raw[df_unit_raw['phan_do_benh'] == 'Sốt xuất huyết Dengue nặng']), 'tong_mac_p': len(df_unit_period), 'tong_mac_cd': len(df_unit_raw), 'chet_p': len(df_unit_period[df_unit_period['tinh_trang_hien_nay'] == 'Tử vong']), 'chet_p_15t': len(df_unit_period[(df_unit_period['tinh_trang_hien_nay'] == 'Tử vong') & (df_unit_period['tuoi'] <= 15)]), 'chet_cd': len(df_unit_raw[df_unit_raw['tinh_trang_hien_nay'] == 'Tử vong'])}
    
    comments = []
    if analysis_periods and comment_details:
        analysis_data = _generate_sxh_analysis_data(db_session, user_don_vi, **analysis_periods)
        comments = _generate_sxh_comments(analysis_data, **comment_details)
 
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet('BaoCaoSXH')
        formats = _create_excel_formats(workbook)
        
        formatted_unit_name = _get_formatted_unit_name(user_don_vi)
        so_hieu_map = {'Tỉnh': 'Số    /BC-KSBT', 'Khu vực': 'Số    /BC-TTYT', 'Xã': 'Số    /BC-TYT'}
        worksheet.merge_range('A1:C1', 'SỞ Y TẾ AN GIANG', formats['sxh_org_header']); worksheet.merge_range('A2:C2', formatted_unit_name, formats['sxh_org_header_bold']); worksheet.merge_range('A4:C4', so_hieu_map.get(user_don_vi.cap_don_vi, '/BC'), formats['sxh_org_header'])
        worksheet.merge_range('F1:L1', 'CỘNG HOÀ XÃ HỘI CHỦ NGHĨA VIỆT NAM', formats['sxh_org_header_bold']); worksheet.merge_range('F2:L2', 'Độc lập - Tự do - Hạnh phúc', formats['sxh_tieungu'])
        worksheet.merge_range('A6:L6', 'BÁO CÁO SỐ LIỆU MẮC CHẾT SỐT XUẤT HUYẾT DENGUE', formats['sxh_title']); worksheet.merge_range('A7:L7', period_name, formats['sxh_title']); worksheet.merge_range('A8:L8', f"Từ ngày {start_of_period_dt.strftime('%d/%m/%Y')} đến ngày {end_of_period_dt.strftime('%d/%m/%Y')}", formats['sxh_italic'])
        h_row = 9
        worksheet.set_row(h_row, 50); worksheet.set_column('A:A', 25)
        worksheet.merge_range(h_row, 0, h_row + 1, 0, 'Địa phương', formats['sxh_header']); worksheet.merge_range(h_row, 1, h_row, 3, 'SXH Dengue và SXH Dengue có dấu hiệu cảnh báo', formats['sxh_header']); worksheet.merge_range(h_row, 4, h_row, 6, 'SXH Dengue nặng', formats['sxh_header']); worksheet.merge_range(h_row, 7, h_row + 1, 7, 'Tổng cộng mắc', formats['sxh_header']); worksheet.merge_range(h_row, 8, h_row + 1, 8, 'Cộng dồn mắc', formats['sxh_header']); worksheet.merge_range(h_row, 9, h_row, 11, 'SỐ CHẾT', formats['sxh_header'])
        for col, label in [(1, 'Tổng'), (2, '≤15T'), (3, 'Cộng dồn'), (4, 'Tổng'), (5, '≤15T'), (6, 'Cộng dồn'), (9, 'Tổng'), (10, '≤15T'), (11, 'Cộng dồn')]: worksheet.write(h_row + 1, col, label, formats['sxh_header'])
        data_start_row = h_row + 2
        for i, unit in enumerate(reporting_units):
            row = data_start_row + i
            res = results.get(unit.id, {})
            worksheet.write(row, 0, unit.ten_don_vi, formats['sxh_cell_left'])
            for col, key in [(1, 'mac_cb_p'), (2, 'mac_cb_p_15t'), (3, 'mac_cb_cd'), (4, 'mac_nang_p'), (5, 'mac_nang_p_15t'), (6, 'mac_nang_cd'), (7, 'tong_mac_p'), (8, 'tong_mac_cd'), (9, 'chet_p'), (10, 'chet_p_15t'), (11, 'chet_cd')]: worksheet.write(row, col, res.get(key, 0), formats['sxh_cell'])
        total_row = data_start_row + len(reporting_units)
        worksheet.write(total_row, 0, "TỔNG CỘNG", formats['sxh_header'])
        for col in range(1, 12): worksheet.write_formula(total_row, col, f'=SUM({chr(65 + col)}{data_start_row + 1}:{chr(65 + col)}{total_row})', formats['sxh_header'])
        footer_base_row = total_row
        if comments:
            worksheet.write(footer_base_row + 1, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row = footer_base_row + 2
            comment_format = workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, 11, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)
        _draw_standard_footer(worksheet, formats, user_don_vi, end_of_period_dt, footer_base_row, 11)
        
        sxh_column_map = {
            'ho_ten': 'Họ và tên',
            'ngay_sinh': 'Ngày sinh',
            'don_vi.ten_don_vi': 'Xã/Phường',
            'dia_chi_ap': 'Ấp/Khu vực',
            'dia_chi_chi_tiet': 'Địa chỉ chi tiết',
            'phan_do_benh': 'Phân độ',
            'ngay_khoi_phat': 'Ngày khởi phát',
            'tinh_trang_hien_nay': 'Tình trạng'
        }
        _draw_details_sheet(
            writer, 
            sheet_name='ChiTiet_CaBenh_SXH', 
            data_objects=list_cases_for_details_sheet, 
            column_map=sxh_column_map,
            title=f"DANH SÁCH CA BỆNH SỐT XUẤT HUYẾT TRONG {period_name.upper()}",
            formats=formats
        )

def generate_sxh_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy tuần {week_number}.")
    prev_week_details = calendar_obj.get_week_details(week_number - 1)
    start_of_year_dt, end_of_week_dt = calendar_obj.get_ytd_range(week_number)
    year = calendar_obj.year
    analysis_periods = {"current_period": (week_details['ngay_bat_dau'].date(), week_details['ngay_ket_thuc'].date()), "prev_period": (prev_week_details['ngay_bat_dau'].date(), prev_week_details['ngay_ket_thuc'].date()) if prev_week_details is not None else None, "cumulative_this_year": (start_of_year_dt.date(), end_of_week_dt.date()), "cumulative_last_year": (date(year - 1, 1, 1), end_of_week_dt.date().replace(year=year - 1))}
    comment_details = {"period_type": "Tuần", "period_number": week_number, "prev_period_number": week_number - 1, "year": year, "end_of_period_dt": end_of_week_dt.date()}
    _generate_sxh_report_base(db_session, start_of_year_dt.date(), end_of_week_dt.date(), week_details['ngay_bat_dau'].date(), user_don_vi, filepath, f"Tuần {week_number} năm {year}", year, analysis_periods=analysis_periods, comment_details=comment_details)

def generate_sxh_report_monthly(db_session: Session, year: int, month: int, user_don_vi: DonViHanhChinh, filepath: str):
    try:
        _, num_days = calendar.monthrange(year, month)
        start_of_month, end_of_month = date(year, month, 1), date(year, month, num_days)
        start_of_year = date(year, 1, 1)
        prev_month = month - 1 if month > 1 else 12
        prev_month_year = year if month > 1 else year - 1
        _, prev_month_num_days = calendar.monthrange(prev_month_year, prev_month)
        start_of_prev_month, end_of_prev_month = date(prev_month_year, prev_month, 1), date(prev_month_year, prev_month, prev_month_num_days)
        analysis_periods = {"current_period": (start_of_month, end_of_month), "prev_period": (start_of_prev_month, end_of_prev_month), "cumulative_this_year": (start_of_year, end_of_month), "cumulative_last_year": (date(year - 1, 1, 1), end_of_month.replace(year=year - 1))}
        comment_details = {"period_type": "Tháng", "period_number": month, "prev_period_number": prev_month, "year": year, "end_of_period_dt": end_of_month}
    except ValueError: raise ValueError(f"Tháng {month} hoặc năm {year} không hợp lệ.")
    _generate_sxh_report_base(db_session, start_of_year, end_of_month, start_of_month, user_don_vi, filepath, f"Tháng {month} năm {year}", year, analysis_periods=analysis_periods, comment_details=comment_details)

# ==============================================================================
# 5. BÁO CÁO Ổ DỊCH
# ==============================================================================

# ... (Toàn bộ code của _generate_odich_sxh_analysis_data và _generate_odich_sxh_comments không thay đổi) ...
def _generate_odich_sxh_analysis_data(db_session: Session, user_don_vi: DonViHanhChinh, calendar_obj: WeekCalendar, week_number: int):
    analysis = {}
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy dữ liệu cho tuần {week_number}")
    prev_week_details = calendar_obj.get_week_details(week_number - 1)
    start_of_this_week, end_of_this_week = week_details['ngay_bat_dau'].date(), week_details['ngay_ket_thuc'].date()
    start_of_prev_week, end_of_prev_week = (prev_week_details['ngay_bat_dau'].date(), prev_week_details['ngay_ket_thuc'].date()) if prev_week_details is not None else (None, None)
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return None
    base_query = db_session.query(O_Dich).filter(O_Dich.xa_id.in_(xa_ids_to_query), O_Dich.loai_benh == 'SXH')
    odich_this_week = base_query.filter(O_Dich.ngay_phat_hien.between(start_of_this_week, end_of_this_week)).all()
    analysis['new_this_week'] = len(odich_this_week)
    analysis['processed_this_week'] = sum(1 for od in odich_this_week if od.ngay_xu_ly is not None)
    analysis['pending_this_week'] = analysis['new_this_week'] - analysis['processed_this_week']
    analysis['new_last_week'] = base_query.filter(O_Dich.ngay_phat_hien.between(start_of_prev_week, end_of_prev_week)).count() if start_of_prev_week else 0
    odich_cumulative = base_query.filter(O_Dich.ngay_phat_hien <= end_of_this_week).all()
    analysis['cumulative_total'] = len(odich_cumulative)
    analysis['cumulative_processed'] = sum(1 for od in odich_cumulative if od.ngay_xu_ly is not None)
    top_locations = db_session.query(DonViHanhChinh.ten_don_vi, func.count(O_Dich.id).label('so_od')).join(O_Dich, O_Dich.xa_id == DonViHanhChinh.id).filter(O_Dich.xa_id.in_(xa_ids_to_query), O_Dich.loai_benh == 'SXH', O_Dich.ngay_phat_hien.between(start_of_this_week, end_of_this_week)).group_by(DonViHanhChinh.ten_don_vi).order_by(func.count(O_Dich.id).desc()).all()
    if top_locations: analysis['top_locations'] = {"locations": [loc.ten_don_vi for loc in top_locations if loc.so_od == top_locations[0].so_od], "count": top_locations[0].so_od}
    else: analysis['top_locations'] = None
    return analysis

def _generate_odich_sxh_comments(analysis: dict, user_don_vi: DonViHanhChinh):
    if not analysis: return ["- Không có dữ liệu để tạo nhận xét."]
    comments = []
    process_rate = (analysis['processed_this_week'] / analysis['new_this_week'] * 100) if analysis['new_this_week'] > 0 else 100
    comments.append(f"- Trong tuần, phát hiện mới {analysis['new_this_week']} ổ dịch SXH, đã xử lý {analysis['processed_this_week']} ổ dịch, đạt tỷ lệ {process_rate:.0f}%. Hiện còn tồn {analysis['pending_this_week']} ổ dịch chưa xử lý.")
    diff_vs_last_week = analysis['new_this_week'] - analysis['new_last_week']
    comparison_text, trend_text = "không đổi", "ổn định"
    if diff_vs_last_week > 0: comparison_text, trend_text = f"tăng {diff_vs_last_week} ổ", "đang có nguy cơ bùng phát dịch"
    elif diff_vs_last_week < 0: comparison_text, trend_text = f"giảm {abs(diff_vs_last_week)} ổ", "đang được kiểm soát"
    comments.append(f"- Số ổ dịch phát hiện mới ({analysis['new_this_week']} ổ) {comparison_text} so với tuần trước ({analysis['new_last_week']} ổ), cho thấy tình hình dịch {trend_text}.")
    cumulative_rate = (analysis['cumulative_processed'] / analysis['cumulative_total'] * 100) if analysis['cumulative_total'] > 0 else 100
    comments.append(f"- Tính từ đầu năm, đã ghi nhận tổng cộng {analysis['cumulative_total']} ổ dịch, trong đó đã xử lý {analysis['cumulative_processed']} ổ, đạt tỷ lệ chung là {cumulative_rate:.0f}%.")
    if analysis['top_locations'] and analysis['top_locations']['count'] > 0: comments.append(f"- {', '.join(analysis['top_locations']['locations'])} là địa phương có số ổ dịch phát sinh cao nhất trong tuần với {analysis['top_locations']['count']} ổ dịch mới.")
    if analysis['pending_this_week'] > 0: comments.append(f"- Đề nghị {user_don_vi.ten_don_vi} tập trung nguồn lực xử lý dứt điểm {analysis['pending_this_week']} ổ dịch còn tồn đọng và tăng cường hoạt động diệt lăng quăng tại các khu vực có nguy cơ cao.")
    return comments

def generate_odich_sxh_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    """Tạo báo cáo hoạt động phòng chống SXHD, hiển thị đúng cấp Ấp cho tài khoản Xã."""
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy tuần {week_number}.")
    start_of_year_dt, end_of_week_dt_obj = calendar_obj.get_ytd_range(week_number)
    end_of_week_dt = end_of_week_dt_obj.date()
    start_of_week_dt = week_details['ngay_bat_dau']
    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    if not reporting_units or not group_by_col: reporting_units, group_by_col = [user_don_vi], 'xa_id'
    
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return
    
    # *** THAY ĐỔI: Tải trước (eager load) các ca bệnh liên quan ***
    query = db_session.query(O_Dich).options(
        joinedload(O_Dich.don_vi),
        joinedload(O_Dich.ca_benh_lien_quan).joinedload(CaBenh.don_vi)
    ).filter(
        O_Dich.xa_id.in_(xa_ids_to_query), 
        O_Dich.loai_benh == 'SXH', 
        O_Dich.ngay_phat_hien >= start_of_year_dt.date(), 
        O_Dich.ngay_phat_hien <= end_of_week_dt
    )
    all_outbreaks = query.all()
    # *** THAY ĐỔI: Lấy danh sách ổ dịch trong tuần để truyền vào hàm vẽ sheet chi tiết ***
    outbreaks_in_week_for_details = [
        od for od in all_outbreaks 
        if od.ngay_phat_hien and start_of_week_dt.date() <= od.ngay_phat_hien <= end_of_week_dt
    ]
    
    if all_outbreaks:
        data_for_df = [{'xa_id': c.xa_id, 'dia_chi_ap': c.dia_chi_ap, 'ngay_phat_hien': c.ngay_phat_hien, 'ngay_xu_ly': c.ngay_xu_ly, 'dia_diem_xu_ly': c.dia_diem_xu_ly} for c in all_outbreaks]
        df_raw = pd.DataFrame(data_for_df)
        df_raw['ngay_phat_hien'] = pd.to_datetime(df_raw['ngay_phat_hien'], errors='coerce')
        df_raw['ngay_xu_ly'] = pd.to_datetime(df_raw['ngay_xu_ly'], errors='coerce')
    else:
        df_raw = pd.DataFrame(columns=['xa_id', 'dia_chi_ap', 'ngay_phat_hien', 'ngay_xu_ly', 'dia_diem_xu_ly'])
    
    results_list = []
    for i, unit in enumerate(reporting_units):
        filter_value = unit.id if group_by_col == 'xa_id' else unit.ten_don_vi
        
        df_unit = df_raw[df_raw[group_by_col] == filter_value]
        df_tuan = df_unit[df_unit['ngay_phat_hien'] >= start_of_week_dt]
        
        dia_diem_list = df_unit.dropna(subset=['ngay_xu_ly', 'dia_diem_xu_ly'])['dia_diem_xu_ly'].tolist()
        results_list.append({
            'STT': i + 1, 'Địa Phương': unit.ten_don_vi, 
            'Phát hiện': len(df_tuan), 
            'Xử lý': len(df_tuan.dropna(subset=['ngay_xu_ly'])), 
            'Phát hiện C.dồn': len(df_unit), 
            'Xử lý C.dồn': len(df_unit.dropna(subset=['ngay_xu_ly'])), 
            'Địa điểm xử lý': '\n'.join(dia_diem_list)
        })
    
    df_to_write = pd.DataFrame(results_list)
    if not df_to_write.empty:
        total_row = df_to_write.drop(columns=['STT', 'Địa Phương']).sum().to_dict()
        total_row['Địa Phương'] = 'Tổng cộng'
        df_to_write = pd.concat([df_to_write, pd.DataFrame([total_row])], ignore_index=True)
    df_to_write = df_to_write.fillna(0)

    analysis_data = _generate_odich_sxh_analysis_data(db_session, user_don_vi, calendar_obj, week_number)
    comments = _generate_odich_sxh_comments(analysis_data, user_don_vi)

    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        # ... (Phần vẽ sheet chính 'BaoCaoOD_SXH' không thay đổi) ...
        workbook = writer.book
        worksheet = workbook.add_worksheet('BaoCaoOD_SXH')
        formats = _create_excel_formats(workbook)
        worksheet.set_column('A:A', 5); worksheet.set_column('B:B', 25); worksheet.set_column('C:F', 12); worksheet.set_column('G:G', 40)
        formatted_unit_name = _get_formatted_unit_name(user_don_vi)
        so_hieu_map = {'Tỉnh': 'Số    /BC-KSBT', 'Khu vực': 'Số    /BC-TTYT', 'Xã': 'Số    /BC-TYT'}
        worksheet.merge_range('A1:C1', 'SỞ Y TẾ AN GIANG', formats['sxh_org_header']); worksheet.merge_range('A2:C2', formatted_unit_name, formats['sxh_org_header_bold']); worksheet.merge_range('A4:C4', so_hieu_map.get(user_don_vi.cap_don_vi, '/BC'), formats['sxh_org_header'])
        worksheet.merge_range('E1:G1', 'CỘNG HOÀ XÃ HỘI CHỦ NGHĨA VIỆT NAM', formats['sxh_org_header_bold']); worksheet.merge_range('E2:G2', 'Độc lập - Tự do - Hạnh phúc', formats['sxh_tieungu'])
        worksheet.merge_range('A6:G6', 'BÁO CÁO HOẠT ĐỘNG PHÒNG CHỐNG SỐT XUẤT HUYẾT DENGUE', formats['sxh_title']); worksheet.merge_range('A7:G7', f"Tuần {week_number} năm {calendar_obj.year}", formats['sxh_title']); worksheet.merge_range('A8:G8', f"Từ ngày {start_of_week_dt.strftime('%d/%m/%Y')} đến ngày {end_of_week_dt.strftime('%d/%m/%Y')}", formats['sxh_italic'])
        h_row = 9
        worksheet.merge_range(h_row, 0, h_row + 1, 0, 'STT', formats['sxh_header']); worksheet.merge_range(h_row, 1, h_row + 1, 1, 'Địa Phương', formats['sxh_header']); worksheet.merge_range(h_row, 2, h_row, 3, 'Số OD', formats['sxh_header']); worksheet.merge_range(h_row, 4, h_row, 5, 'Số OD cộng dồn', formats['sxh_header']); worksheet.merge_range(h_row, 6, h_row + 1, 6, 'Địa điểm xử lý', formats['sxh_header'])
        for col, label in [(2, 'Phát hiện'), (3, 'Xử lý'), (4, 'Phát hiện'), (5, 'Xử lý')]: worksheet.write(h_row + 1, col, label, formats['sxh_header'])
        data_start_row = h_row + 2
        for row_num, row_data in df_to_write.iterrows():
            is_total = (row_data['Địa Phương'] == 'Tổng cộng')
            fmt_center, fmt_left, fmt_left_wrap = (formats['sxh_header'], formats['sxh_header'], formats['sxh_header']) if is_total else (formats['sxh_cell'], formats['sxh_cell_left'], formats['od_cell_left_wrap'])
            stt = int(row_data['STT']) if pd.notna(row_data['STT']) and row_data['STT'] != 0 else ''
            worksheet.write(data_start_row + row_num, 0, stt, fmt_center); worksheet.write(data_start_row + row_num, 1, row_data['Địa Phương'], fmt_left)
            for col, key in [(2, 'Phát hiện'), (3, 'Xử lý'), (4, 'Phát hiện C.dồn'), (5, 'Xử lý C.dồn')]: worksheet.write(data_start_row + row_num, col, int(row_data[key]), fmt_center)
            worksheet.write(data_start_row + row_num, 6, row_data['Địa điểm xử lý'], fmt_left_wrap)
        last_data_row = data_start_row + len(df_to_write) - 1
        worksheet.write(last_data_row + 1, 0, 'Ghi chú: “OD”: ổ dịch; “SXHD”: Sốt xuất huyết Dengue', formats['ghichu'])
        footer_base_row = last_data_row + 2
        if comments:
            worksheet.write(footer_base_row, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row = footer_base_row + 1
            comment_format = workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, 6, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)
        date_line_row, title_line_row, recipient_line_row = footer_base_row + 2, footer_base_row + 3, footer_base_row + 7
        chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC', 'Xã': 'TRƯỞNG TRẠM'}
        worksheet.merge_range(f'E{date_line_row}:G{date_line_row}', f"{user_don_vi.ten_don_vi}, ngày {end_of_week_dt.day} tháng {end_of_week_dt.month} năm {end_of_week_dt.year}", formats['sxh_italic'])
        worksheet.merge_range(f'E{title_line_row}:G{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['sxh_org_header_bold'])
        worksheet.write(f'B{title_line_row}', 'Người báo cáo', formats['sxh_org_header_bold']); worksheet.write(f'A{recipient_line_row}', 'Nơi nhận:', formats['sxh_noi_nhan'])

        # *** THAY ĐỔI: Gọi hàm mới để vẽ sheet chi tiết ***
        cabenh_in_odich_map = {
            'ho_ten': 'Họ và tên',
            'ngay_sinh': 'Ngày sinh',
            'don_vi.ten_don_vi': 'Xã/Phường',
            'dia_chi_ap': 'Ấp/Khu vực',
            'dia_chi_chi_tiet': 'Địa chỉ chi tiết',
            'ngay_khoi_phat': 'Ngày khởi phát',
            'tinh_trang_hien_nay': 'Tình trạng'
        }
        _draw_outbreak_cases_details_sheet(
            writer, 
            'ChiTiet_CaBenh_Trong_OD',
            outbreaks_in_week_for_details,
            cabenh_in_odich_map,
            f"DANH SÁCH CA BỆNH TRONG Ổ DỊCH SXH (TUẦN {week_number}/{calendar_obj.year})",
            formats,
            disease_type='SXH'
        )

# ... (Toàn bộ code của _generate_odich_tcm_analysis_data và _generate_odich_tcm_comments không thay đổi) ...
def _generate_odich_tcm_analysis_data(db_session: Session, user_don_vi: DonViHanhChinh, calendar_obj: WeekCalendar, week_number: int):
    analysis = {}
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy dữ liệu cho tuần {week_number}")
    prev_week_details = calendar_obj.get_week_details(week_number - 1)
    start_of_this_week, end_of_this_week = week_details['ngay_bat_dau'].date(), week_details['ngay_ket_thuc'].date()
    start_of_prev_week, end_of_prev_week = (prev_week_details['ngay_bat_dau'].date(), prev_week_details['ngay_ket_thuc'].date()) if prev_week_details is not None else (None, None)
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return None
    base_query = db_session.query(O_Dich).filter(O_Dich.xa_id.in_(xa_ids_to_query), O_Dich.loai_benh == 'TCM')
    odich_this_week = base_query.filter(O_Dich.ngay_phat_hien.between(start_of_this_week, end_of_this_week)).all()
    analysis['new_total_this_week'] = len(odich_this_week)
    analysis['new_school_this_week'] = sum(1 for od in odich_this_week if od.noi_phat_hien_tcm == 'Trường học')
    analysis['new_community_this_week'] = analysis['new_total_this_week'] - analysis['new_school_this_week']
    analysis['processed_this_week'] = sum(1 for od in odich_this_week if od.ngay_xu_ly is not None)
    if start_of_prev_week: analysis['new_school_last_week'] = base_query.filter(O_Dich.ngay_phat_hien.between(start_of_prev_week, end_of_prev_week), O_Dich.noi_phat_hien_tcm == 'Trường học').count()
    else: analysis['new_school_last_week'] = 0
    odich_cumulative = base_query.filter(O_Dich.ngay_phat_hien <= end_of_this_week).all()
    analysis['cumulative_total'] = len(odich_cumulative)
    analysis['cumulative_school'] = sum(1 for od in odich_cumulative if od.noi_phat_hien_tcm == 'Trường học')
    analysis['cumulative_processed'] = sum(1 for od in odich_cumulative if od.ngay_xu_ly is not None)
    top_locations = db_session.query(DonViHanhChinh.ten_don_vi, func.count(O_Dich.id).label('so_od')).join(O_Dich, O_Dich.xa_id == DonViHanhChinh.id).filter(O_Dich.xa_id.in_(xa_ids_to_query), O_Dich.loai_benh == 'TCM', O_Dich.ngay_phat_hien.between(start_of_this_week, end_of_this_week)).group_by(DonViHanhChinh.ten_don_vi).order_by(func.count(O_Dich.id).desc()).all()
    if top_locations: analysis['top_locations'] = {"locations": [loc.ten_don_vi for loc in top_locations if loc.so_od == top_locations[0].so_od], "count": top_locations[0].so_od}
    else: analysis['top_locations'] = None
    return analysis

def _generate_odich_tcm_comments(analysis: dict):
    if not analysis: return ["- Không có dữ liệu để tạo nhận xét."]
    comments = []
    process_rate = (analysis['processed_this_week'] / analysis['new_total_this_week'] * 100) if analysis['new_total_this_week'] > 0 else 100
    comments.append(f"- Trong tuần, ghi nhận {analysis['new_total_this_week']} ổ dịch TCM mới, trong đó có {analysis['new_school_this_week']} ổ dịch tại trường học và {analysis['new_community_this_week']} ổ dịch tại cộng đồng. Đã xử lý {analysis['processed_this_week']} ổ dịch, đạt tỷ lệ {process_rate:.0f}%.")
    diff_school = analysis['new_school_this_week'] - analysis['new_school_last_week']
    comparison_text, recommend_text = "không đổi", "duy trì"
    if diff_school > 0: comparison_text, recommend_text = f"tăng {diff_school} ổ", "tăng cường"
    elif diff_school < 0: comparison_text, recommend_text = f"giảm {abs(diff_school)} ổ", "tiếp tục duy trì tốt"
    comments.append(f"- Tình hình dịch trong trường học có xu hướng {comparison_text} (ghi nhận {analysis['new_school_this_week']} ổ so với {analysis['new_school_last_week']} ổ tuần trước), cho thấy cần {recommend_text} các biện pháp khử khuẩn tại các cơ sở giáo dục.")
    cumulative_rate = (analysis['cumulative_processed'] / analysis['cumulative_total'] * 100) if analysis['cumulative_total'] > 0 else 100
    school_percent = (analysis['cumulative_school'] / analysis['cumulative_total'] * 100) if analysis['cumulative_total'] > 0 else 0
    comments.append(f"- Lũy kế từ đầu năm, đã ghi nhận {analysis['cumulative_total']} ổ dịch TCM, trong đó {school_percent:.0f}% xảy ra tại trường học. Tỷ lệ xử lý chung đạt {cumulative_rate:.0f}%.")
    if analysis['top_locations'] and analysis['top_locations']['count'] > 0: comments.append(f"- {', '.join(analysis['top_locations']['locations'])} là địa phương cần chú ý nhất trong tuần, với {analysis['top_locations']['count']} ổ dịch mới.")
    return comments

def generate_odich_tcm_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    """Tạo báo cáo hoạt động phòng chống bệnh Tay Chân Miệng, hiển thị đúng cấp Ấp cho tài khoản Xã."""
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy tuần {week_number}.")
    start_of_year_dt, end_of_week_dt_obj = calendar_obj.get_ytd_range(week_number)
    end_of_week_dt = end_of_week_dt_obj.date()
    start_of_week_dt = week_details['ngay_bat_dau']
    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    if not reporting_units or not group_by_col: reporting_units, group_by_col = [user_don_vi], 'xa_id'

    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return
    
    # *** THAY ĐỔI: Tải trước (eager load) các ca bệnh liên quan ***
    query = db_session.query(O_Dich).options(
        joinedload(O_Dich.don_vi),
        joinedload(O_Dich.ca_benh_lien_quan).joinedload(CaBenh.don_vi)
    ).filter(
        O_Dich.xa_id.in_(xa_ids_to_query), 
        O_Dich.loai_benh == 'TCM', 
        O_Dich.ngay_phat_hien >= start_of_year_dt.date(), 
        O_Dich.ngay_phat_hien <= end_of_week_dt
    )
    all_outbreaks = query.all()
    # *** THAY ĐỔI: Lấy danh sách ổ dịch trong tuần để truyền vào hàm vẽ sheet chi tiết ***
    outbreaks_in_week_for_details = [
        od for od in all_outbreaks 
        if od.ngay_phat_hien and start_of_week_dt.date() <= od.ngay_phat_hien <= end_of_week_dt
    ]

    if all_outbreaks:
        data_for_df = [{'xa_id': c.xa_id, 'dia_chi_ap': c.dia_chi_ap, 'ngay_phat_hien': c.ngay_phat_hien, 'ngay_xu_ly': c.ngay_xu_ly, 'noi_phat_hien_tcm': c.noi_phat_hien_tcm, 'dia_diem_xu_ly': c.dia_diem_xu_ly} for c in all_outbreaks]
        df_raw = pd.DataFrame(data_for_df)
        df_raw['ngay_phat_hien'] = pd.to_datetime(df_raw['ngay_phat_hien'], errors='coerce')
        df_raw['ngay_xu_ly'] = pd.to_datetime(df_raw['ngay_xu_ly'], errors='coerce')
    else:
        df_raw = pd.DataFrame(columns=['xa_id', 'dia_chi_ap', 'ngay_phat_hien', 'ngay_xu_ly', 'noi_phat_hien_tcm', 'dia_diem_xu_ly'])
    
    results_list = []
    for i, unit in enumerate(reporting_units):
        filter_value = unit.id if group_by_col == 'xa_id' else unit.ten_don_vi

        df_unit = df_raw[df_raw[group_by_col] == filter_value]
        df_unit_th = df_unit[df_unit['noi_phat_hien_tcm'] == 'Trường học']
        df_unit_cd = df_unit[df_unit['noi_phat_hien_tcm'] == 'Cộng đồng']
        df_tuan_th = df_unit_th[df_unit_th['ngay_phat_hien'] >= start_of_week_dt]
        df_tuan_cd = df_unit_cd[df_unit_cd['ngay_phat_hien'] >= start_of_week_dt]

        dia_diem_list = df_unit.dropna(subset=['ngay_xu_ly', 'dia_diem_xu_ly'])['dia_diem_xu_ly'].tolist()
        results_list.append({'STT': i + 1, 'Địa phương': unit.ten_don_vi, 'PH Tuần TH': len(df_tuan_th), 'XL Tuần TH': len(df_tuan_th.dropna(subset=['ngay_xu_ly'])), 'PH Tuần CĐ': len(df_tuan_cd), 'XL Tuần CĐ': len(df_tuan_cd.dropna(subset=['ngay_xu_ly'])), 'PH CD TH': len(df_unit_th), 'XL CD TH': len(df_unit_th.dropna(subset=['ngay_xu_ly'])), 'PH CD CĐ': len(df_unit_cd), 'XL CD CĐ': len(df_unit_cd.dropna(subset=['ngay_xu_ly'])), 'Địa điểm xử lý': '\n'.join(dia_diem_list)})
    
    df_to_write = pd.DataFrame(results_list)
    if not df_to_write.empty:
        total_row = df_to_write.drop(columns=['STT', 'Địa phương']).sum().to_dict()
        total_row['Địa phương'] = 'Tổng cộng'
        df_to_write = pd.concat([df_to_write, pd.DataFrame([total_row])], ignore_index=True)
    df_to_write = df_to_write.fillna(0)
    
    analysis_data = _generate_odich_tcm_analysis_data(db_session, user_don_vi, calendar_obj, week_number)
    comments = _generate_odich_tcm_comments(analysis_data)
    
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        # ... (Phần vẽ sheet chính 'BaoCaoOD_TCM' không thay đổi) ...
        workbook = writer.book
        worksheet = workbook.add_worksheet('BaoCaoOD_TCM')
        formats = _create_excel_formats(workbook)
        worksheet.set_column('A:A', 5); worksheet.set_column('B:B', 25); worksheet.set_column('C:J', 9); worksheet.set_column('K:K', 35)
        formatted_unit_name = _get_formatted_unit_name(user_don_vi)
        so_hieu_map = {'Tỉnh': 'Số    /BC-KSBT', 'Khu vực': 'Số    /BC-TTYT', 'Xã': 'Số    /BC-TYT'}
        worksheet.merge_range('A1:D1', 'SỞ Y TẾ AN GIANG', formats['sxh_org_header']); worksheet.merge_range('A2:D2', formatted_unit_name, formats['sxh_org_header_bold']); worksheet.merge_range('A4:D4', so_hieu_map.get(user_don_vi.cap_don_vi, '/BC'), formats['sxh_org_header'])
        worksheet.merge_range('H1:K1', 'CỘNG HOÀ XÃ HỘI CHỦ NGHĨA VIỆT NAM', formats['sxh_org_header_bold']); worksheet.merge_range('H2:K2', 'Độc lập - Tự do - Hạnh phúc', formats['sxh_tieungu'])
        worksheet.merge_range('A6:K6', 'BÁO CÁO HOẠT ĐỘNG PHÒNG CHỐNG BỆNH TAY CHÂN MIỆNG', formats['sxh_title']); worksheet.merge_range('A7:K7', f"Tuần {week_number} năm {calendar_obj.year}", formats['sxh_title']); worksheet.merge_range('A8:K8', f"Từ ngày {start_of_week_dt.strftime('%d/%m/%Y')} đến ngày {end_of_week_dt.strftime('%d/%m/%Y')}", formats['sxh_italic'])
        h1, h2, h3 = 9, 10, 11
        worksheet.merge_range(h1, 0, h3, 0, 'STT', formats['sxh_header']); worksheet.merge_range(h1, 1, h3, 1, 'Địa phương', formats['sxh_header']); worksheet.merge_range(h1, 2, h1, 5, 'Số OD trong tuần', formats['sxh_header']); worksheet.merge_range(h1, 6, h1, 9, 'Số OD cộng dồn', formats['sxh_header']); worksheet.merge_range(h1, 10, h3, 10, 'Địa điểm xử lý', formats['sxh_header'])
        worksheet.merge_range(h2, 2, h2, 3, 'Trường học', formats['sxh_header']); worksheet.merge_range(h2, 4, h2, 5, 'Cộng đồng', formats['sxh_header']); worksheet.merge_range(h2, 6, h2, 7, 'Trường học', formats['sxh_header']); worksheet.merge_range(h2, 8, h2, 9, 'Cộng đồng', formats['sxh_header'])
        for col in [2, 4, 6, 8]: worksheet.write(h3, col, 'Phát hiện', formats['sxh_header']); worksheet.write(h3, col + 1, 'Xử lý', formats['sxh_header'])
        data_start_row = h3 + 1
        for row_num, row_data in df_to_write.iterrows():
            is_total = (row_data['Địa phương'] == 'Tổng cộng')
            fmt_center, fmt_left, fmt_left_wrap = (formats['sxh_header'], formats['sxh_header'], formats['sxh_header']) if is_total else (formats['sxh_cell'], formats['sxh_cell_left'], formats['od_cell_left_wrap'])
            stt = int(row_data['STT']) if pd.notna(row_data['STT']) and row_data['STT'] != 0 else ''
            worksheet.write(data_start_row + row_num, 0, stt, fmt_center); worksheet.write(data_start_row + row_num, 1, row_data['Địa phương'], fmt_left)
            for col, key in [(2, 'PH Tuần TH'), (3, 'XL Tuần TH'), (4, 'PH Tuần CĐ'), (5, 'XL Tuần CĐ'), (6, 'PH CD TH'), (7, 'XL CD TH'), (8, 'PH CD CĐ'), (9, 'XL CD CĐ')]: worksheet.write(data_start_row + row_num, col, int(row_data[key]), fmt_center)
            worksheet.write(data_start_row + row_num, 10, row_data['Địa điểm xử lý'], fmt_left_wrap)
        last_data_row = data_start_row + len(df_to_write) - 1
        footer_base_row = last_data_row + 1
        if comments:
            worksheet.write(footer_base_row, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row = footer_base_row + 1
            comment_format = workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, 10, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)
        date_line_row, title_line_row, recipient_line_row = footer_base_row + 2, footer_base_row + 3, footer_base_row + 7
        chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC', 'Xã': 'TRƯỞNG TRẠM'}
        worksheet.merge_range(f'H{date_line_row}:K{date_line_row}', f"{user_don_vi.ten_don_vi}, ngày {end_of_week_dt.day} tháng {end_of_week_dt.month} năm {end_of_week_dt.year}", formats['sxh_italic'])
        worksheet.merge_range(f'H{title_line_row}:K{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['sxh_org_header_bold'])
        worksheet.merge_range(f'A{title_line_row}:D{title_line_row}', 'NGƯỜI BÁO CÁO', formats['sxh_org_header_bold']); worksheet.merge_range(f'A{recipient_line_row}:D{recipient_line_row}', 'Nơi nhận:', formats['sxh_noi_nhan'])

        # *** THAY ĐỔI: Gọi hàm mới để vẽ sheet chi tiết ***
        cabenh_in_odich_map = {
            'ho_ten': 'Họ và tên',
            'ngay_sinh': 'Ngày sinh',
            'don_vi.ten_don_vi': 'Xã/Phường',
            'dia_chi_ap': 'Ấp/Khu vực',
            'dia_chi_chi_tiet': 'Địa chỉ chi tiết',
            'ngay_khoi_phat': 'Ngày khởi phát',
            'tinh_trang_hien_nay': 'Tình trạng'
        }
        _draw_outbreak_cases_details_sheet(
            writer, 
            'ChiTiet_CaBenh_Trong_OD',
            outbreaks_in_week_for_details,
            cabenh_in_odich_map,
            f"DANH SÁCH CA BỆNH TRONG Ổ DỊCH TCM (TUẦN {week_number}/{calendar_obj.year})",
            formats,
            disease_type='TCM'
        )

# ==============================================================================
# 6. HÀM TỔNG HỢP XUẤT TẤT CẢ BÁO CÁO
# ==============================================================================

# ... (Toàn bộ code của phần 6 không thay đổi) ...
def generate_all_reports_zip(db_session: Session, user_don_vi: DonViHanhChinh, year: int, period_type: str, period_number: int, zip_filepath: str):
    temp_dir = os.path.join(os.path.dirname(zip_filepath), str(uuid.uuid4()))
    os.makedirs(temp_dir, exist_ok=True)
    try:
        if period_type == 'week':
            calendar_obj = WeekCalendar(year)
            period_name_part = f"Tuan{period_number}"
        elif period_type == 'month':
            period_name_part = f"Thang{period_number}"
        else: raise ValueError("Loại kỳ báo cáo không hợp lệ. Phải là 'week' hoặc 'month'.")
        
        report_jobs = [
            {'func': generate_benh_truyen_nhiem_report if period_type == 'week' else generate_benh_truyen_nhiem_report_monthly, 'args': (db_session, calendar_obj if period_type == 'week' else year, period_number, user_don_vi), 'filename': f"BaoCao_BTN_{user_don_vi.ten_don_vi}_{year}_{period_name_part}.xlsx"},
            {'func': generate_sxh_report if period_type == 'week' else generate_sxh_report_monthly, 'args': (db_session, calendar_obj if period_type == 'week' else year, period_number, user_don_vi), 'filename': f"BaoCao_SXH_{user_don_vi.ten_don_vi}_{year}_{period_name_part}.xlsx"}
        ]
        if period_type == 'week':
            report_jobs.extend([
                {'func': generate_odich_sxh_report, 'args': (db_session, calendar_obj, period_number, user_don_vi), 'filename': f"BaoCao_ODich_SXH_{user_don_vi.ten_don_vi}_{year}_{period_name_part}.xlsx"},
                {'func': generate_odich_tcm_report, 'args': (db_session, calendar_obj, period_number, user_don_vi), 'filename': f"BaoCao_ODich_TCM_{user_don_vi.ten_don_vi}_{year}_{period_name_part}.xlsx"}
            ])
        
        for job in report_jobs:
            try:
                output_path = os.path.join(temp_dir, job['filename'])
                job['func'](*job['args'], output_path)
            except Exception as e:
                print(f"Lỗi khi tạo file '{job['filename']}': {e}")
             
        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(temp_dir):
                for file in files: zipf.write(os.path.join(root, file), arcname=file)
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)