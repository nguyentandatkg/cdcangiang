# file: report_generator.py
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
LIST_BENH_TRUYỀN_NHIỄM = [
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

def _draw_standard_header(worksheet, formats, user_don_vi, report_title, period_subtitle, date_range_subtitle, last_col_letter):
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

def _draw_standard_footer(worksheet, formats, user_don_vi, end_of_period_dt, last_row_idx, last_col_idx):
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
def _draw_outbreak_cases_details_sheet(writer: pd.ExcelWriter, sheet_name: str, outbreaks_in_period: list, column_map: dict, title: str, formats: dict, disease_type: str):
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
    if not df_this_period_all.empty and 'don_vi_ten' in df_this_period_all.columns:
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

def _generate_btn_report_core(
    filepath: str,
    user_don_vi: DonViHanhChinh,
    report_title: str,
    period_name: str,
    date_range_subtitle: str,
    end_of_period_dt: date,
    reporting_units: list[DonViHanhChinh],
    results: dict,
    totals: dict,
    list_cases_for_details_sheet: list,
    period_label: str,
    note_text: str,
    comments: list,
    analysis_data: dict | None # Dùng để vẽ sheet bổ sung
):
    """
    Hàm lõi CHỈ để vẽ file Excel báo cáo BTN từ các dữ liệu đã được tính toán.
    Hàm này không còn thực hiện logic truy vấn hay phân tích.
    """
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # --- Sheet 1: Báo cáo chính ---
        worksheet = workbook.add_worksheet('BaoCaoBTN_TongHop')
        formats = _create_excel_formats(workbook)
        last_col_idx = 1 + 1 + (len(LIST_BENH_TRUYỀN_NHIỄM) * 2) - 1
        _draw_standard_header(worksheet, formats, user_don_vi, report_title, period_name, date_range_subtitle, chr(ord('A') + last_col_idx))
        
        header_start_row = 9
        worksheet.merge_range(header_start_row, 0, header_start_row + 1, 0, 'Địa phương', formats['header'])
        worksheet.merge_range(header_start_row, 1, header_start_row + 1, 1, 'Loại dịch', formats['header'])
        for i, benh in enumerate(LIST_BENH_TRUYỀN_NHIỄM):
            start_col = 2 + i * 2
            worksheet.set_column(start_col, start_col + 1, max(10, len(benh) // 2))
            worksheet.merge_range(header_start_row, start_col, header_start_row, start_col + 1, benh, formats['header'])
            worksheet.write(header_start_row + 1, start_col, 'Mắc', formats['header'])
            worksheet.write(header_start_row + 1, start_col + 1, 'Chết', formats['header'])
        worksheet.set_column('A:A', 25); worksheet.set_column('B:B', 15); worksheet.set_row(9, 50)
        
        current_row = header_start_row + 2
        for unit in reporting_units:
            for i, label in enumerate([period_label, 'BS', 'CD']):
                worksheet.write(current_row + i, 1, label, formats['cell'])
            worksheet.merge_range(current_row, 0, current_row + 2, 0, unit.ten_don_vi, formats['row_header'])
            for j, benh in enumerate(LIST_BENH_TRUYỀN_NHIỄM):
                start_col = 2 + j * 2
                res = results.get(unit.id, {}).get(benh, {})
                worksheet.write(current_row, start_col, res.get('mac_p', 0), formats['cell'])
                worksheet.write(current_row, start_col + 1, res.get('chet_p', 0), formats['cell'])
                worksheet.write(current_row + 1, start_col, res.get('mac_bs', 0), formats['cell'])
                worksheet.write(current_row + 1, start_col + 1, res.get('chet_bs', 0), formats['cell'])
                worksheet.write(current_row + 2, start_col, res.get('mac_cd', 0), formats['cell'])
                worksheet.write(current_row + 2, start_col + 1, res.get('chet_cd', 0), formats['cell'])
            current_row += 3

        worksheet.merge_range(current_row, 0, current_row + 2, 0, 'TỔNG CỘNG', formats['header'])
        for i, label in enumerate([period_label, 'BS', 'CD']):
            worksheet.write(current_row + i, 1, label, formats['total_header'])
        for j, benh in enumerate(LIST_BENH_TRUYỀN_NHIỄM):
            start_col = 2 + j * 2
            worksheet.write(current_row, start_col, totals[benh]['mac_p'], formats['header'])
            worksheet.write(current_row, start_col + 1, totals[benh]['chet_p'], formats['header'])
            worksheet.write(current_row + 1, start_col, totals[benh]['mac_bs'], formats['header'])
            worksheet.write(current_row + 1, start_col + 1, totals[benh]['chet_bs'], formats['header'])
            worksheet.write(current_row + 2, start_col, totals[benh]['mac_cd'], formats['header'])
            worksheet.write(current_row + 2, start_col + 1, totals[benh]['chet_cd'], formats['header'])
        
        footer_base_row = current_row + 3
        worksheet.write(footer_base_row, 0, note_text, formats['ghichu'])
        if comments:
            worksheet.write(footer_base_row + 1, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row = footer_base_row + 2
            comment_format = workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments):
                worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, last_col_idx, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)

        _draw_standard_footer(worksheet, formats, user_don_vi, end_of_period_dt, footer_base_row, last_col_idx)

        # --- Sheet 2: Chi tiết TẤT CẢ ca bệnh (mới + bổ sung) ---
        cabenh_column_map = {'ho_ten': 'Họ và tên', 'ngay_sinh': 'Ngày sinh', 'don_vi.ten_don_vi': 'Xã/Phường', 'dia_chi_ap': 'Ấp/Khu vực', 'dia_chi_chi_tiet': 'Địa chỉ chi tiết', 'chan_doan_chinh': 'Chẩn đoán', 'ngay_khoi_phat': 'Ngày khởi phát', 'tinh_trang_hien_nay': 'Tình trạng'}
        _draw_details_sheet(writer, 'ChiTiet_CaBenh', list_cases_for_details_sheet, cabenh_column_map, f"DANH SÁCH CA BỆNH GHI NHẬN TRONG {period_name.upper()}", formats)

        # --- Sheet 3: Chi tiết ca bổ sung (PHỤC HỒI) ---
        if analysis_data and analysis_data.get('bs_details'):
            worksheet_bs = workbook.add_worksheet('ChiTiet_CaBoSung')
            df_bs_details = pd.DataFrame(analysis_data['bs_details'])
            
            period_col_name = 'Tuần KP' if 'original_period' in df_bs_details.columns and df_bs_details['original_period'].max() > 12 else 'Tháng KP'
            df_bs_details.rename(columns={
                'chan_doan_chinh': 'Tên bệnh', 'original_year': 'Năm KP',
                'original_period': period_col_name, 'count': 'Số ca bổ sung'
            }, inplace=True)

            worksheet_bs.merge_range('A1:D1', f"Chi tiết ca bệnh bổ sung ghi nhận trong {period_name.lower()}", formats['title'])
            df_bs_details.to_excel(writer, sheet_name='ChiTiet_CaBoSung', startrow=2, index=False)
            
            for idx, col in enumerate(df_bs_details.columns):
                series = df_bs_details[col]
                max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
                worksheet_bs.set_column(idx, idx, max_len)

def _generate_benh_truyen_nhiem_report_base(
    db_session: Session,
    user_don_vi: DonViHanhChinh,
    filepath: str,
    year: int,
    period_type: str,
    period_number: int
):
    # ==================================================================
    # GIAI ĐOẠN 1: CHUẨN BỊ TẤT CẢ THAM SỐ VÀ KHOẢNG THỜI GIAN
    # ==================================================================
    start_of_year_dt = date(year, 1, 1)
    
    if period_type == 'week':
        calendar_obj = WeekCalendar(year)
        current_details = calendar_obj.get_week_details(period_number)
        if current_details is None:
            raise ValueError(f"Không tìm thấy tuần {period_number}.")
        
        prev_details = calendar_obj.get_week_details(period_number - 1)
        start_of_period_dt = current_details['ngay_bat_dau'].date()
        end_of_period_dt = current_details['ngay_ket_thuc'].date()

        # Xử lý prev_details an toàn
        prev_period = None
        if prev_details is not None:
            try:
                prev_start = (
                    prev_details['ngay_bat_dau'].iloc[0].date()
                    if hasattr(prev_details['ngay_bat_dau'], "iloc")
                    else prev_details['ngay_bat_dau'].date()
                )
                prev_end = (
                    prev_details['ngay_ket_thuc'].iloc[0].date()
                    if hasattr(prev_details['ngay_ket_thuc'], "iloc")
                    else prev_details['ngay_ket_thuc'].date()
                )
                prev_period = (prev_start, prev_end)
            except Exception:
                prev_period = None

        analysis_periods = {
            "current_period": (start_of_period_dt, end_of_period_dt),
            "prev_period": prev_period,
        }

        comment_details = {
            "period_type": "Tuần",
            "period_number": period_number,
            "prev_period_number": period_number - 1,
            "user_don_vi": user_don_vi,
        }
        period_name = f"Tuần {period_number} năm {year}"
        period_label, note_text = (
            "TS",
            "Ghi chú: TS: Tổng số ca mắc trong tuần; BS: Bổ sung ca mắc; CD: Số ca mắc cộng dồn",
        )

    elif period_type == 'month':
        _, num_days = calendar.monthrange(year, period_number)
        start_of_period_dt, end_of_period_dt = (
            date(year, period_number, 1),
            date(year, period_number, num_days),
        )

        prev_month, prev_month_year = (
            (period_number - 1, year) if period_number > 1 else (12, year - 1)
        )
        _, prev_month_num_days = calendar.monthrange(prev_month_year, prev_month)
        start_of_prev_month, end_of_prev_month = (
            date(prev_month_year, prev_month, 1),
            date(prev_month_year, prev_month, prev_month_num_days),
        )

        analysis_periods = {
            "current_period": (start_of_period_dt, end_of_period_dt),
            "prev_period": (start_of_prev_month, end_of_prev_month),
        }

        comment_details = {
            "period_type": "Tháng",
            "period_number": period_number,
            "prev_period_number": prev_month,
            "user_don_vi": user_don_vi,
        }
        period_name = f"Tháng {period_number} năm {year}"
        period_label, note_text = (
            "TM",
            "Ghi chú: TM: Tổng số ca mắc trong tháng; BS: Bổ sung ca mắc; CD: Số ca mắc cộng dồn",
        )

    else:
        raise ValueError("Loại kỳ báo cáo không hợp lệ.")
        
    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    
    # ==================================================================
    # GIAI ĐOẠN 2: LẤY DỮ LIỆU THÔ VÀ TẠO DATAFRAME
    # ==================================================================
    all_cases = []
    if reporting_units and xa_ids_to_query:
        query_start_date = start_of_year_dt
        if analysis_periods.get('prev_period'):
            query_start_date = min(start_of_year_dt, analysis_periods['prev_period'][0])
            
        query = db_session.query(CaBenh).options(joinedload(CaBenh.don_vi)).filter(
            CaBenh.xa_id.in_(xa_ids_to_query), 
            CaBenh.ngay_khoi_phat >= query_start_date
        )
        all_cases = query.all()

        supplementary_cases = db_session.query(CaBenh).options(joinedload(CaBenh.don_vi)).filter(
            CaBenh.xa_id.in_(xa_ids_to_query),
            CaBenh.ngay_import.between(start_of_period_dt, end_of_period_dt),
            CaBenh.ngay_khoi_phat < start_of_period_dt
        ).all()
        all_cases.extend(supplementary_cases)

    df_raw = pd.DataFrame()
    if all_cases:
        data_list = [
            {
                'ngay_khoi_phat': c.ngay_khoi_phat,
                'chan_doan_chinh': c.chan_doan_chinh,
                'tinh_trang_hien_nay': c.tinh_trang_hien_nay,
                'xa_id': c.xa_id,
                'dia_chi_ap': c.dia_chi_ap,
                'ngay_import': c.ngay_import,
                'don_vi_ten': c.don_vi.ten_don_vi if c.don_vi else ''
            }
            for c in all_cases
        ]
        df_raw = pd.DataFrame(data_list)
        df_raw['ngay_khoi_phat'] = pd.to_datetime(df_raw['ngay_khoi_phat'], errors='coerce')
        df_raw['ngay_import'] = pd.to_datetime(df_raw['ngay_import'], errors='coerce')

    # ==================================================================
    # GIAI ĐOẠN 3: PHÂN TÍCH VÀ TẠO NHẬN XÉT
    # ==================================================================
    analysis_data = _generate_btn_analysis_data(
        db_session,
        user_don_vi,
        df_raw=df_raw.copy(),
        period_type=period_type,
        **analysis_periods
    )
    comments = _generate_btn_comments(analysis_data, **comment_details)
    
    # ==================================================================
    # GIAI ĐOẠN 4: TÍNH TOÁN SỐ LIỆU CHI TIẾT CHO BẢNG
    # ==================================================================
    results, totals = {}, {
        benh: {'mac_p': 0, 'chet_p': 0, 'mac_bs': 0, 'chet_bs': 0, 'mac_cd': 0, 'chet_cd': 0}
        for benh in LIST_BENH_TRUYỀN_NHIỄM
    }
    list_cases_for_details_sheet = []

    if not df_raw.empty:
        df_ytd = df_raw[
            (df_raw['ngay_khoi_phat'] >= pd.to_datetime(start_of_year_dt)) &
            (df_raw['ngay_khoi_phat'] <= pd.to_datetime(end_of_period_dt))
        ]

        df_in_period = df_raw[
            df_raw['ngay_khoi_phat'].between(
                pd.to_datetime(start_of_period_dt),
                pd.to_datetime(end_of_period_dt)
            )
        ]
        df_supplementary = df_raw[
            (df_raw['ngay_import'] >= pd.to_datetime(start_of_period_dt)) &
            (df_raw['ngay_import'] < pd.to_datetime(end_of_period_dt) + timedelta(days=1)) &
            (df_raw['ngay_khoi_phat'] < pd.to_datetime(start_of_period_dt))
        ]
        
        # Lọc ca bệnh cho sheet chi tiết
        for case in all_cases:
            is_in_period = case.ngay_khoi_phat and start_of_period_dt <= case.ngay_khoi_phat <= end_of_period_dt
            is_supplementary = False
            if case.ngay_import and case.ngay_khoi_phat and case.ngay_khoi_phat < start_of_period_dt:
                ngay_import_date = case.ngay_import.date() if isinstance(case.ngay_import, datetime) else case.ngay_import
                if ngay_import_date and start_of_period_dt <= ngay_import_date <= end_of_period_dt:
                    is_supplementary = True
            if is_in_period or is_supplementary:
                list_cases_for_details_sheet.append(case)

        for unit in reporting_units:
            unit_id_or_name = unit.id if group_by_col == 'xa_id' else unit.ten_don_vi
            df_unit_ytd = df_ytd[df_ytd[group_by_col] == unit_id_or_name]
            unit_results = {}
            for benh in LIST_BENH_TRUYỀN_NHIỄM:
                df_benh_ytd = df_unit_ytd[df_unit_ytd['chan_doan_chinh'] == benh]
                df_benh_period = df_benh_ytd[
                    df_benh_ytd['ngay_khoi_phat'].between(
                        pd.to_datetime(start_of_period_dt),
                        pd.to_datetime(end_of_period_dt)
                    )
                ]
                df_import_in_period = df_benh_ytd[
                    (df_benh_ytd['ngay_import'] >= pd.to_datetime(start_of_period_dt)) &
                    (df_benh_ytd['ngay_import'] < pd.to_datetime(end_of_period_dt) + timedelta(days=1))
                ]
                df_benh_bosung = df_import_in_period[
                    df_import_in_period['ngay_khoi_phat'] < pd.to_datetime(start_of_period_dt)
                ]

                unit_results[benh] = {
                    'mac_p': len(df_benh_period),
                    'chet_p': len(df_benh_period[df_benh_period['tinh_trang_hien_nay'] == 'Tử vong']),
                    'mac_bs': len(df_benh_bosung),
                    'chet_bs': len(df_benh_bosung[df_benh_bosung['tinh_trang_hien_nay'] == 'Tử vong']),
                    'mac_cd': len(df_benh_ytd),
                    'chet_cd': len(df_benh_ytd[df_benh_ytd['tinh_trang_hien_nay'] == 'Tử vong'])
                }
                for key in totals[benh]:
                    totals[benh][key] += unit_results[benh][key]
            results[unit.id] = unit_results

    # ==================================================================
    # GIAI ĐOẠN 5: KẾT XUẤT RA EXCEL
    # ==================================================================
    _generate_btn_report_core(
        filepath=filepath,
        user_don_vi=user_don_vi,
        report_title='BÁO CÁO BỆNH TRUYỀN NHIỄM',
        period_name=period_name,
        date_range_subtitle=f"Từ ngày {start_of_period_dt.strftime('%d/%m/%Y')} đến ngày {end_of_period_dt.strftime('%d/%m/%Y')}",
        end_of_period_dt=end_of_period_dt,
        reporting_units=reporting_units,
        results=results,
        totals=totals,
        list_cases_for_details_sheet=list_cases_for_details_sheet,
        period_label=period_label,
        note_text=note_text,
        comments=comments,
        analysis_data=analysis_data
    )


def generate_benh_truyen_nhiem_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    """Hàm public để tạo báo cáo BTN theo tuần."""
    _generate_benh_truyen_nhiem_report_base(
        db_session, 
        user_don_vi, 
        filepath, 
        year=calendar_obj.year, 
        period_type='week', 
        period_number=week_number
    )

# Hàm generate_benh_truyen_nhiem_report_monthly của bạn sẽ nằm ngay sau đây...
def generate_benh_truyen_nhiem_report_monthly(db_session: Session, year: int, month: int, user_don_vi: DonViHanhChinh, filepath: str):
    _generate_benh_truyen_nhiem_report_base(db_session, user_don_vi, filepath, year=year, period_type='month', period_number=month)
# ==============================================================================
# 4. BÁO CÁO SỐT XUẤT HUYẾT
# ==============================================================================

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
            if df_raw.empty:
                df_raw = pd.DataFrame(columns=['ngay_khoi_phat', 'ngay_sinh', 'phan_do_benh', 'tinh_trang_hien_nay', 'xa_id', 'dia_chi_ap', 'tuoi'])
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

        # --- Sheet 2: Chi tiết TẤT CẢ ca bệnh (mới + bổ sung) ---
        cabenh_column_map = {'ho_ten': 'Họ và tên', 'ngay_sinh': 'Ngày sinh', 'don_vi.ten_don_vi': 'Xã/Phường', 'dia_chi_ap': 'Ấp/Khu vực', 'dia_chi_chi_tiet': 'Địa chỉ chi tiết', 'chan_doan_chinh': 'Chẩn đoán', 'ngay_khoi_phat': 'Ngày khởi phát', 'tinh_trang_hien_nay': 'Tình trạng'}
        _draw_details_sheet(writer, 'ChiTiet_CaBenh', list_cases_for_details_sheet, cabenh_column_map, f"DANH SÁCH CA BỆNH GHI NHẬN TRONG {period_name.upper()}", formats)

        # --- Sheet 3: Chi tiết ca bổ sung (PHỤC HỒI) ---
        if analysis_data and analysis_data.get('bs_details'):
            worksheet_bs = workbook.add_worksheet('ChiTiet_CaBoSung')
            df_bs_details = pd.DataFrame(analysis_data['bs_details'])
            
            period_col_name = 'Tuần KP' if 'original_period' in df_bs_details.columns and df_bs_details['original_period'].max() > 12 else 'Tháng KP'
            df_bs_details.rename(columns={
                'chan_doan_chinh': 'Tên bệnh', 'original_year': 'Năm KP',
                'original_period': period_col_name, 'count': 'Số ca bổ sung'
            }, inplace=True)

            worksheet_bs.merge_range('A1:D1', f"Chi tiết ca bệnh bổ sung ghi nhận trong {period_name.lower()}", formats['title'])
            df_bs_details.to_excel(writer, sheet_name='ChiTiet_CaBoSung', startrow=2, index=False)
            
            for idx, col in enumerate(df_bs_details.columns):
                series = df_bs_details[col]
                max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
                worksheet_bs.set_column(idx, idx, max_len)

def generate_sxh_report(db_session, calendar_obj, week_number, user_don_vi, filepath):
    """
    Hàm public để tạo báo cáo SXH theo tuần.
    """
    year = calendar_obj.year
    # Lấy thông tin tuần
    week_details = calendar_obj.get_week_details(week_number)
    if not week_details:
        raise ValueError(f"Không tìm thấy tuần {week_number}.")
    start_of_period_dt = week_details['ngay_bat_dau'].date()
    end_of_period_dt = week_details['ngay_ket_thuc'].date()
    start_of_year_dt = date(year, 1, 1)

    # Tuần trước
    prev_week_details = calendar_obj.get_week_details(week_number - 1)
    prev_period = None
    if prev_week_details:
        prev_start = prev_week_details['ngay_bat_dau'].date()
        prev_end = prev_week_details['ngay_ket_thuc'].date()
        prev_period = (prev_start, prev_end)

    # Cộng dồn năm nay và năm trước
    cumulative_this_year = (start_of_year_dt, end_of_period_dt)
    cumulative_last_year = (date(year - 1, 1, 1), date(year - 1, 12, 31))

    analysis_periods = {
        "current_period": (start_of_period_dt, end_of_period_dt),
        "prev_period": prev_period,
        "cumulative_this_year": cumulative_this_year,
        "cumulative_last_year": cumulative_last_year,
    }
    comment_details = {
        "period_type": "Tuần",
        "period_number": week_number,
        "prev_period_number": week_number - 1,
        "year": year,
        "end_of_period_dt": end_of_period_dt,
    }
    period_name = f"Tuần {week_number} năm {year}"

    _generate_sxh_report_base(
        db_session=db_session,
        start_of_year_dt=start_of_year_dt,
        end_of_period_dt=end_of_period_dt,
        start_of_period_dt=start_of_period_dt,
        user_don_vi=user_don_vi,
        filepath=filepath,
        period_name=period_name,
        year=year,
        analysis_periods=analysis_periods,
        comment_details=comment_details
    )