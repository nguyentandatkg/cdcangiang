# file: report_generator.py
import pandas as pd
import calendar
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text, func
import os
import uuid
import shutil
import zipfile
import re

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
def _execute_sql_to_df(db_session: Session, sql_query: str, params: dict = None) -> pd.DataFrame:
    """Thực thi SQL và trả về DataFrame một cách an toàn."""
    return pd.read_sql_query(text(sql_query), db_session.bind, params=params)

def _get_formatted_unit_name(user_don_vi: DonViHanhChinh) -> str:
    cap_don_vi, ten_don_vi = user_don_vi.cap_don_vi, user_don_vi.ten_don_vi
    if cap_don_vi == 'Tỉnh': return ten_don_vi.upper()
    if cap_don_vi == 'Khu vực':
        prefixes_to_remove = ["TTYT Khu vực", "Trung tâm y tế Khu vực", "Khu vực TTYT", "TTYT", "Khu vực"]
        for prefix in prefixes_to_remove:
            if ten_don_vi.lower().startswith(prefix.lower()):
                ten_don_vi = ten_don_vi[len(prefix):].strip()
                break
        return f"TRUNG TÂM Y TẾ {ten_don_vi}".upper()
    if cap_don_vi == 'Xã': return f"TRẠM Y TẾ {ten_don_vi}".upper()
    return ten_don_vi.upper()

def _get_reporting_units(db_session: Session, user_don_vi: DonViHanhChinh):
    reporting_units, group_by_col = [], None
    child_level_map = {'Xã': 'Ấp', 'Khu vực': 'Xã'}
    if user_don_vi.cap_don_vi in child_level_map:
        child_level = child_level_map[user_don_vi.cap_don_vi]
        reporting_units = sorted([c for c in user_don_vi.children if c.cap_don_vi == child_level], key=lambda x: x.ten_don_vi)
        group_by_col = 'dia_chi_ap' if child_level == 'Ấp' else 'xa_id'
    elif user_don_vi.cap_don_vi == 'Tỉnh':
        reporting_units = db_session.query(DonViHanhChinh).filter(DonViHanhChinh.cap_don_vi == 'Xã').order_by(DonViHanhChinh.ten_don_vi).all()
        group_by_col = 'xa_id'
    return reporting_units, group_by_col
    
def _get_reporting_logic(db_session: Session, user_don_vi: DonViHanhChinh):
    """
    **SỬA LỖI LOGIC:** Trả về đúng cấp đơn vị báo cáo và các chuỗi SQL cần thiết.
    - Tỉnh -> danh sách Xã
    - Khu vực -> danh sách Xã
    - Xã -> danh sách Ấp
    """
    reporting_units = []
    # Mặc định cho Tỉnh và Khu vực: nhóm theo xã
    group_by_sql_col = "dvh_xa.id"
    join_sql = "" # Không cần join thêm vì đã có dvh_xa
    unit_id_map_key = 'id'

    if user_don_vi.cap_don_vi == 'Tỉnh':
        reporting_units = db_session.query(DonViHanhChinh).filter(DonViHanhChinh.cap_don_vi == 'Xã').order_by(DonViHanhChinh.ten_don_vi).all()
    elif user_don_vi.cap_don_vi == 'Khu vực':
        child_xa_ids = [c.id for c in user_don_vi.children if c.cap_don_vi == 'Xã']
        reporting_units = db_session.query(DonViHanhChinh).filter(DonViHanhChinh.id.in_(child_xa_ids)).order_by(DonViHanhChinh.ten_don_vi).all()
    elif user_don_vi.cap_don_vi == 'Xã':
        reporting_units = db_session.query(DonViHanhChinh).filter(DonViHanhChinh.parent_id == user_don_vi.id, DonViHanhChinh.cap_don_vi == 'Ấp').order_by(DonViHanhChinh.ten_don_vi).all()
        group_by_sql_col = "cb.dia_chi_ap" # Nhóm theo tên ấp (text)
        unit_id_map_key = 'ten_don_vi'

    return reporting_units, join_sql, group_by_sql_col, unit_id_map_key

def _create_excel_formats(workbook):
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
    }

def _draw_standard_header(worksheet, formats, user_don_vi, report_title, period_subtitle, date_range_subtitle, last_col_letter):
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

def _draw_details_sheet(writer: pd.ExcelWriter, sheet_name: str, data_objects: list, column_map: dict, title: str, formats: dict):
    if not data_objects: return
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
        max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
        worksheet.set_column(idx, idx, max_len)

# (Thêm hàm này vào cuối mục 2. CÁC HÀM TRỢ GIÚP)

def _generate_custom_btn_report_core(
    filepath: str, user_don_vi: DonViHanhChinh, start_date: date, end_date: date,
    reporting_units: list[DonViHanhChinh], df_results: pd.DataFrame,
    dynamic_disease_list: list[str], list_cases_for_details_sheet: list
):
    """Vẽ file Excel cho báo cáo BTN tùy chỉnh với cấu trúc đơn giản."""
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet('BaoCaoTuyChinh_BTN')
        formats = _create_excel_formats(workbook)

        # Tính toán cột cuối cùng
        last_col_idx = 1 + 1 + (len(dynamic_disease_list) * 2) + 2 - 1
        last_col_letter = chr(ord('A') + last_col_idx)

        # Vẽ header chuẩn
        report_title = 'BÁO CÁO TÙY CHỈNH BỆNH TRUYỀN NHIỄM'
        period_subtitle = f"Thời gian từ {start_date.strftime('%d/%m/%Y')} đến {end_date.strftime('%d/%m/%Y')}"
        _draw_standard_header(worksheet, formats, user_don_vi, report_title, period_subtitle, None, last_col_letter)

        # Vẽ header của bảng dữ liệu
        h_row = 9
        worksheet.set_column('A:A', 5)
        worksheet.set_column('B:B', 25)
        worksheet.merge_range(h_row, 0, h_row + 1, 0, 'STT', formats['header'])
        worksheet.merge_range(h_row, 1, h_row + 1, 1, 'Địa phương', formats['header'])

        col_offset = 2
        for i, benh in enumerate(dynamic_disease_list):
            start_col = col_offset + i * 2
            worksheet.set_column(start_col, start_col + 1, 10)
            worksheet.merge_range(h_row, start_col, h_row, start_col + 1, benh, formats['header'])
            worksheet.write(h_row + 1, start_col, 'Mắc', formats['header'])
            worksheet.write(h_row + 1, start_col + 1, 'Chết', formats['header'])

        # Cột tổng cộng
        total_start_col = col_offset + len(dynamic_disease_list) * 2
        worksheet.merge_range(h_row, total_start_col, h_row, total_start_col + 1, 'Tổng cộng', formats['header'])
        worksheet.write(h_row + 1, total_start_col, 'Mắc', formats['header'])
        worksheet.write(h_row + 1, total_start_col + 1, 'Chết', formats['header'])

        # Đổ dữ liệu
        data_start_row = h_row + 2
        if not df_results.empty:
            df_results = df_results.set_index('unit_id')
        
        for i, unit in enumerate(reporting_units):
            row_idx = data_start_row + i
            worksheet.write(row_idx, 0, i + 1, formats['cell'])
            worksheet.write(row_idx, 1, unit.ten_don_vi, formats['row_header'])
            
            row_data = df_results.loc[unit.id] if unit.id in df_results.index else pd.Series(dtype='float64')

            for j, benh in enumerate(dynamic_disease_list):
                start_col = col_offset + j * 2
                worksheet.write(row_idx, start_col, int(row_data.get(f"{benh}_mac", 0)), formats['cell'])
                worksheet.write(row_idx, start_col + 1, int(row_data.get(f"{benh}_chet", 0)), formats['cell'])
            
            # Ghi dữ liệu cột tổng cộng
            worksheet.write(row_idx, total_start_col, int(row_data.get("Tổng_mac", 0)), formats['cell'])
            worksheet.write(row_idx, total_start_col + 1, int(row_data.get("Tổng_chet", 0)), formats['cell'])

        # Dòng tổng cộng cuối bảng
        total_row_idx = data_start_row + len(reporting_units)
        worksheet.merge_range(total_row_idx, 0, total_row_idx, 1, 'TỔNG CỘNG', formats['header'])
        for i in range(col_offset, total_start_col + 2):
            col_letter = chr(ord('A') + i)
            formula = f'=SUM({col_letter}{data_start_row + 1}:{col_letter}{total_row_idx})'
            worksheet.write_formula(total_row_idx, i, formula, formats['header'])
        
        # Footer
        footer_base_row = total_row_idx + 2
        chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC'}
        date_line_row, title_line_row = footer_base_row, footer_base_row + 1
        sign_col_start_letter = chr(ord('A') + max(1, last_col_idx - 5))

        worksheet.merge_range(f'{sign_col_start_letter}{date_line_row}:{last_col_letter}{date_line_row}', f"{user_don_vi.ten_don_vi}, ngày {end_date.day} tháng {end_date.month} năm {end_date.year}", formats['italic'])
        worksheet.merge_range(f'{sign_col_start_letter}{title_line_row}:{last_col_letter}{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['org_header'])
        
        # Trang chi tiết ca bệnh
        cabenh_column_map = {'ho_ten': 'Họ và tên', 'ngay_sinh': 'Ngày sinh', 'don_vi.ten_don_vi': 'Xã/Phường', 'dia_chi_ap': 'Ấp/Khu vực', 'chan_doan_chinh': 'Chẩn đoán', 'ngay_khoi_phat': 'Ngày khởi phát', 'tinh_trang_hien_nay': 'Tình trạng'}
        _draw_details_sheet(writer, 'ChiTiet_CaBenh', list_cases_for_details_sheet, cabenh_column_map, f"DANH SÁCH CA BỆNH GHI NHẬN", formats)


# ==============================================================================
# 3. BÁO CÁO BỆNH TRUYỀN NHIỄM
# ==============================================================================
def _generate_btn_analysis_data(db_session: Session, user_don_vi: DonViHanhChinh, current_period: tuple, prev_period: tuple, period_type: str, **kwargs):
    analysis = {}
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return None

    sql_summary_query = """
    SELECT 
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :current_start AND :current_end) as total_ts,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :current_start AND :current_end AND tinh_trang_hien_nay = 'Tử vong') as deaths_ts,
        COUNT(*) FILTER (WHERE ngay_import BETWEEN :current_start AND :current_end AND ngay_khoi_phat < :current_start) as total_bs,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :prev_start AND :prev_end) as total_prev
    FROM ca_benh WHERE xa_id IN :xa_ids;
    """
    params_summary = {
        "xa_ids": tuple(xa_ids_to_query),
        "current_start": current_period[0], "current_end": current_period[1],
        "prev_start": prev_period[0] if prev_period else date(1900, 1, 1),
        "prev_end": prev_period[1] if prev_period else date(1900, 1, 1),
    }
    summary = _execute_sql_to_df(db_session, sql_summary_query, params_summary).iloc[0]
    analysis.update(summary.to_dict())

    top_diseases_sql = """
        SELECT chan_doan_chinh, COUNT(id) as so_ca FROM ca_benh 
        WHERE xa_id IN :xa_ids AND ngay_khoi_phat BETWEEN :start AND :end
        GROUP BY chan_doan_chinh ORDER BY so_ca DESC LIMIT 3;
    """
    df_top_diseases = _execute_sql_to_df(db_session, top_diseases_sql, {"xa_ids": tuple(xa_ids_to_query), "start": current_period[0], "end": current_period[1]})
    analysis['top_diseases'] = pd.Series(df_top_diseases.so_ca.values, index=df_top_diseases.chan_doan_chinh).to_dict()

    top_loc_sql = """
        SELECT dv.ten_don_vi, COUNT(cb.id) as so_ca, mode() WITHIN GROUP (ORDER BY cb.chan_doan_chinh) as top_disease
        FROM ca_benh cb JOIN don_vi_hanh_chinh dv ON cb.xa_id = dv.id
        WHERE cb.xa_id IN :xa_ids AND (
            (cb.ngay_khoi_phat BETWEEN :start AND :end) OR
            (cb.ngay_import BETWEEN :start AND :end AND cb.ngay_khoi_phat < :start)
        ) GROUP BY dv.ten_don_vi ORDER BY so_ca DESC LIMIT 1;
    """
    df_top_loc = _execute_sql_to_df(db_session, top_loc_sql, {"xa_ids": tuple(xa_ids_to_query), "start": current_period[0], "end": current_period[1]})
    if not df_top_loc.empty:
        top_loc = df_top_loc.iloc[0]
        analysis['top_location'] = {'name': top_loc['ten_don_vi'], 'count': int(top_loc['so_ca']), 'disease': top_loc['top_disease']}
    else:
        analysis['top_location'] = None

    if period_type == 'week':
        sql_bs_details = """
        SELECT 
            chan_doan_chinh, 
            EXTRACT(isoyear FROM ngay_khoi_phat) as original_year,
            EXTRACT(week FROM ngay_khoi_phat) as original_period,
            COUNT(id) as count
        FROM ca_benh
        WHERE xa_id IN :xa_ids AND ngay_import BETWEEN :start AND :end AND ngay_khoi_phat < :start
        GROUP BY chan_doan_chinh, original_year, original_period
        ORDER BY count DESC;
        """
    else: # month
        sql_bs_details = """
        SELECT 
            chan_doan_chinh, 
            EXTRACT(year FROM ngay_khoi_phat) as original_year,
            EXTRACT(month FROM ngay_khoi_phat) as original_period,
            COUNT(id) as count
        FROM ca_benh
        WHERE xa_id IN :xa_ids AND ngay_import BETWEEN :start AND :end AND ngay_khoi_phat < :start
        GROUP BY chan_doan_chinh, original_year, original_period
        ORDER BY count DESC;
        """
    params_bs_details = {
        "xa_ids": tuple(xa_ids_to_query),
        "start": current_period[0],
        "end": current_period[1]
    }
    df_bs_details = _execute_sql_to_df(db_session, sql_bs_details, params_bs_details)
    analysis['bs_details'] = df_bs_details.to_dict('records')
    
    return analysis

def _generate_btn_comments(analysis: dict, period_type: str, period_number: int, prev_period_number: int, user_don_vi: DonViHanhChinh):
    if not analysis: return ["- Không có dữ liệu để tạo nhận xét."]
    comments = []
    period_type_lower = "tuần" if period_type == 'week' else "tháng"
    display_don_vi_ten = user_don_vi.ten_don_vi
    if user_don_vi.cap_don_vi == 'Tỉnh': display_don_vi_ten = "tỉnh An Giang"
    elif user_don_vi.cap_don_vi == 'Khu vực': display_don_vi_ten = user_don_vi.ten_don_vi.replace("Trung tâm Y tế", "").strip()
    comments.append(f"- Trong {period_type_lower} {period_number}, toàn {display_don_vi_ten} ghi nhận {analysis.get('total_ts', 0)} ca mắc mới và {analysis.get('deaths_ts', 0)} ca tử vong. Ngoài ra, đã ghi nhận bổ sung {analysis.get('total_bs', 0)} ca mắc từ các kỳ trước.")
    if analysis.get('top_diseases'): comments.append(f"- 03 bệnh có số ca mắc mới cao nhất trong kỳ là: {', '.join([f'{name} ({count} ca)' for name, count in analysis['top_diseases'].items()])}.")
    diff_vs_prev = analysis.get('total_ts', 0) - analysis.get('total_prev', 0)
    comparison_text = "ổn định"
    if diff_vs_prev > 0: comparison_text = f"tăng ({analysis.get('total_ts', 0)} so với {analysis.get('total_prev', 0)} ca)"
    elif diff_vs_prev < 0: comparison_text = f"giảm ({abs(diff_vs_prev)} so với {analysis.get('total_prev', 0)} ca)"
    comments.append(f"- So với {period_type_lower} trước, số ca mắc mới thực tế có xu hướng {comparison_text}.")
    if analysis.get('total_bs', 0) > 0: comments.append(f"- Việc ghi nhận {analysis.get('total_bs', 0)} ca báo cáo bổ sung cho thấy có sự chậm trễ trong công tác giám sát, báo cáo tại tuyến dưới.")
    if analysis.get('top_location'): comments.append(f"- {analysis['top_location']['name']} là địa phương có tổng số ca ghi nhận (cả mới và bổ sung) cao nhất trong kỳ với {analysis['top_location']['count']} ca, chủ yếu là bệnh {analysis['top_location']['disease']}.")
    return comments

def _generate_btn_report_core(
    filepath: str, user_don_vi: DonViHanhChinh, report_title: str, period_name: str,
    date_range_subtitle: str, end_of_period_dt: date, reporting_units: list[DonViHanhChinh],
    df_results: pd.DataFrame, list_cases_for_details_sheet: list, period_label: str, note_text: str, comments: list, analysis_data: dict | None,
    unit_id_map_key: str = 'id'
):
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet('BaoCaoBTN_TongHop')
        formats = _create_excel_formats(workbook)
        last_col_idx = 1 + 1 + (len(LIST_BENH_TRUYEN_NHIEM) * 2) - 1
        last_col_letter = chr(ord('A') + last_col_idx)
        _draw_standard_header(worksheet, formats, user_don_vi, report_title, period_name, date_range_subtitle, last_col_letter)
        
        header_start_row = 9
        worksheet.merge_range(header_start_row, 0, header_start_row + 1, 0, 'Địa phương', formats['header'])
        worksheet.merge_range(header_start_row, 1, header_start_row + 1, 1, 'Loại dịch', formats['header'])
        for i, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
            start_col = 2 + i * 2
            worksheet.set_column(start_col, start_col + 1, max(10, len(benh) // 2))
            worksheet.merge_range(header_start_row, start_col, header_start_row, start_col + 1, benh, formats['header'])
            worksheet.write(header_start_row + 1, start_col, 'Mắc', formats['header'])
            worksheet.write(header_start_row + 1, start_col + 1, 'Chết', formats['header'])
        worksheet.set_column('A:A', 25); worksheet.set_column('B:B', 15); worksheet.set_row(9, 50)
        
        current_row = header_start_row + 2
        
        df_results_filled = df_results.fillna(0)
        if not df_results_filled.empty:
             df_results_filled = df_results_filled.set_index(['unit_id', 'label'])
        
        df_totals = df_results.drop(columns=['unit_id']).groupby('label').sum() if not df_results.empty else pd.DataFrame()

        for unit in reporting_units:
            unit_key = getattr(unit, unit_id_map_key)
            worksheet.merge_range(current_row, 0, current_row + 2, 0, unit.ten_don_vi, formats['row_header'])
            for i, (label_code, label_display) in enumerate([('p', period_label), ('bs', 'BS'), ('cd', 'CD')]):
                worksheet.write(current_row + i, 1, label_display, formats['cell'])
                if not df_results_filled.empty and (unit_key, label_code) in df_results_filled.index:
                    row_data = df_results_filled.loc[(unit_key, label_code)]
                    for j, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
                        start_col = 2 + j * 2
                        worksheet.write(current_row + i, start_col, int(row_data.get(f"{benh}_mac", 0)), formats['cell'])
                        worksheet.write(current_row + i, start_col + 1, int(row_data.get(f"{benh}_chet", 0)), formats['cell'])
                else:
                    for j, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
                        start_col = 2 + j * 2
                        worksheet.write(current_row + i, start_col, 0, formats['cell'])
                        worksheet.write(current_row + i, start_col + 1, 0, formats['cell'])
            current_row += 3
        
        worksheet.merge_range(current_row, 0, current_row + 2, 0, 'TỔNG CỘNG', formats['header'])
        for i, (label_code, label_display) in enumerate([('p', period_label), ('bs', 'BS'), ('cd', 'CD')]):
            worksheet.write(current_row + i, 1, label_display, formats['total_header'])
            if label_code in df_totals.index:
                total_row_data = df_totals.loc[label_code]
                for j, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
                    start_col = 2 + j * 2
                    worksheet.write(current_row + i, start_col, int(total_row_data.get(f"{benh}_mac", 0)), formats['header'])
                    worksheet.write(current_row + i, start_col + 1, int(total_row_data.get(f"{benh}_chet", 0)), formats['header'])
            else:
                 for j, benh in enumerate(LIST_BENH_TRUYEN_NHIEM):
                    start_col = 2 + j * 2
                    worksheet.write(current_row + i, start_col, 0, formats['header'])
                    worksheet.write(current_row + i, start_col + 1, 0, formats['header'])

        footer_base_row = current_row + 3
        worksheet.write(footer_base_row, 0, note_text, formats['ghichu'])
        if comments:
            worksheet.write(footer_base_row + 1, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row, comment_format = footer_base_row + 2, workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, last_col_idx, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)

        # Footer riêng cho báo cáo BTN
        chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC', 'Xã': 'TRƯỞNG TRẠM'}
        date_line_row, title_line_row, recipient_line_row = footer_base_row + 2, footer_base_row + 3, footer_base_row + 9
        sign_col_start_letter = chr(ord('A') + max(1, last_col_idx - 7))
        worksheet.merge_range(f'{sign_col_start_letter}{date_line_row}:{last_col_letter}{date_line_row}', f"{user_don_vi.ten_don_vi}, ngày {end_of_period_dt.day} tháng {end_of_period_dt.month} năm {end_of_period_dt.year}", formats['italic'])
        worksheet.merge_range(f'{sign_col_start_letter}{title_line_row}:{last_col_letter}{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['org_header'])
        worksheet.merge_range(f'A{title_line_row}:D{title_line_row}', 'NGƯỜI BÁO CÁO', formats['org_header'])
        worksheet.merge_range(f'A{recipient_line_row}:D{recipient_line_row}', 'Nơi nhận:', formats['noinhan'])

        cabenh_column_map = {'ho_ten': 'Họ và tên', 'ngay_sinh': 'Ngày sinh', 'don_vi.ten_don_vi': 'Xã/Phường', 'dia_chi_ap': 'Ấp/Khu vực', 'dia_chi_chi_tiet': 'Địa chỉ chi tiết', 'chan_doan_chinh': 'Chẩn đoán', 'ngay_khoi_phat': 'Ngày khởi phát', 'tinh_trang_hien_nay': 'Tình trạng'}
        _draw_details_sheet(writer, 'ChiTiet_CaBenh', list_cases_for_details_sheet, cabenh_column_map, f"DANH SÁCH CA BỆNH GHI NHẬN TRONG {period_name.upper()}", formats)

        if analysis_data and analysis_data.get('bs_details'):
            worksheet_bs = workbook.add_worksheet('ChiTiet_CaBoSung')
            df_bs_details = pd.DataFrame(analysis_data['bs_details'])
            
            period_col_name = 'Tuần KP' if period_name.lower().startswith('tuần') else 'Tháng KP'
            
            df_bs_details.rename(columns={
                'chan_doan_chinh': 'Tên bệnh', 
                'original_year': 'Năm KP',
                'original_period': period_col_name, 
                'count': 'Số ca bổ sung'
            }, inplace=True)

            # Chuyển đổi kiểu dữ liệu để định dạng đúng
            df_bs_details['Năm KP'] = df_bs_details['Năm KP'].astype(int)
            df_bs_details[period_col_name] = df_bs_details[period_col_name].astype(int)
            
            worksheet_bs.merge_range('A1:D1', f"Chi tiết ca bệnh bổ sung ghi nhận trong {period_name.lower()}", formats['title'])
            df_bs_details.to_excel(writer, sheet_name='ChiTiet_CaBoSung', startrow=2, index=False)
            
            for idx, col in enumerate(df_bs_details.columns):
                series = df_bs_details[col]
                max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
                worksheet_bs.set_column(idx, idx, max_len)



def _generate_benh_truyen_nhiem_report_base(
    db_session: Session, user_don_vi: DonViHanhChinh, filepath: str,
    year: int, period_type: str, period_number: int
):
    # GIAI ĐOẠN 1: CHUẨN BỊ THAM SỐ
    start_of_year_dt = date(year, 1, 1)
    
    if period_type == 'week':
        calendar_obj = WeekCalendar(year)
        current_details = calendar_obj.get_week_details(period_number)
        prev_details = calendar_obj.get_week_details(period_number - 1)
        
        if current_details is None:
            raise ValueError(f"Không tìm thấy tuần {period_number}.")
            
        start_of_period_dt, end_of_period_dt = current_details['ngay_bat_dau'].date(), current_details['ngay_ket_thuc'].date()
        prev_period = (prev_details['ngay_bat_dau'].date(), prev_details['ngay_ket_thuc'].date()) if prev_details is not None else None
        
        period_name = f"Tuần {period_number} năm {year}"
        period_label = "TS"
        note_text = "Ghi chú: TS: Tổng số ca mắc trong tuần; BS: Bổ sung ca mắc; CD: Số ca mắc cộng dồn"
        
    elif period_type == 'month':
        _, num_days = calendar.monthrange(year, period_number)
        start_of_period_dt, end_of_period_dt = date(year, period_number, 1), date(year, period_number, num_days)
        
        prev_month, prev_year = (period_number - 1, year) if period_number > 1 else (12, year - 1)
        _, prev_num_days = calendar.monthrange(prev_year, prev_month)
        prev_period = (date(prev_year, prev_month, 1), date(prev_year, prev_month, prev_num_days))
        
        period_name = f"Tháng {period_number} năm {year}"
        period_label = "TM"
        note_text = "Ghi chú: TM: Tổng số ca mắc trong tháng; BS: Bổ sung ca mắc; CD: Số ca mắc cộng dồn"
        
    else:
        raise ValueError("Loại kỳ báo cáo không hợp lệ.")

    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    if not reporting_units:
        return
        
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query:
        return

    # GIAI ĐOẠN 2: TÍNH TOÁN DỮ LIỆU
    is_group_by_ap = (user_don_vi.cap_don_vi == 'Xã')
    group_by_unit_id = "dv.id" if not is_group_by_ap else "cb.dia_chi_ap"  # Sửa lỗi group by ấp
    unit_name_col = "dvh_unit.ten_don_vi"
    if is_group_by_ap:
        unit_name_col = "cb.dia_chi_ap"
    unit_id_mapping = {unit.id: unit.ten_don_vi for unit in reporting_units}

    mac_cases = "\n".join([f", COUNT(*) FILTER (WHERE cb.chan_doan_chinh = '{benh}') AS \"{benh}_mac\"" for benh in LIST_BENH_TRUYEN_NHIEM])
    chet_cases = "\n".join([f", COUNT(*) FILTER (WHERE cb.chan_doan_chinh = '{benh}' AND cb.tinh_trang_hien_nay = 'Tử vong') AS \"{benh}_chet\"" for benh in LIST_BENH_TRUYEN_NHIEM])

    sql_query = f"""
    WITH base_query AS (
        SELECT 'p' as label, {group_by_unit_id} as unit_id {mac_cases} {chet_cases}
        FROM ca_benh cb JOIN don_vi_hanh_chinh dv ON cb.xa_id = dv.id
        WHERE cb.xa_id IN :xa_ids AND cb.ngay_khoi_phat BETWEEN :start_of_period AND :end_of_period
        GROUP BY unit_id
        UNION ALL
        SELECT 'bs' as label, {group_by_unit_id} as unit_id {mac_cases} {chet_cases}
        FROM ca_benh cb JOIN don_vi_hanh_chinh dv ON cb.xa_id = dv.id
        WHERE cb.xa_id IN :xa_ids AND cb.ngay_import BETWEEN :start_of_period AND :end_of_period AND cb.ngay_khoi_phat < :start_of_period
        GROUP BY unit_id
        UNION ALL
        SELECT 'cd' as label, {group_by_unit_id} as unit_id {mac_cases} {chet_cases}
        FROM ca_benh cb JOIN don_vi_hanh_chinh dv ON cb.xa_id = dv.id
        WHERE cb.xa_id IN :xa_ids AND cb.ngay_khoi_phat >= :start_of_year AND cb.ngay_khoi_phat <= :end_of_period
        GROUP BY unit_id
    )
    SELECT * FROM base_query WHERE unit_id IS NOT NULL;
    """
    params = {
        "xa_ids": tuple(xa_ids_to_query),
        "start_of_period": start_of_period_dt,
        "end_of_period": end_of_period_dt,
        "start_of_year": start_of_year_dt
    }
    df_results = _execute_sql_to_df(db_session, sql_query, params)

    # Ánh xạ ID sang Tên cho cấp Khu vực và Tỉnh
    if not is_group_by_ap and not df_results.empty:
        don_vi_map = {d.id: d.ten_don_vi for d in db_session.query(DonViHanhChinh).filter(DonViHanhChinh.id.in_(df_results['unit_id'].unique().tolist())).all()}
        df_results['unit_name'] = df_results['unit_id'].map(don_vi_map)

    analysis_periods = {"current_period": (start_of_period_dt, end_of_period_dt), "prev_period": prev_period}
    comment_details = {
        "period_type": period_type,
        "period_number": period_number,
        "prev_period_number": (period_number - 1 if period_type == 'week' else prev_month),
        "user_don_vi": user_don_vi
    }
    analysis_data = _generate_btn_analysis_data(db_session, user_don_vi, period_type=period_type, **analysis_periods)
    comments = _generate_btn_comments(analysis_data, **comment_details)

    list_cases_for_details_sheet = (
        db_session.query(CaBenh)
        .options(joinedload(CaBenh.don_vi))
        .filter(
            CaBenh.xa_id.in_(xa_ids_to_query),
            (
                (CaBenh.ngay_khoi_phat.between(start_of_period_dt, end_of_period_dt)) |
                ((CaBenh.ngay_import.between(start_of_period_dt, end_of_period_dt)) & (CaBenh.ngay_khoi_phat < start_of_period_dt))
            )
        )
        .all()
    )

    # GIAI ĐOẠN 3: KẾT XUẤT
    _generate_btn_report_core(
        filepath=filepath,
        user_don_vi=user_don_vi,
        report_title='BÁO CÁO BỆNH TRUYỀN NHIỄM',
        period_name=period_name,
        date_range_subtitle=f"Từ ngày {start_of_period_dt.strftime('%d/%m/%Y')} đến ngày {end_of_period_dt.strftime('%d/%m/%Y')}",
        end_of_period_dt=end_of_period_dt,
        reporting_units=reporting_units,
        df_results=df_results,
        list_cases_for_details_sheet=list_cases_for_details_sheet,
        period_label=period_label,
        note_text=note_text,
        comments=comments,
        analysis_data=analysis_data
    )


def generate_benh_truyen_nhiem_report(
    db_session: Session, calendar_obj: WeekCalendar, week_number: int,
    user_don_vi: DonViHanhChinh, filepath: str
):
    _generate_benh_truyen_nhiem_report_base(
        db_session, user_don_vi, filepath,
        year=calendar_obj.year, period_type='week', period_number=week_number
    )


def generate_benh_truyen_nhiem_report_monthly(
    db_session: Session, year: int, month: int,
    user_don_vi: DonViHanhChinh, filepath: str
):
    _generate_benh_truyen_nhiem_report_base(
        db_session, user_don_vi, filepath,
        year=year, period_type='month', period_number=month
    )

# ==============================================================================
# 4. BÁO CÁO SỐT XUẤT HUYẾT
# ==============================================================================
def _generate_sxh_analysis_data(db_session: Session, user_don_vi: DonViHanhChinh, current_period: tuple, prev_period: tuple, cumulative_this_year: tuple, cumulative_last_year: tuple):
    data = {}
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return None
    sql_query = """
    SELECT
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :c_start AND :c_end) as total_this_period,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :c_start AND :c_end AND phan_do_benh != 'Sốt xuất huyết Dengue nặng') as warning_this_period,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :c_start AND :c_end AND phan_do_benh = 'Sốt xuất huyết Dengue nặng') as severe_this_period,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :c_start AND :c_end AND tinh_trang_hien_nay = 'Tử vong') as deaths_this_period,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :p_start AND :p_end) as total_prev_period,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :cty_start AND :cty_end) as cumulative_this_year,
        COUNT(*) FILTER (WHERE ngay_khoi_phat BETWEEN :cly_start AND :cly_end) as cumulative_last_year
    FROM ca_benh WHERE xa_id IN :xa_ids AND chan_doan_chinh LIKE '%%Sốt xuất huyết%%'
    """
    params = {
        "xa_ids": tuple(xa_ids_to_query), "c_start": current_period[0], "c_end": current_period[1],
        "p_start": prev_period[0] if prev_period else date(1900,1,1), "p_end": prev_period[1] if prev_period else date(1900,1,1),
        "cty_start": cumulative_this_year[0], "cty_end": cumulative_this_year[1],
        "cly_start": cumulative_last_year[0], "cly_end": cumulative_last_year[1],
    }
    data = _execute_sql_to_df(db_session, sql_query, params).iloc[0].to_dict()
    top_loc_sql = """
        SELECT dv.ten_don_vi, COUNT(cb.id) as so_ca FROM ca_benh cb JOIN don_vi_hanh_chinh dv ON cb.xa_id = dv.id
        WHERE cb.xa_id IN :xa_ids AND cb.chan_doan_chinh LIKE '%%Sốt xuất huyết%%' AND cb.ngay_khoi_phat BETWEEN :start AND :end
        GROUP BY dv.ten_don_vi ORDER BY so_ca DESC;
    """
    df_top_loc = _execute_sql_to_df(db_session, top_loc_sql, {"xa_ids": tuple(xa_ids_to_query), "start": current_period[0], "end": current_period[1]})
    if not df_top_loc.empty:
        max_count = df_top_loc.iloc[0]['so_ca']
        top_locations = df_top_loc[df_top_loc['so_ca'] == max_count]['ten_don_vi'].tolist()
        data['top_locations_this_period'] = {"locations": top_locations, "count": max_count}
    else: data['top_locations_this_period'] = None
    return data

def _generate_sxh_comments(data: dict, period_type: str, period_number: int, prev_period_number: int, year: int, end_of_period_dt: date):
    if not data: return ["- Không có đủ dữ liệu để tạo nhận xét."]
    comments = []
    period_type_lower = "tuần" if period_type == 'week' else "tháng"
    comments.append(f"- Trong {period_type_lower} {period_number} ghi nhận {data.get('total_this_period', 0)} ca mắc, trong đó SXHD và SXHD có dấu hiệu cảnh báo {data.get('warning_this_period', 0)} ca, SXHD nặng {data.get('severe_this_period', 0)} ca, tử vong {data.get('deaths_this_period', 0)} ca.")
    diff_vs_prev = data.get('total_this_period', 0) - data.get('total_prev_period', 0)
    comparison_text = "bằng"
    if diff_vs_prev > 0: comparison_text = f"tăng {diff_vs_prev} ca"
    elif diff_vs_prev < 0: comparison_text = f"giảm {abs(diff_vs_prev)} ca"
    comments.append(f"- Số mắc SXHD trong {period_type_lower} thứ {period_number} là {data.get('total_this_period', 0)} ca {comparison_text} so với {period_type_lower} {prev_period_number}/{year} ({data.get('total_prev_period', 0)} ca).")
    diff_vs_last_year = data.get('cumulative_this_year', 0) - data.get('cumulative_last_year', 0)
    comparison_text_y = "bằng"
    if diff_vs_last_year > 0: comparison_text_y = f"tăng {diff_vs_last_year} ca"
    elif diff_vs_last_year < 0: comparison_text_y = f"giảm {abs(diff_vs_last_year)} ca"
    comments.append(f"- Số mắc SXHD tính đến ngày {end_of_period_dt.strftime('%d/%m/%Y')} là {data.get('cumulative_this_year', 0)} ca {comparison_text_y} so với cùng kỳ năm {year - 1} ({data.get('cumulative_last_year', 0)} ca).")
    if data.get('top_locations_this_period') and data['top_locations_this_period']['count'] > 0: comments.append(f"- Địa phương có số mắc SXHD cao nhất trong {period_type_lower} là: {', '.join(data['top_locations_this_period']['locations'])} ({data['top_locations_this_period']['count']} ca).")
    else: comments.append(f"- Trong {period_type_lower} không ghi nhận ca mắc SXHD nào.")
    return comments

def _generate_sxh_report_base(db_session: Session, start_of_year_dt: date, end_of_period_dt: date, start_of_period_dt: date, user_don_vi: DonViHanhChinh, filepath: str, period_name: str, year: int, analysis_periods: dict = None, comment_details: dict = None):
    reporting_units, join_sql, group_by_sql_col, unit_id_map_key = _get_reporting_logic(db_session, user_don_vi)
    if not reporting_units: return
    
    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return
    
    sql_query = f"""
    WITH sxh_cases AS (
        SELECT
            cb.phan_do_benh, cb.tinh_trang_hien_nay, cb.ngay_khoi_phat,
            (EXTRACT(YEAR FROM age(cb.ngay_khoi_phat, cb.ngay_sinh)) <= 15) as is_under_15,
            {group_by_sql_col} as unit_id
        FROM ca_benh cb JOIN don_vi_hanh_chinh dvh_xa ON cb.xa_id = dvh_xa.id {join_sql}
        WHERE cb.xa_id IN :xa_ids AND cb.chan_doan_chinh LIKE '%%Sốt xuất huyết%%'
        AND cb.ngay_khoi_phat BETWEEN :start_of_year AND :end_of_period
    )
    SELECT
        unit_id,
        COUNT(*) FILTER (WHERE phan_do_benh != 'Sốt xuất huyết Dengue nặng' AND ngay_khoi_phat >= :start_of_period) as mac_cb_p,
        COUNT(*) FILTER (WHERE phan_do_benh != 'Sốt xuất huyết Dengue nặng' AND ngay_khoi_phat >= :start_of_period AND is_under_15) as mac_cb_p_15t,
        COUNT(*) FILTER (WHERE phan_do_benh = 'Sốt xuất huyết Dengue nặng' AND ngay_khoi_phat >= :start_of_period) as mac_nang_p,
        COUNT(*) FILTER (WHERE phan_do_benh = 'Sốt xuất huyết Dengue nặng' AND ngay_khoi_phat >= :start_of_period AND is_under_15) as mac_nang_p_15t,
        COUNT(*) FILTER (WHERE tinh_trang_hien_nay = 'Tử vong' AND ngay_khoi_phat >= :start_of_period) as chet_p,
        COUNT(*) FILTER (WHERE tinh_trang_hien_nay = 'Tử vong' AND ngay_khoi_phat >= :start_of_period AND is_under_15) as chet_p_15t,
        COUNT(*) FILTER (WHERE phan_do_benh != 'Sốt xuất huyết Dengue nặng') as mac_cb_cd,
        COUNT(*) FILTER (WHERE phan_do_benh = 'Sốt xuất huyết Dengue nặng') as mac_nang_cd,
        COUNT(*) as tong_mac_cd,
        COUNT(*) FILTER (WHERE tinh_trang_hien_nay = 'Tử vong') as chet_cd
    FROM sxh_cases WHERE unit_id IS NOT NULL GROUP BY unit_id;
    """
    params = {"xa_ids": tuple(xa_ids_to_query), "start_of_year": start_of_year_dt, "start_of_period": start_of_period_dt, "end_of_period": end_of_period_dt}
    df_results = _execute_sql_to_df(db_session, sql_query, params)
    
    if not df_results.empty:
        df_results['tong_mac_p'] = df_results['mac_cb_p'] + df_results['mac_nang_p']
        df_results = df_results.set_index('unit_id')

    comments = []
    if analysis_periods and comment_details:
        analysis_data = _generate_sxh_analysis_data(db_session, user_don_vi, **analysis_periods)
        comments = _generate_sxh_comments(analysis_data, **comment_details)
        
    list_cases_for_details_sheet = db_session.query(CaBenh).options(joinedload(CaBenh.don_vi)).filter(CaBenh.xa_id.in_(xa_ids_to_query), CaBenh.chan_doan_chinh.like('%Sốt xuất huyết%'), CaBenh.ngay_khoi_phat.between(start_of_period_dt, end_of_period_dt)).all()

    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook, worksheet = writer.book, writer.book.add_worksheet('BaoCaoSXH')
        formats = _create_excel_formats(workbook)
        _draw_standard_header(worksheet, formats, user_don_vi, 'BÁO CÁO SỐ LIỆU MẮC CHẾT SỐT XUẤT HUYẾT DENGUE', period_name, f"Từ ngày {start_of_period_dt.strftime('%d/%m/%Y')} đến ngày {end_of_period_dt.strftime('%d/%m/%Y')}", 'L')
        h_row = 9
        worksheet.set_row(h_row, 50); worksheet.set_column('A:A', 25)
        worksheet.merge_range(h_row, 0, h_row + 1, 0, 'Địa phương', formats['sxh_header']); worksheet.merge_range(h_row, 1, h_row, 3, 'SXH Dengue và SXH Dengue có dấu hiệu cảnh báo', formats['sxh_header']); worksheet.merge_range(h_row, 4, h_row, 6, 'SXH Dengue nặng', formats['sxh_header']); worksheet.merge_range(h_row, 7, h_row + 1, 7, 'Tổng cộng mắc', formats['sxh_header']); worksheet.merge_range(h_row, 8, h_row + 1, 8, 'Cộng dồn mắc', formats['sxh_header']); worksheet.merge_range(h_row, 9, h_row, 11, 'SỐ CHẾT', formats['sxh_header'])
        for col, label in [(1, 'Tổng'), (2, '≤15T'), (3, 'Cộng dồn'), (4, 'Tổng'), (5, '≤15T'), (6, 'Cộng dồn'), (9, 'Tổng'), (10, '≤15T'), (11, 'Cộng dồn')]: worksheet.write(h_row + 1, col, label, formats['sxh_header'])
        
        data_start_row = h_row + 2
        for i, unit in enumerate(reporting_units):
            row = data_start_row + i
            unit_key = getattr(unit, unit_id_map_key)
            res = df_results.loc[unit_key] if unit_key in df_results.index else pd.Series(dtype='float64')
            worksheet.write(row, 0, unit.ten_don_vi, formats['sxh_cell_left'])
            for col, key in [(1, 'mac_cb_p'), (2, 'mac_cb_p_15t'), (3, 'mac_cb_cd'), (4, 'mac_nang_p'), (5, 'mac_nang_p_15t'), (6, 'mac_nang_cd'), (7, 'tong_mac_p'), (8, 'tong_mac_cd'), (9, 'chet_p'), (10, 'chet_p_15t'), (11, 'chet_cd')]: worksheet.write(row, col, int(res.get(key, 0)), formats['sxh_cell'])
        
        total_row = data_start_row + len(reporting_units)
        worksheet.write(total_row, 0, "TỔNG CỘNG", formats['sxh_header'])
        for col in range(1, 12): worksheet.write_formula(total_row, col, f'=SUM({chr(65 + col)}{data_start_row + 1}:{chr(65 + col)}{total_row})', formats['sxh_header'])
        
        footer_base_row = total_row
        if comments:
            worksheet.write(footer_base_row + 1, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row, comment_format = footer_base_row + 2, workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, 11, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)
        
        chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC', 'Xã': 'TRƯỞNG TRẠM'}
        date_line_row, title_line_row, recipient_line_row = footer_base_row + 2, footer_base_row + 3, footer_base_row + 9
        worksheet.merge_range(f'F{date_line_row}:L{date_line_row}', f"{user_don_vi.ten_don_vi}, ngày {end_of_period_dt.day} tháng {end_of_period_dt.month} năm {end_of_period_dt.year}", formats['sxh_italic'])
        worksheet.merge_range(f'F{title_line_row}:L{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['sxh_org_header_bold'])
        worksheet.merge_range(f'A{title_line_row}:C{title_line_row}', 'NGƯỜI BÁO CÁO', formats['sxh_org_header_bold'])
        worksheet.merge_range(f'A{recipient_line_row}:C{recipient_line_row}', 'Nơi nhận:', formats['sxh_noi_nhan'])

        sxh_column_map = {'ho_ten': 'Họ và tên', 'ngay_sinh': 'Ngày sinh', 'don_vi.ten_don_vi': 'Xã/Phường', 'dia_chi_ap': 'Ấp/Khu vực', 'dia_chi_chi_tiet': 'Địa chỉ chi tiết', 'phan_do_benh': 'Phân độ', 'ngay_khoi_phat': 'Ngày khởi phát', 'tinh_trang_hien_nay': 'Tình trạng'}
        _draw_details_sheet(writer, 'ChiTiet_CaBenh_SXH', list_cases_for_details_sheet, sxh_column_map, f"DANH SÁCH CA BỆNH SỐT XUẤT HUYẾT TRONG {period_name.upper()}", formats)

def generate_sxh_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy tuần {week_number}.")
    prev_week_details = calendar_obj.get_week_details(week_number - 1)
    start_of_year_dt, end_of_week_dt = calendar_obj.get_ytd_range(week_number)
    year = calendar_obj.year
    analysis_periods = {"current_period": (week_details['ngay_bat_dau'].date(), week_details['ngay_ket_thuc'].date()), "prev_period": (prev_week_details['ngay_bat_dau'].date(), prev_week_details['ngay_ket_thuc'].date()) if prev_week_details is not None else None, "cumulative_this_year": (start_of_year_dt.date(), end_of_week_dt.date()), "cumulative_last_year": (date(year - 1, 1, 1), end_of_week_dt.date().replace(year=year - 1))}
    comment_details = {"period_type": "week", "period_number": week_number, "prev_period_number": week_number - 1, "year": year, "end_of_period_dt": end_of_week_dt.date()}
    _generate_sxh_report_base(db_session, start_of_year_dt.date(), end_of_week_dt.date(), week_details['ngay_bat_dau'].date(), user_don_vi, filepath, f"Tuần {week_number} năm {year}", year, analysis_periods=analysis_periods, comment_details=comment_details)

def generate_sxh_report_monthly(db_session: Session, year: int, month: int, user_don_vi: DonViHanhChinh, filepath: str):
    _, num_days = calendar.monthrange(year, month)
    start_of_month, end_of_month = date(year, month, 1), date(year, month, num_days)
    start_of_year = date(year, 1, 1)
    prev_month, prev_year = (month - 1, year) if month > 1 else (12, year - 1)
    _, prev_num_days = calendar.monthrange(prev_year, prev_month)
    start_of_prev_month, end_of_prev_month = date(prev_year, prev_month, 1), date(prev_year, prev_month, prev_num_days)
    analysis_periods = {"current_period": (start_of_month, end_of_month), "prev_period": (start_of_prev_month, end_of_prev_month), "cumulative_this_year": (start_of_year, end_of_month), "cumulative_last_year": (date(year - 1, 1, 1), end_of_month.replace(year=year - 1))}
    comment_details = {"period_type": "month", "period_number": month, "prev_period_number": prev_month, "year": year, "end_of_period_dt": end_of_month}
    _generate_sxh_report_base(db_session, start_of_year, end_of_month, start_of_month, user_don_vi, filepath, f"Tháng {month} năm {year}", year, analysis_periods=analysis_periods, comment_details=comment_details)

# ==============================================================================
# 5. BÁO CÁO Ổ DỊCH
# ==============================================================================
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
    if analysis.get('top_locations') and analysis['top_locations']['count'] > 0: comments.append(f"- {', '.join(analysis['top_locations']['locations'])} là địa phương có số ổ dịch phát sinh cao nhất trong tuần với {analysis['top_locations']['count']} ổ dịch mới.")
    if analysis['pending_this_week'] > 0: comments.append(f"- Đề nghị {user_don_vi.ten_don_vi} tập trung nguồn lực xử lý dứt điểm {analysis['pending_this_week']} ổ dịch còn tồn đọng và tăng cường hoạt động diệt lăng quăng tại các khu vực có nguy cơ cao.")
    return comments

def generate_odich_sxh_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy tuần {week_number}.")
    start_of_year_dt, end_of_week_dt_obj = calendar_obj.get_ytd_range(week_number)
    end_of_week_dt, start_of_week_dt = end_of_week_dt_obj.date(), week_details['ngay_bat_dau'].date()
    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    if not reporting_units or not group_by_col: reporting_units, group_by_col = [user_don_vi], 'xa_id'

    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return

    query = db_session.query(O_Dich).options(joinedload(O_Dich.don_vi)).filter(O_Dich.xa_id.in_(xa_ids_to_query), O_Dich.loai_benh == 'SXH', O_Dich.ngay_phat_hien >= start_of_year_dt.date(), O_Dich.ngay_phat_hien <= end_of_week_dt)
    all_outbreaks = query.all()
    df_raw = pd.DataFrame([{'xa_id': c.xa_id, 'dia_chi_ap': c.dia_chi_ap, 'ngay_phat_hien': c.ngay_phat_hien, 'ngay_xu_ly': c.ngay_xu_ly, 'dia_diem_xu_ly': c.dia_diem_xu_ly} for c in all_outbreaks]) if all_outbreaks else pd.DataFrame(columns=['xa_id', 'dia_chi_ap', 'ngay_phat_hien', 'ngay_xu_ly', 'dia_diem_xu_ly'])
    if not df_raw.empty:
        df_raw['ngay_phat_hien'] = pd.to_datetime(df_raw['ngay_phat_hien'], errors='coerce')
        df_raw['ngay_xu_ly'] = pd.to_datetime(df_raw['ngay_xu_ly'], errors='coerce')

    results_list = []
    for i, unit in enumerate(reporting_units):
        filter_value = unit.id if group_by_col == 'xa_id' else unit.ten_don_vi
        df_unit = df_raw[df_raw[group_by_col] == filter_value]
        df_tuan = df_unit[df_unit['ngay_phat_hien'] >= pd.to_datetime(start_of_week_dt)]
        dia_diem_list = df_unit.dropna(subset=['ngay_xu_ly', 'dia_diem_xu_ly'])['dia_diem_xu_ly'].tolist()
        results_list.append({'STT': i + 1, 'Địa Phương': unit.ten_don_vi, 'Phát hiện': len(df_tuan), 'Xử lý': len(df_tuan.dropna(subset=['ngay_xu_ly'])), 'Phát hiện C.dồn': len(df_unit), 'Xử lý C.dồn': len(df_unit.dropna(subset=['ngay_xu_ly'])), 'Địa điểm xử lý': '\n'.join(dia_diem_list)})
    
    df_to_write = pd.DataFrame(results_list)
    if not df_to_write.empty:
        total_row = df_to_write.drop(columns=['STT', 'Địa Phương']).sum().to_dict(); total_row['Địa Phương'] = 'Tổng cộng'
        df_to_write = pd.concat([df_to_write, pd.DataFrame([total_row])], ignore_index=True)
    df_to_write = df_to_write.fillna(0)
    
    analysis_data = _generate_odich_sxh_analysis_data(db_session, user_don_vi, calendar_obj, week_number)
    comments = _generate_odich_sxh_comments(analysis_data, user_don_vi)
    
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook, worksheet = writer.book, writer.book.add_worksheet('BaoCaoOD_SXH')
        formats = _create_excel_formats(workbook)
        worksheet.set_column('A:A', 5); worksheet.set_column('B:B', 25); worksheet.set_column('C:F', 12); worksheet.set_column('G:G', 40)
        _draw_standard_header(worksheet, formats, user_don_vi, 'BÁO CÁO HOẠT ĐỘNG PHÒNG CHỐNG SỐT XUẤT HUYẾT DENGUE', f"Tuần {week_number} năm {calendar_obj.year}", f"Từ ngày {start_of_week_dt.strftime('%d/%m/%Y')} đến ngày {end_of_week_dt.strftime('%d/%m/%Y')}", 'G')
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
        last_data_row = data_start_row + len(df_to_write) -1
        worksheet.write(last_data_row + 1, 0, 'Ghi chú: “OD”: ổ dịch; “SXHD”: Sốt xuất huyết Dengue', formats['ghichu'])
        footer_base_row = last_data_row + 2
        if comments:
            worksheet.write(footer_base_row, 0, "Nhận xét:", formats['nhanxet'])
            comment_start_row, comment_format = footer_base_row + 1, workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, 6, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)
        
        chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC', 'Xã': 'TRƯỞNG TRẠM'}
        date_line_row, title_line_row, recipient_line_row = footer_base_row + 2, footer_base_row + 3, footer_base_row + 9
        worksheet.merge_range(f'E{date_line_row}:G{date_line_row}', f"{user_don_vi.ten_don_vi}, ngày {end_of_week_dt.day} tháng {end_of_week_dt.month} năm {end_of_week_dt.year}", formats['sxh_italic'])
        worksheet.merge_range(f'E{title_line_row}:G{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['sxh_org_header_bold'])
        worksheet.merge_range(f'A{title_line_row}:C{title_line_row}', 'NGƯỜI BÁO CÁO', formats['sxh_org_header_bold'])
        worksheet.merge_range(f'A{recipient_line_row}:C{recipient_line_row}', 'Nơi nhận:', formats['sxh_noi_nhan'])

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
    if analysis.get('top_locations') and analysis['top_locations']['count'] > 0: comments.append(f"- {', '.join(analysis['top_locations']['locations'])} là địa phương cần chú ý nhất trong tuần, với {analysis['top_locations']['count']} ổ dịch mới.")
    return comments

def generate_odich_tcm_report(db_session: Session, calendar_obj: WeekCalendar, week_number: int, user_don_vi: DonViHanhChinh, filepath: str):
    week_details = calendar_obj.get_week_details(week_number)
    if week_details is None: raise ValueError(f"Không tìm thấy tuần {week_number}.")
    start_of_year_dt, end_of_week_dt_obj = calendar_obj.get_ytd_range(week_number)
    end_of_week_dt, start_of_week_dt = end_of_week_dt_obj.date(), week_details['ngay_bat_dau'].date()
    reporting_units, group_by_col = _get_reporting_units(db_session, user_don_vi)
    if not reporting_units or not group_by_col: reporting_units, group_by_col = [user_don_vi], 'xa_id'

    xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
    if not xa_ids_to_query: return

    query = db_session.query(O_Dich).options(joinedload(O_Dich.don_vi)).filter(O_Dich.xa_id.in_(xa_ids_to_query), O_Dich.loai_benh == 'TCM', O_Dich.ngay_phat_hien >= start_of_year_dt.date(), O_Dich.ngay_phat_hien <= end_of_week_dt)
    all_outbreaks = query.all()
    df_raw = pd.DataFrame([{'xa_id': c.xa_id, 'dia_chi_ap': c.dia_chi_ap, 'ngay_phat_hien': c.ngay_phat_hien, 'ngay_xu_ly': c.ngay_xu_ly, 'noi_phat_hien_tcm': c.noi_phat_hien_tcm, 'dia_diem_xu_ly': c.dia_diem_xu_ly} for c in all_outbreaks]) if all_outbreaks else pd.DataFrame(columns=['xa_id', 'dia_chi_ap', 'ngay_phat_hien', 'ngay_xu_ly', 'noi_phat_hien_tcm', 'dia_diem_xu_ly'])
    if not df_raw.empty:
        df_raw['ngay_phat_hien'] = pd.to_datetime(df_raw['ngay_phat_hien'], errors='coerce')
        df_raw['ngay_xu_ly'] = pd.to_datetime(df_raw['ngay_xu_ly'], errors='coerce')

    results_list = []
    for i, unit in enumerate(reporting_units):
        filter_value = unit.id if group_by_col == 'xa_id' else unit.ten_don_vi
        df_unit = df_raw[df_raw[group_by_col] == filter_value]
        df_unit_th, df_unit_cd = df_unit[df_unit['noi_phat_hien_tcm'] == 'Trường học'], df_unit[df_unit['noi_phat_hien_tcm'] == 'Cộng đồng']
        df_tuan_th, df_tuan_cd = df_unit_th[df_unit_th['ngay_phat_hien'] >= pd.to_datetime(start_of_week_dt)], df_unit_cd[df_unit_cd['ngay_phat_hien'] >= pd.to_datetime(start_of_week_dt)]
        dia_diem_list = df_unit.dropna(subset=['ngay_xu_ly', 'dia_diem_xu_ly'])['dia_diem_xu_ly'].tolist()
        results_list.append({'STT': i + 1, 'Địa phương': unit.ten_don_vi, 'PH Tuần TH': len(df_tuan_th), 'XL Tuần TH': len(df_tuan_th.dropna(subset=['ngay_xu_ly'])), 'PH Tuần CĐ': len(df_tuan_cd), 'XL Tuần CĐ': len(df_tuan_cd.dropna(subset=['ngay_xu_ly'])), 'PH CD TH': len(df_unit_th), 'XL CD TH': len(df_unit_th.dropna(subset=['ngay_xu_ly'])), 'PH CD CĐ': len(df_unit_cd), 'XL CD CĐ': len(df_unit_cd.dropna(subset=['ngay_xu_ly'])), 'Địa điểm xử lý': '\n'.join(dia_diem_list)})
    
    df_to_write = pd.DataFrame(results_list)
    if not df_to_write.empty:
        total_row = df_to_write.drop(columns=['STT', 'Địa phương']).sum().to_dict(); total_row['Địa phương'] = 'Tổng cộng'
        df_to_write = pd.concat([df_to_write, pd.DataFrame([total_row])], ignore_index=True)
    df_to_write = df_to_write.fillna(0)
    
    analysis_data = _generate_odich_tcm_analysis_data(db_session, user_don_vi, calendar_obj, week_number)
    comments = _generate_odich_tcm_comments(analysis_data)
    
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook, worksheet = writer.book, writer.book.add_worksheet('BaoCaoOD_TCM')
        formats = _create_excel_formats(workbook)
        worksheet.set_column('A:A', 5); worksheet.set_column('B:B', 25); worksheet.set_column('C:J', 9); worksheet.set_column('K:K', 35)
        _draw_standard_header(worksheet, formats, user_don_vi, 'BÁO CÁO HOẠT ĐỘNG PHÒNG CHỐNG BỆNH TAY CHÂN MIỆNG', f"Tuần {week_number} năm {calendar_obj.year}", f"Từ ngày {start_of_week_dt.strftime('%d/%m/%Y')} đến ngày {end_of_week_dt.strftime('%d/%m/%Y')}", 'K')
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
            comment_start_row, comment_format = footer_base_row + 1, workbook.add_format({'font_name': 'Times new Roman', 'font_size': 13, 'valign': 'top', 'text_wrap': True})
            for i, comment in enumerate(comments): worksheet.merge_range(comment_start_row + i, 0, comment_start_row + i, 10, comment, comment_format)
            footer_base_row = comment_start_row + len(comments)
        chuc_danh_map = {'Tỉnh': 'GIÁM ĐỐC', 'Khu vực': 'GIÁM ĐỐC', 'Xã': 'TRƯỞNG TRẠM'}
        date_line_row, title_line_row, recipient_line_row = footer_base_row + 2, footer_base_row + 3, footer_base_row + 9
        worksheet.merge_range(f'H{date_line_row}:K{date_line_row}', f"{user_don_vi.ten_don_vi}, ngày {end_of_week_dt.day} tháng {end_of_week_dt.month} năm {end_of_week_dt.year}", formats['sxh_italic'])
        worksheet.merge_range(f'H{title_line_row}:K{title_line_row}', chuc_danh_map.get(user_don_vi.cap_don_vi, 'THỦ TRƯỞNG ĐƠN VỊ'), formats['sxh_org_header_bold'])
        worksheet.merge_range(f'A{title_line_row}:D{title_line_row}', 'NGƯỜI BÁO CÁO', formats['sxh_org_header_bold'])
        worksheet.merge_range(f'A{recipient_line_row}:D{recipient_line_row}', 'Nơi nhận:', formats['sxh_noi_nhan'])

# ==============================================================================
# 6. HÀM TỔNG HỢP XUẤT TẤT CẢ BÁO CÁO
# ==============================================================================
def generate_all_reports_zip(db_session: Session, user_don_vi: DonViHanhChinh, year: int, period_type: str, period_number: int, zip_filepath: str):
    temp_dir = os.path.join(os.path.dirname(zip_filepath), str(uuid.uuid4()))
    os.makedirs(temp_dir, exist_ok=True)
    try:
        calendar_obj = WeekCalendar(year) if period_type == 'week' else None
        period_name_part = f"Tuan{period_number}" if period_type == 'week' else f"Thang{period_number}"
        
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
                args_list = list(job['args'])
                if period_type == 'month' and isinstance(args_list[1], WeekCalendar):
                    args_list[1] = year
                job['func'](*args_list, output_path)
            except Exception as e:
                print(f"Lỗi khi tạo file '{job['filename']}': {e}")
                raise e # Ném lại lỗi để dễ debug
        
        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    zipf.write(os.path.join(root, file), arcname=file)
    finally:
        if os.path.exists(temp_dir): shutil.rmtree(temp_dir)

# ==============================================================================
# 7. BÁO CÁO TÙY CHỈNH
# ==============================================================================
def generate_custom_btn_report(db_session: Session, user_don_vi: DonViHanhChinh, start_date: date, end_date: date, selected_don_vi_ids: list[int], filepath: str):
    # --- PHẦN 1: KIỂM TRA ĐẦU VÀO VÀ QUYỀN ---
    allowed_levels = ['Tỉnh', 'Khu vực']
    if user_don_vi.cap_don_vi not in allowed_levels:
        raise PermissionError("Bạn không có quyền truy cập chức năng này.")
    if not selected_don_vi_ids:
        raise ValueError("Vui lòng chọn ít nhất một đơn vị để báo cáo.")
    if start_date > end_date:
        raise ValueError("Ngày bắt đầu không được lớn hơn ngày kết thúc.")

    selected_units = db_session.query(DonViHanhChinh).filter(DonViHanhChinh.id.in_(selected_don_vi_ids)).order_by(DonViHanhChinh.ten_don_vi).all()
    if len(selected_units) != len(selected_don_vi_ids):
        raise ValueError("Một hoặc nhiều ID đơn vị được chọn không hợp lệ.")

    if user_don_vi.cap_don_vi == 'Khu vực':
        allowed_child_ids = {c.id for c in user_don_vi.children}
        for unit in selected_units:
            if unit.id not in allowed_child_ids:
                raise PermissionError(f"Lỗi: Đơn vị '{unit.ten_don_vi}' không thuộc quyền quản lý của bạn.")

    all_xa_ids_to_query = set()
    for unit in selected_units:
        all_xa_ids_to_query.update(get_all_child_xa_ids(unit))
    if not all_xa_ids_to_query:
        raise ValueError("Các đơn vị được chọn không có đơn vị cấp Xã nào.")

    # --- PHẦN 2: TRUY VẤN VÀ XỬ LÝ DỮ LIỆU ---
    sql_query = """
    SELECT
        dvh_xa.parent_id as unit_id,
        cb.chan_doan_chinh,
        COUNT(cb.id) as so_mac,
        COUNT(cb.id) FILTER (WHERE cb.tinh_trang_hien_nay = 'Tử vong') as so_chet
    FROM ca_benh cb
    JOIN don_vi_hanh_chinh dvh_xa ON cb.xa_id = dvh_xa.id
    WHERE
        cb.xa_id IN :xa_ids
        AND cb.ngay_khoi_phat BETWEEN :start_date AND :end_date
        AND cb.chan_doan_chinh IS NOT NULL AND cb.chan_doan_chinh != ''
    GROUP BY
        dvh_xa.parent_id, cb.chan_doan_chinh;
    """
    params = {
        "xa_ids": tuple(all_xa_ids_to_query),
        "start_date": start_date,
        "end_date": end_date,
    }
    df_long = _execute_sql_to_df(db_session, sql_query, params)
    
    df_results = pd.DataFrame()
    dynamic_disease_list = []

    if not df_long.empty:
        # Lấy danh sách bệnh động từ kết quả truy vấn
        dynamic_disease_list = sorted(df_long['chan_doan_chinh'].unique().tolist())
        
        # Chuyển từ dạng dài sang rộng
        df_pivot = df_long.pivot_table(
            index='unit_id',
            columns='chan_doan_chinh',
            values=['so_mac', 'so_chet'],
            fill_value=0
        )
        
        # Làm phẳng tên cột: ('so_mac', 'Tả') -> 'Tả_mac'
        df_pivot.columns = [f"{disease}_{metric.replace('so_', '')}" for metric, disease in df_pivot.columns]
        
        df_results = df_pivot
        
        # Tính tổng theo hàng
        mac_cols = [f"{benh}_mac" for benh in dynamic_disease_list if f"{benh}_mac" in df_results.columns]
        chet_cols = [f"{benh}_chet" for benh in dynamic_disease_list if f"{benh}_chet" in df_results.columns]
        df_results['Tổng_mac'] = df_results[mac_cols].sum(axis=1)
        df_results['Tổng_chet'] = df_results[chet_cols].sum(axis=1)
        
        df_results = df_results.reset_index()

    # --- PHẦN 3: LẤY DỮ LIỆU CHO TRANG CHI TIẾT ---
    list_cases_for_details_sheet = db_session.query(CaBenh).options(joinedload(CaBenh.don_vi)).filter(
        CaBenh.xa_id.in_(list(all_xa_ids_to_query)),
        CaBenh.ngay_khoi_phat.between(start_date, end_date)
    ).order_by(CaBenh.ngay_khoi_phat).all()

    # --- PHẦN 4: KẾT XUẤT RA EXCEL ---
    _generate_custom_btn_report_core(
        filepath=filepath,
        user_don_vi=user_don_vi,
        start_date=start_date,
        end_date=end_date,
        reporting_units=selected_units,
        df_results=df_results,
        dynamic_disease_list=dynamic_disease_list,
        list_cases_for_details_sheet=list_cases_for_details_sheet
    )

# ==============================================================================
# 8. HÀM XUẤT DANH SÁCH CA BỆNH RA EXCEL
# ==============================================================================
from io import BytesIO

def generate_cases_export(cases: list[CaBenh]) -> BytesIO:
    data_to_export = []
    for case in cases:
        data_to_export.append({
            'ID': case.id, 'Mã BN': case.ma_so_benh_nhan, 'Họ và tên': case.ho_ten,
            'Ngày sinh': case.ngay_sinh.strftime('%d/%m/%Y') if case.ngay_sinh else '', 'Giới tính': case.gioi_tinh,
            'Xã/Phường': case.don_vi.ten_don_vi if case.don_vi else '', 'Ấp/Khu phố': case.dia_chi_ap,
            'Địa chỉ chi tiết': case.dia_chi_chi_tiet,
            'Ngày khởi phát': case.ngay_khoi_phat.strftime('%d/%m/%Y') if case.ngay_khoi_phat else '',
            'Chẩn đoán chính': case.chan_doan_chinh, 'Phân độ bệnh': case.phan_do_benh,
            'Tình trạng hiện nay': case.tinh_trang_hien_nay, 'ID Ổ dịch': case.o_dich_id if case.o_dich_id else ''
        })
    df = pd.DataFrame(data_to_export)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='DanhSachCaBenh', index=False)
        worksheet = writer.sheets['DanhSachCaBenh']
        for idx, col in enumerate(df.columns):
            series = df[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
            worksheet.set_column(idx, idx, max_len)
    output.seek(0)
    return output
