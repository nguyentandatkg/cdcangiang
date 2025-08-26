
# file: core/dashboard_utils.py (Phiên bản nâng cấp so sánh 2 năm)

import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go # <-- Import thêm graph_objects
import json
from sqlalchemy import func
from datetime import date, timedelta

from .database_utils import get_db_session
from .database_setup import CaBenh, DonViHanhChinh as DonVi
from .utils import get_all_child_xa_ids


def get_weekly_case_counts_for_comparison(user_don_vi, disease_filter: str = None, khu_vuc_id: str = None, xa_id: str = None):
    """
    Lấy dữ liệu số ca bệnh theo tuần của NĂM NAY và NĂM TRƯỚC để so sánh.
    """
    db = get_db_session()
    try:
        if xa_id:
            xa_ids_to_query = [int(xa_id)]
        elif khu_vuc_id:
            khu_vuc_don_vi = db.query(DonVi).get(int(khu_vuc_id))
            xa_ids_to_query = get_all_child_xa_ids(khu_vuc_don_vi) if khu_vuc_don_vi else []
        else:
            xa_ids_to_query = get_all_child_xa_ids(user_don_vi)

        if not xa_ids_to_query:
            return pd.DataFrame(), date.today().year, date.today().year - 1

        # 1. Xác định các năm và tuần cần truy vấn
        today = date.today()
        current_year = today.year
        previous_year = current_year - 1
        current_week = today.isocalendar()[1]

        # 2. Truy vấn dữ liệu của cả 2 năm trong một lần
        query = db.query(
            CaBenh.ngay_khoi_phat
        ).filter(
            CaBenh.xa_id.in_(xa_ids_to_query),
            CaBenh.ngay_khoi_phat.isnot(None),
            # Lấy dữ liệu từ đầu năm ngoái đến hết năm nay
            func.extract('year', CaBenh.ngay_khoi_phat).in_([current_year, previous_year])
        )
        
        if disease_filter and disease_filter != 'Tất cả':
            query = query.filter(CaBenh.chan_doan_chinh == disease_filter)
        
        df_dates = pd.read_sql(query.statement, query.session.bind)
        
        if df_dates.empty:
            return pd.DataFrame(), current_year, previous_year
            
        df_dates['ngay_khoi_phat'] = pd.to_datetime(df_dates['ngay_khoi_phat'])
        
        # 3. Nhóm dữ liệu theo năm và tuần
        df_grouped = df_dates.groupby([
            df_dates['ngay_khoi_phat'].dt.isocalendar().year.rename('year'),
            df_dates['ngay_khoi_phat'].dt.isocalendar().week.rename('week')
        ]).size().reset_index(name='case_count')

        # 4. Sử dụng PIVOT để biến năm thành các cột riêng biệt
        df_pivot = df_grouped.pivot(index='week', columns='year', values='case_count').reset_index()
        
        # Đảm bảo có đủ 52 tuần
        all_weeks = pd.DataFrame({'week': range(1, 53)})
        df_final = pd.merge(all_weeks, df_pivot, on='week', how='left')

        # Đổi tên cột và điền giá trị 0 cho các tuần không có dữ liệu
        for year in [current_year, previous_year]:
            if year in df_final.columns:
                df_final.rename(columns={year: f'Năm {year}'}, inplace=True)
            else:
                # Nếu một năm không có dữ liệu nào, tạo cột đó
                df_final[f'Năm {year}'] = 0
        
        df_final = df_final.fillna(0)

        # Xóa dữ liệu tương lai của năm nay
        df_final.loc[df_final['week'] > current_week, f'Năm {current_year}'] = None

        return df_final, current_year, previous_year
    finally:
        db.close()


def create_cases_by_week_chart(user_don_vi, disease_filter: str = None, khu_vuc_id: str = None, xa_id: str = None):
    """
    Tạo biểu đồ đường so sánh số ca mắc giữa năm nay và năm trước.
    """
    df, current_year, previous_year = get_weekly_case_counts_for_comparison(user_don_vi, disease_filter, khu_vuc_id, xa_id)
    
    location_name = user_don_vi.ten_don_vi
    db = get_db_session()
    try:
        if xa_id:
            location_name = db.query(DonVi.ten_don_vi).filter(DonVi.id == int(xa_id)).scalar() or location_name
        elif khu_vuc_id:
            location_name = db.query(DonVi.ten_don_vi).filter(DonVi.id == int(khu_vuc_id)).scalar() or location_name
    finally:
        db.close()

    if disease_filter and disease_filter != 'Tất cả':
        title = f'Diễn biến ca {disease_filter} tại {location_name}'
    else:
        title = f'Tổng số ca mắc mới theo tuần tại {location_name}'
    
    # Sử dụng graph_objects để vẽ nhiều đường
    fig = go.Figure()

    if df.empty or (df[f'Năm {current_year}'].sum() == 0 and df[f'Năm {previous_year}'].sum() == 0):
        fig.update_layout(title=title, annotations=[dict(text='Không có dữ liệu', showarrow=False)], xaxis_title="Tuần", yaxis_title="Số ca mắc")
    else:
        # Thêm đường của năm trước
        fig.add_trace(go.Scatter(
            x=df['week'], 
            y=df[f'Năm {previous_year}'],
            mode='lines+markers',
            name=f'Năm {previous_year}',
            line=dict(color='grey', dash='dash')
        ))

        # Thêm đường của năm nay
        fig.add_trace(go.Scatter(
            x=df['week'], 
            y=df[f'Năm {current_year}'],
            mode='lines+markers',
            name=f'Năm {current_year}',
            line=dict(color='#0077b6', width=3) # Màu xanh đậm, nét dày hơn
        ))

        fig.update_layout(
            title=title,
            xaxis_title="Tuần trong năm",
            yaxis_title="Số ca mắc mới",
            legend_title="Năm",
            hovermode="x unified"
        )
    
    fig.update_layout(font=dict(family="Times New Roman", size=13))
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

# <<< THAY ĐỔI MỚI: Thêm tham số khu_vuc_id và xa_id
def get_top_diseases(user_don_vi, start_date: date, end_date: date, khu_vuc_id: str = None, xa_id: str = None):
    """
    Truy vấn CSDL, tổng hợp và trả về top các bệnh có số ca mắc cao nhất.
    """
    db = get_db_session()
    try:
        # <<< THAY ĐỔI MỚI: Logic xác định danh sách xã cần truy vấn
        if xa_id:
            xa_ids_to_query = [int(xa_id)]
        elif khu_vuc_id:
            khu_vuc_don_vi = db.query(DonVi).get(int(khu_vuc_id))
            xa_ids_to_query = get_all_child_xa_ids(khu_vuc_don_vi)
        else:
            xa_ids_to_query = get_all_child_xa_ids(user_don_vi)
            
        if not xa_ids_to_query:
            return pd.DataFrame()

        query = db.query(
            CaBenh.chan_doan_chinh,
            func.count(CaBenh.id).label('so_ca_mac')
        ).filter(
            CaBenh.xa_id.in_(xa_ids_to_query),
            CaBenh.ngay_khoi_phat >= start_date,
            CaBenh.ngay_khoi_phat <= end_date
        ).group_by(CaBenh.chan_doan_chinh).order_by(func.count(CaBenh.id).desc())
        
        df = pd.read_sql(query.statement, query.session.bind)
        return df
    finally:
        db.close()


# Các hàm tạo biểu đồ còn lại không cần thay đổi
def create_top_diseases_chart(df: pd.DataFrame, time_range_text: str):
    """
    Tạo biểu đồ cột từ DataFrame top bệnh.
    """
    title = f'Top 5 Bệnh mắc cao nhất ({time_range_text})'
    if df.empty:
        fig = px.bar(title=title)
        fig.update_layout(annotations=[dict(text='Không có dữ liệu', showarrow=False)], xaxis_title='Tên bệnh', yaxis_title='Số ca mắc')
    else:
        top_df = df.head(5)
        fig = px.bar(top_df, x='chan_doan_chinh', y='so_ca_mac', title=title,
                     labels={'chan_doan_chinh': 'Tên bệnh', 'so_ca_mac': 'Số ca mắc'}, text_auto=True)
        fig.update_layout(xaxis_title=None, yaxis_title="Số ca mắc")
    
    fig.update_layout(font=dict(family="Times New Roman", size=13))
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

def create_disease_pie_chart(df: pd.DataFrame, time_range_text: str):
    """
    Tạo biểu đồ tròn từ DataFrame top bệnh.
    """
    title = f'Cơ cấu Bệnh truyền nhiễm ({time_range_text})'
    if df.empty:
        fig = px.pie(title=title)
        fig.update_layout(annotations=[dict(text='Không có dữ liệu', showarrow=False)])
    else:
        fig = px.pie(df, names='chan_doan_chinh', values='so_ca_mac', title=title, hole=.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')

    fig.update_layout(font=dict(family="Times New Roman", size=13))
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)