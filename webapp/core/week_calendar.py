# file: week_calendar.py

from datetime import datetime, timedelta
import pandas as pd

class WeekCalendar:
    """Quản lý và tạo lịch tuần làm việc cho một năm cụ thể."""
    def __init__(self, year: int):
        self.year = year
        # Quy ước: Tuần 1 bắt đầu vào ngày Thứ Hai đầu tiên của năm.
        jan_1 = datetime(year, 1, 1)
        # Lùi lại đến ngày Thứ Hai gần nhất (weekday() của Thứ Hai là 0)
        first_monday = jan_1 - timedelta(days=jan_1.weekday())
        self.first_day = first_monday
        self.week_df = self._generate_weeks()

    def _generate_weeks(self):
        """Tạo ra một DataFrame chứa 53 tuần của năm."""
        weeks_data = []
        current_start_date = self.first_day
        for week_num in range(1, 54):
            start_date = current_start_date
            end_date = start_date + timedelta(days=6)
            weeks_data.append({
                'nam': self.year,
                'tuan': week_num,
                'ngay_bat_dau': start_date,
                'ngay_ket_thuc': end_date
            })
            current_start_date = end_date + timedelta(days=1)
            if current_start_date.year > self.year and len(weeks_data) >= 52:
                break
        return pd.DataFrame(weeks_data)

    def get_week_details(self, week_number: int):
        """Lấy thông tin chi tiết (start_date, end_date) của một tuần."""
        week_info = self.week_df[self.week_df['tuan'] == week_number]
        return None if week_info.empty else week_info.iloc[0]

    def get_ytd_range(self, week_number: int):
        """Lấy khoảng thời gian từ đầu năm đến cuối tuần báo cáo (Year-to-Date)."""
        week_info = self.get_week_details(week_number)
        if week_info is None:
            return None, None
        start_of_year = self.first_day
        end_of_week = week_info['ngay_ket_thuc']
        return start_of_year, end_of_week