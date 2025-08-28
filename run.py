# file: run.py (nằm ở thư mục gốc)
from webapp import create_app

app = create_app()

# KHÔNG còn app.run() ở đây nữa.
# Tệp này chỉ dùng để tạo ra đối tượng 'app' để các server khác (wsgi, waitress) có thể import.