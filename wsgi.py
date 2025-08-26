# file: wsgi.py
import sys
import os

# Thêm đường dẫn dự án vào sys.path
project_home = os.path.dirname(os.path.abspath(__file__))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

# Import ứng dụng Flask của bạn
# Giả sử file chính tạo ra app Flask là 'run.py' và biến app tên là 'app'
# Hãy thay đổi 'run' và 'app' cho đúng với dự án của bạn
from webapp import app as application