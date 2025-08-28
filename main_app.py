# file: main_app.py

import webview
import socket
from threading import Thread
from waitress import serve # Sử dụng waitress cho server ổn định hơn
# THAY ĐỔI: Import đối tượng 'app' đã được tạo sẵn
from run import app as flask_app

# --- CÁC HÀM TIỆN ÍCH ---

def find_free_port():
    """Tìm một cổng trống trên localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

# --- KHỞI TẠO ỨNG DỤNG ---

# THAY ĐỔI: Không cần tạo lại app ở đây nữa
# flask_app = create_app()

# Tìm một cổng trống và xây dựng URL
port = find_free_port()
url = f"http://127.0.0.1:{port}"

def run_flask(app, port):
    """Chạy server Flask bằng waitress."""
    print(f"Starting Flask server at {url}")
    serve(app, host='127.0.0.1', port=port)

if __name__ == '__main__':
    # 1. Khởi động server Flask trong một luồng (thread) riêng
    #    Điều này giúp server chạy ngầm mà không chặn cửa sổ ứng dụng.
    flask_thread = Thread(target=run_flask, args=(flask_app, port))
    flask_thread.daemon = True # Đảm bảo thread sẽ tự tắt khi chương trình chính kết thúc
    flask_thread.start()

    # 2. Tạo cửa sổ ứng dụng PyWebView
    #    Nó sẽ trỏ đến URL của server Flask đang chạy ngầm.
    window = webview.create_window(
        'Hệ thống Quản lý bệnh truyền nhiễm', # Tiêu đề của cửa sổ
        url,                                  # URL để hiển thị
        width=1366,
        height=768,
        resizable=True,
        min_size=(1024, 768)
    )

    # 3. Bắt đầu vòng lặp sự kiện của PyWebView
    #    Tham số `gui='cef'` hoặc `gui='qt'` có thể giúp tương thích tốt hơn trên Windows
    #    Hãy thử bỏ comment dòng dưới nếu gặp vấn đề hiển thị.
    #    webview.start(gui='cef', debug=False)
    webview.start(debug=False) # Đặt debug=False khi bạn muốn đóng gói