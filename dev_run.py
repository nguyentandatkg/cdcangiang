# file: dev_run.py (Tệp mới)
# Tệp này chỉ dùng để chạy server phát triển (development server).
# Để chạy, dùng lệnh: python dev_run.py

from run import app

if __name__ == '__main__':
    # debug=True cho phép tự động tải lại khi có thay đổi code và cung cấp trình gỡ lỗi.
    # host='0.0.0.0' cho phép truy cập từ các thiết bị khác trong cùng mạng.
    app.run(host='0.0.0.0', port=5000, debug=True)
