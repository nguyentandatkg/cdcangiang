# file: run.py (nằm ở thư mục gốc)
from webapp import create_app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True)