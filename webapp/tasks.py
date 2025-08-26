# webapp/tasks.py
import os

def import_excel_task(filepath, user_xa_id):
    """
    Hàm này sẽ được RQ worker gọi để xử lý import.
    """
    # Import và tạo app context BÊN TRONG hàm để tránh circular import
    from webapp import create_app
    from webapp.core.data_importer import import_data_from_excel

    app = create_app()
    with app.app_context():
        try:
            # Gọi hàm logic chính để xử lý file
            result = import_data_from_excel(filepath=filepath, user_xa_id=user_xa_id)
            return result

        except Exception as e:
            # Bắt các lỗi không lường trước
            app.logger.error(f"Lỗi nghiêm trọng trong task import: {e}", exc_info=True)
            return {
                'success': False,
                'message': f'Đã xảy ra lỗi không xác định trong quá trình import: {str(e)}',
                'errors': []
            }
        finally:
            # Luôn đảm bảo xóa file tạm sau khi xử lý xong
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except OSError as e:
                    app.logger.error(f"Lỗi khi xóa file tạm {filepath}: {e}")