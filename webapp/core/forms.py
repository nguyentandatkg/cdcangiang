from flask_wtf import FlaskForm
from wtforms import PasswordField, SubmitField
from wtforms.validators import DataRequired, EqualTo, Length

class ChangePasswordForm(FlaskForm):
    """Form cho người dùng thay đổi mật khẩu của họ."""
    current_password = PasswordField(
        'Mật khẩu hiện tại', 
        validators=[DataRequired(message="Vui lòng nhập mật khẩu hiện tại.")]
    )
    new_password = PasswordField(
        'Mật khẩu mới', 
        validators=[
            DataRequired(message="Vui lòng nhập mật khẩu mới."),
            Length(min=6, message="Mật khẩu mới phải có ít nhất 6 ký tự.")
        ]
    )
    confirm_new_password = PasswordField(
        'Xác nhận mật khẩu mới', 
        validators=[
            DataRequired(message="Vui lòng xác nhận mật khẩu mới."),
            EqualTo('new_password', message='Mật khẩu xác nhận không khớp.')
        ]
    )
    submit = SubmitField('Đổi mật khẩu')