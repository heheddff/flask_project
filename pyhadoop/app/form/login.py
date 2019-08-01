from . baseform import BaseForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired, Length, ValidationError
from app.form.custom_global_validators import CustomValidators


class LoginForm(BaseForm):
    username = StringField('username', validators=[DataRequired()], render_kw={'placeholder': 'Your Username'})
    password = PasswordField('Password', validators=[CustomValidators.not_empty(), Length(6, 128)])
    remember = BooleanField('Remember me')
    submit = SubmitField('Log in')

    # 行内验证器
    @staticmethod
    def validate_username(form, field):
        if field.data != 'admin':
            raise ValidationError('you are not admin')
