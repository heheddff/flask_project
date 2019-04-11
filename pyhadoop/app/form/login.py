from . baseform import BaseForm
from wtforms import StringField, PasswordField,BooleanField, SubmitField
from wtforms.validators import DataRequired, Length


class LoginForm(BaseForm):
    username = StringField('username',validators=[DataRequired()],render_kw={'placeholder':'Your Username'})
    password = PasswordField('Password',validators=[DataRequired(),Length(8,128)])
    remember = BooleanField('Remember me')
    submit = SubmitField('Log in')