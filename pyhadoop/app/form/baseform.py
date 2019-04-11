from flask_wtf import FlaskForm


class BaseForm(FlaskForm):
    class Meta:
        locals = ['zh']