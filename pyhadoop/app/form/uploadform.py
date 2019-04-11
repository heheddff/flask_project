from . baseform import BaseForm
from wtforms import SubmitField
from flask_wtf.file import FileField,FileRequired,FileAllowed


class UploadForm(BaseForm):
    photo = FileField('Upload Image',validators=[FileRequired()])
    submit = SubmitField()