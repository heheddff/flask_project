from flask import render_template,redirect,url_for,flash
from app.form.login import LoginForm
from app.form.uploadform import UploadForm

from . bp import auth


@auth.route('/', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        username = form.username.data
        flash('Welcome home,%s' % username)
        return redirect(url_for('front.index'))
    return render_template('auth/login.html', form=form)


@auth.route('/upload', methods=['GET', 'POST'])
def upload():
    form = UploadForm()
    return render_template('auth/upload.html', form=form)


