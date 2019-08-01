import os
from flask import render_template, redirect, url_for, flash, current_app, session, send_from_directory
from app.form.login import LoginForm
from app.form.uploadform import UploadForm
from . bp import auth
from app.help.helper import Helper


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
    if form.validate_on_submit():
        file_data = form.photo.data
        filename = Helper.random_filename(file_data.filename)
        file_data.save(os.path.join(current_app.config['UPLOAD_PATH'],filename))
        flash('Upload Success')
        session['filename'] = filename
        return redirect(url_for('front.index'))
    return render_template('auth/upload.html', form=form)


@auth.route('/uploads/<path:filename>')
def get_file(filename):
    return send_from_directory(current_app.config['UPLOAD_PATH'], filename)


