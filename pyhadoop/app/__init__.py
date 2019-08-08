# -*- coding:utf-8 -*-
import os
from flask import Flask
from app.config import config


def create_app(config_name=None):
    app = Flask(__name__)
    if config_name is None:
        config_name = os.getenv('FLASK_CONFIG', 'development')

    app.config.from_object(config[config_name])
    # app.config.from_object('app.config')
    app.config.from_object('app.mysql_config')
    app.config.from_object('app.hdfs_config')

    register_bp(app)
    register_plugins(app)

    return app


# 注册蓝图
def register_bp(app):
    from app.web.bp import web, front, auth, redis
    app.register_blueprint(web, url_prefix='/admin')
    app.register_blueprint(front, url_prefix='/front')
    app.register_blueprint(auth)
    app.register_blueprint(redis, url_prefix='/redis')


# 注册插件
def register_plugins(app):
    from app.plugins.plugins import bootstrap,db
    bootstrap.init_app(app)
    db.init_app(app)
