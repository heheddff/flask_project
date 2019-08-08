from flask import Blueprint

web = Blueprint('web', __name__)
front = Blueprint('front', __name__)
auth = Blueprint('auth', __name__)
redis = Blueprint('redis', __name__)
