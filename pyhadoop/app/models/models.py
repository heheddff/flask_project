from werkzeug.security import generate_password_hash, check_password_hash
from app.plugins.plugins import db


class User(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20))
    password_hash = db.Column(db.String(128))
    role = db.Column(db.String(60), nullable=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def validate_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return '<User %r>' % self.username


class Permission(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    permission = db.Column(db.String(128))
    description = db.Column(db.String(128), nullable=True)




