import hashlib


class Helper(object):

    @classmethod
    def hash(cls, msg):
        m = hashlib.md5()
        secret = msg.encode(encoding='utf-8')
        m.update(secret)
        return m.hexdigest()

    # check msg length
    @classmethod
    def check_len(cls,msg):
        return True if len(msg) > 0 else False
