from redis import Redis


class RedisClass(object):

    def __init__(self):
        self.conn = Redis(host='192.168.0.220', port=6380,db=0, password="iccredi@123")

    def connect_redis(self):
        return 'connect_redis'
