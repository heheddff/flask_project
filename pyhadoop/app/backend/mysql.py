from flask import current_app
import pymysql

class Mysql(object):

    def __init__(self):
        self.__conn = pymysql.connect(host=current_app.config['DB_HOST'],
                                      port=current_app.config['DB_PORT'],
                                      user=current_app.config['DB_USER'],
                                      passwd=current_app.config['DB_PASS'],
                                      db=current_app.config['DB_NAME'])
        self.cursor = self.__conn.cursor()
        self.table = current_app.config['DB_TABLE']
        self.info_table = "info_status"

    def selects(self, rpt_hash):
        sql = "select * from " + self.table + " where rpt_hash=%s"
        self.cursor.execute(sql, (rpt_hash,))
        return self.cursor.fetchone()

    def updates(self, rpt_time, rpt_hash):
        try:
            sql = "update " + self.table + " set rpt_time=%s where rpt_hash=%s"
            self.cursor.execute(sql, (rpt_time, rpt_hash,))
        except Exception as e:
            print(e)
            print(sql)
            return False
        else:
            return True

    def inserts(self, data):
        data = self.field_maps(data)
        fields = ','.join(list(data.keys()))
        values = list(data.values())
        s = ('%s,' * len(data)).strip(',')

        sql = "insert into " + self.table + " (" + fields + ") values (" + s +")"
        try:
            row = self.cursor.execute(sql, values)
        except Exception as e:
            print(e)
            print(sql)
            print(data)
            return False
        else:
            return row

    def field_maps(self, data):
        maps = {
            "time": "rpt_time",
            "ip": "rpt_ip",
            "uri": "rpt_uri",
            "method": "rpt_method",
            "os": "rpt_os",
            "os_version": "rpt_os_version",
            "device": "rpt_device",
            "browser": "rpt_browser",
            "browser_version": "rpt_browser_version",
            "passport": "rpt_passport",
            "sub_name": "rpt_sub_name",
            "game_id": "rpt_game_id",
            "hash": "rpt_hash",
            "created": "rpt_created"
        }

        new_data = {}
        for k, v in data.items():
            key = maps.get(k,False)
            if key:
                new_data[key] = v
        return new_data

    def fields(self):
        fields = "(rpt_time," \
                 "rpt_ip," \
                 "rpt_uri," \
                 "rpt_method," \
                 "rpt_os," \
                 "rpt_os_version," \
                 "rpt_device," \
                 "rpt_browser," \
                 "rpt_browser_version," \
                 "rpt_passport," \
                 "rpt_sub_name," \
                 "rpt_game_id," \
                 "rpt_hash," \
                 "rpt_created)"

    def inserts_info(self, filename):
        fields = 't_name,t_status,t_date'
        date = filename.split('.')[0]
        values = [filename, 1, date]
        sql = "insert into "+self.info_table+" (" + fields + ") values (%s,%s,%s)"
        try:
            row = self.cursor.execute(sql, values)
        except Exception as e:
            print(e, sql, filename)
            return False
        else:
            return row

    def select_info(self):
        sql = "select t_name,t_status from "+self.info_table
        self.cursor.execute(sql)
        return self.cursor.fetchall()
