from hdfs import InsecureClient
from flask import current_app
import json
import hashlib
from .mysql import Mysql
from .. help.helper import Helper
from os import path
import time


class Read_Hdfs(object):

    def __init__(self):
        self.client = InsecureClient(url=current_app.config['HDFS_URL'], user=current_app.config['HDFS_USER'])
        self.mysql = Mysql()
        self.helper = Helper()

    def get_files(self):
        res = self.mysql.select_info()
        return dict(res)

    def lists(self):
        lists = self.get_files()
        try:
            files = self.client.list(current_app.config['LOG_PATH'])
            for file in files:
                lists[file] = lists.get(file, 2)
            new_lists = list(lists.items())
            new_lists.sort(key=lambda x: x[0], reverse=True)
        except Exception as e:
            print('lists =>{}'.format(e))
            return {}
        else:
            return dict(new_lists)

    def insert_mysql(self, filename):
        print("{:*^30}".format('Start deal '+filename))
        file_path = path.join(current_app.config['LOG_PATH'], filename)
        m, n, z, y = 0, 0, 0, 0
        start = time.perf_counter()
        try:
            lens = self.client.status(file_path)['length'] # get file contents size
            lensum = 0  # init deal size
            with self.client.read(file_path,encoding='utf-8') as reader:
                for line in reader:
                    lensum += len(line)
                    z += 1
                    if self.helper.check_len(line) is False:
                        y += 1
                        continue

                    data = json.loads(line)

                    rpt_time = data.get('time', '0000')
                    hash_r = self.hash(self.remove_time(data.copy()))  # hash content without time field

                    # add hash and created two fields
                    data['hash'] = hash_r
                    data['created'] = rpt_time

                    # Storage Database
                    res_s = self.mysql.selects(hash_r)

                    if res_s:
                        if res_s[0] < rpt_time: # update lately record
                            self.mysql.updates(rpt_time, hash_r)
                            m += 1
                        else:
                            y += 1
                    else:
                        self.mysql.inserts(data)
                        n += 1

                    per = (lensum/lens)*100  # get percent
                    dur = time.perf_counter() - start  # get use time
                    print("\r处理进度:{:.2f}%[用时{:.2f}s]".format(per, dur), end='')
            print('')
            print("{:*^30}".format('End deal ' + filename))
            print("***** update {} records,insert {} records,"
                  "other(no update or no insert) {},all records {} *****".format(m, n, y, z))
            self.mysql.inserts_info(filename)
        except Exception as e:
            print(e, filename)
            return 3
        else:
            return 1

    @staticmethod
    def remove_time(data):
        msg = ''
        if 'time' in data:
            data.pop('time')

        for k in data:
            msg += str(data[k])
        return msg

    @staticmethod
    def hash(msg):
        m = hashlib.md5()
        secret = msg.encode(encoding='utf-8')
        m.update(secret)
        return m.hexdigest()

    def insert_mysql_new(self, filename):
        print("{:*^30}".format('Start deal '+filename))
        file_path = path.join(current_app.config['LOG_PATH'], filename)
        start_file = time.perf_counter()
        try:
            lens = self.client.status(file_path)['length'] # get file contents size

            with self.client.read(file_path,encoding='utf-8') as reader:
                result, y, z = self.deal_records(lens,reader)
            # print(y,z)
            m, n, y = self.deal_result(result, y)
            print('')
            print("{:*^30}".format('End deal ' + filename))
            print("***** update {} records,insert {} records,"
                "other(no update or no insert) {},all records {} *****".format(m, n, y, z))
            print("共用时{:.2f}s".format(time.perf_counter()-start_file))
            self.mysql.inserts_info(filename)
        except Exception as e:
            print(e, filename)
            return 3
        else:
            return 1

    def deal_records(self, lens, reader):
        start = time.perf_counter()
        lensum = 0  # init deal size
        result = {}
        x, z, y = 0, 0, 0
        for line in reader:
            lensum += len(line)
            z += 1
            if self.helper.check_len(line) == False:
                y += 1
                continue

            data = json.loads(line)

            rpt_time = data.get('time', '0000')
            hash_r = self.hash(self.remove_time(data.copy()))  # hash content without time field

            # add hash and created two fields
            data['hash'] = hash_r
            data['created'] = rpt_time

            if hash_r not in result:
                result[hash_r] = data
            else:
                if result[hash_r]['time'] >= rpt_time:
                    result[hash_r] = data
                y += 1

            per = (lensum / lens) * 100  # get percent
            dur = time.perf_counter() - start  # get use time
            print("\r文件处理进度:{:.2f}%[用时{:.2f}s]".format(per, dur), end='')
        print('')
        # print("y={},x={},len={}".format(y,x,len(result)))
        return result, y, z

    def deal_result(self, result, y=0):
        start = time.perf_counter()
        m, n, lensum = 0, 0, 0
        lens = len(result)

        for hash_r, data in result.items():
            lensum += 1
            res_s = self.mysql.selects(hash_r)
            rpt_time = data['time']

            if res_s:
                if res_s[0] < rpt_time:  # update lately record
                    self.mysql.updates(rpt_time, hash_r)
                    m += 1
                else:
                    y += 1
            else:
                self.mysql.inserts(data)
                n += 1

            per = (lensum / lens) * 100  # get percent
            dur = time.perf_counter() - start  # get use time
            print("\r入库处理进度:{:.2f}%[用时{:.2f}s]".format(per, dur), end='')
        # print("m={},n={},y={}".format(m,n,y))
        return m, n, y
        # self.mysql.inserts_info(filename)
