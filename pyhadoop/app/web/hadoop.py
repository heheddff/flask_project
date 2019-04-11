from flask import (
    render_template,make_response,request,jsonify,redirect
)
from app.web.bp import web
from app.backend.interaction_hdfs import Read_Hdfs
import time


# @web.route('/admin')
@web.route('/')
def index():

    php = request.args.get('api', default=None)
    lists = Read_Hdfs().lists()
    print(lists)
    #result = Read_Hdfs().get_files()
    if php:
        return jsonify(lists)

    return render_template('hadoop/index.html', posts=lists)


@web.route('/add')
def add():
    serid = request.args.get('serid', False, )
    stype = request.args.get('stype', 0, type=int)
    serid = str(serid)
    #time.sleep(5)
    if stype not in [1, 2, 3]:  # 操作状态
        return jsonify(result={serid: 4})

    if serid == False:
        return jsonify(result={serid: 4})

    serids = serid.strip(',').split(',')
    if len(serids) == 0:
        return jsonify(result={serid: 4})

    print(serids)
    hdfs = Read_Hdfs()
    res = {}
    for serid in serids:
        res[serid] = hdfs.insert_mysql_new(serid)

    #res = Read_Hdfs().insert_mysql(serid)

    response = make_response(jsonify(result=res))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST'
    response.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    return response

    #return Read_Hdfs().reads('/tmp/input/20190322.log')

@web.route('/test')
def test():
    hdfs = Read_Hdfs()
    res = {}

    hdfs.insert_mysql('20190410.log')

    return 'admin test'
