from .bp import redis
from app.py_redis.RedisClass import RedisClass
import time
import threading
import os

redis_conn = RedisClass().conn
ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLES_PER_PAGE = 25


@redis.route('/connect')
def connect():
    print(redis_conn.incr('zxy1'))
    # trans()
    begin_trans()

    return '123'


# 第三章基本事务
def trans():
    # this function is not begin trans
    def notrans():
        print(redis_conn.incr('notrans:'))
        time.sleep(.1)
        redis_conn.incr('notrans:',-1)
    if 1:
        for i in range(3):
            threading.Thread(target=notrans).start()
        time.sleep(.5)


def begin_trans():
    def trans():
        pipeline = redis_conn.pipeline()
        pipeline.incr('trans:')
        time.sleep(.1)
        pipeline.incr('trans:', -1)
        print(pipeline.execute())

    if 1:
        for i in range(3):
            threading.Thread(target=trans).start()
        time.sleep(.5)


# 投票
def article_vote(conn, user, article, flags = True):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore("time:", article) < cutoff:
        return
    """
    partition() 方法用来根据指定的分隔符将字符串进行分割。
    如果字符串包含指定的分隔符，则返回一个3元的元组，
    第一个为分隔符左边的子串，
    第二个为分隔符本身，
    第三个为分隔符右边的子串。
    partition() 方法是在2.5版中新增的。
    """
    article_id = article.partition(':')[-1]
    if conn.sadd("voted:" + article_id, user):
        if flags:
            conn.zincrby('score:', VOTE_SCORE, article)
            conn.hincrby(article, 'votes', 1)
        else:
            conn.zincrby('score:', -VOTE_SCORE, article)
            conn.hincrby(article, 'votes', -1)


# 文章发布
def post_article(conn, user, title, link):
    article_id = str(conn.incr('article:'))

    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    now = time.time()
    article = 'article:' + article_id
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1,
        'blacks': 0,
    })
    conn.zadd('score:', article, now + VOTE_SCORE)
    conn.zadd('time:', article, now)

    return article_id


# 获取文章列表
def get_articles(conn, page, order='score:'):
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    ids = conn.zrevrange(order, start, end)
    articles = []
    pipeline = conn.pipeline()

    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)

    return articles


# 群组功能
def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)

    for group in to_remove:
        conn.srem('group:' + group, article)


# 群组文章排序
def get_group_articles(conn, group, page, order = 'score:'):
    key = order + group
    if not conn.exists(key):
        conn.zinterstore(key,
                         ['group:' + group, order],
                         aggregate='max')
        conn.expire(key, 60)
    return get_articles(conn, page, key)


# 调换支持票与反对票
def change_votes(conn, article):
    # 获取文章支持票与反对票
    dates = conn.hgetall(article)
    votes = dates['votes'] - dates['blacks']
    conn.zincrby('score:', VOTE_SCORE * votes, article)


"""
第2章
"""


# 令牌检查
def check_token(conn,token):
    return conn.hget('login:', token)


# 更新令牌
def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)

    if item:
        # conn.zadd('viewed:' + token, item, timestamp)
        # conn.zremrangebyrank('viewed:' + token, 0, -26)
        # 使用列表
        conn.lpush('viewed:' + token, item)
        conn.ltrim('viewed:' + token, 0, 24)


# 清理回话，只保留最新的1000万次回话
QUIT = False
LIMIT = 10000000


def clean_sessions(conn):
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue

        end_index = min(size - LIMIT, 100)
        tokens = conn.zrange('recent:', 0, end_index - 1)
        session_keys = []

        for token in tokens:
            session_keys.append('viewed:' + token)
            session_keys.append('cart:' + token)

        conn.delete(*session_keys)
        conn.hdel('login:', *tokens)
        conn.zrem('recent:', * tokens)


# 购物车处理
def add_to_cart(conn, session, item, count):
    if count <= 0:
        conn.hrem('cart:' + session, item)
    else:
        conn.hset('cart:' + session, item, count)


# 网页缓存
def cache_request(conn, request, callback):
    if not can_cache(conn, request):
        return callback(request)

    page_key = 'cache:' + hash_request(request)
    content = conn.get(page_key)

    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300) # 300秒

    return content


def can_cache(request):
    pass


def hash_request(request):
    pass


# 恢复因故障而中断处理的日志
def process_logs(conn, path, callback):
    current_file, offset = conn.mget('process:file', 'process:position')
    pipe = conn.pipeline()

    # 更新处理进度
    def update_process():
        pipe.mset({'process:file': filename, 'process:position': offset})
        pipe.execute()

    for filename in sorted(os.listdir(path)):
        if filename < current_file:
            continue

        fd = open(os.path.join(path, filename), 'rb')
        if fd == current_file:
            fd.seek(int(offset, 10))
        else:
            offset = 0

        for lno, line in enumerate(fd):
            callback(pipe, line)
            offset += int(offset, 10) + len(line)

            if not (lno+1) % 1000:
                update_process()
        update_process()
        fd.close()

