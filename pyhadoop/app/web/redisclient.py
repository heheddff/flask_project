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


# 检测从服务器是否完成同步
def wait_for_sync(mconn, sconn):
    import uuid
    identifier = str(uuid.uuid4())
    mconn.zadd('sync:wait', identifier, time.time())
    # master_link_status: 连接状态（up或者down）
    while not sconn.info()['master_link_status'] != 'up':
        time.sleep(.001)

    while not sconn.zscore('sync:wait', identifier):
        time.sleep(.001)

    deadline = time.time() + 1.01
    while time.time() < deadline:
        # aof_pending_bio_fsync:
        # 在后台IO队列中等待fsync处理的任务数
        # 0表示从服务器已处理完所有任务，即同步完成
        if sconn.info()['aof_pending_bio_fsync'] == 0:
            break
        time.sleep(.001)

    mconn.zrem('sync:wait', identifier)
    mconn.zremrangebyscore('sync:wait', 0, time.time() - 900)

"""
redis_version => 4.0.14
redis_git_sha1 => 0
redis_git_dirty => 0
redis_build_id => 9227baf766cd3177
redis_mode => standalone
os => Linux 2.6.32-754.12.1.el6.x86_64 x86_64
arch_bits => 64
multiplexing_api => epoll
atomicvar_api => sync-builtin
gcc_version => 4.4.7
process_id => 18737
run_id => c0cefc1bf71378047f53af8e5ef285bba7792545
tcp_port => 6380
uptime_in_seconds => 6150975
uptime_in_days => 71
hz => 10
lru_clock => 4965317
executable => /usr/local/redis/src/redis-server
config_file => /usr/local/redis/conf/6380.conf
connected_clients => 3
client_longest_output_list => 0
client_biggest_input_buf => 0
blocked_clients => 0
used_memory => 2026448
used_memory_human => 1.93M
used_memory_rss => 10113024
used_memory_rss_human => 9.64M
used_memory_peak => 2251856
used_memory_peak_human => 2.15M
used_memory_peak_perc => 89.99%
used_memory_overhead => 1959958
used_memory_startup => 786680
used_memory_dataset => 66490
used_memory_dataset_perc => 5.36%
total_system_memory => 33787936768
total_system_memory_human => 31.47G
used_memory_lua => 37888
used_memory_lua_human => 37.00K
maxmemory => 0
maxmemory_human => 0B
maxmemory_policy => noeviction
mem_fragmentation_ratio => 4.99
mem_allocator => jemalloc-4.0.3
active_defrag_running => 0
lazyfree_pending_objects => 0
loading => 0
rdb_changes_since_last_save => 0
rdb_bgsave_in_progress => 0
rdb_last_save_time => 1565232635
rdb_last_bgsave_status => ok
rdb_last_bgsave_time_sec => 0
rdb_current_bgsave_time_sec => -1
rdb_last_cow_size => 6647808
aof_enabled => 0
aof_rewrite_in_progress => 0
aof_rewrite_scheduled => 0
aof_last_rewrite_time_sec => -1
aof_current_rewrite_time_sec => -1
aof_last_bgrewrite_status => ok
aof_last_write_status => ok
aof_last_cow_size => 0
total_connections_received => 12326
total_commands_processed => 21895861
instantaneous_ops_per_sec => 3
total_net_input_bytes => 1017660671
total_net_output_bytes => 3179047631
instantaneous_input_kbps => 0.12
instantaneous_output_kbps => 0.03
rejected_connections => 0
sync_full => 2
sync_partial_ok => 0
sync_partial_err => 2
expired_keys => 2
expired_stale_perc => 0.0
expired_time_cap_reached_count => 0
evicted_keys => 0
keyspace_hits => 12770
keyspace_misses => 93
pubsub_channels => 1
pubsub_patterns => 0
latest_fork_usec => 807
migrate_cached_sockets => 0
slave_expires_tracked_keys => 0
active_defrag_hits => 0
active_defrag_misses => 0
active_defrag_key_hits => 0
active_defrag_key_misses => 0
role => master
connected_slaves => 2
slave0 => {'ip': '127.0.0.1', 'port': 6379, 'state': 'online', 'offset': 420728750, 'lag': 0}
slave1 => {'ip': '127.0.0.1', 'port': 6381, 'state': 'online', 'offset': 420728750, 'lag': 0}
master_replid => dda28c7ae1e6c9657397c7b2c552c1295974ff74
master_replid2 => 0
master_repl_offset => 420728750
second_repl_offset => -1
repl_backlog_active => 1
repl_backlog_size => 1048576
repl_backlog_first_byte_offset => 419680175
repl_backlog_histlen => 1048576
used_cpu_sys => 3982.01
used_cpu_user => 1727.93
used_cpu_sys_children => 7.56
used_cpu_user_children => 2.49
cluster_enabled => 0
db0 => {'keys': 138, 'expires': 0, 'avg_ttl': 0}
http://www.redis.cn/commands/info.html
"""
"""
redis_version: Redis 服务器版本
redis_git_sha1: Git SHA1
redis_git_dirty: Git dirty flag
redis_build_id: 构建ID
redis_mode: 服务器模式（standalone，sentinel或者cluster）
os: Redis 服务器的宿主操作系统
arch_bits: 架构（32 或 64 位）
multiplexing_api: Redis 所使用的事件处理机制
atomicvar_api: Redis使用的Atomicvar API
gcc_version: 编译 Redis 时所使用的 GCC 版本
process_id: 服务器进程的 PID
run_id: Redis 服务器的随机标识符（用于 Sentinel 和集群）
tcp_port: TCP/IP 监听端口
uptime_in_seconds: 自 Redis 服务器启动以来，经过的秒数
uptime_in_days: 自 Redis 服务器启动以来，经过的天数
hz: 服务器的频率设置
lru_clock: 以分钟为单位进行自增的时钟，用于 LRU 管理
executable: 服务器的可执行文件路径
config_file: 配置文件路径
下面是所有 clients 相关的信息:

connected_clients: 已连接客户端的数量（不包括通过从属服务器连接的客户端）
client_longest_output_list: 当前连接的客户端当中，最长的输出列表
client_biggest_input_buf: 当前连接的客户端当中，最大输入缓存
blocked_clients: 正在等待阻塞命令（BLPOP、BRPOP、BRPOPLPUSH）的客户端的数量
下面是所有 memory 相关的信息:

used_memory: 由 Redis 分配器分配的内存总量，以字节（byte）为单位
used_memory_human: 以人类可读的格式返回 Redis 分配的内存总量
used_memory_rss: 从操作系统的角度，返回 Redis 已分配的内存总量（俗称常驻集大小）。这个值和 top 、 ps 等命令的输出一致。
used_memory_peak: Redis 的内存消耗峰值（以字节为单位）
used_memory_peak_human: 以人类可读的格式返回 Redis 的内存消耗峰值
used_memory_peak_perc: 使用内存占峰值内存的百分比
used_memory_overhead: 服务器为管理其内部数据结构而分配的所有开销的总和（以字节为单位）
used_memory_startup: Redis在启动时消耗的初始内存大小（以字节为单位）
used_memory_dataset: 以字节为单位的数据集大小（used_memory减去used_memory_overhead）
used_memory_dataset_perc: used_memory_dataset占净内存使用量的百分比（used_memory减去used_memory_startup）
total_system_memory: Redis主机具有的内存总量
total_system_memory_human: 以人类可读的格式返回 Redis主机具有的内存总量
used_memory_lua: Lua 引擎所使用的内存大小（以字节为单位）
used_memory_lua_human: 以人类可读的格式返回 Lua 引擎所使用的内存大小
maxmemory: maxmemory配置指令的值
maxmemory_human: 以人类可读的格式返回 maxmemory配置指令的值
maxmemory_policy: maxmemory-policy配置指令的值
mem_fragmentation_ratio: used_memory_rss 和 used_memory 之间的比率
mem_allocator: 在编译时指定的， Redis 所使用的内存分配器。可以是 libc 、 jemalloc 或者 tcmalloc 。
active_defrag_running: 指示活动碎片整理是否处于活动状态的标志
lazyfree_pending_objects: 等待释放的对象数（由于使用ASYNC选项调用UNLINK或FLUSHDB和FLUSHALL）
在理想情况下， used_memory_rss 的值应该只比 used_memory 稍微高一点儿。

当 rss > used ，且两者的值相差较大时，表示存在（内部或外部的）内存碎片。

内存碎片的比率可以通过 mem_fragmentation_ratio 的值看出。

当 used > rss 时，表示 Redis 的部分内存被操作系统换出到交换空间了，在这种情况下，操作可能会产生明显的延迟。

由于Redis无法控制其分配的内存如何映射到内存页，因此常住内存（used_memory_rss）很高通常是内存使用量激增的结果。

当 Redis 释放内存时，内存将返回给分配器，分配器可能会，也可能不会，将内存返还给操作系统。

如果 Redis 释放了内存，却没有将内存返还给操作系统，那么 used_memory 的值可能和操作系统显示的 Redis 内存占用并不一致。

查看 used_memory_peak 的值可以验证这种情况是否发生。

要获得有关服务器内存的其他内省信息，可以参考MEMORY STATS和MEMORY DOCTOR。

下面是所有 persistence 相关的信息:

loading: 指示转储文件（dump）的加载是否正在进行的标志
rdb_changes_since_last_save: 自上次转储以来的更改次数
rdb_bgsave_in_progress: 指示RDB文件是否正在保存的标志
rdb_last_save_time: 上次成功保存RDB的基于纪年的时间戳
rdb_last_bgsave_status: 上次RDB保存操作的状态
rdb_last_bgsave_time_sec: 上次RDB保存操作的持续时间（以秒为单位）
rdb_current_bgsave_time_sec: 正在进行的RDB保存操作的持续时间（如果有）
rdb_last_cow_size: 上次RDB保存操作期间copy-on-write分配的字节大小
aof_enabled: 表示AOF记录已激活的标志
aof_rewrite_in_progress: 表示AOF重写操作正在进行的标志
aof_rewrite_scheduled: 表示一旦进行中的RDB保存操作完成，就会安排进行AOF重写操作的标志
aof_last_rewrite_time_sec: 上次AOF重写操作的持续时间，以秒为单位
aof_current_rewrite_time_sec: 正在进行的AOF重写操作的持续时间（如果有）
aof_last_bgrewrite_status: 上次AOF重写操作的状态
aof_last_write_status: 上一次AOF写入操作的状态
aof_last_cow_size: 上次AOF重写操作期间copy-on-write分配的字节大小
changes_since_last_save指的是从上次调用SAVE或者BGSAVE以来，在数据集中产生某种变化的操作的数量。

如果启用了AOF，则会添加以下这些额外的字段：

aof_current_size: 当前的AOF文件大小
aof_base_size: 上次启动或重写时的AOF文件大小
aof_pending_rewrite: 指示AOF重写操作是否会在当前RDB保存操作完成后立即执行的标志。
aof_buffer_length: AOF缓冲区大小
aof_rewrite_buffer_length: AOF重写缓冲区大小
aof_pending_bio_fsync: 在后台IO队列中等待fsync处理的任务数
aof_delayed_fsync: 延迟fsync计数器
如果正在执行加载操作，将会添加这些额外的字段：

loading_start_time: 加载操作的开始时间（基于纪元的时间戳）
loading_total_bytes: 文件总大小
loading_loaded_bytes: 已经加载的字节数
loading_loaded_perc: 已经加载的百分比
loading_eta_seconds: 预计加载完成所需的剩余秒数
下面是所有 stats 相关的信息:

total_connections_received: 服务器接受的连接总数
total_commands_processed: 服务器处理的命令总数
instantaneous_ops_per_sec: 每秒处理的命令数
rejected_connections: 由于maxclients限制而拒绝的连接数
expired_keys: key到期事件的总数
evicted_keys: 由于maxmemory限制而导致被驱逐的key的数量
keyspace_hits: 在主字典中成功查找到key的次数
keyspace_misses: 在主字典中查找key失败的次数
pubsub_channels: 拥有客户端订阅的全局pub/sub通道数
pubsub_patterns: 拥有客户端订阅的全局pub/sub模式数
latest_fork_usec: 最新fork操作的持续时间，以微秒为单位
下面是所有 replication 相关的信息:

role: 如果实例不是任何节点的从节点，则值是”master”，如果实例从某个节点同步数据，则是”slave”。 请注意，一个从节点可以是另一个从节点的主节点（菊花链）。
如果实例是从节点，则会提供以下这些额外字段：

master_host: 主节点的Host名称或IP地址
master_port: 主节点监听的TCP端口
master_link_status: 连接状态（up或者down）
master_last_io_seconds_ago: 自上次与主节点交互以来，经过的秒数
master_sync_in_progress: 指示主节点正在与从节点同步
如果SYNC操作正在进行，则会提供以下这些字段：

master_sync_left_bytes: 同步完成前剩余的字节数
master_sync_last_io_seconds_ago: 在SYNC操作期间自上次传输IO以来的秒数
如果主从节点之间的连接断开了，则会提供一个额外的字段：

master_link_down_since_seconds: 自连接断开以来，经过的秒数
以下字段将始终提供：

connected_slaves: 已连接的从节点数
对每个从节点，将会添加以下行：

slaveXXX: id，地址，端口号，状态
下面是所有 cpu 相关的信息:

used_cpu_sys: 由Redis服务器消耗的系统CPU
used_cpu_user: 由Redis服务器消耗的用户CPU
used_cpu_sys_children: 由后台进程消耗的系统CPU
used_cpu_user_children: 由后台进程消耗的用户CPU
commandstats部分提供基于命令类型的统计，包含调用次数，这些命令消耗的总CPU时间，以及每个命令执行所消耗的平均CPU。

对于每一个命令类型，添加以下行：

cmdstat_XXX: calls=XXX,usec=XXX,usec_per_call=XXX
cluster部分当前只包含一个唯一的字段：

cluster_enabled: 表示已启用Redis集群
keyspace部分提供有关每个数据库的主字典的统计，统计信息是key的总数和过期的key的总数。

对于每个数据库，提供以下行：

dbXXX: keys=XXX,expires=XXX
"""