#!/usr/bin/env python3
"""
pg_redis.py — Redis engine for Ninja Console's multi-engine console.

The editor is a Redis command console: type any command (GET, HGETALL, KEYS,
SCAN, LRANGE, TYPE, …) and see the result in the grid. The schema tree lists
key prefixes (grouped by the first ':' segment) so you can browse the keyspace.

Common engine interface: connect / run / tables / columns / classify / version /
close. The redis package is imported lazily (optional).
"""
import shlex

CONNECT_TIMEOUT = 6

ENGINE = 'redis'
DIALECT = 'redis'

# Commands that mutate — feed the write-safety gate (reads pass straight through).
_WRITE = {
    'SET', 'SETEX', 'PSETEX', 'SETNX', 'MSET', 'MSETNX', 'APPEND', 'SETRANGE', 'GETSET', 'GETDEL',
    'DEL', 'UNLINK', 'EXPIRE', 'PEXPIRE', 'EXPIREAT', 'PERSIST', 'RENAME', 'RENAMENX', 'MOVE', 'COPY',
    'RESTORE', 'INCR', 'INCRBY', 'INCRBYFLOAT', 'DECR', 'DECRBY',
    'HSET', 'HMSET', 'HSETNX', 'HDEL', 'HINCRBY', 'HINCRBYFLOAT',
    'LPUSH', 'RPUSH', 'LPUSHX', 'RPUSHX', 'LPOP', 'RPOP', 'LSET', 'LREM', 'LTRIM', 'LINSERT', 'RPOPLPUSH',
    'SADD', 'SREM', 'SPOP', 'SMOVE', 'SINTERSTORE', 'SUNIONSTORE', 'SDIFFSTORE',
    'ZADD', 'ZREM', 'ZINCRBY', 'ZPOPMIN', 'ZPOPMAX', 'ZREMRANGEBYRANK', 'ZREMRANGEBYSCORE',
    'XADD', 'XDEL', 'XTRIM', 'PFADD', 'PFMERGE', 'GEOADD', 'BITOP', 'SETBIT',
    'FLUSHDB', 'FLUSHALL', 'SWAPDB',
}


def _driver():
    try:
        import redis
        return redis
    except ImportError as e:
        raise RuntimeError("Redis support needs the redis package:  pip install redis") from e


def connect(info):
    redis = _driver()
    host = info.get('host', 'localhost')
    port = int(info.get('port') or info.get('localPort') or 6379)
    pw = info.get('password') or None
    dbnum = 0
    d = str(info.get('database') or '').strip()
    if d.isdigit():
        dbnum = int(d)          # `database` holds the numeric Redis DB index if set
    r = redis.Redis(host=host, port=port, password=pw, db=dbnum, decode_responses=True,
                    socket_timeout=CONNECT_TIMEOUT, socket_connect_timeout=CONNECT_TIMEOUT)
    r.ping()                    # fail fast on unreachable / auth error
    return r


def _format(res):
    """Map a Redis reply to the console's result envelope."""
    if res is None:
        return {'result_kind': 'status', 'status': '(nil)', 'rowcount': 0}
    if isinstance(res, bool):
        return {'result_kind': 'status', 'status': str(res), 'rowcount': 0}
    if isinstance(res, (str, int, float, bytes)):
        return {'result_kind': 'rows', 'columns': ['result'], 'rows': [[res]]}
    if isinstance(res, dict):        # HGETALL, CONFIG GET, …
        return {'result_kind': 'rows', 'columns': ['field', 'value'],
                'rows': [[k, v] for k, v in res.items()]}
    if isinstance(res, (list, tuple, set)):
        res = list(res)
        # SCAN returns [cursor, [keys]]
        if len(res) == 2 and isinstance(res[1], list) and str(res[0]).lstrip('-').isdigit():
            return {'result_kind': 'rows', 'columns': ['key'], 'rows': [[k] for k in res[1]]}
        return {'result_kind': 'rows', 'columns': ['value'], 'rows': [[x] for x in res]}
    return {'result_kind': 'rows', 'columns': ['result'], 'rows': [[str(res)]]}


def run(handle, text, info):
    args = shlex.split(text.strip())
    if not args:
        raise RuntimeError('type a Redis command, e.g.  GET mykey   or   SCAN 0 MATCH user:* COUNT 50')
    return _format(handle.execute_command(*args))


def tables(handle, info):
    """Key prefixes (first ':' segment) as browsable groups — a lightweight
    'schema' for a schemaless store. Samples up to ~2000 keys via SCAN."""
    seen, cursor, scanned = {}, 0, 0
    while scanned < 2000:
        cursor, keys = handle.scan(cursor=cursor, count=500)
        for k in keys:
            pre = k.split(':', 1)[0] if ':' in k else k
            seen[pre] = seen.get(pre, 0) + 1
        scanned += len(keys)
        if cursor == 0:
            break
    return [{'schema': 'keys', 'table': f'{p}:*', 'kind': f'{n} keys'}
            for p, n in sorted(seen.items())]


def columns(handle, info, schema, table):
    return []                    # not applicable to Redis keys


def classify(text):
    try:
        cmd = (shlex.split(text.strip())[:1] or [''])[0].upper()
    except Exception:
        cmd = (text.strip().split()[:1] or [''])[0].upper()
    return 'write' if cmd in _WRITE else 'read'


def version(handle):
    try:
        return 'Redis ' + handle.info('server').get('redis_version', '?')
    except Exception:
        return 'Redis'


def close(handle):
    try:
        handle.close()
    except Exception:
        try:
            handle.connection_pool.disconnect()
        except Exception:
            pass
