#!/usr/bin/env python3
"""
pg_mongo.py — MongoDB engine for pg-console's multi-engine console.

MongoDB has no query-string language, so the editor takes a small JSON spec:

  {"db":"<database>", "find":"<collection>", "filter":{...},
   "projection":{...}, "sort":{...}, "limit":50}
  {"db":"<database>", "aggregate":"<collection>", "pipeline":[...]}

Results are documents (result_kind='docs') shown in the JSON viewer. The pymongo
import is LAZY (only when a Mongo source is used). directConnection=True is used
so a single port-forwarded replica-set member is queried directly instead of the
driver trying to reach internal cluster hostnames.
"""
import json

MAX_ROWS = 2000
CONNECT_TIMEOUT = 8

ENGINE = 'mongo'
DIALECT = 'mongo'

_WRITE_KEYS = {'insert', 'insertone', 'insertmany', 'update', 'updateone', 'updatemany',
               'delete', 'deleteone', 'deletemany', 'drop', 'replaceone', 'findandmodify',
               'remove', 'save', 'createindex', 'renamecollection'}


def _driver():
    try:
        from pymongo import MongoClient
        return MongoClient
    except ImportError as e:
        raise RuntimeError("Mongo support needs the pymongo package:  pip install pymongo") from e


def connect(info):
    MongoClient = _driver()
    host = info.get('host', 'localhost')
    port = int(info.get('port') or info.get('localPort') or 27017)
    user = info.get('username') or None
    pw = info.get('password') or None
    dbname = (info.get('database') or '').strip() or None
    kw = dict(host=host, port=port, directConnection=True,
              serverSelectionTimeoutMS=CONNECT_TIMEOUT * 1000,
              connectTimeoutMS=CONNECT_TIMEOUT * 1000)
    if user:
        kw.update(username=user, password=pw,
                  authSource=info.get('authSource') or dbname or 'admin')
    client = MongoClient(**kw)
    client.admin.command('ping')          # fail fast if unreachable / auth wrong
    return {'client': client, 'dbname': dbname}


def _spec(text):
    text = (text or '').strip()
    try:
        s = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError('Mongo query must be a JSON spec, e.g. '
                           '{"db":"mydb","find":"mycoll","limit":50} — ' + str(e))
    if not isinstance(s, dict):
        raise RuntimeError('Mongo query must be a JSON object spec.')
    return s


def run(handle, text, info):
    s = _spec(text)
    client = handle['client']
    dbname = s.get('db') or handle.get('dbname')
    if not dbname:
        raise RuntimeError('specify the database: {"db":"...","find":"<collection>",...}')
    db = client[dbname]
    if 'aggregate' in s:
        docs = list(db[s['aggregate']].aggregate(s.get('pipeline', [])))[:MAX_ROWS]
    else:
        coll = s.get('find') or s.get('collection')
        if not coll:
            raise RuntimeError('specify a collection: {"find":"<collection>", ...}')
        cur = db[coll].find(s.get('filter', {}), s.get('projection'))
        if s.get('sort'):
            cur = cur.sort(list(s['sort'].items()))
        docs = list(cur.limit(int(s.get('limit', 50))))
    # ObjectId/datetime/etc. are stringified by the server's JSON default on send.
    return {'result_kind': 'docs', 'docs': docs}


def tables(handle, info):
    client = handle['client']
    dbname = handle.get('dbname')
    dbs = [dbname] if dbname else [d for d in client.list_database_names()
                                   if d not in ('admin', 'local', 'config')]
    out = []
    for db in dbs:
        try:
            for coll in client[db].list_collection_names():
                out.append({'schema': db, 'table': coll, 'kind': 'collection'})
        except Exception:
            pass
    return sorted(out, key=lambda x: (x['schema'], x['table']))


def columns(handle, info, schema, table):
    fields = {}
    for d in handle['client'][schema][table].find().limit(25):
        for k, v in d.items():
            fields.setdefault(k, type(v).__name__)
    return [{'name': k, 'type': t, 'nullable': True} for k, t in sorted(fields.items())]


def classify(text):
    try:
        s = json.loads(text)
        keys = {str(k).lower() for k in (s.keys() if isinstance(s, dict) else [])}
    except Exception:
        return 'read'
    return 'write' if keys & _WRITE_KEYS else 'read'


def version(handle):
    try:
        return 'MongoDB ' + handle['client'].server_info().get('version', '?')
    except Exception:
        return 'MongoDB'


def close(handle):
    try:
        handle['client'].close()
    except Exception:
        pass
