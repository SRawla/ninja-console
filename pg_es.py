#!/usr/bin/env python3
"""
pg_es.py — Elasticsearch / OpenSearch engine for Ninja Console's multi-engine console.

Talks to the (forwarded) HTTP endpoint using only the standard library — NO extra
dependency. Two query modes, auto-detected from the editor text:

  * SQL text            -> POST /_sql   -> tabular rows  (result_kind='rows' -> grid)
  * JSON (starts '{')   -> POST /<idx>/_search -> documents (result_kind='docs' -> viewer)

Common engine interface: connect / run / tables / columns / classify / version / close.
"""
import base64
import json
import re
import urllib.error
import urllib.request

MAX_ROWS = 5000
TIMEOUT = 30

ENGINE = 'es'
DIALECT = 'es'


def connect(info):
    """Handle is just the base URL + optional basic-auth header (HTTP is stateless)."""
    host = info.get('host', 'localhost')
    port = int(info.get('port') or info.get('localPort') or 9200)
    scheme = info.get('scheme') or 'http'
    headers = {'Content-Type': 'application/json'}
    user, pw = info.get('username'), info.get('password')
    if user:
        token = base64.b64encode(f"{user}:{pw or ''}".encode()).decode()
        headers['Authorization'] = 'Basic ' + token
    handle = {'base': f'{scheme}://{host}:{port}', 'headers': headers,
              'index': (info.get('database') or '').strip()}
    version(handle)          # probe so connect fails fast if unreachable
    return handle


def _req(handle, method, path, body=None):
    url = handle['base'] + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=handle['headers'])
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            raw = r.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        detail = e.read().decode('utf-8', 'replace')
        try:
            j = json.loads(detail)
            msg = j.get('error', {}).get('reason') or j.get('error') or detail
        except Exception:
            msg = detail
        raise RuntimeError(f'ES {e.code}: {str(msg)[:300]}')
    except urllib.error.URLError as e:
        raise RuntimeError(f'ES unreachable: {e.reason}')
    return json.loads(raw) if raw.strip() else {}


def run(handle, text, info):
    """JSON body -> _search (docs); anything else -> _sql (rows)."""
    text = (text or '').strip().rstrip(';').strip()
    if text.startswith('{'):
        try:
            dsl = json.loads(text)
        except json.JSONDecodeError as e:
            raise RuntimeError(f'invalid JSON query: {e}')
        idx = dsl.pop('index', None) or handle.get('index') or '_all'
        dsl.setdefault('size', min(MAX_ROWS, 100))
        res = _req(handle, 'POST', f'/{idx}/_search', dsl)
        hits = (res.get('hits', {}) or {}).get('hits', []) or []
        docs = [{'_index': h.get('_index'), '_id': h.get('_id'), '_score': h.get('_score'),
                 **(h.get('_source') or {})} for h in hits]
        return {'result_kind': 'docs', 'docs': docs}
    # Elasticsearch SQL -> tabular
    res = _req(handle, 'POST', '/_sql?format=json', {'query': text, 'fetch_size': MAX_ROWS})
    cols = [c.get('name') for c in res.get('columns', [])]
    rows = res.get('rows', []) or []
    return {'result_kind': 'rows', 'columns': cols, 'rows': rows,
            'truncated': bool(res.get('cursor'))}


def tables(handle, info):
    """Indices as tables under a single 'indices' group (ES has no schema layer)."""
    rows = _req(handle, 'GET', '/_cat/indices?format=json&h=index')
    out = []
    for r in rows:
        idx = r.get('index', '')
        if idx.startswith('.'):        # hide internal indices (.kibana, .security…)
            continue
        out.append({'schema': 'indices', 'table': idx, 'kind': 'index'})
    return sorted(out, key=lambda x: x['table'])


def columns(handle, info, schema, table):
    mp = _req(handle, 'GET', f'/{table}/_mapping')
    # {<index>: {mappings: {properties: {field: {type: ...}}}}}
    props = {}
    for idx in mp.values():
        props.update(((idx.get('mappings') or {}).get('properties') or {}))
    out = []

    def walk(prefix, fields):
        for name, spec in fields.items():
            full = f'{prefix}{name}'
            if 'properties' in spec:                # nested/object
                walk(full + '.', spec['properties'])
            else:
                out.append({'name': full, 'type': spec.get('type', 'object'), 'nullable': True})
    walk('', props)
    return sorted(out, key=lambda x: x['name'])


_ES_WRITE = re.compile(r'(?is)"?(index|create|update|delete|bulk)"?\s*:')


def classify(text):
    # The queries this console issues (_sql SELECT, _search) are read-only.
    return 'read'


def version(handle):
    info = _req(handle, 'GET', '/')
    v = (info.get('version', {}) or {}).get('number', '?')
    dist = (info.get('version', {}) or {}).get('distribution', 'elasticsearch')
    return f'{dist} {v}'


def close(handle):
    pass
