#!/usr/bin/env python3
"""
pg_cql.py — Cassandra / Scylla (CQL) engine for pg-console's multi-engine console.

Implements the common engine interface the console dispatches to for non-Postgres
data sources:

    connect(info) -> handle (a cassandra Session)
    run(handle, text, info) -> {result_kind, columns?, rows?, status?, ...}
    tables(handle, info) -> [{schema, table, kind}]
    columns(handle, info, schema, table) -> [{name, type, nullable}]
    classify(text) -> 'read' | 'write'
    version(handle) -> str
    close(handle)

The cassandra-driver import is LAZY (only when a CQL source is used), so
Postgres-only users never need it. Nothing here stores secrets — creds come from
the (discovered) source dict and are used only to open the session.
"""
import re

MAX_ROWS = 5000
CONNECT_TIMEOUT = 8
QUERY_TIMEOUT = 30

ENGINE = 'cql'
DIALECT = 'cql'          # UI hint: SQL-shaped editor

_CQL_WRITE = re.compile(
    r'(?is)\b(insert|update|delete|truncate|drop|alter|create|grant|revoke|use)\b')
_COMMENT = re.compile(r'--[^\n]*|/\*.*?\*/', re.S)


def _driver():
    """Import cassandra-driver lazily. On Python 3.12+ the driver won't import by
    default (asyncore was removed; the libev C-extension isn't built here), and it
    never auto-selects its own asyncio reactor. So we shim the libev import to the
    pure-python AsyncioConnection BEFORE `cassandra.cluster` loads, and also pass
    connection_class explicitly. Returns (Cluster, PlainTextAuthProvider, AsyncioConnection)."""
    try:
        import sys as _sys
        import types as _types
        from cassandra.io.asyncioreactor import AsyncioConnection
        if 'cassandra.io.libevreactor' not in _sys.modules:
            try:
                import cassandra.io.libevreactor  # noqa: F401  (works only with the C-ext)
            except Exception:
                shim = _types.ModuleType('cassandra.io.libevreactor')
                shim.LibevConnection = AsyncioConnection
                _sys.modules['cassandra.io.libevreactor'] = shim
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        return Cluster, PlainTextAuthProvider, AsyncioConnection
    except ImportError as e:
        raise RuntimeError(
            "CQL support needs the cassandra-driver package. Install it with:  "
            "pip install cassandra-driver") from e


def connect(info):
    """Open a CQL session to the (forwarded) host/port. `database` is the keyspace
    (may be blank — the user can then USE/pick one from the schema tree)."""
    Cluster, PlainTextAuthProvider, AsyncioConnection = _driver()
    host = info.get('host', 'localhost')
    port = int(info.get('port') or info.get('localPort') or 9042)
    user = info.get('username') or 'cassandra'
    pw = info.get('password', '') or 'cassandra'
    keyspace = (info.get('database') or info.get('keyspace') or '').strip() or None

    cluster = Cluster(
        [host], port=port, connection_class=AsyncioConnection,
        auth_provider=PlainTextAuthProvider(username=user, password=pw),
        connect_timeout=CONNECT_TIMEOUT, control_connection_timeout=CONNECT_TIMEOUT)
    session = cluster.connect(keyspace) if keyspace else cluster.connect()
    session.default_timeout = QUERY_TIMEOUT
    session._pgc_cluster = cluster        # keep a ref so close() can shut it down
    return session


def run(handle, text, info):
    """Execute a CQL statement. Returns rows (SELECT) or a status (writes/DDL)."""
    from cassandra.query import SimpleStatement
    rs = handle.execute(SimpleStatement(text, fetch_size=MAX_ROWS + 1), timeout=QUERY_TIMEOUT)
    cols = list(rs.column_names) if rs.column_names else None
    if not cols:
        return {'result_kind': 'status', 'status': 'OK', 'rowcount': -1}
    rows, truncated = [], False
    for row in rs:
        if len(rows) >= MAX_ROWS:
            truncated = True
            break
        rows.append(list(row))            # namedtuple -> list; JSON default stringifies UUID/datetime
    return {'result_kind': 'rows', 'columns': cols, 'rows': rows, 'truncated': truncated}


def tables(handle, info):
    """User keyspaces -> tables (schema = keyspace). Excludes system keyspaces."""
    out = []
    for r in handle.execute("SELECT keyspace_name, table_name FROM system_schema.tables"):
        ks = r.keyspace_name
        if ks.startswith('system'):
            continue
        out.append({'schema': ks, 'table': r.table_name, 'kind': 'table'})
    return sorted(out, key=lambda x: (x['schema'], x['table']))


def columns(handle, info, schema, table):
    q = ("SELECT column_name, type, kind FROM system_schema.columns "
         "WHERE keyspace_name=%s AND table_name=%s")
    out = []
    for r in handle.execute(q, (schema, table)):
        is_key = r.kind in ('partition_key', 'clustering')
        label = r.type + (' ·PK' if r.kind == 'partition_key'
                          else ' ·CK' if r.kind == 'clustering' else '')
        out.append({'name': r.column_name, 'type': label, 'nullable': not is_key})
    return out


def classify(text):
    stripped = _COMMENT.sub(' ', text or '')
    return 'write' if _CQL_WRITE.search(stripped) else 'read'


def version(handle):
    try:
        r = handle.execute("SELECT release_version FROM system.local").one()
        return 'Cassandra/Scylla ' + (getattr(r, 'release_version', '') or '?')
    except Exception:
        return 'Cassandra/Scylla'


def close(handle):
    try:
        cluster = getattr(handle, '_pgc_cluster', None)
        handle.shutdown()
        if cluster:
            cluster.shutdown()
    except Exception:
        pass
