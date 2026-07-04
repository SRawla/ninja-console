#!/usr/bin/env python3
"""
pg_console.py  -  a local, DataGrip-style Postgres SQL console.

Reads ONLY the generator's artifact (pg-services.json or postgres-credentials.md),
lists your services as data sources, and lets you explicitly Connect to each one
and run SQL against it. Multiple services can be connected at once, each with its
own console + result grid. Single user, localhost only, no build step.

    pip install "psycopg[binary]"
    python pg_console.py                         # auto-finds the artifact under ./pg-artifacts
    python pg_console.py --services .\\pg-artifacts\\postgres-credentials.md
    python pg_console.py --services .\\pg-artifacts\\pg-services.json --port 8765

Then open http://127.0.0.1:8765  (it tries to open your browser for you).

Port-forwards are assumed to already be running. Connect will tell you if one isn't.
"""
import argparse
import configparser
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import uuid

import pg_forward
import pg_aws
import pg_discovery
import pg_pipeline
import pg_cql
import pg_es
import pg_mongo

# Multi-engine registry: non-Postgres data sources dispatch to a per-engine
# module implementing the common interface (connect/run/tables/columns/classify/
# version/close). Postgres keeps its inline psycopg path. Drivers are imported
# lazily inside each module, so they're optional.
ENGINES = {'cql': pg_cql, 'es': pg_es, 'mongo': pg_mongo}
_engine_cache = {}               # service -> {'info': info, 'handle': handle}


def _engine(info):
    return (info.get('engine') or 'postgres').lower()

try:
    import psycopg
except ImportError:
    sys.exit('psycopg is not installed. Run:  pip install "psycopg[binary]"')

MAX_ROWS = 5000
CONNECT_TIMEOUT = 5
FORWARD_TIMEOUT = 20        # seconds to wait for a port-forward to become reachable
KUBECTL = 'kubectl'
AUTO_FORWARD = True
# Okta profiles carry no region (§9 R-REGION); the UI needs an editable default.
# ca-central-1 is where the real clusters live (confirmed on the machine).
DEFAULT_REGION = 'ca-central-1'

SERVICES_PATH = None
_conn_cache = {}                 # service -> {"info": {...}, "conn": connection}
_locks = {}
_locks_guard = threading.Lock()

# The port-forward lifecycle now lives in pg_forward.ForwardManager. It's
# constructed in main() once config (kubectl path, auto-forward) is known,
# and wired to the console via three small callbacks. Until then it's None.
fm = None

def _build_forward_manager():
    """Instantiate the ForwardManager with console callbacks. Called from main()."""
    return pg_forward.ForwardManager(
        get_service_info=_service_info,
        get_context=lambda: (_console_services()[1] or ''),
        invalidate_conn=lambda name: _conn_cache.pop(name, None),
        kubectl=KUBECTL, auto_forward=AUTO_FORWARD, forward_timeout=FORWARD_TIMEOUT,
    )

# --------------------------------------------------------------------------- #
# Per-source safety mode: 'readonly' | 'confirm' | 'unrestricted'
#
# The SERVER is the source of truth, not the browser. A mode set here is
# enforced on every /api/query call regardless of which client/tab is asking,
# and it survives a server restart. This matters: a client-only (localStorage)
# gate is a UX nicety, not a safety boundary — anything hitting the HTTP API
# directly (a stray curl, a second tab, a bug) would bypass it. Persisting and
# enforcing mode server-side closes that gap.
# --------------------------------------------------------------------------- #
MODES = {}                       # service -> 'readonly'|'confirm'|'unrestricted'
DEFAULT_MODE = 'confirm'
_modes_guard = threading.Lock()


# Modes/profiles side-files are SHARED across clusters (spec R13) and must
# resolve even before any artifact is loaded (R1 not-connected boot). Anchor
# them to the artifact's dir when we have one, else the default artifacts dir
# (or cwd) — a stable location that doesn't change when the first artifact loads.
DEFAULT_ARTIFACT_DIR = 'pg-artifacts'


def _state_dir():
    if SERVICES_PATH:
        return os.path.dirname(SERVICES_PATH) or '.'
    return DEFAULT_ARTIFACT_DIR if os.path.isdir(DEFAULT_ARTIFACT_DIR) else '.'


def _modes_path():
    return os.path.join(_state_dir(), '.pg-console-modes.json')


def load_modes():
    global MODES
    try:
        with open(_modes_path(), 'r', encoding='utf-8') as f:
            MODES = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        MODES = {}


def save_modes():
    try:
        with open(_modes_path(), 'w', encoding='utf-8') as f:
            json.dump(MODES, f, indent=2)
    except Exception:
        pass  # best-effort; mode still works for this process's lifetime


def get_mode(name):
    return MODES.get(name, DEFAULT_MODE)


def set_mode(name, mode):
    if mode not in ('readonly', 'confirm', 'unrestricted'):
        raise ValueError(f'invalid mode: {mode}')
    with _modes_guard:
        MODES[name] = mode
        save_modes()


# Any of these appearing as a whole word anywhere in the statement (after
# stripping comments) marks it as a WRITE. Biased toward false positives
# (an unnecessary confirm click) over false negatives (an unconfirmed write
# slipping through) — that's the correct direction to err in a safety gate.
_WRITE_RE = re.compile(
    r'(?is)\b(insert|update|delete|truncate|drop|alter|create|grant|revoke|'
    r'merge|replace|lock|vacuum|reindex|call|copy|refresh)\b'
)
_COMMENT_RE = re.compile(r'--[^\n]*|/\*.*?\*/', re.S)


def classify_sql(sql):
    """Returns 'write' or 'read'. SELECT/SHOW/EXPLAIN/WITH-that-only-selects
    classify as 'read'; anything containing a DDL/DML keyword classifies 'write'."""
    stripped = _COMMENT_RE.sub(' ', sql or '')
    return 'write' if _WRITE_RE.search(stripped) else 'read'


# --------------------------------------------------------------------------- #
# Registry: read either pg-services.json or postgres-credentials.md
# --------------------------------------------------------------------------- #
def load_services():
    path = SERVICES_PATH
    # Not-connected boot (R1): no artifact loaded yet is a NORMAL state, not an
    # error. Callers (services/appservices endpoints, forward context) all treat
    # an empty list / blank context as "nothing to show yet".
    if not path or not os.path.exists(path):
        return [], '', ''
    if path.lower().endswith('.md'):
        return _load_md(path)
    with open(path, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)
    services = data.get('services', [])
    for s in services:
        s.setdefault('kind', 'db')          # backward-compat: pre-kind artifacts are all DB
        # '(assumed)' is a display marker discovery appends when it falls back to
        # the 'postgres' superuser (no username key found). It must never reach
        # psycopg as a literal user name — strip it on load, same as _load_md does.
        if s['kind'] == 'db' and isinstance(s.get('username'), str):
            s['username'] = re.sub(r'\s*\(assumed\)$', '', s['username'])
    return services, data.get('context', ''), data.get('generated', '')


def _load_md(path):
    with open(path, 'r', encoding='utf-8-sig') as f:
        lines = f.read().splitlines()
    context, header_idx = '', None
    for i, l in enumerate(lines):
        low = l.lower()
        if 'context:' in low:
            m = re.search(r'context:\s*`?([^`|]+)`?', l, re.I)
            if m:
                context = m.group(1).strip()
        if l.strip().startswith('|') and 'service' in low and 'local port' in low:
            header_idx = i
            break
    if header_idx is None:
        return [], context, ''

    cols = [c.strip().lower() for c in lines[header_idx].strip().strip('|').split('|')]

    def col(name):
        for j, c in enumerate(cols):
            if name in c:
                return j
        return None

    iS, iN, iP = col('service'), col('namespace'), col('local port')
    iD, iU, iW = col('database'), col('username'), col('password')
    out = []
    for l in lines[header_idx + 1:]:
        s = l.strip()
        if not s.startswith('|'):
            break
        if set(s) <= set('|-: '):           # separator row
            continue
        cells = [c.strip() for c in s.strip().strip('|').split('|')]
        if iS is None or iS >= len(cells) or not cells[iS]:
            continue
        try:
            port = int(cells[iP])
        except (TypeError, ValueError, IndexError):
            continue
        get = lambda i, d='': cells[i] if i is not None and i < len(cells) else d
        out.append({
            'kind': 'db',
            'service': cells[iS],
            'namespace': get(iN),
            'host': 'localhost',
            'localPort': port,
            'remotePort': 5432,
            'database': get(iD, 'postgres') or 'postgres',
            'username': re.sub(r'\s*\(assumed\)$', '', get(iU, 'postgres')) or 'postgres',
            'password': get(iW),
        })
    return out, context, ''


def _service_info(name):
    for s in _current_services():
        if s['service'] == name and s.get('kind', 'db') == 'db':
            return s
    raise KeyError(f'unknown db service: {name}')


def load_db_services():
    services, context, generated = _console_services()
    return [s for s in services if s.get('kind', 'db') == 'db'], context, generated


def load_app_services():
    services, context, generated = _console_services()
    return [s for s in services if s.get('kind') == 'app'], context, generated


def _app_service_info(name):
    services, _, _ = load_app_services()
    for s in services:
        if s['service'] == name:
            return s
    raise KeyError(f'unknown app service: {name}')


def _lock_for(name):
    with _locks_guard:
        if name not in _locks:
            _locks[name] = threading.Lock()
        return _locks[name]


def get_conn(name):
    info = _service_info(name)
    cached = _conn_cache.get(name)
    if cached and cached['info'] == info and not cached['conn'].closed:
        return cached['conn']
    if cached:
        try:
            cached['conn'].close()
        except Exception:
            pass
    conn = psycopg.connect(
        host=info.get('host', 'localhost'),
        port=int(fm.active_port(name) or info['localPort']),
        dbname=info.get('database') or 'postgres',
        user=info['username'],
        password=info.get('password', ''),
        connect_timeout=CONNECT_TIMEOUT,
        autocommit=True,
    )
    _conn_cache[name] = {'info': info, 'conn': conn}
    return conn


def _engine_connect(name, info, mod):
    """Cached connect for a non-Postgres engine. Handle is opaque to the console;
    the engine module knows how to use/close it."""
    cached = _engine_cache.get(name)
    if cached and cached['info'] == info:
        return cached['handle']
    if cached:
        try:
            mod.close(cached['handle'])
        except Exception:
            pass
    port = int(fm.active_port(name) or info['localPort'])
    handle = mod.connect({**info, 'host': info.get('host', 'localhost'), 'port': port})
    _engine_cache[name] = {'info': info, 'handle': handle}
    return handle


# --------------------------------------------------------------------------- #
# Forwarding profiles: developer-authored sets of {service, namespace,
# remotePort, localPort}, saved next to the artifact. Separate from the
# artifact itself (which is machine-discovered) — see requirements §8.
# --------------------------------------------------------------------------- #
def _profiles_path():
    return os.path.join(_state_dir(), '.pg-console-profiles.json')


def load_profiles():
    try:
        with open(_profiles_path(), 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_profile(name, entries):
    if not name or not str(name).strip():
        raise ValueError('profile name is required')
    profiles = load_profiles()
    profiles[name] = entries
    with open(_profiles_path(), 'w', encoding='utf-8') as f:
        json.dump(profiles, f, indent=2)


def delete_profile(name):
    profiles = load_profiles()
    profiles.pop(name, None)
    with open(_profiles_path(), 'w', encoding='utf-8') as f:
        json.dump(profiles, f, indent=2)


# --------------------------------------------------------------------------- #
# DB operations
# --------------------------------------------------------------------------- #
def connect_service(name):
    info = _service_info(name)
    eng = _engine(info)
    started_fwd = fm.ensure_forward(name)     # start tunnel if needed
    try:
        with _lock_for(name):
            if eng == 'postgres':
                conn = get_conn(name)
                with conn.cursor() as cur:
                    cur.execute('select version()')
                    ver = cur.fetchone()[0]
            else:
                ver = ENGINES[eng].version(_engine_connect(name, info, ENGINES[eng]))
    except Exception as e:
        if eng == 'postgres' and isinstance(e, psycopg.Error) and not started_fwd:
            raise RuntimeError(
                str(e).strip() +
                f'  ·  note: something was already listening on :{info["localPort"]} so no new '
                f'port-forward was started — it may be a stale/broken forward. '
                f'Close it (Get-Process kubectl | Stop-Process) and reconnect.')
        raise
    return {'version': ver, 'port': fm.active_port(name) or info['localPort'],
            'declared': info['localPort'], 'database': info.get('database', ''),
            'engine': eng, 'forward': started_fwd}


def disconnect_service(name):
    cached = _conn_cache.pop(name, None)
    if cached:
        try:
            cached['conn'].close()
        except Exception:
            pass
    ecached = _engine_cache.pop(name, None)
    if ecached:
        eng = _engine(ecached['info'])
        try:
            ENGINES[eng].close(ecached['handle'])
        except Exception:
            pass
    fm.stop_forward(name)                     # tear down the tunnel we opened


def run_sql(name, sql, confirmed=False):
    info = _service_info(name)
    eng = _engine(info)
    kind = classify_sql(sql) if eng == 'postgres' else ENGINES[eng].classify(sql)
    mode = get_mode(name)

    if kind == 'write':
        if mode == 'readonly':
            return {'ok': False, 'blocked': True,
                    'error': f'{name} is in Read-only mode — write statements are not allowed.'}
        if mode == 'confirm' and not confirmed:
            return {'ok': False, 'needsConfirm': True, 'kind': 'write', 'sql': sql,
                    'error': 'This statement modifies data. Confirm to run it.'}

    started = time.perf_counter()
    if eng == 'postgres':
        with _lock_for(name):
            conn = get_conn(name)
            sets = []
            with conn.cursor() as cur:
                cur.execute(sql)
                while True:
                    if cur.description:
                        cols = [d.name for d in cur.description]
                        rows = cur.fetchmany(MAX_ROWS + 1)
                        truncated = len(rows) > MAX_ROWS
                        sets.append({'columns': cols, 'rows': rows[:MAX_ROWS],
                                     'rowcount': len(rows[:MAX_ROWS]), 'truncated': truncated})
                    else:
                        sets.append({'columns': None, 'status': cur.statusmessage, 'rowcount': cur.rowcount})
                    if not cur.nextset():
                        break
        return {'ok': True, 'sets': sets, 'elapsed_ms': round((time.perf_counter() - started) * 1000)}

    # non-Postgres engine: normalize its result into the same envelope
    with _lock_for(name):
        res = ENGINES[eng].run(_engine_connect(name, info, ENGINES[eng]), sql, info)
    elapsed = round((time.perf_counter() - started) * 1000)
    rk = res.get('result_kind')
    if rk == 'docs':
        docs = res.get('docs', [])
        return {'ok': True, 'result_kind': 'docs', 'docs': docs,
                'rowcount': len(docs), 'elapsed_ms': elapsed}
    if rk == 'rows':
        rows = res.get('rows', [])
        sets = [{'columns': res.get('columns') or [], 'rows': rows,
                 'rowcount': len(rows), 'truncated': res.get('truncated', False)}]
        return {'ok': True, 'sets': sets, 'elapsed_ms': elapsed}
    # status (writes/DDL)
    return {'ok': True, 'sets': [{'columns': None, 'status': res.get('status', 'OK'),
            'rowcount': res.get('rowcount', -1)}], 'elapsed_ms': elapsed}


def list_tables(name):
    info = _service_info(name)
    eng = _engine(info)
    if eng != 'postgres':
        with _lock_for(name):
            return ENGINES[eng].tables(_engine_connect(name, info, ENGINES[eng]), info)
    sql = """select table_schema, table_name, table_type
             from information_schema.tables
             where table_schema not in ('pg_catalog','information_schema')
             order by table_schema, table_name"""
    with _lock_for(name):
        conn = get_conn(name)
        with conn.cursor() as cur:
            cur.execute(sql)
            return [{'schema': r[0], 'table': r[1], 'kind': r[2]} for r in cur.fetchall()]


def list_columns(name, schema, table):
    info = _service_info(name)
    eng = _engine(info)
    if eng != 'postgres':
        with _lock_for(name):
            return ENGINES[eng].columns(_engine_connect(name, info, ENGINES[eng]), info, schema, table)
    sql = """select column_name, data_type, is_nullable
             from information_schema.columns
             where table_schema = %s and table_name = %s
             order by ordinal_position"""
    with _lock_for(name):
        conn = get_conn(name)
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return [{'name': r[0], 'type': r[1], 'nullable': r[2] == 'YES'} for r in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Connection dashboard state + background jobs (spec §5-§7)
#
# The live "which profile / region / cluster / context are we on" state is held
# in a single pg_pipeline instance (PIPE) — the login -> clusters -> kubeconfig
# dependency chain is exactly what Pipeline encodes. Gate inputs (profile /
# region / cluster) are set via /api/conn/select; the slow ops (login, clusters,
# kubeconfig, discover) run as background JOBS the browser polls, so the UI
# never blocks (R7). Discovery result lives in memory until an explicit Save.
# --------------------------------------------------------------------------- #
PIPE = None                      # pg_pipeline.Pipeline; built in main()
_conn_guard = threading.Lock()   # serializes PIPE.context mutation
IN_MEMORY_SERVICES = None        # discovered-but-unsaved services; None => use artifact

JOBS = {}                        # job_id -> {status, lines, result, error, op}
_jobs_guard = threading.Lock()

CONN_OPS = ('login', 'clusters', 'kubeconfig', 'discover')
_OP_STEP = {'login': 'login', 'clusters': 'clusters',
            'kubeconfig': 'kubeconfig', 'discover': 'discover'}
_OP_RESULT_KEYS = {'login': ('aws_ok',), 'clusters': ('clusters',),
                   'kubeconfig': ('context',), 'discover': ('artifact',)}


def _run_kubectl_json(active_ctx, args):
    """Shell to real kubectl for discovery, targeting the ACTIVE context that
    update-kubeconfig wrote (§9 R-CTXNAME) — never a hardcoded alias. Returns
    parsed JSON; raises with kubectl's stderr on failure."""
    base = [KUBECTL]
    if active_ctx:
        base += ['--context', active_ctx]
    exe = shutil.which(base[0]) or base[0]
    proc = subprocess.run([exe, *base[1:], *args], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or 'kubectl failed').strip())
    return json.loads(proc.stdout or '{}')


def _build_pipeline():
    """Wire the bootstrap pipeline to the real pg_aws / pg_discovery functions.
    Discovery populates IN_MEMORY_SERVICES and returns a small summary."""
    def do_discover(ctx, emit):
        active_ctx = ctx.get('context') or ''
        result = pg_discovery.discover(
            lambda a: _run_kubectl_json(active_ctx, a),
            include_app_services=True, progress=emit)
        global IN_MEMORY_SERVICES
        IN_MEMORY_SERVICES = result.get('services', [])
        return {'db_count': result.get('db_count', 0),
                'app_count': result.get('app_count', 0)}

    return pg_pipeline.build_bootstrap_pipeline(
        list_profiles=pg_aws.list_okta_profiles,
        do_login=pg_aws.aws_login,
        list_clusters=pg_aws.list_eks_clusters,
        update_kubeconfig=pg_aws.update_kubeconfig,
        do_discover=do_discover,
    )


def _current_services():
    """Services the console should show: freshly-discovered (in memory) if any,
    else the saved artifact."""
    if IN_MEMORY_SERVICES is not None:
        return IN_MEMORY_SERVICES
    return load_services()[0]


def _console_services():
    """(services, context, generated) for the working area + header. When a
    discovery result is held in memory (a switched-but-unsaved cluster), show
    THAT with the live context — not the on-disk artifact, which still reflects
    the last Save. This keeps the header/data-sources consistent with the top
    bar's conn_state counts."""
    if IN_MEMORY_SERVICES is not None:
        ctx = (PIPE.context.get('context') if PIPE else '') or ''
        return IN_MEMORY_SERVICES, ctx, 'in memory (unsaved)'
    return load_services()


def _artifact_cluster():
    """The `cluster` stamp of the saved artifact (§8), or None."""
    if not SERVICES_PATH or not os.path.exists(SERVICES_PATH) or SERVICES_PATH.lower().endswith('.md'):
        return None
    try:
        with open(SERVICES_PATH, 'r', encoding='utf-8-sig') as f:
            return json.load(f).get('cluster') or None
    except Exception:
        return None


def conn_state():
    """R2: the authoritative connection-state snapshot the top bar renders."""
    ctx = PIPE.context if PIPE else {}
    services = _current_services()
    db = [s for s in services if s.get('kind', 'db') == 'db']
    app = [s for s in services if s.get('kind') == 'app']
    context = ctx.get('context') or ''
    return {
        'ok': True,
        'connected': bool(context),
        'profile': ctx.get('profile'),
        'cluster': ctx.get('cluster'),
        'context': context or None,
        'region': ctx.get('region'),
        'db_count': len(db),
        'app_count': len(app),
        'artifact_cluster': _artifact_cluster(),
    }


def _new_job(op):
    job_id = uuid.uuid4().hex[:12]
    with _jobs_guard:
        JOBS[job_id] = {'status': 'running', 'lines': [], 'result': None,
                        'error': None, 'op': op}
    return job_id


def _job_emit(job_id, line):
    with _jobs_guard:
        j = JOBS.get(job_id)
        if j is not None:
            j['lines'].append(str(line))


def start_conn_job(op):
    """R7: run a pipeline op on a daemon thread; return its job_id immediately.
    The op is re-runnable in place — a fresh job_id each call, failures land as
    status:'error' with a readable message, no crash."""
    if op not in CONN_OPS:
        raise ValueError(f'unknown op: {op}')
    job_id = _new_job(op)

    def worker():
        emit = lambda l: _job_emit(job_id, l)
        try:
            # Cluster switch: tear down the previous cluster's forwards/conns
            # BEFORE update-kubeconfig activates the new context (R4/R8).
            if op == 'kubeconfig':
                emit('tearing down previous cluster forwards…')
                _switch_teardown()
            with _conn_guard:
                PIPE.run_step(_OP_STEP[op], emit)
                result = {k: PIPE.context.get(k) for k in _OP_RESULT_KEYS[op]}
            # Discovery auto-persists to the current cluster's file so a revisit
            # loads instantly (R13 per-cluster single copy). Only when a cluster
            # is active — never write a stray file for a context-less discover.
            if op == 'discover' and PIPE.context.get('cluster'):
                sv = save_conn()
                emit(f"saved · {sv.get('db_count')} db / {sv.get('app_count')} app -> {sv.get('path')}"
                     if sv.get('ok') else f"save skipped: {sv.get('error')}")
            with _jobs_guard:
                JOBS[job_id].update(status='done', result=result)
        except Exception as e:
            with _jobs_guard:
                JOBS[job_id].update(status='error', error=str(e).strip() or 'failed')

    threading.Thread(target=worker, daemon=True).start()
    return job_id


def job_snapshot(job_id):
    with _jobs_guard:
        j = JOBS.get(job_id)
        if j is None:
            return None
        return {'ok': True, 'status': j['status'], 'lines': list(j['lines']),
                'result': j['result'], 'error': j['error']}


def _apply_aws_profile():
    """Mirror the active cred profile into AWS_PROFILE so every kubectl/aws
    subprocess (port-forward, get-token, discovery) authenticates as it — the
    process is otherwise launched without AWS_PROFILE, and ~/.aws/credentials
    usually has no [default], so kubectl's exec-auth would fail with 'the server
    has asked for the client to provide credentials'."""
    ap = PIPE.context.get('aws_profile') if PIPE else None
    if ap:
        os.environ['AWS_PROFILE'] = ap


def select_conn(profile=None, region=None, cluster=None):
    """Set gate inputs and validate them via the pipeline's gate steps. Each
    selection invalidates everything downstream (reset_from), so re-picking a
    profile/region/cluster cleanly forces the later steps to re-run."""
    with _conn_guard:
        if profile is not None:
            PIPE.reset_from('pick_profile')
            PIPE.context['profile'] = profile
            PIPE.run_step('pick_profile')
            _apply_aws_profile()
        if region is not None:
            PIPE.reset_from('pick_region')
            PIPE.context['region'] = region
            PIPE.run_step('pick_region')
        if cluster is not None:
            PIPE.reset_from('pick_cluster')
            PIPE.context['cluster'] = cluster
            PIPE.run_step('pick_cluster')
    return conn_state()


def conn_profiles():
    """R3: okta profiles for the top bar. Missing/empty config is not an error —
    return an empty list, the config path, and a clear message. Never crashes,
    never leaks secrets (names + org url + region only)."""
    path = os.path.expanduser(pg_aws.OKTA_CONFIG)
    try:
        profs = pg_aws.list_okta_profiles()
    except Exception as e:
        return {'ok': True, 'profiles': [], 'config_path': path,
                'message': f'could not read okta config: {e}'}
    out = {'ok': True, 'profiles': profs, 'config_path': path,
           'default_region': DEFAULT_REGION}
    if not profs:
        out['message'] = (f'no okta profiles found in {path} — create it with '
                          f'gimme-aws-creds/okta config, then reload.')
    return out


def detect_conn(profile, region):
    """Fast path: if creds for this profile are ALREADY valid (logged in via a
    terminal, or still within an earlier session), list clusters directly with
    NO new login/MFA. Sets aws_ok + clusters so the rest of the flow (pick
    cluster -> kubeconfig -> discover) proceeds. Only when creds are genuinely
    absent/expired do we report needs_login so the UI runs the login job."""
    select_conn(profile=profile, region=region)          # gates + resolve aws_profile
    aws_profile = PIPE.context.get('aws_profile')
    ok, res = pg_aws.list_eks_clusters(region, aws_profile=aws_profile)
    if ok:
        with _conn_guard:
            PIPE.context['aws_ok'] = True
            PIPE.context['clusters'] = res
        return {'ok': True, 'authed': True, 'clusters': res}
    low = str(res).lower()
    needs_login = any(t in low for t in (
        'unable to locate credentials', 'nocredentials', 'expired',
        'securitytoken', 'invalidclienttokenid', 'credential'))
    return {'ok': True, 'authed': False, 'needs_login': needs_login, 'error': str(res)}


def autodetect_conn():
    """Boot helper for the 'already logged in elsewhere' case: probe each okta
    profile's creds and, if any are valid, return the profile + its clusters so
    the UI can fill (and enable) the cluster dropdown with no user action. When
    an artifact/context is already active, prefer the profile whose cluster list
    contains it. Returns {authed:false} when no creds are valid anywhere."""
    region = DEFAULT_REGION
    seeded = (PIPE.context.get('context') if PIPE else None)
    seeded_cluster = _artifact_cluster()          # capture before detect clears it
    fallback = None
    for p in pg_aws.list_okta_profiles():
        ok, res = pg_aws.list_eks_clusters(region, aws_profile=p.get('cred_profile'))
        if not ok:
            continue
        if seeded and seeded in res:
            d = detect_conn(p['name'], region)
            with _conn_guard:                     # restore the active cluster/context
                PIPE.context['context'] = seeded
                PIPE.context['cluster'] = seeded_cluster or seeded
            d.update(profile=p['name'], region=region)
            return d
        if fallback is None:
            fallback = p['name']
    if fallback:
        d = detect_conn(fallback, region)
        d.update(profile=fallback, region=region)
        return d
    return {'ok': True, 'authed': False}


def _remove_aws_credentials(profiles, path=None):
    """Remove the given profile section(s) from ~/.aws/credentials — this is how
    logout ends the local AWS session (the temporary STS creds are deleted, so
    the next connect must log in again). Other profiles are left intact. Returns
    the list of sections actually removed."""
    path = path or os.path.expanduser('~/.aws/credentials')
    profiles = {p for p in (profiles or []) if p}
    if not profiles or not os.path.exists(path):
        return []
    cp = configparser.ConfigParser()
    cp.read(path)
    removed = [n for n in cp.sections() if n in profiles]
    for n in removed:
        cp.remove_section(n)
    if removed:
        with open(path, 'w', encoding='utf-8') as f:
            cp.write(f)
    return removed


def logout_conn():
    """Full logout: tear down every DB connection + port-forward, end the AWS
    session (remove the cred profile from ~/.aws/credentials), and reset to a
    not-connected state. Saved per-cluster discovery files on disk are kept."""
    global SERVICES_PATH
    profs = set()
    if PIPE and PIPE.context.get('aws_profile'):
        profs.add(PIPE.context['aws_profile'])
    try:                                     # cover all known cred profiles too
        profs |= {p.get('cred_profile') for p in pg_aws.list_okta_profiles()}
    except Exception:
        pass
    _switch_teardown()                       # close conns + stop forwards; IN_MEMORY=None
    removed = _remove_aws_credentials(profs)
    if PIPE:
        PIPE.reset_from('pick_profile')      # clear profile/creds/cluster/context
    SERVICES_PATH = None                      # nothing active to show
    return {'ok': True, 'logged_out': True, 'removed_profile': removed}


def _switch_teardown():
    """R4/R8: before activating a new cluster, drop every DB connection and tear
    down all port-forwards from the previous cluster so nothing is orphaned, and
    forget the previous cluster's in-memory discovery."""
    global IN_MEMORY_SERVICES
    for name in list(_conn_cache.keys()):
        cached = _conn_cache.pop(name, None)
        if cached:
            try:
                cached['conn'].close()
            except Exception:
                pass
    if fm:
        fm.stop_everything()
    IN_MEMORY_SERVICES = None


def _counts(svcs):
    return {'db_count': sum(1 for s in svcs if s.get('kind', 'db') == 'db'),
            'app_count': sum(1 for s in svcs if s.get('kind') == 'app')}


def _cluster_artifact_path(cluster):
    """Each cluster keeps its OWN saved discovery (R13): one file per cluster, so
    switching between already-discovered clusters loads instantly instead of
    re-running discovery every time."""
    safe = re.sub(r'[^A-Za-z0-9._-]', '_', cluster or 'unknown')
    return os.path.abspath(os.path.join(DEFAULT_ARTIFACT_DIR, f'pg-services.{safe}.json'))


def load_conn(discover=False):
    """Activate the current cluster's services. If that cluster already has a
    saved copy, load it instantly (no discovery). Otherwise: start a discovery
    job when discover=True (first visit / cluster switch), or just report
    needs_discovery when discover=False (boot — don't auto-scan)."""
    global SERVICES_PATH, IN_MEMORY_SERVICES
    cluster = PIPE.context.get('cluster')
    if not cluster:                                   # legacy/no specific cluster
        return {'ok': True, 'loaded': True, 'source': 'artifact', **_counts(_current_services())}
    path = _cluster_artifact_path(cluster)
    if os.path.exists(path):
        SERVICES_PATH = path
        IN_MEMORY_SERVICES = None
        return {'ok': True, 'loaded': True, 'source': 'saved', **_counts(load_services()[0])}
    SERVICES_PATH = path                              # discovery will auto-save here
    if discover:
        return {'ok': True, 'source': 'discovery', 'job_id': start_conn_job('discover')}
    return {'ok': True, 'needs_discovery': True, 'source': 'none', **_counts([])}


def reload_conn():
    """Force re-discovery of the current cluster and overwrite its saved copy."""
    global SERVICES_PATH
    cluster = PIPE.context.get('cluster')
    if not cluster:
        return {'ok': False, 'error': 'no active cluster to reload'}
    SERVICES_PATH = _cluster_artifact_path(cluster)   # discovery auto-saves here
    return {'ok': True, 'source': 'discovery', 'job_id': start_conn_job('discover')}


def save_conn():
    """R5 Save. Persist the currently-shown services to pg-services.json, stamped
    with the active cluster/context (overwrite the single file). Only meaningful
    once Load/discovery has populated services."""
    global SERVICES_PATH, IN_MEMORY_SERVICES
    svcs = _current_services()
    if not svcs:
        return {'ok': False, 'error': 'nothing to save — Load or discover a cluster first'}
    cluster = PIPE.context.get('cluster')
    context = PIPE.context.get('context') or ''
    # With an active cluster, ALWAYS write that cluster's own file (R13) — never
    # the shared legacy single file — so per-cluster copies stay independent.
    path = _cluster_artifact_path(cluster) if cluster else \
        (SERVICES_PATH or os.path.join(DEFAULT_ARTIFACT_DIR, 'pg-services.json'))
    os.makedirs(os.path.dirname(os.path.abspath(path)) or '.', exist_ok=True)
    data = {'context': context, 'cluster': cluster,
            'generated': time.strftime('%Y-%m-%d %H:%M'), 'services': svcs}
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    SERVICES_PATH = os.path.abspath(path)
    IN_MEMORY_SERVICES = None                # now durable; read from the file
    db = [s for s in svcs if s.get('kind', 'db') == 'db']
    app = [s for s in svcs if s.get('kind') == 'app']
    return {'ok': True, 'cluster': cluster, 'path': SERVICES_PATH,
            'db_count': len(db), 'app_count': len(app)}


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _json_default(o):
    if isinstance(o, (bytes, bytearray, memoryview)):
        return '\\x' + bytes(o).hex()
    return str(o)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype='application/json'):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, default=_json_default).encode('utf-8')
        elif isinstance(body, str):
            body = body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', ctype + '; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        # Never let the browser cache the UI or API responses — otherwise an
        # edited pg_ui.html (e.g. a rebrand) keeps showing the stale cached page.
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.end_headers()
        self.wfile.write(body)

    def _err(self, code, msg):
        self._send(code, {'ok': False, 'error': msg})

    def _body(self):
        n = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(n) or b'{}')

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == '/':
                return self._send(200, INDEX_HTML, 'text/html')
            if u.path == '/api/services':
                services, context, generated = load_db_services()
                safe = [{'service': s['service'], 'svcName': s.get('svcName', s['service']),
                         'namespace': s.get('namespace', ''),
                         'database': s.get('database', ''), 'username': s.get('username', ''),
                         'engine': s.get('engine', 'postgres'),
                         'localPort': s.get('localPort')} for s in services]
                return self._send(200, {'ok': True, 'context': context,
                                        'generated': generated, 'services': safe})
            if u.path == '/api/appservices':
                services, context, generated = load_app_services()
                safe = [{'service': s['service'], 'svcName': s.get('svcName', s['service']),
                         'namespace': s.get('namespace', ''),
                         'ports': s.get('ports', [])} for s in services]
                return self._send(200, {'ok': True, 'context': context,
                                        'generated': generated, 'services': safe})
            if u.path == '/api/tables':
                return self._send(200, {'ok': True, 'tables': list_tables(q['service'][0])})
            if u.path == '/api/columns':
                cols = list_columns(q['service'][0], q['schema'][0], q['table'][0])
                return self._send(200, {'ok': True, 'columns': cols})
            if u.path == '/api/mode':
                return self._send(200, {'ok': True, 'modes': dict(MODES), 'default': DEFAULT_MODE})
            if u.path == '/api/profiles':
                return self._send(200, {'ok': True, 'profiles': load_profiles()})
            if u.path == '/api/app/status':
                name = q['service'][0]
                port = fm.app_active_port(name)
                up = bool(port) and pg_forward.port_open('127.0.0.1', port)
                return self._send(200, {'ok': True, 'service': name, 'connected': up, 'port': port})
            if u.path == '/api/conn/state':
                return self._send(200, conn_state())
            if u.path == '/api/conn/profiles':
                return self._send(200, conn_profiles())
            if u.path == '/api/conn/autodetect':
                return self._send(200, autodetect_conn())
            if u.path.startswith('/api/conn/job/'):
                snap = job_snapshot(u.path.rsplit('/', 1)[-1])
                if snap is None:
                    return self._err(404, 'no such job')
                return self._send(200, snap)
            return self._err(404, 'not found')
        except (ConnectionError, BrokenPipeError):
            return                      # client disconnected mid-response; nothing to send
        except KeyError as e:
            return self._err(400, f'missing/invalid: {e}')
        except psycopg.Error as e:
            return self._send(200, {'ok': False, 'error': str(e).strip()})
        except Exception as e:
            return self._err(500, str(e))

    def do_POST(self):
        u = urlparse(self.path)
        try:
            if u.path == '/api/connect':
                info = connect_service(self._body()['service'])
                return self._send(200, {'ok': True, **info})
            if u.path == '/api/disconnect':
                disconnect_service(self._body()['service'])
                return self._send(200, {'ok': True})
            if u.path == '/api/mode':
                p = self._body()
                set_mode(p['service'], p['mode'])
                return self._send(200, {'ok': True, 'service': p['service'], 'mode': p['mode']})
            if u.path == '/api/query':
                p = self._body()
                sql = (p.get('sql') or '').strip()
                if not sql:
                    return self._err(400, 'empty query')
                return self._send(200, run_sql(p['service'], sql, confirmed=bool(p.get('confirmed'))))
            if u.path == '/api/profiles':
                p = self._body()
                action = p.get('action')
                if action == 'save':
                    save_profile(p['name'], p.get('entries', []))
                    return self._send(200, {'ok': True, 'profiles': load_profiles()})
                if action == 'delete':
                    delete_profile(p['name'])
                    return self._send(200, {'ok': True, 'profiles': load_profiles()})
                return self._err(400, f'unknown action: {action}')
            if u.path == '/api/app/connect':
                p = self._body()
                svc_name = _app_service_info(p['service']).get('svcName')
                return self._send(200, fm.start_app_forward(
                    p['service'], p['namespace'], int(p['remotePort']), int(p['localPort']),
                    svc_name=svc_name))
            if u.path == '/api/app/disconnect':
                p = self._body()
                fm.stop_app_forward(p['service'])
                return self._send(200, {'ok': True})
            if u.path.startswith('/api/conn/run/'):
                op = u.path.rsplit('/', 1)[-1]
                if op not in CONN_OPS:
                    return self._err(400, f'unknown op: {op}')
                return self._send(200, {'ok': True, 'job_id': start_conn_job(op)})
            if u.path == '/api/conn/select':
                p = self._body()
                return self._send(200, select_conn(
                    profile=p.get('profile'), region=p.get('region'),
                    cluster=p.get('cluster')))
            if u.path == '/api/conn/detect':
                p = self._body()
                if not p.get('profile') or not p.get('region'):
                    return self._err(400, 'profile and region are required')
                return self._send(200, detect_conn(p['profile'], p['region']))
            if u.path == '/api/conn/load':
                return self._send(200, load_conn(discover=bool(self._body().get('discover'))))
            if u.path == '/api/conn/reload':
                return self._send(200, reload_conn())
            if u.path == '/api/conn/logout':
                return self._send(200, logout_conn())
            if u.path == '/api/conn/save':
                return self._send(200, save_conn())
            return self._err(404, 'not found')
        except (ConnectionError, BrokenPipeError):
            return                      # client disconnected mid-response; nothing to send
        except psycopg.Error as e:
            return self._send(200, {'ok': False, 'error': str(e).strip()})
        except KeyError as e:
            return self._err(400, f'missing field: {e}')
        except Exception as e:
            return self._send(200, {'ok': False, 'error': str(e)})


# --------------------------------------------------------------------------- #
# Embedded UI (no external assets)
# --------------------------------------------------------------------------- #
def _load_index_html():
    """UI lives in pg_ui.html next to this script (extracted from the old
    inline string). Loaded once at import; keeps the entrypoint lean and the
    UI editable without touching Python."""
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "pg_ui.html"), encoding="utf-8") as fh:
        return fh.read()

INDEX_HTML = _load_index_html()


# --------------------------------------------------------------------------- #
def find_services_file(explicit):
    if explicit:
        return explicit
    for c in (os.path.join('pg-artifacts', 'pg-services.json'),
              'pg-services.json',
              os.path.join('pg-artifacts', 'postgres-credentials.md'),
              'postgres-credentials.md'):
        if os.path.exists(c):
            return c
    return None


# Optional config file — lets you set region / okta config path / executables /
# port etc. without passing CLI flags every time. JSON always works (no extra
# dependency, per the single-dep goal); YAML is read too IF PyYAML is installed.
CONFIG_CANDIDATES = ('pg-console.yaml', 'pg-console.yml', 'pg-console.json', '.pg-console.json')


def load_app_config(explicit):
    """Return (config_dict, path_or_None). Searches the explicit path, then the
    cwd, then next to this script. Unknown keys are ignored by the caller."""
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [explicit] if explicit else \
        list(CONFIG_CANDIDATES) + [os.path.join(here, c) for c in CONFIG_CANDIDATES]
    for c in candidates:
        if not c or not os.path.exists(c):
            continue
        try:
            text = open(c, encoding='utf-8').read()
            if c.lower().endswith(('.yaml', '.yml')):
                try:
                    import yaml
                except ImportError:
                    print(f'note: {c} needs PyYAML (pip install pyyaml); skipping it.')
                    continue
                data = yaml.safe_load(text) or {}
            else:
                data = json.loads(text or '{}')
            if isinstance(data, dict):
                return data, c
            print(f'note: config {c} is not a key/value mapping; ignoring.')
        except Exception as e:
            print(f'note: could not read config {c}: {e}')
    return {}, None


def main():
    global SERVICES_PATH, KUBECTL, AUTO_FORWARD, fm, PIPE, DEFAULT_REGION
    ap = argparse.ArgumentParser(description='Local Postgres SQL console + AWS/cluster dashboard.')
    ap.add_argument('--services', help='path to pg-services.json OR postgres-credentials.md')
    ap.add_argument('--config', help='path to a config file (JSON, or YAML if PyYAML installed)')
    ap.add_argument('--port', type=int, default=None)
    ap.add_argument('--host', default=None)
    ap.add_argument('--region', default=None, help='default AWS region for the dashboard (e.g. ca-central-1)')
    ap.add_argument('--okta-config', default=None,
                    help='path to the okta login config (default ~/.okta_aws_login_config)')
    ap.add_argument('--gimme', default=None, help='gimme-aws-creds executable to use')
    ap.add_argument('--aws', default=None, help='aws executable to use')
    ap.add_argument('--kubectl', default=None, help='kubectl executable to use')
    ap.add_argument('--no-forward', action='store_true',
                    help='do not auto-start kubectl port-forward (assume forwards are already running)')
    ap.add_argument('--no-browser', action='store_true')
    args = ap.parse_args()

    # Settings precedence: CLI flag > config file > built-in default.
    cfg, cfg_path = load_app_config(args.config)
    def pick(cli_val, key, default):
        return cli_val if cli_val is not None else cfg.get(key, default)

    host = pick(args.host, 'host', '127.0.0.1')
    port = int(pick(args.port, 'port', 8765))
    KUBECTL = pick(args.kubectl, 'kubectl', 'kubectl')
    AUTO_FORWARD = not (args.no_forward or cfg.get('no_forward', False))
    DEFAULT_REGION = pick(args.region, 'region', DEFAULT_REGION)
    pg_aws.OKTA_CONFIG = os.path.expanduser(pick(args.okta_config, 'okta_config', pg_aws.OKTA_CONFIG))
    pg_aws.GIMME_EXE = pick(args.gimme, 'gimme', pg_aws.GIMME_EXE)
    pg_aws.AWS_EXE = pick(args.aws, 'aws', pg_aws.AWS_EXE)

    # R1: boot even with no artifact and no creds. A missing artifact is a
    # normal not-connected state — the dashboard lets the user connect from the
    # browser. SERVICES_PATH stays None until the first Load/Save populates it.
    path = find_services_file(args.services or cfg.get('services'))
    if args.services and not (path and os.path.exists(path)):
        print(f'note: --services {args.services} not found; starting not-connected.')
    if path and os.path.exists(path):
        SERVICES_PATH = os.path.abspath(path)
    load_modes()

    try:
        services, context, _ = load_services()
    except Exception as e:
        # A present-but-unreadable artifact shouldn't kill the server; fall back
        # to not-connected so the user can still reconnect/rediscover.
        print(f'note: failed to read {SERVICES_PATH}: {e}; starting not-connected.')
        SERVICES_PATH, services, context = None, [], ''

    fm = _build_forward_manager()      # now that KUBECTL/AUTO_FORWARD/SERVICES_PATH are set

    # Connection dashboard: build the pipeline, preload okta profiles for the
    # top bar, and seed the active context from an existing artifact so a
    # boot-with-artifact lands already-connected (§5 boot step 2).
    PIPE = _build_pipeline()
    try:
        PIPE.run_step('profiles')
    except Exception as e:
        print(f'note: could not read okta profiles: {e}')
    if context:
        PIPE.context['context'] = context
        ac = _artifact_cluster()
        PIPE.context['cluster'] = ac
        # Migrate a legacy single artifact to the per-cluster filename (R13) so
        # the current cluster's saved discovery loads instantly at boot.
        if ac:
            per = _cluster_artifact_path(ac)
            if not os.path.exists(per):
                try:
                    shutil.copyfile(SERVICES_PATH, per)
                except Exception as e:
                    print(f'note: could not migrate artifact to {per}: {e}')
            if os.path.exists(per):
                SERVICES_PATH = per

    url = f'http://{host}:{port}'
    print(f'Ninja Console  ->  {url}')
    if cfg_path:
        print(f'config      ->  {os.path.abspath(cfg_path)}')
    print(f'region      ->  {DEFAULT_REGION}    okta config -> {pg_aws.OKTA_CONFIG}')
    if SERVICES_PATH:
        print(f'artifact    ->  {SERVICES_PATH}  ({len(services)} services, context: {context})')
    else:
        print('artifact    ->  none yet (not connected; connect from the browser)')
    print(f'forwards    ->  {"auto (kubectl port-forward on connect)" if AUTO_FORWARD else "manual (--no-forward)"}')
    print('Ctrl+C to stop.')
    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    httpd = ThreadingHTTPServer((host, port), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nstopping…')
        for c in _conn_cache.values():
            try:
                c['conn'].close()
            except Exception:
                pass
        fm.stop_everything()
        httpd.shutdown()


if __name__ == '__main__':
    main()
