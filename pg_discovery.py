#!/usr/bin/env python3
"""
pg_discovery.py — native-Python port of Generate-PgArtifacts.ps1's discovery
and credential-resolution logic. No PowerShell dependency.

Produces the same pg-services.json the console already consumes:
  - kind:"db"  entries with resolved database/username/password + assigned localPort
  - kind:"app" entries with their exposed ports (no localPort — user assigns it)

This module is import-safe and pure aside from the kubectl calls it makes via
the injected `run_kubectl` callable, so it can be unit-tested with a fake.
"""
import base64
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

# --- credential key patterns (ported verbatim from the .ps1) --------------- #
_USER_KEYS = re.compile(
    r'^(spring[-_]?datasource[-_]?user(name)?|postgres(ql)?[-_]?user(name)?|'
    r'pg[-_]?user(name)?|db[-_]?user(name)?|jdbc[-_]?user(name)?|'
    r'master[-_]?user(name)?|admin[-_]?user(name)?|app[-_]?user(name)?|user(name)?)$', re.I)
_PASS_KEYS = re.compile(
    r'^(spring[-_]?datasource[-_]?password|postgres(ql)?[-_]?password|pg[-_]?password|'
    r'db[-_]?pass(word)?|jdbc[-_]?password|master[-_]?password|admin[-_]?password|'
    r'app[-_]?password|pass(word)?)$', re.I)
_DB_KEYS = re.compile(
    r'^(spring[-_]?datasource[-_]?(db|database|name)|postgres(ql)?[-_]?(db|database)|'
    r'pg[-_]?database|db[-_]?name|database(name)?|dbname)$', re.I)
_URL_KEYS = re.compile(
    r'^(spring[-_]?datasource[-_]?(url|jdbc[-_]?url)|jdbc[-_]?url|'
    r'database[-_]?url|datasource[-_]?url)$', re.I)

_DEFAULT_EXCLUDE_NS = ['kube-system', 'kube-node-lease', 'kube-public', 'linkerd',
                       'linkerd-viz', 'cert-manager', 'ingress-nginx', 'argocd',
                       'monitoring', 'logging', 'istio-system']

# Non-Postgres engines detected as first-class data sources (queryable in the
# console via their engine module). Matched by well-known port OR service-name.
# `resolve` = pull creds from the backing pod/secret (only where auth is needed;
# CQL defaults to cassandra/cassandra, ES is usually open — keeping discovery fast).
# (engine, remote_port, name_regex, default_user, default_password, resolve)
_ENGINE_SPECS = [
    ('cql',   9042,  r'cassandra|scylla|cql',             'cassandra', 'cassandra', False),
    ('es',    9200,  r'elasticsearch|elastic|opensearch', '',          '',          False),
    ('mongo', 27017, r'mongo',                             'root',      '',          True),
]


def _pick(d, *names):
    """First value in dict d whose key matches (case-insensitive) any of names."""
    low = {k.lower(): v for k, v in d.items()}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return None


def _resolve_engine_creds(engine, plain_env, decoded, du, dp):
    """Best-effort creds from the backing pod's env + secrets. Returns
    (username, password, auth_source)."""
    if engine == 'mongo':
        user = _pick(plain_env, 'MONGODB_ROOT_USER', 'MONGO_INITDB_ROOT_USERNAME',
                     'MONGODB_USERNAME', 'MONGO_USERNAME') or du or 'root'
        pw = (_pick(decoded, 'mongodb-root-password', 'mongo-root-password', 'mongodb-password',
                    'MONGODB_ROOT_PASSWORD', 'MONGO_INITDB_ROOT_PASSWORD')
              or _pick(plain_env, 'MONGODB_ROOT_PASSWORD', 'MONGO_INITDB_ROOT_PASSWORD') or dp)
        return user, pw, 'admin'
    if engine == 'es':
        user = _pick(plain_env, 'ELASTIC_USERNAME', 'ELASTICSEARCH_USERNAME') or du
        pw = (_pick(decoded, 'elastic-password', 'elasticsearch-password')
              or _pick(plain_env, 'ELASTIC_PASSWORD', 'ELASTICSEARCH_PASSWORD') or dp)
        return user, pw, ''
    if engine == 'cql':
        user = _pick(plain_env, 'CQL_USERNAME', 'CASSANDRA_USER', 'CASSANDRA_USERNAME') or du or 'cassandra'
        pw = (_pick(decoded, 'cql-password', 'cassandra-password')
              or _pick(plain_env, 'CQL_PASSWORD', 'CASSANDRA_PASSWORD') or dp or 'cassandra')
        return user, pw, ''
    return du, dp, ''


def _live_endpoints(run_kubectl, namespace):
    """Set of (namespace, name) for services that have at least one READY endpoint
    address — i.e. something is actually running behind them. One kubectl call.
    Returns None if endpoints can't be fetched (then callers don't filter).
    This drops dead services (e.g. a stale duplicate in another namespace)."""
    args = ['get', 'endpoints', '-o', 'json'] + (['-n', namespace] if namespace else ['--all-namespaces'])
    try:
        items = run_kubectl(args).get('items', [])
    except Exception:
        return None
    live = set()
    for ep in items:
        if any((sub.get('addresses') for sub in (ep.get('subsets') or []))):
            m = ep.get('metadata', {})
            live.add((m.get('namespace'), m.get('name')))
    return live or None


def _detect_engine_sources(run_kubectl, all_svc, claimed, exclude_namespace,
                           include_headless, is_headless, is_live):
    """Find CQL/ES/Mongo services (by well-known port or name), skipping anything
    already claimed as a Postgres source or in an excluded namespace. Resolves
    creds best-effort from the backing pod, and assigns a unique local port so
    two same-port services (e.g. two mongos on 27017) don't collide."""
    out = []
    used_local = set()
    for engine, rport, name_pat, du, dp, resolve in _ENGINE_SPECS:
        nre = re.compile(name_pat, re.I)
        for s in all_svc:
            ns = s['metadata']['namespace']
            name = s['metadata']['name']
            key = f'{ns}/{name}'
            if key in claimed or ns in exclude_namespace:
                continue
            if not is_live(s):                  # skip dead services (no ready endpoints)
                continue
            # skip per-node / internal / headless helper services (e.g. Scylla's
            # <name>-0-internal, <name>-headless) — the main service is the entry point.
            if re.search(r'(-\d+)?-internal$|-headless$', name, re.I):
                continue
            if is_headless(s) and not include_headless:
                continue
            ports = (s.get('spec', {}) or {}).get('ports', []) or []
            # require the engine's well-known port to actually be exposed — avoids
            # name-only false positives like elasticsearch-kibana (:5601, not :9200).
            if not any(p.get('port') == rport for p in ports):
                continue
            remote = rport
            lp = rport
            while lp in used_local:
                lp += 1
            used_local.add(lp)

            if resolve:                 # only where auth is needed (mongo) — keeps discovery fast
                try:
                    secret_names, plain_env = _harvest(run_kubectl, s, ns, name, nre)
                    decoded = _decode_secrets(run_kubectl, secret_names, ns)
                    user, pw, auth_source = _resolve_engine_creds(engine, plain_env, decoded, du, dp)
                except Exception:
                    user, pw, auth_source = du, dp, 'admin' if engine == 'mongo' else ''
            else:
                user, pw, auth_source = du, dp, ''

            entry = {
                'kind': 'db', 'engine': engine, 'service': name, 'namespace': ns,
                'host': 'localhost', 'localPort': lp, 'remotePort': remote,
                'database': '', 'username': user or '', 'password': pw or '',
            }
            if auth_source:
                entry['authSource'] = auth_source
            out.append(entry)
            claimed.add(key)
    return out


def _pass_score(key):
    k = key.lower()
    if re.match(r'^spring[-_]?datasource[-_]?password$', k): return 100
    if re.match(r'^(db|app|jdbc|admin|master)[-_]?pass(word)?$', k): return 80
    if re.match(r'^password$', k): return 70
    if re.match(r'^pg[-_]?password$', k): return 30
    if re.match(r'^postgres(ql)?[-_]?password$', k): return 10   # superuser: last resort
    return 50


def _user_score(key):
    k = key.lower()
    if re.match(r'^spring[-_]?datasource[-_]?user(name)?$', k): return 100
    if re.match(r'^(db|app|jdbc|admin|master)[-_]?user(name)?$', k): return 80
    if re.match(r'^user(name)?$', k): return 70
    if re.match(r'^postgres(ql)?[-_]?user(name)?$', k): return 60
    return 50


def resolve_credentials(decoded):
    """decoded: {key: value}. Returns {username, password, database}.
    Passwords are RANKED (app password beats postgres-password superuser)."""
    result = {'username': '', 'password': '', 'database': ''}

    user_cands = sorted([k for k in decoded if _USER_KEYS.match(k)], key=_user_score, reverse=True)
    pass_cands = sorted([k for k in decoded if _PASS_KEYS.match(k)], key=_pass_score, reverse=True)
    if user_cands:
        result['username'] = decoded[user_cands[0]]
    if pass_cands:
        result['password'] = decoded[pass_cands[0]]

    for k in decoded:
        if not result['database'] and _DB_KEYS.match(k):
            result['database'] = decoded[k]
    if not result['database']:
        for k in decoded:
            if _URL_KEYS.match(k):
                m = re.search(r'/([^/?]+)(\?|$)', decoded[k])
                if m:
                    result['database'] = m.group(1)
                    break
    return result


def _b64(v):
    try:
        return base64.b64decode(v).decode('utf-8')
    except Exception:
        return v


def discover(run_kubectl, *, namespace=None, name_pattern='postgres|postgresql|pg|pgsql|timescale|citus',
             pg_port=5432, base_port=5433, include_headless=False,
             include_app_services=False, exclude_namespace=None, progress=None):
    """
    run_kubectl(args:list) -> parsed JSON dict (raises on failure).
    progress(str) -> optional callback for streaming status lines.
    Returns {'services': [...], 'db_count': int, 'app_count': int}.
    """
    exclude_namespace = exclude_namespace or _DEFAULT_EXCLUDE_NS
    name_re = re.compile(name_pattern, re.I)
    log = progress or (lambda *_: None)

    svc_args = ['get', 'svc', '-o', 'json']
    svc_args += (['-n', namespace] if namespace else ['--all-namespaces'])
    all_svc = run_kubectl(svc_args).get('items', [])
    log(f'scanning {len(all_svc)} services…')

    def is_headless(s):
        return (s.get('spec', {}) or {}).get('clusterIP') == 'None'

    ready = _live_endpoints(run_kubectl, namespace)

    def is_live(s):
        if ready is None:
            return True
        return (s['metadata']['namespace'], s['metadata']['name']) in ready

    # --- Postgres (db) services --- #
    pg_services = []
    for s in all_svc:
        ports = (s.get('spec', {}) or {}).get('ports', []) or []
        by_port = any(p.get('port') == pg_port or (p.get('name') and re.search(r'postgres|pgsql', p['name'], re.I))
                      for p in ports)
        by_name = bool(name_re.search(s['metadata']['name']))
        if (by_port or by_name) and (include_headless or not is_headless(s)) and is_live(s):
            pg_services.append(s)

    # Resolve each service's creds (the kubectl-heavy step) IN PARALLEL — doing it
    # serially for 25+ services took minutes and could outrun the short-lived EKS
    # token. A small thread pool keeps discovery well under the token lifetime.
    def _resolve_pg(svc):
        ns = svc['metadata']['namespace']
        name = svc['metadata']['name']
        ports = (svc.get('spec', {}) or {}).get('ports', []) or []
        remote_port = next((p['port'] for p in ports if p.get('port') == pg_port), None)
        if remote_port is None and ports:
            remote_port = ports[0]['port']
        # Resilient: a single service's cred lookup failing (throttle, missing
        # pod, transient error) must NOT abort the whole discovery.
        try:
            secret_names, plain_env = _harvest(run_kubectl, svc, ns, name, name_re)
            decoded = _decode_secrets(run_kubectl, secret_names, ns)
            for k, v in plain_env.items():
                decoded.setdefault(k, v)
            creds = resolve_credentials(decoded)
        except Exception:
            secret_names, decoded, creds = set(), {}, {'username': '', 'password': '', 'database': ''}
        user_assumed = False
        if not creds['username']:
            creds['username'] = 'postgres'; user_assumed = True
        if not creds['database']:
            creds['database'] = creds['username'] if creds['username'] != 'postgres' else 'postgres'
        return {'ns': ns, 'name': name, 'remote': remote_port or pg_port, 'creds': creds,
                'assumed': user_assumed, 'secret_names': secret_names, 'keys': sorted(decoded.keys())}

    log(f'resolving credentials for {len(pg_services)} db service(s)…')
    with ThreadPoolExecutor(max_workers=5) as ex:
        resolved = list(ex.map(_resolve_pg, pg_services))

    results = []
    local_port = base_port
    for r in resolved:
        results.append({
            'kind': 'db', 'engine': 'postgres', 'service': r['name'], 'namespace': r['ns'], 'host': 'localhost',
            'localPort': local_port, 'remotePort': r['remote'],
            'database': r['creds']['database'],
            'username': r['creds']['username'] + (' (assumed)' if r['assumed'] else ''),
            'password': r['creds']['password'],
            'secretNames': ', '.join(sorted(r['secret_names'])),
            'availKeys': ', '.join(r['keys']),
        })
        local_port += 1

    # --- Other engines (CQL / ES / Mongo) as data sources --- #
    claimed = {f"{r['namespace']}/{r['service']}" for r in results}
    engine_results = _detect_engine_sources(run_kubectl, all_svc, claimed, exclude_namespace,
                                            include_headless, is_headless, is_live)
    results.extend(engine_results)
    if engine_results:
        log(f'found {len(engine_results)} non-postgres engine source(s)')

    # --- App services --- #
    app_results = []
    if include_app_services:
        for s in all_svc:
            ns = s['metadata']['namespace']
            name = s['metadata']['name']
            key = f'{ns}/{name}'
            if key in claimed or ns in exclude_namespace:
                continue
            if is_headless(s) and not include_headless:
                continue
            ports = (s.get('spec', {}) or {}).get('ports', []) or []
            if not ports:
                continue
            app_results.append({
                'kind': 'app', 'service': name, 'namespace': ns,
                'ports': [{'name': p.get('name') or str(p['port']), 'port': p['port']} for p in ports],
            })

    log(f'found {len(results)} db, {len(app_results)} app services')
    # Strip the diagnostic-only fields from db entries for the artifact proper,
    # but keep them retrievable — the console never needs secretNames/availKeys
    # in pg-services.json, so we drop them to keep the file clean.
    clean_db = [{k: v for k, v in r.items() if k not in ('secretNames', 'availKeys')} for r in results]
    services = clean_db + app_results
    # Namespace-aware identity: the console keys sources by their `service` field.
    # Keep the real k8s name in `svcName` (used for kubectl/port-forward), and make
    # `service` unique by suffixing @<namespace> ONLY when a name collides across
    # namespaces — so two live same-named services stay distinct.
    for s in services:
        s['svcName'] = s['service']
    dups = {n for n, c in Counter(s['svcName'] for s in services).items() if c > 1}
    for s in services:
        if s['svcName'] in dups:
            s['service'] = f"{s['svcName']}@{s['namespace']}"
    return {'services': services,
            'db_count': len(results), 'app_count': len(app_results),
            'diagnostics': {r['service']: {'secretNames': r['secretNames'], 'availKeys': r['availKeys']}
                            for r in results if 'secretNames' in r}}


def _harvest(run_kubectl, svc, ns, name, name_re):
    """Return (secret_names:set, plain_env:dict) for a service's backing pod."""
    secret_names, plain_env = set(), {}
    selector = (svc.get('spec', {}) or {}).get('selector')
    if selector:
        label_sel = ','.join(f'{k}={v}' for k, v in selector.items())
        try:
            pods = run_kubectl(['get', 'pods', '-n', ns, '-l', label_sel, '-o', 'json']).get('items', [])
        except Exception:
            pods = []
        if pods:
            for c in (pods[0].get('spec', {}) or {}).get('containers', []):
                for e in c.get('env', []) or []:
                    ref = (e.get('valueFrom', {}) or {}).get('secretKeyRef', {}) or {}
                    if ref.get('name'):
                        secret_names.add(ref['name'])
                    elif e.get('value') is not None:
                        plain_env[e['name']] = str(e['value'])
                for ef in c.get('envFrom', []) or []:
                    sref = (ef.get('secretRef', {}) or {})
                    if sref.get('name'):
                        secret_names.add(sref['name'])
    # fallback: namespace secrets whose name relates to the service
    if not secret_names:
        try:
            ns_secrets = run_kubectl(['get', 'secret', '-n', ns, '-o', 'json']).get('items', [])
            for s in ns_secrets:
                sn = s['metadata']['name']
                if re.search(re.escape(name), sn) or name_re.search(sn):
                    secret_names.add(sn)
        except Exception:
            pass
    return secret_names, plain_env


def _decode_secrets(run_kubectl, secret_names, ns):
    decoded = {}
    for sn in secret_names:
        try:
            secret = run_kubectl(['get', 'secret', sn, '-n', ns, '-o', 'json'])
            for k, v in (secret.get('data', {}) or {}).items():
                decoded.setdefault(k, _b64(v))
        except Exception:
            pass
    return decoded
