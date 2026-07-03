#!/usr/bin/env python3
"""
pg_forward.py — port-forward lifecycle for both DB sources and App Services.

Extracted verbatim (behavior-preserving) from pg_console.py. The two paths
differ ON PURPOSE:
  - DB (ensure_forward): auto-remaps to a free local port if the declared one
    is unbindable — safe, because the console is that port's only consumer.
  - App (start_app_forward): FAILS LOUD on a collision, never remaps — the
    local port is a contract with code the developer already wrote.

State (the running kubectl subprocesses, active ports) lives on the manager
instance rather than module globals, so the console owns exactly one
ForwardManager and its lifecycle is explicit. Config + the few things it
needs from the console (how to look up a service, the kube context, how to
invalidate a cached DB connection) are injected at construction — the same
dependency-injection discipline that keeps pg_discovery testable.
"""
import socket
import subprocess
import threading
import time


def port_open(host, port, timeout=0.4):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, int(port)))
        return True
    except OSError:
        return False
    finally:
        s.close()


def bindable(port):
    """True if we can actually bind 127.0.0.1:port. Fails for occupied ports AND
    for ports inside a Windows/WinNAT excluded range (Hyper-V/WSL2/Docker)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('127.0.0.1', int(port)))
        return True
    except OSError:
        return False
    finally:
        s.close()


def pick_local(pref):
    """Prefer the declared port; if it can't be bound, let the OS choose a free one
    (which skips WinNAT excluded ranges automatically)."""
    if bindable(pref):
        return pref
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ForwardManager:
    def __init__(self, *, get_service_info, get_context, invalidate_conn=None,
                 kubectl='kubectl', auto_forward=True, forward_timeout=20):
        """
        get_service_info(name) -> dict with localPort/remotePort/namespace/service
        get_context()          -> kube context string ('' for current)
        invalidate_conn(name)  -> optional; drop a cached DB connection on remap
        """
        self._get_service_info = get_service_info
        self._get_context = get_context
        self._invalidate_conn = invalidate_conn or (lambda name: None)
        self.KUBECTL = kubectl
        self.AUTO_FORWARD = auto_forward
        self.FORWARD_TIMEOUT = forward_timeout

        self._forwards = {}       # db service -> {"proc", "err"}
        self._active_port = {}    # db service -> local port actually in use
        self._app_forwards = {}   # app service -> {"proc", "err"}
        self._app_active_port = {}  # app service -> local port in use

    # --- shared helpers ---------------------------------------------------- #
    def active_port(self, name):
        return self._active_port.get(name)

    def app_active_port(self, name):
        return self._app_active_port.get(name)

    def _spawn(self, args):
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL,
                                stderr=subprocess.PIPE, text=True)
        err_lines = []
        threading.Thread(target=lambda: [err_lines.append(l) for l in proc.stderr],
                         daemon=True).start()
        return proc, err_lines

    def _base_args(self):
        args = [self.KUBECTL]
        ctx = self._get_context()
        if ctx:
            args += ['--context', ctx]
        return args

    # --- DB forwards (auto-remap allowed) ---------------------------------- #
    def ensure_forward(self, name):
        """Ensure a working local listener for a DB service. Returns True if WE
        started it. Auto-remaps to a free local port if declared one is unbindable."""
        info = self._get_service_info(name)
        pref = int(info['localPort'])

        if port_open('127.0.0.1', pref):
            self._active_port[name] = pref
            return False
        if not self.AUTO_FORWARD:
            raise RuntimeError(f'nothing is listening on :{pref} and auto port-forward is off')

        lp = pick_local(pref)
        self._invalidate_conn(name)      # force reconnect to the chosen local port
        self._active_port[name] = lp

        rp = int(info.get('remotePort') or 5432)
        ns = info.get('namespace') or 'default'
        svc = info.get('svcName') or info['service']    # real k8s name (service may be a name@ns key)
        args = self._base_args() + ['-n', ns, 'port-forward', f'svc/{svc}', f'{lp}:{rp}']

        try:
            proc, err_lines = self._spawn(args)
        except FileNotFoundError:
            raise RuntimeError(f"'{self.KUBECTL}' not found on PATH")
        self._forwards[name] = {'proc': proc, 'err': err_lines}

        deadline = time.time() + self.FORWARD_TIMEOUT
        while time.time() < deadline:
            if proc.poll() is not None:
                self._forwards.pop(name, None)
                raise RuntimeError('port-forward exited: ' + (''.join(err_lines).strip()[:300] or 'unknown error'))
            if port_open('127.0.0.1', lp):
                return True
            time.sleep(0.3)

        detail = ''.join(err_lines).strip()[:300]
        self.stop_forward(name)
        raise RuntimeError(f'port-forward to {svc} (:{lp}) not ready after {self.FORWARD_TIMEOUT}s'
                           + (f' — kubectl said: {detail}' if detail else ' (kubectl printed nothing)'))

    def stop_forward(self, name):
        self._terminate(self._forwards.pop(name, None))

    def stop_all_forwards(self):
        for name in list(self._forwards):
            self.stop_forward(name)

    # --- App forwards (fail loud, no remap) -------------------------------- #
    def start_app_forward(self, service, namespace, remote_port, local_port, svc_name=None):
        local_port = int(local_port)
        remote_port = int(remote_port)
        svc = svc_name or service       # `service` is the unique key; svc_name is the real k8s name

        if port_open('127.0.0.1', local_port) and self._app_active_port.get(service) == local_port:
            return {'ok': True, 'port': local_port, 'already': True}

        if not bindable(local_port):
            return {'ok': False, 'error': f':{local_port} is already in use — pick a different '
                    'local port and retry (this port is not auto-changed for app services).'}

        args = self._base_args() + ['-n', namespace, 'port-forward',
                                    f'svc/{svc}', f'{local_port}:{remote_port}']
        try:
            proc, err_lines = self._spawn(args)
        except FileNotFoundError:
            return {'ok': False, 'error': f"'{self.KUBECTL}' not found on PATH"}
        self._app_forwards[service] = {'proc': proc, 'err': err_lines}

        deadline = time.time() + self.FORWARD_TIMEOUT
        while time.time() < deadline:
            if proc.poll() is not None:
                self._app_forwards.pop(service, None)
                detail = ''.join(err_lines).strip()[:300]
                return {'ok': False, 'error': 'port-forward exited: ' + (detail or 'unknown error')}
            if port_open('127.0.0.1', local_port):
                self._app_active_port[service] = local_port
                return {'ok': True, 'port': local_port}
            time.sleep(0.3)

        detail = ''.join(err_lines).strip()[:300]
        self.stop_app_forward(service)
        return {'ok': False, 'error': f'not ready after {self.FORWARD_TIMEOUT}s'
                                      + (f' — kubectl said: {detail}' if detail else '')}

    def stop_app_forward(self, service):
        self._app_active_port.pop(service, None)
        self._terminate(self._app_forwards.pop(service, None))

    def stop_all_app_forwards(self):
        for name in list(self._app_forwards):
            self.stop_app_forward(name)

    # --- teardown ---------------------------------------------------------- #
    def _terminate(self, f):
        if not f:
            return
        p = f['proc']
        try:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        except Exception:
            pass

    def stop_everything(self):
        self.stop_all_forwards()
        self.stop_all_app_forwards()
