#!/usr/bin/env python3
"""
pg_aws.py — AWS/Okta bootstrap for the pg-console setup screen.

Chain: pick okta profile -> gimme-aws-creds (Okta MFA push) -> list EKS
clusters -> update kubeconfig. Everything shells out to the CLIs the user
already has (gimme-aws-creds, aws, kubectl); nothing here stores secrets.

The okta config (~/.okta_aws_login_config) is a plain INI file
(configparser format: a [DEFAULT] section plus named [profile] sections),
so stdlib configparser reads it with no extra dependency.
"""
import configparser
import json
import os
import re
import shutil
import subprocess

# Overridable settings (pg_console.main() may reassign these from CLI/config).
# Functions below read the module globals at call time, so reassigning
# pg_aws.OKTA_CONFIG / GIMME_EXE / AWS_EXE takes effect immediately.
OKTA_CONFIG = os.path.expanduser(r'~/.okta_aws_login_config')
GIMME_EXE = 'gimme-aws-creds'
AWS_EXE = 'aws'


# --------------------------------------------------------------------------- #
# okta profiles
# --------------------------------------------------------------------------- #
def list_okta_profiles(path=None):
    """Return [{name, okta_org_url, aws_region}] from the okta config file.
    'DEFAULT' is included as a selectable profile only if it has its own keys.
    path defaults to the (overridable) module OKTA_CONFIG."""
    path = path or OKTA_CONFIG
    if not os.path.exists(path):
        return []
    cp = configparser.ConfigParser()
    try:
        cp.read(path)
    except configparser.Error:
        return []
    def _entry(name, sec):
        return {
            'name': name,
            'okta_org_url': sec.get('okta_org_url', ''),
            'aws_region': sec.get('aws_region', '') or sec.get('region', ''),
            # cred_profile is the NAMED profile gimme writes to ~/.aws/credentials
            # (e.g. US-Int-SRE-EngineerPrivileged) — NOT the okta profile name.
            # Subsequent `aws` calls must target it, else NoCredentials. Falls
            # back to the okta profile name when the key is absent.
            'cred_profile': sec.get('cred_profile', '') or name,
        }

    profiles = [_entry(name, cp[name]) for name in cp.sections()]
    # DEFAULT only if it actually carries config (not just inherited blanks)
    if cp.defaults():
        profiles.insert(0, _entry('DEFAULT', cp['DEFAULT']))
    return profiles


# --------------------------------------------------------------------------- #
# subprocess helpers — each returns (ok, combined_output)
# --------------------------------------------------------------------------- #
def _run(args, timeout=180, on_line=None, env=None):
    """Run a CLI, streaming stdout lines to on_line(line) as they arrive.
    Returns (returncode, full_output).

    args[0] is resolved via shutil.which first: on Windows the tools ship as
    .cmd launchers (e.g. gimme-aws-creds.CMD), and Popen by bare name can't
    find those without shell=True — which(...) resolves the real path so we
    keep shell=False. stdin is closed (DEVNULL): these run as no-terminal
    background jobs, so anything that would prompt must fail fast, never hang.
    """
    exe = shutil.which(args[0]) or args[0]
    args = [exe, *args[1:]]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            stdin=subprocess.DEVNULL, text=True, bufsize=1, env=env)
    lines = []
    try:
        for line in proc.stdout:
            lines.append(line)
            if on_line:
                on_line(line.rstrip('\n'))
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        return 124, ''.join(lines) + '\n[timed out]'
    return proc.returncode, ''.join(lines)


def aws_login(profile, gimme=None, on_line=None):
    """Run gimme-aws-creds for a profile. The Okta MFA push fires during this
    call; output streams so the UI can show progress. Returns (ok, output).
    Note: gimme-aws-creds writes creds to ~/.aws/credentials on success."""
    gimme = gimme or GIMME_EXE
    args = [gimme, '--profile', profile]
    try:
        rc, out = _run(args, timeout=180, on_line=on_line)
    except FileNotFoundError:
        return False, f"'{gimme}' not found on PATH"
    return rc == 0, out


def list_eks_clusters(region, aws_profile=None, aws=None, on_line=None):
    """aws eks list-clusters. Returns (ok, [cluster_names] or error_str).
    aws_profile targets the named profile gimme wrote (see cred_profile); with
    no [default] profile present, omitting it yields NoCredentials."""
    aws = aws or AWS_EXE
    args = [aws, 'eks', 'list-clusters', '--region', region, '--output', 'json']
    if aws_profile:
        args += ['--profile', aws_profile]
    try:
        rc, out = _run(args, timeout=60, on_line=on_line)
    except FileNotFoundError:
        return False, f"'{aws}' not found on PATH"
    if rc != 0:
        return False, out.strip()
    try:
        return True, json.loads(out).get('clusters', [])
    except json.JSONDecodeError:
        return False, out.strip()


def update_kubeconfig(cluster, region, aws_profile=None, alias=None, aws=None, on_line=None):
    """aws eks update-kubeconfig — (re)generates the kubeconfig entry and
    returns (ok, context_name) so discovery/forwards can target the EXACT
    context written.

    By default update-kubeconfig names the context after the cluster ARN
    (arn:aws:eks:<region>:<acct>:cluster/<name>), which will NOT match a
    hand-made short alias already in the kubeconfig. We pass --alias so the
    context name is deterministic and equal to the cluster name, matching the
    artifact's `cluster` stamp; alias defaults to the cluster name.
    On failure returns (False, error_str)."""
    aws = aws or AWS_EXE
    alias = alias or cluster
    args = [aws, 'eks', 'update-kubeconfig', '--name', cluster, '--region', region,
            '--alias', alias]
    if aws_profile:
        args += ['--profile', aws_profile]
    try:
        rc, out = _run(args, timeout=60, on_line=on_line)
    except FileNotFoundError:
        return False, f"'{aws}' not found on PATH"
    if rc != 0:
        return False, out.strip()
    # We forced --alias, so the written context name is exactly `alias`. Still
    # parse the CLI's own "context <name> in ..." as a sanity fallback.
    m = re.search(r'context\s+(\S+)\s+in\b', out)
    written = m.group(1) if m else alias
    return True, written
