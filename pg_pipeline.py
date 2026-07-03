#!/usr/bin/env python3
"""
pg_pipeline.py — a tiny Step/Pipeline abstraction for the AWS bootstrap chain.

The bootstrap is a sequence where each step's output feeds the next:

    pick_profile -> pick_region -> aws_login -> list_clusters
                 -> pick_cluster -> update_kubeconfig -> discover

State flows through a shared `context` dict. A step CANNOT run until the
inputs it declares are present in the context — that's what makes the flow
impossible to get into a broken half-state, and it's the literal encoding of
"output of one step is the input of the next".

Two kinds of steps interleave:
  - ACTION steps run code (login, list clusters, discover). Slow ones
    (login waits on your phone; discover makes many kubectl calls) are marked
    `background=True` so the server runs them as jobs the UI can poll.
  - GATE steps pause for user input (pick a profile / region / cluster).
    Their `run` just validates and stores what the user chose.

This module is deliberately dependency-free and side-effect-free on its own —
the actual login/discovery work is injected as callables, so the pipeline is
unit-testable with fakes, same discipline as pg_discovery / pg_forward.
"""
from dataclasses import dataclass, field
from typing import Callable, Optional


class PipelineError(Exception):
    """Raised when a step's declared inputs are missing, or a step fails."""


@dataclass
class Step:
    name: str
    run: Callable[[dict, Callable[[str], None]], dict]
    requires: tuple = ()          # keys that must exist in context before this runs
    produces: tuple = ()          # keys this step adds to context (for validation/UX)
    gate: bool = False            # True = waits for user input rather than doing work
    background: bool = False      # True = long-running; server should run as a job

    def ready(self, context: dict) -> bool:
        return all(k in context and context[k] not in (None, '') for k in self.requires)

    def missing(self, context: dict) -> list:
        return [k for k in self.requires if k not in context or context[k] in (None, '')]


@dataclass
class Pipeline:
    steps: list = field(default_factory=list)
    context: dict = field(default_factory=dict)

    def by_name(self, name: str) -> Optional[Step]:
        return next((s for s in self.steps if s.name == name), None)

    def next_step(self) -> Optional[Step]:
        """First step whose outputs aren't all present yet."""
        for s in self.steps:
            if not all(k in self.context and self.context[k] not in (None, '') for k in s.produces):
                return s
        return None

    def run_step(self, name: str, emit: Callable[[str], None] = None) -> dict:
        """Run a single step by name. Validates its inputs first, merges its
        output into the shared context, and returns the updated context."""
        emit = emit or (lambda *_: None)
        step = self.by_name(name)
        if step is None:
            raise PipelineError(f'no such step: {name}')
        if not step.ready(self.context):
            raise PipelineError(f"step '{name}' missing inputs: {', '.join(step.missing(self.context))}")
        out = step.run(self.context, emit) or {}
        self.context.update(out)
        return self.context

    def reset_from(self, name: str):
        """Invalidate a step and everything downstream of it — used when a user
        goes back and re-picks (e.g. changes region, so clusters must re-list)."""
        seen = False
        for s in self.steps:
            if s.name == name:
                seen = True
            if seen:
                for k in s.produces:
                    self.context.pop(k, None)


def build_bootstrap_pipeline(*, list_profiles, do_login, list_clusters,
                             update_kubeconfig, do_discover):
    """Wire the concrete AWS bootstrap. Each *_fn is injected (from pg_aws /
    pg_discovery in production, or fakes in tests)."""

    def _profiles(ctx, emit):
        profs = list_profiles()
        emit(f'found {len(profs)} okta profile(s)')
        return {'profiles': profs}

    def _pick_profile(ctx, emit):
        # gate: UI must have set ctx['profile']; validate it exists and resolve
        # the AWS cred profile gimme writes to (needed by every later `aws` call).
        sel = next((p for p in ctx.get('profiles', []) if p['name'] == ctx.get('profile')), None)
        if sel is None:
            raise PipelineError('selected profile is not in the okta config')
        return {'aws_profile': sel.get('cred_profile') or sel['name']}

    def _pick_region(ctx, emit):
        if not ctx.get('region'):
            raise PipelineError('region is required (your okta profiles do not set one)')
        return {}

    def _login(ctx, emit):
        ok, out = do_login(ctx['profile'], on_line=emit)
        if not ok:
            raise PipelineError('AWS login failed: ' + (out.strip()[-300:] or 'unknown error'))
        return {'aws_ok': True}

    def _clusters(ctx, emit):
        ok, res = list_clusters(ctx['region'], aws_profile=ctx.get('aws_profile'), on_line=emit)
        if not ok:
            raise PipelineError('list-clusters failed: ' + str(res)[-300:])
        emit(f'found {len(res)} cluster(s)')
        return {'clusters': res}

    def _pick_cluster(ctx, emit):
        if ctx.get('cluster') not in ctx.get('clusters', []):
            raise PipelineError('selected cluster is not in the discovered list')
        return {}

    def _kubeconfig(ctx, emit):
        ok, out = update_kubeconfig(ctx['cluster'], ctx['region'],
                                    aws_profile=ctx.get('aws_profile'), on_line=emit)
        if not ok:
            raise PipelineError('update-kubeconfig failed: ' + str(out)[-300:])
        # `out` is the exact context name written (we force --alias). Discovery
        # and forwards must target THIS, not a short alias that may not exist.
        emit(f'kubeconfig context: {out}')
        return {'kubeconfig_ok': True, 'context': out}

    def _discover(ctx, emit):
        result = do_discover(ctx, emit)   # writes pg-services.json, returns summary
        return {'artifact': result}

    return Pipeline(steps=[
        Step('profiles',        _profiles,     produces=('profiles',)),
        Step('pick_profile',    _pick_profile, requires=('profiles',), produces=('profile', 'aws_profile'), gate=True),
        Step('pick_region',     _pick_region,  requires=('profile',),  produces=('region',),  gate=True),
        Step('login',           _login,        requires=('profile', 'region'), produces=('aws_ok',), background=True),
        Step('clusters',        _clusters,     requires=('aws_ok', 'region'),  produces=('clusters',), background=True),
        Step('pick_cluster',    _pick_cluster, requires=('clusters',), produces=('cluster',), gate=True),
        Step('kubeconfig',      _kubeconfig,   requires=('cluster', 'region'), produces=('kubeconfig_ok', 'context'), background=True),
        Step('discover',        _discover,     requires=('kubeconfig_ok',), produces=('artifact',), background=True),
    ])
