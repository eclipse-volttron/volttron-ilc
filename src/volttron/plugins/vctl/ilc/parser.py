#!/usr/bin/env python
"""ILC commands: ``vctl ilc <command>`` (this module is a vctl plugin, modular platform) and the standalone ``vilc``
script (``vilc ilc <command>``, Monolithic platform, installed with ``setup_vilc.py``).

Importing this module has no side effects: vctl imports every module under ``volttron.plugins.vctl`` on every
invocation, so the connection to the platform is made only when a command runs. Under vctl the command reuses vctl's
own control connection; standalone, one is built on first use from the platform's control credentials. Both share
that identity with vctl, so do not run them alongside another vctl command.

Commands:
    target  Publish a demand target on ``<analysis_prefix_topic>/target_agent``.
"""
from __future__ import annotations

import argparse
import os
import sys

from ilc.target_message import DEFAULT_PREFIX, DEFAULT_TZ, make_target, target_topic

_standalone_connection = None


def _modular() -> bool:
    from importlib.metadata import PackageNotFoundError, distribution
    try:
        distribution('volttron-core')
        return True
    except PackageNotFoundError:
        return False


def connection(opts=None):
    """The control connection: vctl's own (``opts.connection``) when the command runs under vctl, else one built on
    first use. Raises ValueError when the platform's control credentials are absent (platform not running)."""
    global _standalone_connection
    existing = getattr(opts, 'connection', None)
    if existing is not None:
        return existing
    if _standalone_connection is None:
        if _modular():
            from volttron.client.commands.connection import ControlConnection
            from volttron.utils import ClientContext as cc
            _standalone_connection = ControlConnection(cc.get_address())
        else:                                                   # Monolithic platform
            from volttron.platform import get_address
            from volttron.platform.control.control_connection import ControlConnection
            _standalone_connection = ControlConnection(get_address())
    return _standalone_connection


# ---------------------------------------------------------------- commands
def ilc_target(opts):
    """Publish a demand target to the ILC agent. Returns None: vctl uses a handler's return value as its exit code."""
    message = make_target(target=opts.target, start=opts.start, end=opts.end,
                          target_id=opts.target_id, tz_name=opts.tz_name)
    topic = target_topic(opts.prefix)
    timeout = getattr(opts, 'timeout', None) or 30
    print(f'Publishing target to {topic}: {message}')
    connection(opts).server.vip.pubsub.publish('pubsub', topic, headers={}, message=message).get(timeout=timeout)


def build_ilc_commands(add_parser):
    """Add the ILC subcommands through ``add_parser(name, help=...)`` (vctl's registering function, or
    ``subparsers.add_parser`` for vilc)."""
    p = add_parser('target', help='Publish a demand target to the ILC agent.')
    p.add_argument('target', type=float, help='The target power for ILC.')
    p.add_argument('--start', default=None, help='Start time for the target in ISO format (default: now).')
    p.add_argument('--end', default=None, help='End time for the target in ISO format.')
    p.add_argument('--target-id', default=None, help='Unique identifier for the target.')
    p.add_argument('--tz-name', default=DEFAULT_TZ, help=f'Time zone name for the target (default: {DEFAULT_TZ}).')
    p.add_argument('--prefix', default=DEFAULT_PREFIX,
                   help=f"The agent's analysis_prefix_topic; the target is published on <prefix>/target_agent "
                        f'(default: {DEFAULT_PREFIX}).')
    p.set_defaults(func=ilc_target)


# ---------------------------------------------------------------- vctl plugin (modular platform)
try:
    from volttron.client.decorators import vctl_subparser
    from volttron.types.factories import ControlParser
except ImportError:                          # Monolithic platform: no plugin API; vilc still works
    vctl_subparser = None

if vctl_subparser is not None:
    @vctl_subparser
    class IlcVCtlParser(ControlParser):
        """``vctl ilc …``: the ILC agent's commands."""

        class Meta:
            name = 'ilc'

        def configure(self, ctx):
            ilc = ctx.register_command('ilc', help='ILC agent commands')
            # required: a bare "vctl ilc" errors with the usage instead of needing a parent default, which argparse
            # would apply over the subcommand's func (set_defaults of a parent wins over the subparser's).
            subs = ilc.add_subparsers(title='subcommands', metavar='', dest='ilc_command', required=True)
            build_ilc_commands(lambda name, **kw: ctx.register_subcommand(subs, name, **kw))


# ---------------------------------------------------------------- vilc (Monolithic platform)
def main(argv=None):
    parser = argparse.ArgumentParser(prog='vilc', description='ILC agent commands (the same as vctl ilc).')
    common = argparse.ArgumentParser(add_help=False)       # accepted after the subcommand, as vctl's global args are
    common.add_argument('--timeout', type=float, default=30, help='Seconds to wait for the platform (default: 30).')
    subparsers = parser.add_subparsers(dest='command')
    ilc = subparsers.add_parser('ilc', help='ILC agent commands')
    ilc_subs = ilc.add_subparsers(title='subcommands', metavar='', dest='ilc_command', required=True)
    build_ilc_commands(lambda name, **kw: ilc_subs.add_parser(name, parents=[common], **kw))
    opts = parser.parse_args(argv)
    if not hasattr(opts, 'func'):
        parser.print_help()
        return 0
    try:
        opts.func(opts)
    except ValueError as e:                 # no control credentials: the platform is not running in this VOLTTRON_HOME
        sys.stderr.write(f'{e}\nIs the platform running, and is VOLTTRON_HOME '
                         f'({os.environ.get("VOLTTRON_HOME", "unset")}) the right home?\n')
        return 1
    finally:
        if _standalone_connection is not None:
            _standalone_connection.kill()
    return 0


if __name__ == '__main__':
    sys.exit(main())
