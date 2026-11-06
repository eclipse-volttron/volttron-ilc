"""ILC commands for vctl: ``vctl ilc <command>``.

Importing this module has no side effects: vctl imports every module under ``volttron.plugins.vctl`` on every
invocation, so nothing here touches the platform until a command runs. Commands publish and call through vctl's own
control connection (``opts.connection``).

Commands:
    target  Publish a demand target on ``<analysis_prefix_topic>/target_agent``.
"""
from __future__ import annotations

from ilc.target_message import DEFAULT_PREFIX, DEFAULT_TZ, make_target, target_topic
from volttron.client.decorators import vctl_subparser
from volttron.types.factories import ControlParser


def ilc_target(opts):
    """Publish a demand target to the ILC agent. Returns None: vctl uses a handler's return value as its exit code."""
    message = make_target(target=opts.target, start=opts.start, end=opts.end,
                          target_id=opts.target_id, tz_name=opts.tz_name)
    topic = target_topic(opts.prefix)
    print(f'Publishing target to {topic}: {message}')
    opts.connection.server.vip.pubsub.publish('pubsub', topic, headers={}, message=message).get(timeout=opts.timeout)


@vctl_subparser
class IlcVCtlParser(ControlParser):
    """``vctl ilc …``: the ILC agent's commands."""

    class Meta:
        name = 'ilc'

    def configure(self, ctx):
        ilc = ctx.register_command('ilc', help='ILC agent commands')
        # required: a bare "vctl ilc" errors with the usage instead of needing a parent default, which argparse would
        # apply over the subcommand's func (set_defaults of a parent wins over the subparser's).
        subs = ilc.add_subparsers(title='subcommands', metavar='', dest='ilc_command', required=True)

        p = ctx.register_subcommand(subs, 'target', help='Publish a demand target to the ILC agent.')
        p.add_argument('target', type=float, help='The target power for ILC.')
        p.add_argument('--start', default=None, help='Start time for the target in ISO format (default: now).')
        p.add_argument('--end', default=None, help='End time for the target in ISO format.')
        p.add_argument('--target-id', default=None, help='Unique identifier for the target.')
        p.add_argument('--tz-name', default=DEFAULT_TZ, help=f'Time zone name for the target (default: {DEFAULT_TZ}).')
        p.add_argument('--prefix', default=DEFAULT_PREFIX,
                       help=f"The agent's analysis_prefix_topic; the target is published on <prefix>/target_agent "
                            f'(default: {DEFAULT_PREFIX}).')
        p.set_defaults(func=ilc_target)
