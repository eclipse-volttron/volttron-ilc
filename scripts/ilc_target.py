"""Publish a demand target to the ILC agent from the command line.

The message has the shape the agent's ``demand_limit_handler`` expects on ``<analysis_prefix_topic>/target_agent``:
a two-element list holding the target (start, target, id, optional end) and the time zone name.

The script connects to the platform the way ``vctl`` does, using the ``control.connection`` credentials under
``VOLTTRON_HOME``. Set ``VOLTTRON_HOME`` to the instance to target when it is not ``~/.volttron``. Do not run it
while another ``vctl`` command is in progress: both use the same connection identity and the platform drops one.

Example:
    VOLTTRON_HOME=~/.volttron python scripts/ilc_target.py 150 --start 2026-11-05T14:00:00 --end 2026-11-05T16:00:00
"""
from argparse import ArgumentParser
from datetime import datetime
from uuid import uuid4

from volttron.client.commands.connection import ControlConnection
from volttron.utils import ClientContext as cc

DEFAULT_TOPIC = 'record/target_agent'
DEFAULT_TZ = 'US/Pacific'


def _isoformat(value):
    """Normalise an ISO 8601 string, raising ValueError if it does not parse."""
    return datetime.fromisoformat(value).isoformat()


def make_target(target, start=None, end=None, target_id=None, tz_name=DEFAULT_TZ):
    """Build the target message for the ILC agent.

    :param target: Target power (kW).
    :param start: Start time as an ISO 8601 string; defaults to now.
    :param end: Optional end time as an ISO 8601 string. ILC defaults to 23:59:45 on the start day.
    :param target_id: Identifier for the target; a random hex id when omitted.
    :param tz_name: IANA time zone name the start and end times are interpreted in.
    """
    target_message = [
        {
            'value': {
                'start': _isoformat(start) if start else datetime.now().isoformat(),
                'target': float(target),
                'id': target_id if target_id else uuid4().hex,
            }
        },
        {'value': {'tz': tz_name}},
    ]
    if end is not None:
        target_message[0]['value']['end'] = _isoformat(end)
    return target_message


def publish_target(target_message, topic=DEFAULT_TOPIC, address=None, timeout=30.0):
    """Publish ``target_message`` on ``topic`` through a one-shot platform connection."""
    connection = ControlConnection(address or cc.get_address())
    try:
        connection.server.vip.pubsub.publish('pubsub', topic, headers={}, message=target_message).get(timeout=timeout)
    finally:
        connection.kill()


def main():
    parser = ArgumentParser(description='Publish a demand target to the ILC agent.')
    parser.add_argument('target', type=float, help='The target power for ILC.')
    parser.add_argument('--start', default=None, help='Start time for the target in ISO format (default: now).')
    parser.add_argument('--end', default=None, help='End time for the target in ISO format.')
    parser.add_argument('--target-id', default=None, help='Unique identifier for the target.')
    parser.add_argument('--tz-name', default=DEFAULT_TZ, help=f'Time zone name for the target (default: {DEFAULT_TZ}).')
    parser.add_argument('--topic', default=DEFAULT_TOPIC,
                        help=f"Topic to publish on; <analysis_prefix_topic>/target_agent (default: {DEFAULT_TOPIC}).")
    parser.add_argument('--address', default=None, help='VIP address of the platform (default: from VOLTTRON_HOME).')
    parser.add_argument('--timeout', type=float, default=30.0, help='Seconds to wait for the publish (default: 30).')
    args = parser.parse_args()

    target_message = make_target(target=args.target, start=args.start, end=args.end,
                                 target_id=args.target_id, tz_name=args.tz_name)
    print(f'Publishing target message to {args.topic}: {target_message}')
    publish_target(target_message, topic=args.topic, address=args.address, timeout=args.timeout)
    print('Published.')


if __name__ == '__main__':
    main()
