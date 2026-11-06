"""Build the demand-target message the ILC agent consumes.

The agent subscribes to ``<analysis_prefix_topic>/target_agent`` and reads a two-element list: the target itself
(start, target, id and an optional end) followed by the time zone the times are interpreted in. This module has no
platform dependencies so command-line tools can import it cheaply.
"""
from datetime import datetime
from uuid import uuid4

DEFAULT_PREFIX = 'record'
DEFAULT_TZ = 'US/Pacific'
TARGET_TOPIC_SUFFIX = 'target_agent'


def target_topic(prefix=DEFAULT_PREFIX):
    """The topic the agent listens on for targets, given its ``analysis_prefix_topic``."""
    return f'{prefix}/{TARGET_TOPIC_SUFFIX}'


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
