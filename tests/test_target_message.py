"""Unit tests for the ILC target message builder and the ``vctl ilc`` / ``vilc`` command parser."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from dateutil import parser as dtparser, tz

from ilc.target_message import DEFAULT_PREFIX, DEFAULT_TZ, make_target, target_topic
from volttron.plugins.vctl.ilc import parser as ilc_cli


def test_make_target_full_message():
    message = make_target(150, start='2026-11-05T14:00:00', end='2026-11-05T16:00:00', target_id='abc',
                          tz_name='US/Pacific')
    assert message == [
        {'value': {'start': '2026-11-05T14:00:00', 'target': 150.0, 'id': 'abc', 'end': '2026-11-05T16:00:00'}},
        {'value': {'tz': 'US/Pacific'}},
    ]


def test_make_target_defaults():
    message = make_target('75.5')
    value = message[0]['value']
    assert value['target'] == 75.5
    assert 'end' not in value
    assert len(value['id']) == 32
    dtparser.parse(value['start'])            # start defaults to a parseable "now"
    assert message[1]['value']['tz'] == DEFAULT_TZ


def test_make_target_rejects_bad_iso():
    with pytest.raises(ValueError):
        make_target(1, start='yesterday')


def test_agent_side_parse_matches():
    """The agent parses start with dateutil and localises it to the message's time zone."""
    message = make_target(1, start='2026-11-05T14:00:00', tz_name='US/Pacific')
    info, tz_name = message[0]['value'], message[1]['value']['tz']
    start = dtparser.parse(info['start']).astimezone(tz.gettz(tz_name))
    assert (start.hour, start.utcoffset().total_seconds() / 3600) == (14, -8)


def test_target_topic():
    assert target_topic() == f'{DEFAULT_PREFIX}/target_agent'
    assert target_topic('analysis') == 'analysis/target_agent'


def test_vilc_target_publishes_through_connection(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr(ilc_cli, 'connection', lambda opts=None: fake)
    rc = ilc_cli.main(['ilc', 'target', '120', '--start', '2026-11-05T14:00:00', '--target-id', 't1',
                       '--prefix', 'analysis', '--timeout', '5'])
    assert rc == 0
    args, kwargs = fake.server.vip.pubsub.publish.call_args
    assert args == ('pubsub', 'analysis/target_agent')
    assert kwargs['message'][0]['value'] == {'start': '2026-11-05T14:00:00', 'target': 120.0, 'id': 't1'}
    fake.server.vip.pubsub.publish.return_value.get.assert_called_once_with(timeout=5)


def test_vilc_reports_missing_platform(monkeypatch, capsys):
    def no_platform(opts=None):
        raise ValueError('Control connection credentials not found')
    monkeypatch.setattr(ilc_cli, 'connection', no_platform)
    assert ilc_cli.main(['ilc', 'target', '1']) == 1
    assert 'Is the platform running' in capsys.readouterr().err


def test_ilc_target_under_vctl_reuses_vctl_connection():
    vctl_connection = MagicMock()
    opts = SimpleNamespace(target=10, start=None, end=None, target_id='x', tz_name=DEFAULT_TZ, prefix=DEFAULT_PREFIX,
                           timeout=20, connection=vctl_connection)
    ilc_cli.ilc_target(opts)
    args, _ = vctl_connection.server.vip.pubsub.publish.call_args
    assert args == ('pubsub', 'record/target_agent')
    vctl_connection.kill.assert_not_called()


def test_plugin_registers_ilc_command():
    """The plugin registers through the vctl context and builds a parser accepting the target command."""
    import argparse
    root = argparse.ArgumentParser()
    subs = root.add_subparsers(dest='command')
    global_args = argparse.ArgumentParser(add_help=False)
    global_args.add_argument('--timeout', type=float, default=30)
    from volttron.types.factories import VctlParserContext
    ctx = VctlParserContext(subparsers=subs, global_args=global_args, filterable=argparse.ArgumentParser(add_help=False))
    ilc_cli.IlcVCtlParser().configure(ctx)
    opts = root.parse_args(['ilc', 'target', '99', '--timeout', '7'])
    assert opts.func is ilc_cli.ilc_target and opts.target == 99.0 and opts.timeout == 7
    from volttron.client.decorators import vctl_subparser
    assert ilc_cli.IlcVCtlParser in vctl_subparser.registry.values()


def test_ilc_target_returns_none_for_vctl_exit_code():
    opts = SimpleNamespace(target=1, start=None, end=None, target_id='x', tz_name=DEFAULT_TZ, prefix=DEFAULT_PREFIX,
                           timeout=1, connection=MagicMock())
    assert ilc_cli.ilc_target(opts) is None
