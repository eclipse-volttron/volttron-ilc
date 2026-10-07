"""
Unit tests for repeated ("dollar" mode) curtailment and step-wise release.

Scenario from the design discussion: four zones with a 70F normal set-point,
curtailed on three passes.  vav1 (1F increment) is curtailed on every pass,
vav2 (2F) on passes 1 and 3, vav3 on pass 3 only and vav4 on pass 1 only,
giving 7 curtailment steps.  With a 20-minute stagger and 5-minute confirm
time the agent releases them in batches [2, 2, 1, 1, 1], one step at a time
in criteria order: vav1, vav1 | vav1, vav2 | vav2 | vav3 | vav4.
"""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock
from weakref import WeakSet

import pytest

from ilc.control_handler import ControlStep, OffsetControlSetting, ValueControlSetting
from ilc.ilc_agent import ILCAgent


class FakeActuator:
    """Minimal stand-in for the platform driver RPC surface."""

    def __init__(self, values):
        self.values = dict(values)
        self.calls = []

    def call(self, actuator, method, path, point, *args):
        topic = f"{path}/{point}"
        self.calls.append((method, topic) + args)
        if method == "get_point":
            value = self.values[topic]
        elif method == "set_point":
            self.values[topic] = args[0]
            value = args[0]
        elif method == "revert_point":
            value = None
        else:
            raise AssertionError(method)
        result = MagicMock()
        result.get.return_value = value
        return result

    def sets(self, topic=None):
        return [
            (call[1], call[2]) if topic is None else call[2]
            for call in self.calls
            if call[0] == "set_point" and (topic is None or call[1] == topic)
        ]


def make_agent(actuator):
    agent = MagicMock()
    agent.vip.rpc.call = actuator.call
    agent.base_rpc_path = lambda path="": f"devices/{path}" if path else "devices"
    agent.update_base_topic = "record/ilc"
    agent.publish_record = MagicMock()
    return agent


def make_offset(agent, device, offset, minimum=None, maximum=None, mode="dollar",
                finalize_release_with_revert=False):
    controls = SimpleNamespace(id=device, manager=SimpleNamespace(name=device))
    return OffsetControlSetting(
        offset=offset,
        logging_topic="log",
        agent=agent,
        controls_object=controls,
        point="ZoneTemperatureSetPoint",
        load=1.0,
        minimum=minimum,
        maximum=maximum,
        control_mode=mode,
        default_device=f"campus/building/{device}",
        finalize_release_with_revert=finalize_release_with_revert,
    )


TOPIC = "devices/campus/building/{}/ZoneTemperatureSetPoint"


# --------------------------------------------------------------------------- #
# ControlSetting behaviour
# --------------------------------------------------------------------------- #
def test_offset_passes_compound_until_clamped():
    actuator = FakeActuator({TOPIC.format("vav1"): 70.0})
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0, maximum=72.0)

    assert setting.modify_load() == 1.0
    assert setting.modify_load() == 1.0
    assert setting.modify_load() == 0.0, "third pass is clamped and must report no load"

    assert actuator.sets(TOPIC.format("vav1")) == [71.0, 72.0]
    assert setting.revert_value == 70.0, "revert value stays at the original reading"
    assert setting.pending_releases == 2
    assert [(s.previous, s.applied) for s in setting.steps] == [(70.0, 71.0), (71.0, 72.0)]


def test_release_undoes_one_step_at_a_time():
    actuator = FakeActuator({TOPIC.format("vav1"): 70.0})
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0)
    for _ in range(3):
        setting.modify_load()
    actuator.calls.clear()

    assert setting.release() is False
    assert setting.release() is False
    assert setting.release() is True
    assert actuator.sets(TOPIC.format("vav1")) == [72.0, 71.0, 70.0]
    assert setting.pending_releases == 1  # a controlled setting always needs one release
    assert setting.steps == []


def test_full_and_trigger_release_restore_original_in_one_write():
    for kwargs in ({"full": True}, {"trigger": True}):
        actuator = FakeActuator({TOPIC.format("vav1"): 70.0})
        setting = make_offset(make_agent(actuator), "vav1", offset=1.0)
        for _ in range(3):
            setting.modify_load()
        actuator.calls.clear()

        assert setting.release(**kwargs) is True
        assert actuator.sets(TOPIC.format("vav1")) == [70.0]
        assert setting.steps == []


def test_finalize_revert_only_on_last_step():
    actuator = FakeActuator({TOPIC.format("vav1"): 70.0})
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0,
                          finalize_release_with_revert=True)
    for _ in range(2):
        setting.modify_load()
    actuator.calls.clear()

    setting.release()
    assert "revert_point" not in [c[0] for c in actuator.calls]
    setting.release()
    assert "revert_point" in [c[0] for c in actuator.calls]


def test_release_with_no_steps_falls_back_to_revert_point():
    actuator = FakeActuator({TOPIC.format("vav1"): 70.0})
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0)
    assert setting.release() is True
    assert [c[0] for c in actuator.calls] == ["revert_point"]


def test_read_failure_returns_none():
    import gevent

    actuator = FakeActuator({})

    def failing_call(*args, **kwargs):
        # volttron-core's RemoteError cannot be constructed from a bare
        # message, so simulate the other handled failure: an RPC timeout.
        raise gevent.Timeout()

    agent = make_agent(actuator)
    agent.vip.rpc.call = failing_call
    setting = make_offset(agent, "vav1", offset=1.0)
    assert setting.modify_load() is None
    assert setting.steps == []


def test_repeatable_flags():
    actuator = FakeActuator({TOPIC.format("vav1"): 70.0})
    agent = make_agent(actuator)
    assert make_offset(agent, "vav1", offset=1.0).repeatable is True
    controls = SimpleNamespace(id="vav1", manager=SimpleNamespace(name="vav1"))
    value_setting = ValueControlSetting(
        value=65.0, logging_topic="log", agent=agent, controls_object=controls,
        point="ZoneTemperatureSetPoint", load=1.0, default_device="campus/building/vav1",
    )
    assert value_setting.repeatable is False


# --------------------------------------------------------------------------- #
# Agent-level staggered release
# --------------------------------------------------------------------------- #
def make_agent_shell(settings, score_order):
    """Build an ILCAgent without running __init__, with just the release state."""
    agent = object.__new__(ILCAgent)
    agent.devices = WeakSet(settings)
    agent.criteria_container = MagicMock()
    agent.criteria_container.get_score_order.return_value = score_order
    agent.control_container = MagicMock()
    agent.stagger_release = True
    agent.stagger_release_time = timedelta(minutes=20)
    agent.confirm_time = timedelta(minutes=5)
    agent.current_time = datetime(2026, 10, 2, 12, 0)
    agent.state_at_actuation = "curtail"
    agent.state = "curtail_releasing"
    agent.finished = MagicMock()
    agent.lock = True
    return agent


def build_scenario():
    actuator = FakeActuator({TOPIC.format(f"vav{i}"): 70.0 for i in range(1, 5)})
    vip_agent = make_agent(actuator)
    vav1 = make_offset(vip_agent, "vav1", offset=1.0)
    vav2 = make_offset(vip_agent, "vav2", offset=2.0)
    vav3 = make_offset(vip_agent, "vav3", offset=2.0)
    vav4 = make_offset(vip_agent, "vav4", offset=2.0)

    # Pass 1: vav1, vav2, vav4.  Pass 2: vav1.  Pass 3: vav1, vav2, vav3.
    for setting in (vav1, vav2, vav4, vav1, vav1, vav2, vav3):
        assert setting.modify_load() == 1.0

    assert actuator.values[TOPIC.format("vav1")] == 73.0
    assert actuator.values[TOPIC.format("vav2")] == 74.0
    assert actuator.values[TOPIC.format("vav3")] == 72.0
    assert actuator.values[TOPIC.format("vav4")] == 72.0
    actuator.calls.clear()
    return actuator, [vav1, vav2, vav3, vav4]


def test_setup_release_counts_steps_not_devices():
    _, settings = build_scenario()
    agent = make_agent_shell(settings, [(s.device_name, s.device_id) for s in settings])

    agent.setup_release()

    assert agent.device_group_size == [2, 2, 1, 1, 1]
    assert agent.current_stagger == [5, 5, 5, 5]


def test_staggered_release_sequence_matches_table():
    actuator, settings = build_scenario()
    # reset_devices releases in reverse score order, so score vav4 first to
    # make the criteria release vav1 first.
    score_order = [(s.device_name, s.device_id) for s in reversed(settings)]
    agent = make_agent_shell(settings, score_order)
    agent.setup_release()

    batches = []
    for _ in range(5):
        before = len(actuator.calls)
        agent.reset_devices()
        batches.append(
            [(topic.split("/")[3], value) for topic, value in actuator.sets()[len(actuator.sets()) - (len(actuator.calls) - before):]]
        )

    assert batches == [
        [("vav1", 72.0), ("vav1", 71.0)],   # t = 0
        [("vav1", 70.0), ("vav2", 72.0)],   # t = 5
        [("vav2", 70.0)],                   # t = 10
        [("vav3", 70.0)],                   # t = 15
        [("vav4", 70.0)],                   # t = 20
    ]
    assert all(value == 70.0 for value in actuator.values.values())
    assert len(agent.devices) == 0
    assert agent.device_group_size == []
    # reset_control_status is called exactly once per device, when fully released
    assert agent.control_container.get_device.return_value.reset_control_status.call_count == 4
    agent.finished.assert_called_once()


def test_filter_keeps_only_repeatable_dollar_devices():
    actuator = FakeActuator({TOPIC.format("vav1"): 70.0, TOPIC.format("vav2"): 70.0})
    vip_agent = make_agent(actuator)
    dollar = make_offset(vip_agent, "vav1", offset=1.0, mode="dollar")
    comfort = make_offset(vip_agent, "vav2", offset=1.0, mode="comfort")
    agent = make_agent_shell([dollar, comfort], [])

    candidates = [
        ("vav1", "vav1", "platform.driver"),
        ("vav2", "vav2", "platform.driver"),
    ]
    assert agent._filter_already_controlled(candidates) == [("vav1", "vav1", "platform.driver")]
    assert agent._existing_setting("vav1", "vav1") is dollar
    assert agent._existing_setting("vav9", "vav9") is None


# --------------------------------------------------------------------------- #
# Hidden BAS limit detection
# --------------------------------------------------------------------------- #
class LimitedActuator(FakeActuator):
    """Actuator whose BAS silently rejects or clamps writes above a limit."""

    def __init__(self, values, limit, clamp=False):
        super().__init__(values)
        self.limit = limit
        self.clamp = clamp

    def call(self, actuator, method, path, point, *args):
        if method == "set_point" and args[0] > self.limit:
            topic = f"{path}/{point}"
            self.calls.append((method, topic) + args)
            if self.clamp:
                self.values[topic] = self.limit
            result = MagicMock()
            result.get.return_value = args[0]
            return result
        return super().call(actuator, method, path, point, *args)


def test_rejected_write_is_detected_and_dropped():
    topic = TOPIC.format("vav1")
    actuator = LimitedActuator({topic: 70.0}, limit=72.0)
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0)

    assert setting.modify_load() == 1.0          # 71 accepted
    assert setting.modify_load() == 1.0          # 72 accepted
    assert setting.modify_load() == 1.0          # 73 sent, silently ignored
    assert actuator.values[topic] == 72.0
    assert setting.pending_releases == 3

    writes_before = len(actuator.sets(topic))
    assert setting.modify_load() == 0.0, "pass after rejection must report no load"
    assert len(actuator.sets(topic)) == writes_before, "and must not write again"
    assert setting.steps[-1].applied == 72.0, "rejected step dropped"
    assert setting.pending_releases == 2
    assert setting.limit_value == 72.0

    assert setting.modify_load() == 0.0, "still pinned: no-op"
    assert len(actuator.sets(topic)) == writes_before

    actuator.calls.clear()
    assert setting.release() is False
    assert setting.release() is True
    assert actuator.sets(topic) == [71.0, 70.0]
    assert setting.limit_value is None


def test_clamped_write_corrects_step():
    topic = TOPIC.format("vav1")
    actuator = LimitedActuator({topic: 70.0}, limit=72.5, clamp=True)
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0)

    for _ in range(3):
        assert setting.modify_load() == 1.0      # 71, 72, 73 -> BAS holds 72.5
    assert actuator.values[topic] == 72.5

    assert setting.modify_load() == 0.0
    assert setting.steps[-1] == setting.steps[-1]._replace(previous=72.0, applied=72.5)
    assert setting.pending_releases == 3
    assert setting.limit_value == 72.5

    actuator.calls.clear()
    assert [setting.release() for _ in range(3)] == [False, False, True]
    assert actuator.sets(topic) == [72.0, 71.0, 70.0]


def test_limit_clears_when_value_moves_away():
    topic = TOPIC.format("vav1")
    actuator = LimitedActuator({topic: 70.0}, limit=72.0)
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0)
    for _ in range(3):
        setting.modify_load()
    assert setting.modify_load() == 0.0          # detects rejection of 73
    assert setting.limit_value == 72.0

    actuator.values[topic] = 68.0                # BAS schedule / occupant moves it
    actuator.calls.clear()
    assert setting.modify_load() == 1.0
    assert actuator.sets(topic) == [69.0]
    assert setting.limit_value is None


def test_float_noise_within_tolerance_counts_as_took_effect():
    topic = TOPIC.format("vav1")
    actuator = FakeActuator({topic: 70.0})
    setting = make_offset(make_agent(actuator), "vav1", offset=1.0)
    assert setting.modify_load() == 1.0          # 71 written
    actuator.values[topic] = 70.995              # BACnet real round-trip noise
    assert setting.modify_load() == 1.0
    assert actuator.sets(topic) == [71.0, 71.995]
    assert setting.limit_value is None
