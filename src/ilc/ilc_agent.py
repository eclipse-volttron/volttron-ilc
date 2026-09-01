# -*- coding: utf-8 -*-
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2026 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import logging
import math
import sys
import time
from datetime import datetime as dt, timedelta as td
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from weakref import WeakSet

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
import dateutil.tz
import gevent
from dateutil import parser
from transitions import Machine

# ---------------------------------------------------------------------------
# Volttron (supports both volttron-core and legacy platform packages)
# ---------------------------------------------------------------------------
from importlib.metadata import PackageNotFoundError, distribution

try:
    distribution("volttron-core")
    from volttron.client.logs import setup_logging
    from volttron.client.messaging import headers as headers_mod
    from volttron.client.messaging import topics
    from volttron.client.vip.agent import Agent, Core, RPC
    from volttron.utils import (
        format_timestamp,
        get_aware_utc_now,
        parse_timestamp_string,
        vip_main,
    )
    from volttron.utils.jsonrpc import RemoteError
    from volttron.utils.math_utils import mean
except PackageNotFoundError:
    from volttron.platform.agent.math_utils import mean
    from volttron.platform.agent.utils import (
        format_timestamp,
        get_aware_utc_now,
        load_config,  # noqa: F401
        parse_timestamp_string,
        setup_logging,
        vip_main,
    )
    from volttron.platform.jsonrpc import RemoteError
    from volttron.platform.messaging import headers as headers_mod
    from volttron.platform.messaging import topics
    from volttron.platform.vip.agent import Agent, Core, RPC

# ---------------------------------------------------------------------------
# Local
# ---------------------------------------------------------------------------
from ilc.control_handler import ControlCluster, ControlContainer
from ilc.criteria_handler import CriteriaCluster, CriteriaContainer
from ilc.ilc_matrices import (
    calc_column_sums,
    extract_criteria,
    normalize_matrix,
    validate_input,
)
from ilc.utils import sympy_evaluate

# ---------------------------------------------------------------------------
# Module-level setup
# ---------------------------------------------------------------------------
__version__ = "3.0.1"

setup_logging()
_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
APP_CATEGORY = "LOAD CONTROL"
APP_NAME = "ILC"
DEFAULT_ACTUATOR = "platform.driver"
DEFAULT_TIMEZONE = "US/Pacific"
PUBSUB_PEER = "pubsub"

# ---------------------------------------------------------------------------
# Typing aliases
# ---------------------------------------------------------------------------
TimestampedPower = Tuple[dt, float]
DeviceToken = Tuple[str, str, str]

# ---------------------------------------------------------------------------
# State-machine definitions
# ---------------------------------------------------------------------------
AGENT_STATES = [
    "inactive",
    "curtail",
    "curtail_holding",
    "curtail_releasing",
    "augment",
    "augment_holding",
    "augment_releasing",
]

_CURTAIL_TRANSITIONS = [
    {"trigger": "curtail_load", "source": "inactive", "dest": "curtail"},
    {"trigger": "curtail_load", "source": "curtail", "dest": "="},
    {"trigger": "hold", "source": "curtail", "dest": "curtail_holding"},
    {
        "trigger": "curtail_load",
        "source": "curtail_holding",
        "dest": "curtail",
        "conditions": "confirm_elapsed",
    },
    {
        "trigger": "release",
        "source": "curtail_holding",
        "dest": "curtail_releasing",
        "conditions": "confirm_start_release",
        "after": "reset_devices",
    },
    {
        "trigger": "release",
        "source": "curtail",
        "dest": "curtail_releasing",
        "conditions": "confirm_start_release",
        "after": "reset_devices",
    },
    {
        "trigger": "curtail_load",
        "source": "curtail_releasing",
        "dest": "curtail",
        "conditions": "confirm_next_release",
    },
]

_AUGMENT_TRANSITIONS = [
    {"trigger": "augment_load", "source": "inactive", "dest": "augment"},
    {"trigger": "augment_load", "source": "augment", "dest": "="},
    {"trigger": "hold", "source": "augment", "dest": "augment_holding"},
    {
        "trigger": "augment_load",
        "source": "augment_holding",
        "dest": "augment",
        "conditions": "confirm_elapsed",
    },
    {
        "trigger": "release",
        "source": "augment_holding",
        "dest": "augment_releasing",
        "conditions": "confirm_start_release",
        "after": "reset_devices",
    },
    {
        "trigger": "release",
        "source": "augment",
        "dest": "augment_releasing",
        "conditions": "confirm_start_release",
        "after": "reset_devices",
    },
    {
        "trigger": "augment_load",
        "source": "augment_releasing",
        "dest": "augment",
        "conditions": "confirm_next_release",
    },
]

_SHARED_TRANSITIONS = [
    {
        "trigger": "release",
        "source": ["curtail_releasing", "augment_releasing"],
        "dest": None,
        "after": "reset_devices",
        "conditions": "confirm_next_release",
    },
    {
        "trigger": "curtail_load",
        "source": ["augment", "augment_holding", "augment_releasing"],
        "dest": "curtail_holding",
        "after": "reinitialize_release",
    },
    {
        "trigger": "augment_load",
        "source": ["curtail", "curtail_holding", "curtail_releasing"],
        "dest": "augment_holding",
        "after": "reinitialize_release",
    },
    {
        "trigger": "finished",
        "source": ["curtail_releasing", "augment_releasing"],
        "dest": "inactive",
        "after": "reinitialize_release",
    },
    {
        "trigger": "no_target",
        "source": "*",
        "dest": "inactive",
        "after": "reinitialize_release",
    },
]

AGENT_TRANSITIONS = _CURTAIL_TRANSITIONS + _AUGMENT_TRANSITIONS + _SHARED_TRANSITIONS

_ACTIVE_STATES = frozenset(
    {
        "curtail",
        "curtail_holding",
        "curtail_releasing",
        "augment",
        "augment_holding",
        "augment_releasing",
    }
)

# ---------------------------------------------------------------------------
# Default configuration
# ---------------------------------------------------------------------------
DEFAULT_CONFIG: Dict[str, Any] = {
    "campus": "CAMPUS",
    "building": "BUILDING",
    "power_meter": {},
    "agent_id": APP_NAME,
    "demand_limit": 30.0,
    "control_time": 20.0,
    "curtailment_confirm": 5.0,
    "curtailment_break": 20.0,
    "average_building_power_window": 15.0,
    "stagger_release": True,
    "stagger_off_time": True,
    "simulation_running": False,
    "confirm_time": 5,
    "clusters": [],
}


def _build_topic(*parts: str) -> str:
    """
    Join non-empty topic fragments with ``"/"``.

    :param parts: Topic fragments.
    :returns: A normalized topic string.
    """
    return "/".join(part for part in parts if part)


def _make_headers(current_time: Optional[dt], sim_running: bool) -> Dict[str, str]:
    """
    Create standard publish headers.

    In simulation mode, the provided ``current_time`` is preferred. Otherwise,
    the current UTC time is used.

    :param current_time: Simulated or current timestamp.
    :param sim_running: Whether the agent is in simulation mode.
    :returns: A VOLTTRON-style headers dictionary.
    """
    timestamp = current_time if sim_running and current_time else get_aware_utc_now()
    return {headers_mod.DATE: format_timestamp(timestamp)}


def _parse_power_meta_fallback() -> Dict[str, str]:
    """
    Return default metadata for power values.

    :returns: Default metadata dictionary.
    """
    return {"tz": "UTC", "units": "kiloWatts", "type": "float"}


def _exponential_moving_average(power_readings: List[TimestampedPower]) -> float:
    """
    Compute an exponential moving average over power readings.

    Readings are sorted newest-first to apply the decay factor from the most
    recent reading backward.

    :param power_readings: A list of ``(timestamp, value)`` tuples.
    :returns: The exponential moving average.
    """
    if not power_readings:
        return 0.0

    count = len(power_readings)
    alpha = min((2.0 / (count + 1.0)) * 2.0, 1.0)
    sorted_desc = sorted(power_readings, reverse=True)

    ema = sum(
        value * alpha * ((1.0 - alpha) ** index)
        for index, (_, value) in enumerate(sorted_desc)
    )
    ema += sorted_desc[-1][1] * ((1.0 - alpha) ** count)
    return ema


# ═══════════════════════════════════════════════════════════════════════════
# ILC Agent
# ═══════════════════════════════════════════════════════════════════════════

class ILCAgent(Agent):
    """
    Finite-state-machine agent for intelligent building load control.

    The agent monitors building power consumption and transitions between
    curtailment and augmentation modes to keep demand within a configurable
    target.
    """

    def __init__(self, config_path: str, **kwargs: Any) -> None:
        """
        Initialize the ILC agent.

        :param config_path: Path to the agent configuration.
        :param kwargs: Additional keyword arguments passed to the base agent.
        """
        super().__init__(**kwargs)
        self.device_group_size = None
        self.state: Optional[str] = None

        self._init_state_machine()
        self._init_instance_defaults()

        self.vip.config.set_default("config", self.default_config)
        self.vip.config.subscribe(
            self.configure_main,
            actions=["NEW", "UPDATE"],
            pattern="config",
        )

    def _init_state_machine(self) -> None:
        """
        Create the transitions state machine and register state callbacks.

        :returns: ``None``
        """
        self.state_machine = Machine(
            model=self,
            states=AGENT_STATES,
            transitions=AGENT_TRANSITIONS,
            initial="inactive",
            queued=True,
        )
        self.state_machine.on_enter_curtail("modify_load")
        self.state_machine.on_enter_augment("modify_load")
        self.state_machine.on_enter_curtail_releasing("setup_release")
        self.state_machine.on_enter_augment_releasing("setup_release")

    def _init_instance_defaults(self) -> None:
        """
        Initialize all instance attributes to safe default values.

        :returns: ``None``
        """
        self.default_config = DEFAULT_CONFIG.copy()

        # Timing
        self.confirm_time: td = td(minutes=5)
        self.current_time: Optional[dt] = None
        self.action_time: td = td(minutes=15)
        self.average_window: td = td(minutes=15)
        self.actuator_schedule_buffer: td = td(minutes=15) + self.action_time
        self.longest_possible_curtail: td = td(minutes=0)
        self.stagger_release_time: td = self.action_time
        self.next_confirm: Optional[dt] = None
        self.action_end: Optional[dt] = None
        self.next_release: Optional[dt] = None

        # Power tracking
        self.bldg_power: List[TimestampedPower] = []
        self.avg_power: Optional[float] = None
        self.power_meta: Optional[Dict[str, Any]] = None
        self.power_point: Optional[str] = None
        self.power_meter_topic: Optional[str] = None

        # Demand
        self.demand_limit: Optional[float] = None
        self.demand_schedule: Optional[Any] = None
        self.demand_threshold: float = 5.0
        self.demand_expr: Optional[str] = None
        self.demand_args: Optional[List[str]] = None
        self.calculate_demand: bool = False

        # Device management
        self.devices: WeakSet = WeakSet()
        self.scheduled_devices: Set[Tuple[str, str, str]] = set()
        self.device_group_size: Optional[List[int]] = None
        self.current_stagger: Optional[List[int]] = None
        self.device_topic_list: List[str] = []
        self.state_at_actuation: Optional[str] = None

        # Agent identity / topics
        self.agent_id: str = APP_NAME
        self.record_topic: str = "record"
        self.target_agent_subscription: str = "record/target_agent"
        self.update_base_topic: str = self.record_topic
        self.ilc_start_topic: str = f"{self.agent_id}/ilc/start"
        self.base_rpc_path = topics.RPC_DEVICE_PATH(
            campus="",
            building="",
            unit="",
            path=None,
            point="",
        )

        # Operational flags
        self.kill_signal_received: bool = False
        self.kill_device_topic: Optional[str] = None
        self.kill_pt: Optional[str] = None
        self.lock: bool = False
        self.sim_running: bool = False
        self.sim_time: int = 0
        self.config_reload_needed: bool = False
        self.saved_config: Optional[Dict[str, Any]] = None
        self.stagger_release: bool = False
        self.need_actuator_schedule: bool = False
        self.load_control_modes: List[str] = ["curtail"]
        self.schedule: Dict[Any, Any] = {}
        self.tasks: Dict[Any, Any] = {}
        self.tz: Optional[Any] = None

        # Containers
        self.criteria_container = CriteriaContainer()
        self.control_container = ControlContainer()

        # Topic caches
        self.criteria_topics: Dict[Any, List[str]] = {}
        self.control_topics: Dict[Any, List[str]] = {}
        self.all_criteria_topics: List[str] = []
        self.all_control_topics: List[str] = []

    def configure_main(
        self,
        config_name: str,
        action: str,
        contents: Dict[str, Any],
    ) -> None:
        """
        Handle configuration-store ``NEW`` and ``UPDATE`` actions.

        If the agent is currently active, configuration changes are deferred
        until the agent returns to the inactive state.

        :param config_name: Name of the config object.
        :param action: Config store action.
        :param contents: Configuration payload.
        :returns: ``None``
        """
        config = self.default_config.copy()
        config.update(contents)

        if action in ("NEW", "UPDATE"):
            _log.debug(
                "Config %s | action=%s | state=%s",
                config_name,
                action,
                self.state,
            )
            if self.state not in _ACTIVE_STATES:
                self.reset_parameters(config)
            else:
                _log.debug("Curtailment active — deferring config update")
                self.config_reload_needed = True
                self.saved_config = self.default_config.copy()
                self.saved_config.update(contents)

    @RPC.export
    def update_configurations(self, data: Dict[str, Any], force_demand_update: bool=False) -> bool:
        """
        Update ILC configuration objects via RPC.

        The ``data`` payload must contain a ``"config"`` key. All remaining
        keys are written as named configuration objects.

        :param data: Configuration payload.
        :returns: ``True`` on success, otherwise ``False``.
        """
        config = data.pop("config", None)
        if config is None:
            _log.warning("RPC update_configurations: 'config' key missing")
            return False

        if force_demand_update:
            demand_limit = config.get("demand_limit")
            self.demand_limit = demand_limit if isinstance(demand_limit, (float, int)) else None
            _log.debug(f"Force demand limit update: {demand_limit}")
            return

        for name, payload in data.items():
            self.vip.config.set(name, payload)

        self.vip.config.set(
            "config",
            config,
            send_update=True,
            trigger_callback=True,
        )
        return True

    def reset_parameters(self, config: Dict[str, Any]) -> None:
        """
        Apply configuration values to agent parameters.

        :param config: Effective configuration dictionary.
        :returns: ``None``
        """
        self.agent_id = config.get("agent_id", APP_NAME)
        self.load_control_modes = config.get("load_control_modes", ["curtail"])

        campus = config.get("campus", "")
        building = config.get("building", "")

        self.record_topic = config.get("analysis_prefix_topic", self.record_topic)
        self.target_agent_subscription = f"{self.record_topic}/target_agent"
        self.update_base_topic = _build_topic(self.record_topic, campus, building)
        self.ilc_start_topic = _build_topic(self.agent_id, campus, building, "ilc/start")

        self._configure_clusters(config["clusters"])
        self._configure_power_meter(config.get("power_meter", {}))
        self._configure_kill_switch(config.get("kill_switch"), campus, building)
        self._configure_demand(config)
        self._configure_timing(config)

        self.stagger_release = config.get("stagger_release", self.stagger_release)
        self.need_actuator_schedule = config.get(
            "need_actuator_schedule",
            self.need_actuator_schedule,
        )
        self.demand_threshold = config.get("demand_threshold", self.demand_threshold)
        self.sim_running = config.get("simulation_running", self.sim_running)

        self.starting_base("core")
        self.config_reload_needed = False

    def _configure_clusters(self, cluster_configs: List[Dict[str, Any]]) -> None:
        """
        Parse cluster configuration and populate criteria and control containers.

        :param cluster_configs: List of cluster configuration dictionaries.
        :returns: ``None``
        """
        self.criteria_container = CriteriaContainer()
        self.control_container = ControlContainer()
        self.device_topic_list = []

        for cluster_cfg in cluster_configs:
            _log.debug("Cluster config: %s", cluster_cfg)

            pairwise_cfg = cluster_cfg.get("pairwise_criteria_config")
            criteria_cfg = cluster_cfg.get("device_criteria_config")
            control_cfg = cluster_cfg.get("device_control_config")
            priority = cluster_cfg["cluster_priority"]
            actuator = cluster_cfg.get("cluster_actuator", DEFAULT_ACTUATOR)

            if not all((pairwise_cfg, criteria_cfg, control_cfg)):
                _log.warning("Incomplete cluster config — skipping: %s", cluster_cfg)
                continue

            criteria_labels, criteria_array, self.load_control_modes = extract_criteria(
                pairwise_cfg
            )
            col_sums = calc_column_sums(criteria_array)

            if not validate_input(criteria_array, col_sums):
                _log.error("Inconsistent pairwise config — check: %s", pairwise_cfg)
                sys.exit(1)

            row_average = normalize_matrix(criteria_array, col_sums)

            criteria_cluster = CriteriaCluster(
                priority,
                criteria_labels,
                row_average,
                criteria_cfg,
                self.record_topic,
                self,
            )
            self.criteria_container.add_criteria_cluster(criteria_cluster)

            control_cluster = ControlCluster(
                control_cfg,
                actuator,
                self.record_topic,
                self,
            )
            self.control_container.add_control_cluster(control_cluster)

        for device_name in self.control_container.get_device_topic_set():
            device_topic = topics.DEVICES_VALUE(
                campus="",
                building="",
                unit="",
                path=device_name,
                point="all",
            )
            self.device_topic_list.append(device_topic)

    def _configure_power_meter(self, power_meter_info: Dict[str, Any]) -> None:
        """
        Configure the power meter topic, point, and optional demand formula.

        :param power_meter_info: Power meter configuration dictionary.
        :returns: ``None``
        """
        power_meter = power_meter_info.get("device_topic")
        self.power_point = power_meter_info.get("point")
        self.power_meter_topic = topics.DEVICES_VALUE(
            campus="",
            building="",
            unit="",
            path=power_meter,
            point="all",
        )

        demand_formula = power_meter_info.get("demand_formula")
        self.calculate_demand = False
        self.demand_expr = None
        self.demand_args = None

        if demand_formula is not None:
            try:
                self.demand_expr = demand_formula["operation"]
                self.demand_args = demand_formula["operation_args"]
                self.calculate_demand = True
                _log.debug("Demand formula expression: %s", self.demand_expr)
            except (KeyError, ValueError) as exc:
                _log.warning("Bad demand formula config: %s", exc)

    def _configure_kill_switch(
        self,
        kill_token: Optional[Dict[str, Any]],
        campus: str,
        building: str,
    ) -> None:
        """
        Configure the optional kill-switch subscription.

        :param kill_token: Kill-switch configuration dictionary.
        :param campus: Campus name.
        :param building: Building name.
        :returns: ``None``
        """
        self.kill_device_topic = None
        self.kill_pt = None

        if kill_token is None:
            return

        self.kill_pt = kill_token["point"]
        self.kill_device_topic = topics.DEVICES_VALUE(
            campus=campus,
            building=building,
            unit=kill_token["device"],
            path="",
            point="all",
        )

    def _configure_demand(self, config: Dict[str, Any]) -> None:
        """
        Configure demand limit and demand schedule.

        :param config: Effective configuration dictionary.
        :returns: ``None``
        """
        raw_limit = config.get("demand_limit")
        try:
            self.demand_limit = float(raw_limit) if raw_limit is not None else None
        except (ValueError, TypeError):
            _log.warning("Could not parse demand_limit=%r — setting to None", raw_limit)
            self.demand_limit = None

        self.demand_schedule = config.get("demand_schedule", self.demand_schedule)

    def _configure_timing(self, config: Dict[str, Any]) -> None:
        """
        Derive timing-related parameters from configuration.

        :param config: Effective configuration dictionary.
        :returns: ``None``
        """
        self.action_time = td(
            minutes=config.get("control_time", self.action_time.total_seconds() / 60.0)
        )
        self.average_window = td(
            minutes=config.get(
                "average_building_power_window",
                self.average_window.total_seconds() / 60.0,
            )
        )
        self.confirm_time = td(
            minutes=config.get("confirm_time", self.confirm_time.total_seconds() / 60.0)
        )
        self.actuator_schedule_buffer = (
            td(minutes=config.get("actuator_schedule_buffer", 15)) + self.action_time
        )

        all_devices = self.control_container.get_device_topic_set()
        self.longest_possible_curtail = td(
            seconds=len(all_devices) * self.action_time.total_seconds() * 2
        )

        self.stagger_release_time = td(
            minutes=config.get("release_time", self.action_time.total_seconds() / 60.0)
        )

    def starting_base(self, sender: str, **kwargs: Any) -> None:
        """
        Subscribe to all runtime topics and publish the startup message.

        :param sender: Sender identity.
        :param kwargs: Additional callback keyword arguments.
        :returns: ``None``
        """
        for device_topic in self.device_topic_list:
            _log.debug("Subscribing to %s", device_topic)
            self.vip.pubsub.subscribe(
                peer=PUBSUB_PEER,
                prefix=device_topic,
                callback=self.new_data,
            )

        if self.power_meter_topic is not None:
            _log.debug("Subscribing to %s", self.power_meter_topic)
            self.vip.pubsub.subscribe(
                peer=PUBSUB_PEER,
                prefix=self.power_meter_topic,
                callback=self.load_message_handler,
            )

        if self.kill_device_topic is not None:
            _log.debug("Subscribing to %s", self.kill_device_topic)
            self.vip.pubsub.subscribe(
                peer=PUBSUB_PEER,
                prefix=self.kill_device_topic,
                callback=self.handle_agent_kill,
            )

        handler = (
            self.simulation_demand_limit_handler
            if self.sim_running
            else self.demand_limit_handler
        )

        if self.demand_schedule is not None:
            if self.sim_running:
                self.setup_demand_schedule_sim()
            else:
                self.setup_demand_schedule()

        self.vip.pubsub.subscribe(
            peer=PUBSUB_PEER,
            prefix=self.target_agent_subscription,
            callback=handler,
        )
        _log.debug("Target agent subscription: %s", self.target_agent_subscription)

        self.vip.pubsub.publish(PUBSUB_PEER, self.ilc_start_topic, headers={}, message={})
        self.setup_topics()

    def setup_topics(self) -> None:
        """
        Cache flattened lists of criteria and control ingest topics.

        :returns: ``None``
        """
        self.criteria_topics = self.criteria_container.get_ingest_topic_dict()
        self.control_topics = self.control_container.get_ingest_topic_dict()
        self.all_criteria_topics = [
            topic for topic_list in self.criteria_topics.values() for topic in topic_list
        ]
        self.all_control_topics = [
            topic for topic_list in self.control_topics.values() for topic in topic_list
        ]

    @Core.receiver("onstop")
    def shutdown(self, sender: str, **kwargs: Any) -> None:
        """
        Release all device controls when the agent shuts down.

        :param sender: Sender identity.
        :param kwargs: Additional callback keyword arguments.
        :returns: ``None``
        """
        _log.debug("Shutting down ILC — releasing all controls")
        self.reinitialize_release()

    def confirm_elapsed(self) -> bool:
        """
        Determine whether the next confirmation time has passed.

        :returns: ``True`` if confirmation time has elapsed.
        """
        return self.current_time is not None and self.next_confirm is not None and self.current_time > self.next_confirm

    def confirm_end(self) -> bool:
        """
        Determine whether the action window has expired.

        :returns: ``True`` if the action end time has passed.
        """
        return self.action_end is not None and self.current_time is not None and self.current_time >= self.action_end

    def confirm_next_release(self) -> bool:
        """
        Determine whether the next staggered release time has arrived.

        :returns: ``True`` if the next release time has passed.
        """
        return self.next_release is not None and self.current_time is not None and self.current_time >= self.next_release

    def confirm_start_release(self) -> bool:
        """
        Determine whether the release cycle should begin.

        If the action end has been reached, the agent lock is enabled.

        :returns: ``True`` if release should start.
        """
        if self.action_end is not None and self.current_time is not None and self.current_time >= self.action_end:
            self.lock = True
            return True
        return False

    def setup_demand_schedule_sim(self) -> None:
        """
        Parse simulation-mode demand schedules into an internal weekday map.

        :returns: ``None``
        """
        if not self.demand_schedule:
            return

        for day_str, info in self.demand_schedule.items():
            weekday = parser.parse(day_str).weekday()
            if info in ("always_on", "always_off"):
                self.schedule[weekday] = info
            else:
                self.schedule[weekday] = {
                    "start": parser.parse(info["start"]).time(),
                    "end": parser.parse(info["end"]).time(),
                    "target": info.get("target"),
                }

    def setup_demand_schedule(self) -> None:
        """
        Create scheduled tasks for the next demand window.

        :returns: ``None``
        """
        self.tasks = {}

        now = dt.now()
        demand_goal = self.demand_schedule[0]
        start = parser.parse(self.demand_schedule[1])
        end = parser.parse(self.demand_schedule[2])

        start = now.replace(hour=start.hour, minute=start.minute) + td(days=1)
        end = now.replace(hour=end.hour, minute=end.minute) + td(days=1)

        _log.debug("Demand goal=%s  start=%s  end=%s", demand_goal, start, end)

        self.tasks[start] = {
            "schedule": [
                self.core.schedule(start, self.demand_limit_update, demand_goal, start),
                self.core.schedule(end, self.demand_limit_update, None, start),
            ],
        }

    def demand_limit_update(self, demand_goal: Optional[float], task_id: Any) -> None:
        """
        Update the active demand limit and reschedule recurring windows.

        :param demand_goal: New demand goal or ``None`` to clear it.
        :param task_id: Task identifier.
        :returns: ``None``
        """
        _log.debug("Updating demand limit: %s", demand_goal)
        self.demand_limit = demand_goal

        if demand_goal is None and task_id in self.tasks:
            self.tasks.pop(task_id)
            if self.demand_schedule is not None:
                self.setup_demand_schedule()

    def check_schedule(self, current_time: dt) -> None:
        """
        Update the current demand limit based on simulation schedules.

        :param current_time: Current simulation time.
        :returns: ``None``
        """
        if self.schedule:
            localized_time = current_time.replace(tzinfo=self.tz)
            current_schedule = self.schedule.get(localized_time.weekday())

            if current_schedule is None:
                return
            if current_schedule == "always_off":
                self.demand_limit = None
                return
            if current_schedule == "always_on":
                return

            if current_schedule["start"] <= localized_time.time() < current_schedule["end"]:
                self.demand_limit = current_schedule["target"]
            else:
                self.demand_limit = None

        if self.tasks:
            expired_keys: List[Any] = []
            localized_time = current_time.replace(tzinfo=self.tz)

            for key, value in self.tasks.items():
                if value["start"] <= localized_time < value["end"]:
                    self.demand_limit = value["target"]
                elif localized_time >= value["end"]:
                    self.demand_limit = None
                    expired_keys.append(key)

            for key in expired_keys:
                self.tasks.pop(key)

    def _parse_target_message(self, message: Any) -> Tuple[Dict[str, Any], str]:
        """
        Extract target information and timezone from a target-agent message.

        :param message: Incoming pubsub message.
        :returns: Tuple of ``(target_info, timezone_string)``.
        """
        if isinstance(message, list):
            return message[0]["value"], message[1]["value"]["tz"]
        return message, DEFAULT_TIMEZONE

    def _remove_overlapping_tasks(self, start_time: dt, end_time: dt) -> None:
        """
        Remove any existing demand tasks that overlap a new time window.

        :param start_time: New task start time.
        :param end_time: New task end time.
        :returns: ``None``
        """
        overlapping = [
            key
            for key, val in self.tasks.items()
            if (start_time < val["end"] and end_time > val["start"])
            or val["start"] <= start_time <= val["end"]
        ]

        for key in overlapping:
            task_info = self.tasks.pop(key)
            for sched in task_info.get("schedule", []):
                sched.cancel()

    def demand_limit_handler(self, peer, sender, bus, topic, headers, message) -> None:
        """
        Process a real-time demand-limit target from the target agent.

        :param peer: PubSub peer.
        :param sender: Sender identity.
        :param bus: Bus identity.
        :param topic: Topic name.
        :param headers: Message headers.
        :param message: Message payload.
        :returns: ``None``
        """
        self.sim_time = 0

        target_info, tz_info = self._parse_target_message(message)
        self.tz = to_zone = dateutil.tz.gettz(tz_info)

        start_time = parser.parse(target_info["start"]).astimezone(to_zone)
        end_default = start_time.replace(hour=23, minute=59, second=45).isoformat()
        end_time = parser.parse(target_info.get("end", end_default)).astimezone(to_zone)

        target = target_info["target"]
        demand_goal = float(target) if target is not None else None
        task_id = target_info["id"]

        _log.debug("TARGET id=%s start=%s goal=%s", task_id, start_time, demand_goal)

        self._remove_overlapping_tasks(start_time, end_time)

        existing = self.tasks.pop(task_id, None)
        if existing is not None:
            _log.debug("TARGET: cancelling duplicate task %s", task_id)
            for sched in existing.get("schedule", []):
                sched.cancel()

        _log.debug("TARGET: creating schedule id=%s", task_id)
        self.tasks[task_id] = {
            "schedule": [
                self.core.schedule(start_time, self.demand_limit_update, demand_goal, task_id),
                self.core.schedule(end_time, self.demand_limit_update, None, task_id),
            ],
            "start": start_time,
            "end": end_time,
            "target": demand_goal,
        }

    def simulation_demand_limit_handler(self, peer, sender, bus, topic, headers, message) -> None:
        """
        Process a simulation-mode demand-limit target.

        :param peer: PubSub peer.
        :param sender: Sender identity.
        :param bus: Bus identity.
        :param topic: Topic name.
        :param headers: Message headers.
        :param message: Message payload.
        :returns: ``None``
        """
        self.sim_time = 0

        target_info, tz_info = self._parse_target_message(message)
        self.tz = to_zone = dateutil.tz.gettz(tz_info)

        start_time = parser.parse(target_info["start"]).astimezone(to_zone)
        end_default = start_time.replace(hour=23, minute=59, second=59)
        end_time = parser.parse(target_info.get("end", end_default)).astimezone(to_zone)
        demand_goal = target_info["target"]

        _log.debug(
            "TARGET (sim): start=%s end=%s target=%s",
            start_time,
            end_time,
            demand_goal,
        )

        overlapping = [
            key
            for key, val in self.tasks.items()
            if (start_time < val["end"] and end_time > val["start"])
            or val["start"] <= start_time < val["end"]
        ]
        for key in overlapping:
            self.tasks.pop(key)

        self.tasks[target_info["id"]] = {
            "start": start_time,
            "end": end_time,
            "target": demand_goal,
        }

    @staticmethod
    def _strip_device_topic(topic: str) -> str:
        """
        Strip leading ``devices/`` and trailing ``/all`` from a topic.

        :param topic: Device topic string.
        :returns: Trimmed device path.
        """
        parts = topic.split("/")
        start = int(parts[0] == "devices")
        end = -int(parts[-1] == "all") or None
        return "/".join(parts[start:end])

    def _breakout_all_publish(
        self,
        topic: str,
        message: Tuple[Dict[str, Any], Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Expand an ``all`` publish into per-point topic/value and topic/meta maps.

        :param topic: Device topic.
        :param message: Tuple of value and metadata dictionaries.
        :returns: Tuple of ``(values_map, meta_map)``.
        """
        base = self._strip_device_topic(topic)
        values, meta = message

        values_map = {f"{base}/{point}": value for point, value in values.items()}
        meta_map = {f"{base}/{point}": meta[point] for point in values if point in meta}
        return values_map, meta_map

    def _ingest_data_for_container(
        self,
        data_topics: Dict[str, Any],
        now: dt,
        all_topics: List[str],
        topic_map: Dict[Any, List[str]],
    ) -> None:
        """
        Feed matching incoming topics into container-managed devices.

        :param data_topics: Incoming topic-to-value mapping.
        :param now: Current timestamp.
        :param all_topics: Flattened list of relevant topics.
        :param topic_map: Device-to-topic-list mapping.
        :returns: ``None``
        """
        relevant = set(data_topics) & set(all_topics)
        if not relevant:
            return

        filtered = {topic: data_topics[topic] for topic in relevant}
        for device, topic_list in topic_map.items():
            if set(topic_list) & relevant:
                device.ingest_data(now, filtered)

    def new_data(self, peer, sender, bus, topic, header, message) -> None:
        """
        Handle incoming data for curtailable devices.

        :param peer: PubSub peer.
        :param sender: Sender identity.
        :param bus: Bus identity.
        :param topic: Topic name.
        :param header: Message headers.
        :param message: Message payload.
        :returns: ``None``
        """
        if self.kill_signal_received:
            return

        start_time = time.time()
        _log.info("Data received for %s", topic)

        now = parse_timestamp_string(header[headers_mod.TIMESTAMP])
        data_topics, _meta_topics = self._breakout_all_publish(topic, message)

        self._ingest_data_for_container(
            data_topics,
            now,
            self.all_criteria_topics,
            self.criteria_topics,
        )
        self._ingest_data_for_container(
            data_topics,
            now,
            self.all_control_topics,
            self.control_topics,
        )

        _log.debug("Processing time for %s: %.4fs", topic, time.time() - start_time)

    def _sync_criteria_status(self) -> None:
        """
        Synchronize criteria active/inactive flags with controlled devices.

        :returns: ``None``
        """
        controlled = {(device.device_id, device.device_name) for device in self.devices}

        for device_name, device_criteria in self.criteria_container.devices.items():
            for subdevice, state in device_criteria.criteria:
                is_active = (subdevice, device_name) in controlled if self.devices else False
                device_criteria.criteria_status((subdevice, state), is_active)
                _log.debug(
                    "Device=%s subdevice=%s active=%s",
                    device_name,
                    subdevice,
                    is_active,
                )

    def calculate_average_power(
        self,
        current_power: float,
        current_time: dt,
    ) -> Tuple[float, float, td]:
        """
        Calculate exponential and simple averages for building power.

        :param current_power: Current instantaneous power.
        :param current_time: Timestamp of the reading.
        :returns: Tuple of ``(exp_avg, simple_avg, window_duration)``.
        """
        if self.sim_running:
            self.check_schedule(current_time)

        if self.bldg_power:
            window = self.bldg_power[-1][0] - self.bldg_power[0][0] + td(seconds=15)
        else:
            window = td(minutes=0)

        if current_power > 0:
            self.bldg_power.append((current_time, current_power))
            if window >= self.average_window:
                self.bldg_power.pop(0)

        exp_power = _exponential_moving_average(self.bldg_power)
        values = [value for _, value in self.bldg_power]
        avg_power = mean(values) if values else 0.0

        _log.debug("Time=%s  instant_power=%.2f", current_time, current_power)
        _log.debug(
            "Window=%s  avg_power=%.2f  exp_power=%.2f",
            window,
            avg_power,
            exp_power,
        )
        return exp_power, avg_power, window

    def load_message_handler(self, peer, sender, bus, topic, headers, message) -> None:
        """
        Handle incoming building power-meter data.

        This callback calculates average demand, publishes power statistics,
        and triggers the load-check state machine.

        :param peer: PubSub peer.
        :param sender: Sender identity.
        :param bus: Bus identity.
        :param topic: Topic name.
        :param headers: Message headers.
        :param message: Message payload.
        :returns: ``None``
        """
        self._sync_criteria_status()
        self.sim_time += 1

        if self.kill_signal_received:
            return

        data, meta = message
        _log.debug("Reading building power data")

        current_power = self._read_current_power(data)
        self.current_time = parser.parse(headers["Date"])

        self.avg_power, average_power, average_time = self.calculate_average_power(
            current_power,
            self.current_time,
        )

        if self.power_meta is None:
            self.power_meta = meta.get(self.power_point, _parse_power_meta_fallback())

        if not self.lock and len(self.bldg_power) >= 5:
            self.check_load()

        self._publish_power_data(average_power, average_time)

        if self.sim_running:
            gevent.sleep(0.1)
            self.vip.pubsub.publish(
                PUBSUB_PEER,
                "applications/ilc/advance",
                headers={},
                message={},
            )

    def _read_current_power(self, data: Dict[str, Any]) -> float:
        """
        Extract the current power value from power-meter data.

        If a demand formula is configured, it is evaluated first.

        :param data: Power-meter point/value mapping.
        :returns: Current power.
        """
        if self.calculate_demand:
            try:
                points = [(point, data[point]) for point in self.demand_args]
                power = sympy_evaluate(self.demand_expr, points)
                _log.debug("Calculated demand power: %s", power)
                return power
            except (KeyError, TypeError, ValueError) as exc:
                _log.debug("Demand calc failed (%s) — falling back to meter", exc)

        return float(data[self.power_point])

    def _publish_pubsub(self, topic: str, headers: Dict[str, Any], message: Any, timeout: float = 30.0) -> None:
        """
        Publish a PubSub message and wait for completion.

        :param topic: Destination topic.
        :param headers: Message headers.
        :param message: Message payload.
        :param timeout: Publish timeout in seconds.
        :returns: ``None``
        """
        self.vip.pubsub.publish(
            PUBSUB_PEER,
            topic,
            headers=headers,
            message=message,
        ).get(timeout=timeout)

    def _publish_power_data(self, average_power: float, average_time: td) -> None:
        """
        Publish building-power statistics.

        :param average_power: Simple average building power.
        :param average_time: Averaging window duration.
        :returns: ``None``
        """
        try:
            headers = _make_headers(self.current_time, self.sim_running)
            load_topic = _build_topic(self.update_base_topic, self.agent_id, "BuildingPower")
            demand_display = self.demand_limit if self.demand_limit is not None else "None"
            power_meta = self.power_meta or _parse_power_meta_fallback()

            message = [
                {
                    "AverageBuildingPower": float(average_power),
                    "AverageTimeLength": int(average_time.total_seconds() / 60),
                    "LoadControlPower": float(self.avg_power),
                    "Timestamp": format_timestamp(self.current_time),
                    "Target": demand_display,
                },
                {
                    "AverageBuildingPower": {
                        "tz": power_meta["tz"],
                        "type": "float",
                        "units": power_meta["units"],
                    },
                    "AverageTimeLength": {
                        "tz": power_meta["tz"],
                        "type": "integer",
                        "units": "minutes",
                    },
                    "LoadControlPower": {
                        "tz": power_meta["tz"],
                        "type": "float",
                        "units": power_meta["units"],
                    },
                    "Timestamp": {
                        "tz": power_meta["tz"],
                        "type": "timestamp",
                        "units": "None",
                    },
                    "Target": {
                        "tz": power_meta["tz"],
                        "type": "float",
                        "units": power_meta["units"],
                    },
                },
            ]

            self._publish_pubsub(load_topic, headers, message)
        except Exception:
            _log.debug("Unable to publish average power information")

    def check_load(self) -> None:
        """
        Evaluate building load against the current demand goal.

        Based on the result, trigger the state machine for curtailment,
        augmentation, release, or inactivity.

        :returns: ``None``
        """
        _log.debug("Checking building load (demand_limit=%s)", self.demand_limit)

        if self.demand_limit is None:
            result = f"Demand goal not set. Current load: {self.avg_power:.1f} kW"
            if self.state != "inactive":
                self.no_target()
        elif (
            "curtail" in self.load_control_modes
            and self.avg_power > self.demand_limit + self.demand_threshold
        ):
            result = (
                f"Load {self.avg_power:.1f} kW exceeds limit "
                f"{self.demand_limit + self.demand_threshold:.1f} kW"
            )
            self.curtail_load()
        elif (
            "augment" in self.load_control_modes
            and self.avg_power < self.demand_limit - self.demand_threshold
        ):
            result = (
                f"Load {self.avg_power:.1f} kW below limit "
                f"{self.demand_limit - self.demand_threshold:.1f} kW"
            )
            self.augment_load()
        elif self.state != "inactive":
            result = f"Load {self.avg_power:.1f} kW meets goal {self.demand_limit:.1f} kW"
            self.release()
        else:
            result = (
                f"ILC inactive — load: {self.avg_power:.1f} kW, "
                f"goal: {self.demand_limit:.1f} kW"
            )

        _log.debug("Result: %s", result)
        self.create_application_status(result)

    def modify_load(self) -> None:
        """
        Actuate devices in priority order until the demand gap is met.

        :returns: ``None``
        """
        _log.debug("Entering modify_load (state=%s)", self.state)

        scored_devices = self.criteria_container.get_score_order(self.state)
        active_devices = self.control_container.get_devices_status(self.state)

        score_order = [
            device
            for scored in scored_devices
            for device in active_devices
            if scored == (device[0], device[1])
        ]
        _log.debug("Scored + active devices: %s", score_order)

        score_order = self.actuator_request(score_order)
        remaining = self._filter_already_controlled(score_order)

        if not remaining:
            _log.debug("All available devices already curtailed")
            self.lock = False
            return

        self.lock = True
        self.state_at_actuation = self.state
        self.action_end = self.current_time + self.action_time
        self.next_confirm = self.current_time + self.confirm_time

        need_curtailed = abs(self.avg_power - self.demand_limit)
        est_curtailed = 0.0

        for device_name, device_id, actuator in remaining:
            if self.kill_signal_received:
                break

            control_manager = self.control_container.get_device((device_name, actuator))
            control_setting = control_manager.get_control_setting(device_id, self.state)

            if control_setting is None:
                continue

            _log.debug(
                "State=%s device=(%s, %s) action=%s",
                self.state,
                device_name,
                device_id,
                control_setting.get_control_info(),
            )

            try:
                error = control_setting.modify_load()
            except (RemoteError, gevent.Timeout) as exc:
                _log.warning(
                    "Failed to set %s: %s",
                    control_setting.control_point_topic,
                    exc,
                )
                continue

            if error:
                gevent.sleep(1)
                continue

            est_curtailed += control_setting.control_load
            control_manager.increment_control(device_id)
            _log.debug(f"Estimated load: {est_curtailed} -- Needed load: {need_curtailed}")
            if self._is_new_device(device_name, device_id):
                self.devices.add(control_setting)

            if est_curtailed >= need_curtailed:
                _log.debug(f"Breaking out of actuation loop: {est_curtailed} >= {need_curtailed}")
                break

        self.lock = False
        self.hold()

    def _filter_already_controlled(self, score_order: List[DeviceToken]) -> List[DeviceToken]:
        """
        Remove devices already under non-dollar control.

        :param score_order: Candidate devices in priority order.
        :returns: Filtered candidate devices.
        """
        already = {
            (device.device_name, device.device_id, device.device_actuator)
            for device in self.devices
            if device.control_mode != "dollar"
        }
        return [device for device in score_order if device not in already]

    def _is_new_device(self, device_name: str, device_id: str) -> bool:
        """
        Determine whether a device is not yet in the controlled set.

        :param device_name: Device name.
        :param device_id: Device identifier.
        :returns: ``True`` if the device is not already controlled.
        """
        return not any(
            device.device_name == device_name and device.device_id == device_id
            for device in self.devices
        )

    def actuator_request(self, score_order: List[DeviceToken]) -> List[DeviceToken]:
        """
        Request actuator schedules and return devices granted for control.

        :param score_order: Candidate devices in priority order.
        :returns: Devices with usable actuator access.
        """
        current_time = get_aware_utc_now()
        start_str = format_timestamp(current_time)
        end_str = format_timestamp(current_time + self.longest_possible_curtail + self.actuator_schedule_buffer)

        control_devices: List[DeviceToken] = []
        already_handled: Dict[str, bool] = {device[0]: True for device in self.scheduled_devices}

        for device_name, token, actuator in score_order:
            point_device = (
                self.control_container
                .get_device((device_name, actuator))
                .get_point_device(token, self.state)
            )
            if point_device is None:
                continue

            control_device = self.base_rpc_path(path=point_device)

            if not self.need_actuator_schedule:
                self.scheduled_devices.add((device_name, actuator, control_device))
                control_devices.append((device_name, token, actuator))
                continue

            if device_name in already_handled:
                if already_handled[device_name]:
                    control_devices.append((device_name, token, actuator))
                continue

            _log.debug("Reserving device: %s", device_name)
            schedule_request = [[control_device, start_str, end_str]]

            try:
                if self.kill_signal_received:
                    break

                result = self.vip.rpc.call(
                    actuator,
                    "request_new_schedule",
                    self.agent_id,
                    control_device,
                    "HIGH",
                    schedule_request,
                ).get(timeout=30)
            except RemoteError as exc:
                _log.warning("Failed to schedule %s: %s", device_name, exc)
                already_handled[device_name] = False
                continue

            if result is not None and result.get("result") == "FAILURE":
                _log.warning("Device unavailable: %s", device_name)
                already_handled[device_name] = False
            else:
                already_handled[device_name] = True
                self.scheduled_devices.add((device_name, actuator, control_device))
                control_devices.append((device_name, token, actuator))

        return control_devices

    def setup_release(self) -> None:
        """
        Compute staggered release groups and intervals.

        :returns: ``None``
        """
        if not (self.stagger_release and self.devices):
            self.device_group_size = [len(self.devices)]
            self.current_stagger = []
            return

        num_devices = len(self.devices)
        release_steps = max(
            1,
            math.floor(self.stagger_release_time / self.confirm_time + 1),
        )

        _log.debug(
            "setup_release: devices=%d steps=%d stagger=%s confirm=%s",
            num_devices,
            release_steps,
            self.stagger_release_time,
            self.confirm_time,
        )

        self.device_group_size = self._compute_group_sizes(num_devices, release_steps)
        self.current_stagger = self._compute_stagger_intervals(release_steps)

        _log.debug("Group sizes: %s", self.device_group_size)
        _log.debug("Stagger intervals: %s", self.current_stagger)

    @staticmethod
    def _compute_group_sizes(num_devices: int, steps: int) -> List[int]:
        """
        Distribute devices across staggered release groups.

        :param num_devices: Number of controlled devices.
        :param steps: Number of staggered release steps.
        :returns: Group sizes per release step.
        """
        if num_devices > steps:
            base = int(math.floor(num_devices / steps))
            groups = [base] * steps
            for index in range(num_devices % steps):
                groups[index] += 1
            return groups

        groups = [0] * steps
        interval = int(math.ceil(float(steps) / num_devices))
        for index in range(0, len(groups), interval):
            groups[index] = 1

        unassigned = num_devices - sum(groups)
        for index, value in enumerate(groups):
            if unassigned <= 0:
                break
            if value == 0:
                groups[index] = 1
                unassigned -= 1

        return groups

    def _compute_stagger_intervals(self, steps: int) -> List[int]:
        """
        Compute per-step release delays in minutes.

        :param steps: Number of release steps.
        :returns: Minute delays between release groups.
        """
        if steps <= 1:
            return []

        total_minutes = self.stagger_release_time.total_seconds() / 60.0
        base = int(math.floor(total_minutes / (steps - 1)))
        intervals = [base] * (steps - 1)

        remainder = int(total_minutes) % (steps - 1)
        for index in range(remainder):
            intervals[index] += 1

        return intervals

    def reset_devices(self) -> None:
        """
        Release control of the next group of devices.

        :returns: ``None``
        """
        scored = self.criteria_container.get_score_order(self.state_at_actuation)
        controlled = [
            device
            for score in scored
            for device in self.devices
            if score == (device.device_name, device.device_id)
        ]

        release_queue = controlled[::-1]
        release_count = self.device_group_size.pop(0) if self.device_group_size else 0
        released_indices: List[int] = []

        for index in range(min(release_count, len(release_queue))):
            device = release_queue[index]

            if device.revert_priority is not None:
                same_name = [d for d in self.devices if d.device_name == device.device_name]
                device = max(same_name, key=lambda item: item.revert_priority)

            try:
                device.release()
                self.control_container.get_device(
                    (device.device_name, device.device_actuator)
                ).reset_control_status(device.device_id)
                device.clear_state()
                released_indices.append(index)
            except RemoteError as exc:
                _log.warning("Failed to revert %s: %s", device.point, exc)

        remaining = [
            device for index, device in enumerate(release_queue) if index not in released_indices
        ]
        self.devices = WeakSet(remaining)

        if self.current_stagger:
            minutes = self.current_stagger.pop(0)
            self.next_release = self.current_time + td(minutes=minutes)
        elif self.state not in (
            "curtail_holding",
            "augment_holding",
            "augment",
            "curtail",
            "inactive",
        ):
            self.finished()

        self.lock = False

    def reinitialize_release(self) -> None:
        """
        Fully release all devices and reset release state.

        :returns: ``None``
        """
        if self.devices:
            self.device_group_size = [len(self.devices)]
            self.reset_devices()

        self.devices = WeakSet()
        self.device_group_size = None
        self.next_release = None
        self.action_end = None
        self.next_confirm = self.current_time + self.confirm_time if self.current_time else None

        if self.state == "inactive" and self.config_reload_needed and self.saved_config is not None:
            _log.debug("Reloading deferred config parameters")
            self.reset_parameters(self.saved_config)

    def reset_all_devices(self) -> None:
        """
        Revert every scheduled device and cancel all schedules.

        :returns: ``None``
        """
        for device_name, actuator, control_device in self.scheduled_devices:
            try:
                result = self.vip.rpc.call(
                    actuator,
                    "revert_device",
                    "ilc",
                    control_device,
                ).get(timeout=30)
                _log.debug("Reverted %s: %s", control_device, result)
            except RemoteError as exc:
                _log.warning("Failed to revert %s: %s", control_device, exc)

            self.vip.rpc.call(
                actuator,
                "request_cancel_schedule",
                self.agent_id,
                control_device,
            ).get(timeout=30)

        self.scheduled_devices = set()

    def handle_agent_kill(self, peer, sender, bus, topic, headers, message) -> None:
        """
        Handle an external kill signal and shut down the agent.

        :param peer: PubSub peer.
        :param sender: Sender identity.
        :param bus: Bus identity.
        :param topic: Topic name.
        :param headers: Message headers.
        :param message: Message payload.
        :returns: ``None``
        """
        data = message[0]
        _log.info("Checking kill signal")

        if not bool(data.get(self.kill_pt)):
            return

        _log.info("Kill signal received — shutting down")
        self.kill_signal_received = True
        gevent.sleep(8)
        self.device_group_size = [len(self.devices)]
        self.reset_devices()
        sys.exit()

    def publish_record(self, topic_suffix: str, message: Dict[str, Any]) -> None:
        """
        Publish a timestamped analysis record.

        :param topic_suffix: Topic suffix appended to the record base topic.
        :param message: Message payload.
        :returns: ``None``
        """
        headers = _make_headers(self.current_time, self.sim_running)
        message["TimeStamp"] = format_timestamp(self.current_time)
        topic = _build_topic(self.record_topic, topic_suffix)
        self._publish_pubsub(topic, headers, message)

    @Core.periodic(300)
    def create_device_status_publish(self) -> None:
        """
        Periodically publish status for every controlled device.

        :returns: ``None``
        """
        for control_setting in self.devices:
            topic = _build_topic(
                self.update_base_topic,
                self.agent_id,
                control_setting.device_name,
            )
            now = get_aware_utc_now()
            headers = {
                "Date": format_timestamp(now),
                "TimeStamp": format_timestamp(now),
            }
            message = [{
                "ControlMode": control_setting.control_mode,
                "PreviousValue": (
                    control_setting.revert_value
                    if control_setting.revert_value is not None
                    else "None"
                ),
                "Active": 1,
            }]

            self._publish_pubsub(topic, headers, message, timeout=4.0)

    def create_application_status(self, result: str) -> None:
        """
        Publish the overall application status.

        :param result: Human-readable application status string.
        :returns: ``None``
        """
        try:
            topic = _build_topic(self.update_base_topic, self.agent_id)
            headers = _make_headers(self.current_time, self.sim_running)
            power_meta = self.power_meta or _parse_power_meta_fallback()

            message = [
                {
                    "Timestamp": format_timestamp(self.current_time),
                    "Result": result,
                    "ApplicationState": "Active" if self.devices else "Inactive",
                },
                {
                    "Timestamp": {
                        "tz": power_meta["tz"],
                        "type": "timestamp",
                        "units": "None",
                    },
                    "Result": {
                        "tz": power_meta["tz"],
                        "type": "string",
                        "units": "None",
                    },
                    "ApplicationState": {
                        "tz": power_meta["tz"],
                        "type": "string",
                        "units": "None",
                    },
                },
            ]

            self._publish_pubsub(topic, headers, message)
        except Exception:
            _log.debug("Unable to publish application status message")

    @staticmethod
    def intersection(topics_list: Iterable[Any], data_list: Iterable[Any]) -> Set[Any]:
        """
        Return the intersection of two iterables.

        :param topics_list: First iterable.
        :param data_list: Second iterable.
        :returns: Set intersection.
        """
        return set(topics_list) & set(data_list)


def main() -> None:
    """
    Run the ILC agent using ``vip_main``.

    :returns: ``None``
    """
    try:
        vip_main(ILCAgent)
    except Exception as exception:
        _log.exception("unhandled exception")
        _log.error(repr(exception))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass