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

"""
Control Handler Module for Intelligent Load Control (ILC)
=========================================================

Provides a layered architecture for managing device controls within
the VOLTTRON platform:

* **ControlContainer** — top-level aggregator of clusters.
* **ControlCluster** — groups device managers sharing one actuator.
* **ControlManager** — manages every controllable point on a single device.
* **Controls** — holds curtailment / augment settings and device status
  for one logical control point.
* **ControlSetting** *(abstract)* — base class for the four concrete
  control strategies: *Equation*, *Offset*, *Ramp*, and *Value*.
* **DeviceStatus** — evaluates a boolean condition against live data to
  decide whether a device is *on* or *off* for a given state.
"""

from __future__ import annotations

import abc
import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import gevent
from importlib.metadata import distribution, PackageNotFoundError

# ---------------------------------------------------------------------------
# VOLTTRON compatibility imports (supports both volttron-core and legacy)
# ---------------------------------------------------------------------------
try:
    distribution("volttron-core")
    from volttron.client.logs import setup_logging
    from volttron.client.messaging import headers as headers_mod
    from volttron.client.vip.agent import Agent
    from volttron.utils import format_timestamp, get_aware_utc_now
    from volttron.utils.jsonrpc import RemoteError
except PackageNotFoundError:
    from volttron.platform.vip.agent import Agent
    from volttron.platform.messaging import headers as headers_mod
    from volttron.platform.agent.utils import (
        format_timestamp,
        get_aware_utc_now,
        setup_logging,
    )
    from volttron.platform.jsonrpc import RemoteError

from ilc.utils import (
    create_device_topic_map,
    fix_up_point_name,
    parse_sympy,
    sympy_evaluate,
)

setup_logging()
_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_ACTUATOR: str = "platform.driver"
"""Default VIP identity of the platform actuator agent."""

RPC_TIMEOUT: int = 30
"""Seconds to wait for an RPC response before raising ``gevent.Timeout``."""

STATE_CURTAIL: str = "curtail"
"""Constant key representing the curtailment state."""

STATE_AUGMENT: str = "augment"
"""Constant key representing the augmentation state."""

def publish_data(
    time_stamp,
    message: Dict[str, Any],
    topic: str,
    publish_method,
) -> None:
    """
    Publish a timestamped message to the VOLTTRON message bus.

    :param time_stamp:
        An aware datetime used as the message body timestamp.
    :param message:
        Mutable dictionary; a ``"TimeStamp"`` key will be injected.
    :param topic:
        The topic string to publish under.
    :param publish_method:
        Callable with signature ``(bus, topic, headers, message)``.
    """
    headers = {headers_mod.DATE: format_timestamp(get_aware_utc_now())}
    message["TimeStamp"] = format_timestamp(time_stamp)
    publish_method("pubsub", topic, headers, message).get()


class DeviceStatus:
    """
    Evaluate a boolean condition against live device data.

    A ``DeviceStatus`` instance watches one or more device points and
    continuously re-evaluates a *SymPy* expression to decide whether
    the associated command is considered **active**.

    :param logging_topic:
        Base topic used when publishing diagnostic messages.
    :param parent:
        The owning VOLTTRON agent (used for publishing).
    :param device_status_args:
        List of point descriptors fed to
        :func:`~ilc.utils.create_device_topic_map`.
    :param condition:
        A string expression parsable by :func:`~ilc.utils.parse_sympy`.
    :param default_device:
        Fallback device topic prefix when a point descriptor omits one.
    """

    def __init__(
        self,
        logging_topic: str,
        parent: Agent,
        device_status_args: Optional[list] = None,
        condition: List[str]|str = "",
        default_device: str = "",
    ) -> None:
        self.logging_topic = logging_topic
        self.parent = parent
        self.default_device = default_device
        self.command_status: bool = False

        device_status_args = device_status_args or []
        self.device_topic_map, self.device_topics = create_device_topic_map(
            device_status_args, default_device
        )
        _log.debug("Device topic map: %s", self.device_topic_map)

        self.expr = parse_sympy(condition)
        self.current_device_values: Dict[str, Any] = {}

    def ingest_data(self, time_stamp, data: Dict[str, Any]) -> None:
        """
        Update internal point values and re-evaluate the condition.

        :param time_stamp:
            Timestamp associated with the incoming data snapshot.
        :param data:
            Mapping of *full topic → value* from the platform driver.
        """
        for topic, point in self.device_topic_map.items():
            if topic in data:
                self.current_device_values[point] = data[topic]
                _log.debug(
                    "DEVICE_STATUS: %s - %s current values: %s",
                    topic,
                    self.expr,
                    self.current_device_values,
                )

        # Wait until every required point has been received at least once.
        if len(self.current_device_values) < len(self.device_topic_map):
            return

        conditional_value = False
        if self.current_device_values:
            conditional_value = sympy_evaluate(
                self.expr, self.current_device_values.items()
            )

        try:
            self.command_status = bool(conditional_value)
        except TypeError:
            self.command_status = False


class ControlSetting(abc.ABC):
    """
    Abstract base for a single device-point control strategy.

    Concrete subclasses must implement:

    * :meth:`_determine_control_value` — compute ``self.control_value``.
    * :meth:`_actuate` — send the computed value (or revert) to the
      actuator.

    The class also serves as a **factory** via the :meth:`make_setting`
    class method, which dispatches to the appropriate subclass based on
    the ``control_method`` string in the configuration.

    :param logging_topic:
        Base topic for diagnostic publishing.
    :param agent:
        VOLTTRON ``Agent`` instance used for RPC and publishing.
    :param controls_object:
        Parent :class:`Controls` that owns this setting.
    :param point:
        The device point name to control.  **Required.**
    :param load:
        A scalar load estimate *or* a dict describing a load equation.
        **Required.**
    :param maximum:
        Upper bound applied after control-value calculation.
    :param minimum:
        Lower bound applied after control-value calculation.
    :param revert_priority:
        Priority label passed to the actuator when reverting.
    :param control_mode:
        Human-readable mode label (e.g. ``"comfort"``).
    :param condition:
        Optional SymPy expression gating this setting.
    :param conditional_args:
        Point descriptors consumed by *condition*.
    :param default_device:
        Fallback device topic prefix.
    :param device_actuator:
        VIP identity of the actuator agent.
    :param finalize_release_with_revert:
        If ``True``, a ``revert_point`` RPC follows a release actuation
        to fully cede control of the point.
    """
    def __init__(
        self,
        logging_topic: str,
        agent: Agent,
        controls_object: "Controls",
        point: Optional[str] = None,
        load: Optional[Union[float, dict]] = None,
        maximum: Optional[float] = None,
        minimum: Optional[float] = None,
        revert_priority: Optional[int] = None,
        control_mode: str = "comfort",
        condition: str = "",
        conditional_args: Optional[list] = None,
        default_device: str = "",
        device_actuator: str = DEFAULT_ACTUATOR,
        finalize_release_with_revert: bool = True,
    ) -> None:
        if point is None:
            raise ValueError(
                "Missing device control 'point' configuration parameter!"
            )
        if load is None:
            raise ValueError(
                "Missing device 'load' estimation configuration parameter!"
            )

        # --- Injected collaborators ---
        self.agent: Agent = agent
        self.controls_object = controls_object

        # --- Device / actuator identity ---
        self.default_device = default_device
        self.device_actuator = device_actuator
        self.point, self.point_device = fix_up_point_name(point, default_device)
        self.control_point_topic: str = self.agent.base_rpc_path(path=self.point)

        # --- Static configuration ---
        self.control_mode = control_mode
        self.revert_priority = revert_priority
        self.maximum = maximum
        self.minimum = minimum
        self.logging_topic = logging_topic
        self.finalize_release_with_revert = finalize_release_with_revert
        self.load = self._build_load(load, default_device)

        # --- Conditional gating ---
        self.conditional_control = None
        self.device_topic_map: Dict[str, str] = {}
        self.device_topics: Set[str] = set()
        self.current_device_values: Dict[str, Any] = {}
        self.conditional_points: list = []

        if conditional_args and condition:
            self.conditional_control = parse_sympy(condition)
            self.device_topic_map, self.device_topics = create_device_topic_map(
                conditional_args, default_device
            )
        self.device_topics.add(self.point_device)

        # --- Mutable control state ---
        self.control_load: float = self.load
        self.control_time = None
        self.control_value: Optional[float] = None
        self.revert_value: Optional[float] = None

    @property
    def device_id(self) -> str:
        """Return the logical device identifier from the parent Controls."""
        return self.controls_object.id

    @property
    def device_name(self) -> str:
        """Return the human-readable device name from the ControlManager."""
        return self.controls_object.manager.name

    @classmethod
    def make_setting(cls, control_method: str, **kwargs) -> "ControlSetting":
        """
        Instantiate the correct :class:`ControlSetting` subclass.

        :param control_method:
            One of ``"equation"``, ``"offset"``, ``"ramp"``, or ``"value"``.
        :param kwargs:
            Remaining keyword arguments forwarded to the chosen subclass
            constructor.
        :returns:
            A fully initialised concrete ``ControlSetting``.
        :raises ValueError:
            If *control_method* is not recognised.
        """
        registry = {
            "equation": EquationControlSetting,
            "offset": OffsetControlSetting,
            "ramp": RampControlSetting,
            "value": ValueControlSetting,
        }
        key = control_method.lower()
        if key not in registry:
            raise ValueError(
                f"Invalid 'control_method': '{control_method}'. "
                f"Expected one of {list(registry)}."
            )
        return registry[key](**kwargs)

    def clear_state(self) -> None:
        """
        Resets the state variables of the instance to their default values.

        This method sets all state variables to their respective initial states
        or values, enabling the instance to be prepared for fresh usage or to
        clear any modifications done during its operation.

        :raises: This method does not raise any exceptions.
        """
        self.control_load = self.load
        self.control_time = None
        self.control_value = None
        self.revert_value = None

    def modify_load(self) -> bool:
        """
        Execute the full control cycle: compute load, read the current
        value, determine the new set-point, and actuate.

        :returns:
            ``True`` if an error prevented actuation, ``False`` on success.
        """
        if isinstance(self.load, dict):
            self._evaluate_load_equation()

        if not self._fetch_revert_value():
            return True  # error flag

        self._determine_control_value()
        self.control_time = get_aware_utc_now()
        self._actuate()
        return False

    def release(self, trigger: bool = False) -> None:
        """
        Restore the device point to its pre-control value.

        :param trigger:
            If ``True``, the release was caused by an external trigger
            rather than a normal shed expiration.
        """
        if self.revert_value is None:
            path, point = self.point.rsplit("/", 1)
            result = self.agent.vip.rpc.call(self.device_actuator,
                                             "revert_point",
                                             path,
                                             point).get(timeout=RPC_TIMEOUT)
            _log.debug("Reverted point: %s — Result: %s", self.point, result)
        else:
            self._actuate(release=True, trigger=trigger)

    @abc.abstractmethod
    def _determine_control_value(self) -> None:
        """
        Abstract method that determines and processes the control value. This method is intended
        to be overridden by subclasses to define specific behavior. The implementation should
        compute the `control_value` and apply clamping or other constraints as required.

        An implementation of this method must ensure that `control_value` is set and correctly
        processed before further use.

        :return: None
        """
        self.control_value = self._clamp(self.control_value)

    @abc.abstractmethod
    def _actuate(self, release: bool = False, trigger: bool = False) -> None:
        """
        Send the computed control value (or the revert value) to the
        actuator and publish a diagnostic record.

        :param release:
            ``True`` when restoring the original value.
        :param trigger:
            ``True`` when the actuation is driven by a release trigger.
        """
        target_value = self.revert_value if release else self.control_value
        action_label = "Release" if release else "Actuate"

        try:
            path, point = self.control_point_topic.rsplit("/", 1)
            self.agent.vip.rpc.call(self.device_actuator,
                                    "set_point",
                                    path,
                                    point,
                                    target_value).get(timeout=RPC_TIMEOUT)
            prefix = self.agent.update_base_topic.split("/")[0]
            topic = "/".join([prefix, self.control_point_topic, action_label])
            message = {
                "Value": self.control_value,
                "PreviousValue": self.revert_value,
            }
            self.agent.publish_record(topic, message)

            if release and self.finalize_release_with_revert:
                path, point = self.point.rsplit("/", 1)
                result = self.agent.vip.rpc.call(self.device_actuator,
                                                 "revert_point",
                                                 path,
                                                 point).get(timeout=RPC_TIMEOUT)
                _log.debug("Reverted point: %s — Result: %s", self.point, result)

        except (Exception, gevent.Timeout) as exc:
            _log.warning("Exception during %s: %s", action_label, exc)

    def check_condition(self) -> bool:
        """
        Evaluate a condition based on the `conditional_control` expression and `conditional_points`.

        This method checks if a given condition is satisfied through symbolic evaluation
        of `conditional_control` using the provided `conditional_points`.
        If `conditional_control` is `None`, the method automatically evaluates to `True`.
        If `conditional_points` is empty or not provided, it evaluates to `False`.

        :raises TypeError: If `conditional_control` or `conditional_points` are of invalid type.

        :returns: Boolean result of the evaluated condition based on `conditional_control`
            and `conditional_points`.
        :rtype: bool
        """
        if self.conditional_control is None:
            return True
        if not self.conditional_points:
            return False
        value = sympy_evaluate(self.conditional_control, self.conditional_points)
        _log.debug(
            "%s (conditional_control) evaluated to %s",
            self.conditional_control,
            value,
        )
        return bool(value)

    def ingest_data(self, time_stamp, data: Dict[str, Any]) -> None:
        """
        Update conditional point values from an incoming data snapshot.

        :param time_stamp:
            Timestamp associated with the data.
        :param data:
            Mapping of *full topic → value*.
        """
        for topic, point in self.device_topic_map.items():
            if topic in data:
                self.current_device_values[point] = data[topic]

        if len(self.current_device_values) < len(self.device_topic_map):
            return

        self.conditional_points = list(self.current_device_values.items())

    def get_point_device(self) -> str:
        """
        Retrieves the point device as a string.

        This method returns the value of the `point_device` attribute. The `point_device`
        attribute is expected to be a string that represents information about a particular
        pointing device.

        :return: String representation of the `point_device` attribute.
        :rtype: str
        """
        return self.point_device

    def get_control_info(self) -> Dict[str, Any]:
        """
        Retrieves control information represented as a dictionary containing details
        about the control point, load, revert priority, and defined limits, as well
        as the control mode.

        :return: A dictionary containing the following keys:
                 - "point": Value of the control point.
                 - "load": Load information associated with the control.
                 - "revert_priority": Configuration for revert priority of the control.
                 - "maximum": Maximum permissible value for the control.
                 - "minimum": Minimum permissible value for the control.
                 - "control_mode": Current mode of the control.
        :rtype: Dict[str, Any]
        """
        return {
            "point": self.point,
            "load": self.load,
            "revert_priority": self.revert_priority,
            "maximum": self.maximum,
            "minimum": self.minimum,
            "control_mode": self.control_mode,
        }

    def _clamp(self, value: Optional[float]) -> Optional[float]:
        """
        Apply configured minimum / maximum bounds to *value*.

        :param value: The raw control value to clamp.
        :returns: The bounded value, or ``None`` if *value* was ``None``.
        """
        if value is None:
            return None
        if self.minimum is not None:
            value = max(self.minimum, value)
        if self.maximum is not None:
            value = min(self.maximum, value)
        return value

    def _build_load(
        self, load: Union[float, dict], default_device: str
    ) -> Union[float, dict]:
        """
        Normalise the *load* parameter into a scalar or equation dict.

        :param load: Raw load config (scalar or dict with equation).
        :param default_device: Fallback device prefix.
        :returns: The normalised load representation.
        """
        if isinstance(load, dict):
            args = load["equation_args"]
            return {
                "load_equation": load["operation"],
                "load_equation_args": self._setup_equation_args(default_device, args),
                "actuator_args": args,
            }
        return load

    def _evaluate_load_equation(self) -> None:
        """
        Evaluates a load equation defined in the load configuration and computes the
        control load based on dynamic device point values. The method processes the
        load equation and its arguments, fetches the corresponding point values,
        and evaluates the equation to determine the control load.

        :return: This method does not return any value but updates the `control_load`
                 attribute of the instance.
        :rtype: None
        :raises: Does not propagate exceptions but safely handles and logs specific
                 errors encountered during remote communication or equation evaluation.
        """
        load_equation = self.load["load_equation"]
        load_point_values: List[Tuple[str, Any]] = []

        for load_arg in self.load["load_equation_args"]:
            path = self.agent.base_rpc_path(path="")
            point_to_get = load_arg[1]
            try:
                value = self.agent.vip.rpc.call(self.device_actuator,
                                                "get_point",
                                                path,
                                                point_to_get).get(timeout=30)
            except (RemoteError, gevent.Timeout) as exc:
                _log.warning(
                    "Failed to get point for load calculation %s: %s",
                    point_to_get,
                    exc,
                )
                self.control_load = 0.0
                return
            load_point_values.append((load_arg[0], value))

        try:
            self.control_load = sympy_evaluate(load_equation, load_point_values)
        except Exception:
            _log.debug(
                "Could not evaluate load equation: %s — %s",
                load_equation,
                load_point_values,
            )
            self.control_load = 0.0

    def _fetch_revert_value(self) -> bool:
        """
        Read and cache the current point value so we can revert later.

        :returns: ``True`` on success, ``False`` on failure.
        """
        if self.revert_value is not None:
            return True
        try:
            path, point = self.control_point_topic.rsplit("/", 1)
            self.revert_value = self.agent.vip.rpc.call(self.device_actuator,
                                                        "get_point",
                                                        path,
                                                        point).get(timeout=RPC_TIMEOUT)
            return True
        except (RemoteError, gevent.Timeout) as exc:
            _log.warning(
                "Failed to get revert value for %s: %s",
                self.control_point_topic,
                exc,
            )
            self.control_value = None
            return False

    @staticmethod
    def _setup_equation_args(
        default_device: str, equation_args: list
    ) -> List[List[str]]:
        """
        Resolve each equation argument into a ``[token, full_point]`` pair.

        :param default_device: Fallback device prefix.
        :param equation_args: Raw argument list from config.
        :returns: List of ``[token, resolved_point]`` pairs.
        """
        result: List[List[str]] = []
        for arg in equation_args:
            point, _ = fix_up_point_name(arg, default_device)
            token = arg[0] if isinstance(arg, list) else arg
            result.append([token, point])
        return result


class EquationControlSetting(ControlSetting):
    """
    Determine the control value by evaluating a SymPy equation whose
    arguments are read from device points at control time.

    :param default_device:
        Fallback device topic prefix (also forwarded to the base class).
    :param equation:
        Dictionary containing ``"operation"``, ``"equation_args"``,
        ``"maximum"``, and ``"minimum"`` keys.
    :param kwargs:
        Remaining arguments forwarded to :class:`ControlSetting`.
    """

    def __init__(self, default_device: str = "", equation: Optional[dict] = None, **kwargs) -> None:
        super().__init__(default_device=default_device, **kwargs)
        equation = equation or {}
        self.equation_args = self._setup_equation_args(
            default_device, equation.get("equation_args", [])
        )
        self.control_value_formula = equation.get("operation", "")
        self.maximum = equation.get("maximum", self.maximum)
        self.minimum = equation.get("minimum", self.minimum)

    def get_control_info(self) -> Dict[str, Any]:
        """
        Retrieves detailed control information related to the equation-based control
        method. This information includes the control equation, the method type, and
        any arguments required for the equation.

        :return: A dictionary containing control information, including the control
            equation, the method type, and arguments for the equation.
        :rtype: Dict[str, Any]
        """
        info = super().get_control_info()
        info.update(
            {
                "control_equation": self.control_value_formula,
                "control_method": "equation",
                "equation_args": self.equation_args,
            }
        )
        return info

    def _determine_control_value(self) -> None:
        """
        Private method to determine and set the control value based on the given formula
        and equation arguments. It evaluates the control value by fetching data points
        from a device actuator and calculating the result utilizing sympy.

        This method interacts with the agent's Base RPC Path and a device actuator to
        collect real-time data points required for the evaluation. It then uses the
        `control_value_formula` and calculates the control value by substituting the
        data points into the formula. Once calculated, the control value is updated.

        :return: This method does not return any value.
        """
        equation_point_values: List[Tuple[str, Any]] = []
        for eq_arg in self.equation_args:
            path = self.agent.base_rpc_path(path="")
            point_get = eq_arg[1]
            value = self.agent.vip.rpc.call(self.device_actuator,
                                            "get_point",
                                            path,
                                            point_get).get(timeout=RPC_TIMEOUT)
            equation_point_values.append((eq_arg[0], value))
        self.control_value = sympy_evaluate(
            self.control_value_formula, equation_point_values
        )
        super()._determine_control_value()

    def _actuate(self, release: bool = False, **kwargs) -> None:
        super()._actuate(release=release)


class OffsetControlSetting(ControlSetting):
    """
    Determine the control value by adding a fixed offset to the
    current (revert) value.

    :param offset:
        Signed numeric offset applied to the current point value.
    :param kwargs:
        Remaining arguments forwarded to :class:`ControlSetting`.
    """

    def __init__(self, offset: float = 0.0, **kwargs) -> None:
        super().__init__(**kwargs)
        self.offset = offset

    def get_control_info(self) -> Dict[str, Any]:
        """Return metadata including the offset."""
        info = super().get_control_info()
        info.update({"control_method": "offset", "offset": self.offset})
        return info

    def _determine_control_value(self) -> None:
        """Add the offset to the stored revert value."""
        self.control_value = self.revert_value + self.offset
        super()._determine_control_value()

    def _actuate(self, release: bool = False, **kwargs) -> None:
        super()._actuate(release=release)


class RampControlSetting(ControlSetting):
    """
    Gradually ramp the point value from its current reading to a
    target in discrete increments, each separated by a sleep interval.

    :param destination_value:
        Final target value for the ramp.
    :param increment_time:
        Seconds to sleep between each step.
    :param increment_value:
        Absolute change applied at each step.
    :param kwargs:
        Remaining arguments forwarded to :class:`ControlSetting`.
    """

    def __init__(
        self,
        destination_value: float,
        increment_time: float,
        increment_value: float,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.destination_value = destination_value
        self.increment_time = increment_time
        self.increment_value = increment_value
        self._greenlet: Optional[gevent.Greenlet] = None

    def get_control_info(self) -> Dict[str, Any]:
        """
        Retrieves control information in a structured dictionary format.

        This method gathers control information by first collecting the data
        from the superclass implementation and then updating it with additional
        details specific to the "ramp" control method such as the destination
        value, increment time, and increment value.

        :return: A dictionary containing combined control details from the
                 superclass and specific details for the "ramp" method.
        :rtype: Dict[str, Any]
        """
        info = super().get_control_info()
        info.update(
            {
                "control_method": "ramp",
                "destination_value": self.destination_value,
                "increment_time": self.increment_time,
                "increment_value": self.increment_value,
            }
        )
        return info

    def _determine_control_value(self) -> None:
        """
        Determines and assigns the control value.

        This method sets the control value to match the destination value and
        then invokes the parent class implementation of the control value determination.
        It ensures that any additional control logic defined in the superclass
        is executed after the assignment of the `control_value`.

        :return: None
        """
        self.control_value = self.destination_value
        super()._determine_control_value()

    def _actuate(self, release: bool = False, trigger: bool = False) -> None:
        """
        Spawn (or respawn) a greenlet that ramps to the target value.

        :param release:
            If ``True``, ramp back to ``self.revert_value``.
        :param trigger:
            If ``True``, the release was externally triggered — skip
            the stepping loop and jump straight to finalisation.
        """
        target_value = self.revert_value if release else self.control_value
        action_label = "Release" if release else "Actuate"

        try:
            last_value = self._kill_existing_greenlet()
            path, point = self.control_point_topic.rsplit("/", 1)
            start_value = self.agent.vip.rpc.call(self.device_actuator,
                                                  "get_point",
                                                  path,
                                                  point).get(timeout=RPC_TIMEOUT)

            steps = int(abs((start_value - target_value) / self.increment_value))
            sign = 1 if start_value >= target_value else -1

            _log.debug(
                "Ramp %s for %s: %d steps from %.2f → %.2f "
                "(control=%.2f, increment=%.2f, last_loop=%.2f)",
                action_label,
                self.point,
                steps,
                start_value,
                target_value,
                self.control_value,
                self.increment_value,
                last_value if last_value is not None else float("nan"),
            )

            def _ramp_worker():
                return self._run_ramp(
                    action_label, steps, sign, start_value, target_value,
                    release, trigger,
                )

            self._greenlet = gevent.spawn(_ramp_worker)

        except (Exception, gevent.Timeout) as exc:
            _log.warning("Exception starting ramp %s: %s", action_label, exc)

    # -- Ramp internals ------------------------------------------------ #

    def _kill_existing_greenlet(self) -> Optional[float]:
        """
        Kill a running ramp greenlet and return its last value.

        :returns:
            The final value from the previous greenlet, or ``None``.
        """
        if self._greenlet is None:
            return None
        _log.debug("Killing existing ramp greenlet for %s", self.point)
        self._greenlet.kill()
        try:
            return self._greenlet.get(timeout=1)
        except (Exception, gevent.Timeout):
            return None

    def _run_ramp(
        self,
        action_label: str,
        steps: int,
        sign: int,
        start_value: float,
        target_value: float,
        release: bool,
        trigger: bool,
    ) -> Optional[float]:
        """
        Execute the full ramp sequence inside a spawned greenlet.

        :returns:
            The last value successfully written, or ``None`` on error.
        """
        final: Optional[float] = None
        try:
            if trigger:
                final = start_value
            else:
                final = self._execute_ramp_steps(
                    action_label, steps, sign, start_value
                )
            if release and self.finalize_release_with_revert:
                path, point = self.point.rsplit("/", 1)
                final = self.agent.vip.rpc.call(self.device_actuator,
                                                "revert_point",
                                                path,
                                                point).get(timeout=RPC_TIMEOUT)
                _log.debug("##### Reverted point: {} - Result: {}".format(self.point, final))
            elif final != target_value:
                path, point = self.control_point_topic.rsplit("/", 1)
                final = self.agent.vip.rpc.call(self.device_actuator,
                                                "set_point",
                                                path,
                                                point,
                                                target_value).get(timeout=RPC_TIMEOUT)
        except (Exception, gevent.Timeout) as exc:
            _log.warning("Exception in ramp %s: %s", action_label, exc)
        return final

    def _execute_ramp_steps(
        self,
        action_label: str,
        steps: int,
        sign: int,
        start_value: float,
    ) -> float:
        """
        Iterate through discrete ramp increments, actuating at each step.

        :param action_label: ``"Actuate"`` or ``"Release"`` (for logging).
        :param steps: Number of increments to execute.
        :param sign: Direction multiplier (``1`` or ``-1``).
        :param start_value: Point value at the beginning of the ramp.
        :returns: The last value successfully written.
        """
        current_value = start_value
        prefix = self.agent.update_base_topic.split("/")[0]

        for _ in range(steps):
            previous_value = current_value
            current_value -= sign * self.increment_value
            _log.debug(
                "Ramp step for %s: %.2f → %.2f",
                self.point,
                previous_value,
                current_value,
            )
            try:
                path, point = self.control_point_topic.rsplit("/", 1)
                self.agent.vip.rpc.call(self.device_actuator,
                                        "set_point",
                                        path,
                                        point,
                                        current_value).get(timeout=RPC_TIMEOUT)
                topic = "/".join([prefix, self.control_point_topic, action_label])
                message = {"Value": current_value, "PreviousValue": previous_value}
                self.agent.publish_record(topic, message)
            except (Exception, gevent.Timeout) as exc:
                _log.warning("Ramp step failed for %s: %s", self.point, exc)
                break
            gevent.sleep(self.increment_time)

        return current_value


class ValueControlSetting(ControlSetting):
    """
    Set the control point to a fixed, pre-configured value.

    :param value:
        The static value to write when this setting is activated.
    :param kwargs:
        Remaining arguments forwarded to :class:`ControlSetting`.
    """

    def __init__(self, value: float = 0.0, **kwargs) -> None:
        super().__init__(**kwargs)
        self.value = value

    def get_control_info(self) -> Dict[str, Any]:
        """Return metadata including the static value."""
        info = super().get_control_info()
        info.update({"control_method": "value", "value": self.value})
        return info  # BUG FIX: original was missing this return

    def _determine_control_value(self) -> None:
        """Assign the static value as the control value."""
        self.control_value = self.value
        super()._determine_control_value()

    def _actuate(self, release: bool = False, **kwargs) -> None:
        super()._actuate(release=release)


class Controls:
    """
    Orchestrate all control settings and device statuses for one
    logical device point.

    Holds separate lists of conditional curtailment and augmentation
    settings.  The first setting whose condition evaluates to ``True``
    is selected when a control action is requested.

    :param device_id:
        Unique identifier for this control point.
    :param control_config:
        Mutable dictionary of configuration keys (consumed via ``.pop``).
    :param logging_topic:
        Base topic for diagnostic publishing.
    :param agent:
        The VOLTTRON agent instance.
    :param manager:
        Parent :class:`ControlManager`.
    :param default_device:
        Fallback device topic prefix.
    :param device_actuator:
        VIP identity of the actuator agent.
    """

    def __init__(
        self,
        device_id: str,
        control_config: dict,
        logging_topic: str,
        agent: Agent,
        manager: "ControlManager",
        default_device: str = "",
        device_actuator: str = DEFAULT_ACTUATOR,
    ) -> None:
        self.id = device_id
        self.manager = manager
        self.currently_controlled: bool = False

        self.device_topics: Set[str] = set()
        self.device_status: Dict[str, DeviceStatus] = {}
        self.device_release_trigger: Dict[str, DeviceStatus] = {}
        self.conditional_curtailments: List[ControlSetting] = []
        self.conditional_augments: List[ControlSetting] = []

        device_topic = control_config.pop("device_topic", default_device)
        self.device_topics.add(device_topic)

        self.conditional_curtailments = self._build_conditional_settings(
            control_config.pop("curtail_settings", []),
            logging_topic, agent, device_topic, device_actuator,
        )
        self.conditional_augments = self._build_conditional_settings(
            control_config.pop("augment_settings", []),
            logging_topic, agent, device_topic, device_actuator,
        )
        self.device_status = self._build_device_status(
            control_config.pop("device_status"), logging_topic, agent, device_topic,
        )

        release_trigger = control_config.pop("release_trigger", {})
        if release_trigger:
            self.device_release_trigger = self._build_device_status(
                release_trigger, logging_topic, agent, device_topic,
            )

    def ingest_data(self, time_stamp, data: Dict[str, Any]) -> None:
        """
        Distribute incoming data to every sub-component and evaluate
        release triggers.

        :param time_stamp: Timestamp of the data snapshot.
        :param data: Mapping of *topic → value*.
        """
        for setting in self.conditional_curtailments:
            setting.ingest_data(time_stamp, data)
        for setting in self.conditional_augments:
            setting.ingest_data(time_stamp, data)
        for status in self.device_status.values():
            status.ingest_data(time_stamp, data)

        self._evaluate_release_triggers(time_stamp, data)

    def get_settings_by_state(self, state: str) -> List[ControlSetting]:
        """
        Return the list of conditional settings for *state*.

        :param state: ``"curtail"`` or ``"augment"``.
        """
        if state == STATE_CURTAIL:
            return self.conditional_curtailments
        return self.conditional_augments

    def get_control_info(self, state: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves control information for a specific state based on associated
        settings. The function iterates through the settings linked to the
        provided state, checks predefined conditions for each setting, and
        returns the control information for the first setting that satisfies
        its condition. If no setting meets the condition, the function returns
        None.

        :param state: The state for which the control information is requested.
        :type state: str
        :return: A dictionary containing the control information for the
            specified state if a valid condition is met, otherwise None.
        :rtype: Optional[Dict[str, Any]]
        """
        for setting in self.get_settings_by_state(state):
            if setting.check_condition():
                return setting.get_control_info()
        return None

    def get_control_setting(self, state: str) -> Optional[ControlSetting]:
        """
        Determines and retrieves the appropriate ControlSetting for the given state,
        if a valid condition is met. The method evaluates a list of settings
        corresponding to the provided state, and returns the first setting that satisfies
        the required condition.

        :param state: A string representing the state for which the settings are filtered.
        :type state: str
        :return: Returns a ControlSetting object if a condition is met; otherwise, None.
        :rtype: Optional[ControlSetting]
        """
        for setting in self.get_settings_by_state(state):
            if setting.check_condition():
                return setting
        return None

    def get_point_device(self, state: str) -> Optional[str]:
        """
        Fetches the point device associated with the given state.

        This method iterates through all settings associated with the provided
        state and checks specific conditions for each. If a condition is met,
        it returns the related point device. If no conditions are satisfied,
        it returns None.

        :param state: The state identifier used to retrieve relevant settings.
        :type state: str
        :return: The point device associated with the state if a condition
            is met, otherwise None.
        :rtype: Optional[str]
        """
        for setting in self.get_settings_by_state(state):
            if setting.check_condition():
                return setting.get_point_device()
        return None

    def get_topic_maps(self) -> List[str]:
        """
        Collect all device-topic-map keys across every sub-component.

        :returns: Flat list of topic strings.
        """
        keys: List[str] = []
        for source in (
            self.conditional_augments
            + self.conditional_curtailments
            + list(self.device_status.values())
            + list(self.device_release_trigger.values())
        ):
            keys.extend(source.device_topic_map.keys())
        return keys

    def increment_control(self) -> None:
        """
        Sets the currently_controlled attribute to True to indicate that the object or
        entity represented by this class is now under control. This method does not
        take any arguments and doesn't return any value.

        :return: None
        """
        self.currently_controlled = True

    def reset_control_status(self) -> None:
        """
        Resets the control status of the current instance.

        This method sets the `currently_controlled` attribute to False, indicating
        that the instance is no longer under control. It is a utility function to
        modify the internal state related to control status.

        :return: None
        """
        self.currently_controlled = False

    def _build_conditional_settings(
        self,
        settings: Union[dict, list],
        logging_topic: str,
        agent: Agent,
        default_device: str,
        device_actuator: str,
    ) -> List[ControlSetting]:
        """
        Instantiate :class:`ControlSetting` objects from raw config.

        :param settings: One dict or a list of dicts.
        :returns: List of fully initialised settings.
        """
        if isinstance(settings, dict):
            settings = [settings]

        result: List[ControlSetting] = []
        for cfg in settings:
            setting = ControlSetting.make_setting(
                logging_topic=logging_topic,
                agent=agent,
                controls_object=self,
                default_device=default_device,
                device_actuator=device_actuator,
                **cfg,
            )
            self.device_topics |= setting.device_topics
            result.append(setting)
        return result

    def _build_device_status(
        self,
        status_config: dict,
        logging_topic: str,
        agent: Agent,
        default_device: str,
    ) -> Dict[str, DeviceStatus]:
        """
        Build a mapping of *state → DeviceStatus* from config.

        If neither ``"curtail"`` nor ``"augment"`` key exists, the
        config is treated as a curtail-only definition.

        :returns: ``{state_key: DeviceStatus}`` dictionary.
        """
        statuses: Dict[str, DeviceStatus] = {}

        if STATE_CURTAIL not in status_config and STATE_AUGMENT not in status_config:
            ds = DeviceStatus(logging_topic, agent, default_device=default_device, **status_config)
            statuses[STATE_CURTAIL] = ds
            self.device_topics |= ds.device_topics
        else:
            for state, params in status_config.items():
                ds = DeviceStatus(logging_topic, agent, default_device=default_device, **params)
                statuses[state] = ds
                self.device_topics |= ds.device_topics

        return statuses

    def _evaluate_release_triggers(self, time_stamp, data: Dict[str, Any]) -> None:
        """
        Evaluates release triggers for devices and updates their statuses based on the
        provided timestamp and data. This method iterates over the device release triggers,
        processes data ingestion for each trigger, evaluates their command status, and
        performs actions to release the devices under certain conditions.

        :param time_stamp: The timestamp of the data being evaluated.
        :param data: A dictionary containing the data to be processed.
        :type data: Dict[str, Any]
        :return: None
        """
        if not self.device_release_trigger:
            return

        for state, trigger_status in self.device_release_trigger.items():
            trigger_status.ingest_data(time_stamp, data)
            if not trigger_status.command_status:
                continue

            self.device_status[state].command_status = False
            if self.currently_controlled:
                setting = self.get_control_setting(state)
                if setting and setting in getattr(setting.agent, "devices", set()):
                    setting.release(trigger=True)
                    self.reset_control_status()
                    setting.agent.devices.discard(setting)


class ControlManager:
    """
    Manage all :class:`Controls` instances for a named device.

    :param name:
        Human-readable device name.
    :param device_config:
        ``{device_id: control_config}`` mapping from the agent config.
    :param logging_topic:
        Base topic for diagnostics.
    :param agent:
        The VOLTTRON agent.
    :param default_device:
        Fallback device topic prefix.
    :param device_actuator:
        VIP identity of the actuator agent.
    """

    def __init__(
        self,
        name: str,
        device_config: dict,
        logging_topic: str,
        agent: Agent,
        default_device: str = "",
        device_actuator: str = DEFAULT_ACTUATOR,
    ) -> None:
        self.name = name
        self.device_topics: Set[str] = set()
        self.controls: Dict[str, Controls] = {}

        for device_id, control_config in device_config.items():
            ctrl = Controls(
                device_id,
                control_config,
                logging_topic,
                agent,
                manager=self,
                default_device=default_device,
                device_actuator=device_actuator,
            )
            self.controls[device_id] = ctrl
            self.device_topics |= ctrl.device_topics

    def ingest_data(self, time_stamp, data: Dict[str, Any]) -> None:
        """
        Processes and ingests provided data into controls associated with the instance.

        The method iterates through all the available controls and passes
        the provided time stamp and data to their respective ingestors.
        This facilitates centralized handling and systematic data ingestion
        across all controls.

        :param time_stamp: The timestamp when the data is recorded.
        :type time_stamp: Any
        :param data: Dictionary containing the data to be ingested.
        :type data: Dict[str, Any]
        :return: None
        """
        for control in self.controls.values():
            control.ingest_data(time_stamp, data)

    def get_control_info(self, device_id: str, state: str):
        """
        Retrieves control information of a specified device and its state.

        This method accesses the `controls` dictionary using the provided
        device identifier and fetches control information based on the
        given state. This can be primarily used to trace the current
        settings or configurations for a specific device.

        :param device_id: The unique identifier of the device. This value
            is used to find the corresponding device object in the controls
            dictionary.
        :type device_id: str
        :param state: The specific state for which the control information
            is to be retrieved. The state acts as a key or identifier within
            the device's control interface.
        :type state: str
        :return: The control information corresponding to the given device
            ID and its state.
        """
        return self.controls[device_id].get_control_info(state)

    def get_control_setting(self, device_id: str, state: str):
        """
        Retrieve the control setting based on the device ID and its state.

        This function fetches the control setting for a given device and state
        from the `controls` dictionary, providing access to its corresponding
        data.

        :param device_id: The identifier for the device.
        :type device_id: str
        :param state: The state of the device for which the control setting
            needs to be fetched.
        :type state: str
        :return: The control setting of the specified device and state.
        :rtype: Any
        """
        return self.controls[device_id].get_control_setting(state)

    def get_point_device(self, device_id: str, state: str):
        """
        Fetches a specific point device associated with a given device ID and state.

        The method accesses the controls dictionary and retrieves the point device for
        the provided device_id and state. Each device ID maps to an object that contains
        the point device functionality, and this function ensures that the appropriate
        device state is returned.

        :param device_id: The unique identifier of the device.
        :type device_id: str
        :param state: The state of the device for which the point device is being retrieved.
        :type state: str
        :return: Returns the point device associated with the specified device ID and
            state.
        :rtype: Any
        """
        return self.controls[device_id].get_point_device(state)

    def increment_control(self, device_id: str) -> None:
        """
        Increments the control count associated with a specific device ID by
        invoking the `increment_control` method of the corresponding control
        instance stored in the `self.controls` dictionary.

        :param device_id: The unique identifier of the device for which the
            control count should be incremented. This must correspond to a key
            in `self.controls`.
        :return: None
        """
        self.controls[device_id].increment_control()

    def reset_control_status(self, device_id: str) -> None:
        """
        Resets the control status of a given device in the system. This action will
        reset any ongoing or temporary control states associated with the specified
        device. The method requires that the provided device exists in the controls
        dictionary and updates its state by invoking the reset functionality.

        :param device_id: A string representing the unique identifier of the device
                          whose control status is to be reset.
        :type device_id: str
        :return: None
        """
        self.controls[device_id].reset_control_status()

    def get_device_status(self, state: str) -> List[str]:
        """
        Return IDs of all devices whose status is active for *state*.

        :param state: ``"curtail"`` or ``"augment"``.
        :returns: List of device-id strings.
        """
        return [
            device_id
            for device_id, ctrl in self.controls.items()
            if state in ctrl.device_status and ctrl.device_status[state].command_status
        ]


class ControlCluster:
    """
    Group :class:`ControlManager` instances that share the same actuator.

    :param cluster_config:
        ``{device_name: device_config}`` mapping.
    :param actuator:
        VIP identity of the shared actuator agent.
    :param logging_topic:
        Base topic for diagnostics.
    :param parent:
        The VOLTTRON agent.
    """

    def __init__(
        self,
        cluster_config: dict,
        actuator: str,
        logging_topic: str,
        parent: Agent,
    ) -> None:
        self.devices: Dict[Tuple[str, str], ControlManager] = {}
        self.device_topics: Set[str] = set()

        for device_name, device_config in cluster_config.items():
            manager = ControlManager(
                device_name,
                device_config,
                logging_topic,
                parent,
                device_actuator=actuator,
            )
            self.devices[device_name, actuator] = manager
            self.device_topics |= manager.device_topics

    def get_all_devices_status(
        self, state: str
    ) -> List[Tuple[str, str, str]]:
        """
        Query every managed device and return those with active status.

        :param state: ``"curtail"`` or ``"augment"``.
        :returns:
            List of ``(device_name, device_id, actuator)`` tuples.
        """
        results: List[Tuple[str, str, str]] = []
        for (device_name, actuator), manager in self.devices.items():
            for device_id in manager.get_device_status(state):
                results.append((device_name, device_id, actuator))
        return results


class ControlContainer:
    """
    Top-level aggregator that holds multiple :class:`ControlCluster`
    instances and exposes a unified interface for data ingestion and
    status queries.
    """

    def __init__(self) -> None:
        self.clusters: List[ControlCluster] = []
        self.devices: Dict[Tuple[str, str], ControlManager] = {}
        self.device_topics: Set[str] = set()
        self.control_topics: Dict[Controls, list] = {}

    def add_control_cluster(self, cluster: ControlCluster) -> None:
        """
        Register a :class:`ControlCluster` with this container.

        :param cluster: The cluster to add.
        """
        self.clusters.append(cluster)
        self.devices.update(cluster.devices)
        self.device_topics |= cluster.device_topics

    def get_device_name_list(self):
        """
        Retrieves a list of device names from the internal storage of devices.

        This method accesses the stored devices and extracts their names as a list. The
        names are derived from the keys of the devices dictionary. It assumes that the
        devices dictionary is populated with valid data.

        :return: A list of device names.
        :rtype: list
        """
        return self.devices.keys()

    def get_device(self, device_name) -> ControlManager:
        """
        Retrieves a device from the list of devices using the provided device name.

        This method allows you to access a specific device by its name from a collection
        of devices stored within the object. If the given device name is not present
        in the collection, it will raise an exception.

        :param device_name: The name of the device to retrieve.
        :type device_name: str
        :return: The corresponding device object for the given device name.
        :rtype: ControlManager
        :raises KeyError: If the device with the specified name does not exist.
        """
        return self.devices[device_name]

    def get_device_topic_set(self) -> Set[str]:
        """
        Retrieve the set of device topics.

        This method returns a set containing all the device topics that are currently
        stored. It does not take any arguments and directly retrieves the value.

        :returns: A set containing the device topics.
        :rtype: Set[str]
        """
        return self.device_topics

    def get_devices_status(
        self, state: str
    ) -> List[Tuple[str, str, str]]:
        """
        Aggregate active-device tuples from every cluster.

        :param state: ``"curtail"`` or ``"augment"``.
        :returns:
            List of ``(device_name, device_id, actuator)`` tuples.
        """
        results: List[Tuple[str, str, str]] = []
        for cluster in self.clusters:
            results.extend(cluster.get_all_devices_status(state))
        return results

    def ingest_data(self, time_stamp, data: Dict[str, Any]) -> None:
        """
        Ingests data into all devices managed by the system. This method iterates
        through all devices and processes the provided data along with the given
        timestamp, ensuring the data is delivered to each device correctly.

        :param time_stamp: A timestamp representing the time at which the data is
                           being ingested.
        :type time_stamp: Any
        :param data: A dictionary containing the data to be ingested, where keys
                     represent data identifiers and values represent the data
                     content.
        :type data: Dict[str, Any]
        :return: This method does not return any value.
        :rtype: None
        """
        for device in self.devices.values():
            device.ingest_data(time_stamp, data)

    def get_ingest_topic_dict(self) -> Dict[Controls, list]:
        """
        Constructs and retrieves a dictionary mapping control instances to their
        associated topic maps. This method iterates through all device managers
        and their respective controls, extracts the topic mapping for each
        control, and stores them in the control_topics attribute for later
        reference.

        :return: A dictionary where keys are control instances and values are
        associated lists of topic mappings.
        :rtype: Dict[Controls, list]
        """
        self.control_topics.clear()
        for manager in self.devices.values():
            for ctrl in manager.controls.values():
                self.control_topics[ctrl] = ctrl.get_topic_maps()
        return self.control_topics