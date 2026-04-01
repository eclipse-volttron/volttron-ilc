# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2022 Battelle Memorial Institute
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
# }}}
import abc
import logging

import gevent

from importlib.metadata import distribution, PackageNotFoundError

try:
    distribution('volttron-core')
    from volttron.client.logs import setup_logging
    from volttron.client.messaging import headers as headers_mod
    from volttron.client.vip.agent import Agent
    from volttron.utils import format_timestamp, get_aware_utc_now
    from volttron.utils.jsonrpc import RemoteError
except PackageNotFoundError:
    from volttron.platform.vip.agent import Agent
    from volttron.platform.messaging import headers as headers_mod
    from volttron.platform.agent.utils import format_timestamp, get_aware_utc_now, setup_logging
    from volttron.platform.jsonrpc import RemoteError

from ilc.utils import parse_sympy, sympy_evaluate, create_device_topic_map, fix_up_point_name

setup_logging()
_log = logging.getLogger(__name__)


def publish_data(time_stamp, message, topic, publish_method):
    headers = {headers_mod.DATE: format_timestamp(get_aware_utc_now())}
    message["TimeStamp"] = format_timestamp(time_stamp)
    publish_method("pubsub", topic, headers, message).get()


class ControlCluster(object):
    def __init__(self, cluster_config, actuator, logging_topic, parent):
        self.devices = {}
        self.device_topics = set()
        for device_name, device_config in cluster_config.items():
            control_manager = ControlManager(device_name, device_config, logging_topic, parent,
                                             device_actuator=actuator)
            self.devices[device_name, actuator] = control_manager
            self.device_topics |= control_manager.device_topics

    def get_all_devices_status(self, state):
        results = []
        for device_info, device in self.devices.items():
            for device_id in device.get_device_status(state):
                results.append((device_info[0], device_id, device_info[1]))
        return results


class ControlContainer(object):
    def __init__(self):
        self.clusters = []
        self.devices = {}
        self.device_topics = set()
        self.topics_per_device = {}
        self.control_topics = {}

    def add_control_cluster(self, cluster):
        self.clusters.append(cluster)
        self.devices.update(cluster.devices)
        self.device_topics |= cluster.device_topics

    def get_device_name_list(self):
        return self.devices.keys()

    def get_device(self, device_name):
        return self.devices[device_name]

    def get_device_topic_set(self):
        return self.device_topics

    def get_devices_status(self, state):
        all_on_devices = []
        for cluster in self.clusters:
            on_device = cluster.get_all_devices_status(state)
            all_on_devices.extend(on_device)
        return all_on_devices

    def ingest_data(self, time_stamp, data):
        for device in self.devices.values():
            device.ingest_data(time_stamp, data)

    def get_ingest_topic_dict(self):
        for device in self.devices.values():
            for cls in device.controls.values():
                self.control_topics[cls] = cls.get_topic_maps()
        return self.control_topics


class DeviceStatus(object):
    def __init__(self, logging_topic, parent, device_status_args=None, condition="", default_device=""):
        self.current_device_values = {}
        device_status_args = device_status_args if device_status_args else []

        self.device_topic_map, self.device_topics = create_device_topic_map(device_status_args, default_device)

        _log.debug("Device topic map: {}".format(self.device_topic_map))

        # self.device_status_args = device_status_args
        self.condition = parse_sympy(condition)
        self.expr = self.condition
        self.command_status = False
        self.default_device = default_device
        self.parent = parent
        self.logging_topic = logging_topic

    def ingest_data(self, time_stamp, data):
        for topic, point in self.device_topic_map.items():
            if topic in data:
                self.current_device_values[point] = data[topic]
                _log.debug("DEVICE_STATUS: {} - {} current device values: {}".format(topic,
                                                                                     self.condition,
                                                                                     self.current_device_values))
        # bail if we are missing values.
        if len(self.current_device_values) < len(self.device_topic_map):
            return

        conditional_points = self.current_device_values.items()
        conditional_value = False
        if conditional_points:
            conditional_value = sympy_evaluate(self.expr, conditional_points)
        try:
            self.command_status = bool(conditional_value)
        except TypeError:
            self.command_status = False


class Controls(object):
    def __init__(self, device_id, control_config, logging_topic, agent,
                 manager, default_device="", device_actuator='platform.actuator'):
        self.id = device_id
        self.device_topics = set()
        self.manager = manager
        self.device_status = {}
        self.device_release_trigger = {}
        self.conditional_curtailments = []
        self.conditional_augments = []
        self.currently_controlled = False
        device_topic = control_config.pop("device_topic", default_device)
        self.device_topics.add(device_topic)
        self.conditional_curtailments = self.process_conditional_settings(
            control_config.pop("curtail_settings", []),
            logging_topic, agent, device_topic, device_actuator
        )
        self.conditional_augments = self.process_conditional_settings(
            control_config.pop("augment_settings", []),
            logging_topic, agent, device_topic, device_actuator
        )
        self.device_status = self.process_device_status(
            control_config.pop("device_status"), logging_topic, agent, device_topic
        )
        release_trigger = control_config.pop("release_trigger", {})
        if release_trigger:
            self.device_release_trigger = self.process_device_status(
                release_trigger, logging_topic, agent, device_topic
            )

    def process_conditional_settings(self, settings, logging_topic, agent, default_device, device_actuator):
        if isinstance(settings, dict):
            settings = [settings]
        conditional_settings = []
        for setting in settings:
            conditional = ControlSetting.make_setting(
                logging_topic=logging_topic, agent=agent,
                controls_object=self, default_device=default_device,
                device_actuator=device_actuator, **setting
            )
            self.device_topics |= conditional.device_topics
            conditional_settings.append(conditional)
        return conditional_settings

    def process_device_status(self, status_config, logging_topic, agent, default_device):
        device_status = {}
        if "curtail" not in status_config and "augment" not in status_config:
            device_status["curtail"] = DeviceStatus(logging_topic, agent, default_device=default_device,
                                                    **status_config)
            self.device_topics |= device_status["curtail"].device_topics
        else:
            for state, params in status_config.items():
                device_status[state] = DeviceStatus(logging_topic, agent, default_device=default_device, **params)
                self.device_topics |= device_status[state].device_topics
        return device_status

    def ingest_data(self, time_stamp, data):
        def process_ingestion(target):
            for item in target:
                item.ingest_data(time_stamp, data)

        # Ingest data for conditional curtailments
        process_ingestion(self.conditional_curtailments)

        # Ingest data for conditional augments
        process_ingestion(self.conditional_augments)

        # Ingest data for device status
        for status_key, status_value in self.device_status.items():
            status_value.ingest_data(time_stamp, data)

        # Return early if no release trigger exists
        if not self.device_release_trigger:
            return

        # Process device release triggers
        for release_state, release_cls in self.device_release_trigger.items():
            release_cls.ingest_data(time_stamp, data)
            if release_cls.command_status:
                self.device_status[release_state].command_status = False
                if self.currently_controlled:
                    control_setting = self.get_control_setting(release_state)
                    if control_setting in control_setting.agent.devices:
                        control_setting.release(trigger=True)
                        self.reset_control_status()
                        control_setting.agent.devices.discard(control_setting)

    def get_settings_by_state(self, state):
        """Helper to fetch settings based on state."""
        return self.conditional_curtailments if state == 'curtail' else self.conditional_augments

    def get_control_info(self, state):
        """Get control information based on the state."""
        for setting in self.get_settings_by_state(state):
            if setting.check_condition():
                return setting.get_control_info()
        return None

    def get_control_setting(self, state):
        """Get specific control setting based on the state."""
        for setting in self.get_settings_by_state(state):
            if setting.check_condition():
                return setting
        return None

    def get_point_device(self, state):
        """Get point device based on the state."""
        for setting in self.get_settings_by_state(state):
            if setting.check_condition():
                return setting.get_point_device()
        return None

    def increment_control(self):
        """Mark control as currently active."""
        self.currently_controlled = True

    def reset_control_status(self):
        """Reset the current control status to inactive."""
        self.currently_controlled = False

    def _get_device_topic_map_keys(self, sources):
        """Helper method to collect topic map keys from provided sources."""
        topic_keys = []
        for source in sources:
            topic_keys.extend(list(source.device_topic_map.keys()))
        return topic_keys

    def get_topic_maps(self):
        """Get all device topics from various sources."""
        augments_topics = self._get_device_topic_map_keys(self.conditional_augments)
        curtailments_topics = self._get_device_topic_map_keys(self.conditional_curtailments)
        status_topics = self._get_device_topic_map_keys(self.device_status.values())
        release_trigger_topics = self._get_device_topic_map_keys(self.device_release_trigger.values())
        all_topics = augments_topics + curtailments_topics + status_topics + release_trigger_topics
        return all_topics


class ControlManager(object):
    def __init__(self, name, device_config, logging_topic, agent, default_device="",
                 device_actuator='platform.actuator'):
        self.name = name
        self.device_topics = set()
        self.controls = {}
        self.topics_per_device = {}

        for device_id, control_config in device_config.items():
            controls = Controls(device_id, control_config, logging_topic, agent, manager=self,
                                default_device=default_device, device_actuator=device_actuator)
            self.controls[device_id] = controls
            self.device_topics |= controls.device_topics

    def ingest_data(self, time_stamp, data):
        for control in self.controls.values():
            control.ingest_data(time_stamp, data)

    def get_control_info(self, device_id, state):
        return self.controls[device_id].get_control_info(state)

    def get_control_setting(self, device_id, state):
        return self.controls[device_id].get_control_setting(state)

    def get_point_device(self, device_id, state):
        return self.controls[device_id].get_point_device(state)

    def increment_control(self, device_id):
        self.controls[device_id].increment_control()

    def reset_control_status(self, device_id):
        self.controls[device_id].reset_control_status()

    def get_device_status(self, state):
        return [command for command, control in self.controls.items() if
                (state in control.device_status and control.device_status[state].command_status)]

    def get_control_topics(self):
        pass

    def get_topics(self, time_stamp, data):
        self.topics_per_device = {}
        for control in self.controls.values():
            self.topics_per_device[control] = control.get_topic_maps()


class ControlSetting(object):
    def __init__(self, logging_topic, agent, controls_object, point=None, load=None, maximum=None, minimum=None,
                 revert_priority=None, control_mode="comfort", condition="", conditional_args=None, default_device="",
                 device_actuator='platform.actuator', finalize_release_with_revert=True):
        if point is None:
            raise ValueError("Missing device control 'point' configuration parameter!")
        if load is None:
            raise ValueError("Missing device 'load' estimation configuration parameter!")

        self.agent: Agent = agent
        self.controls_object = controls_object
        self.default_device = default_device
        self.device_actuator = device_actuator
        self.point, self.point_device = fix_up_point_name(point, default_device)
        self.control_mode = control_mode
        self.revert_priority = revert_priority
        self.maximum = maximum
        self.minimum = minimum
        self.logging_topic = logging_topic
        self.finalize_release_with_revert = finalize_release_with_revert  # TODO: Should default really be True?

        if isinstance(load, dict):
            args = load['equation_args']
            self.load = {
                'load_equation': load['operation'],
                'load_equation_args': self._setup_equation_args(default_device, args),
                'actuator_args': args
            }
        else:
            self.load = load

        self.control_point_topic = self.agent.base_rpc_path(path=self.point)

        self.conditional_control = None
        self.device_topic_map, self.device_topics = {}, set()
        self.current_device_values = {}

        if conditional_args and condition:
            self.conditional_control = parse_sympy(condition)

            self.device_topic_map, self.device_topics = create_device_topic_map(conditional_args, default_device)
        self.device_topics.add(self.point_device)
        self.conditional_points = []

        #### State ####
        self.control_load = 0.0
        self.control_time = None
        self.control_value = None
        self.revert_value = None

    @property
    def device_id(self):
        return self.controls_object.id

    @property
    def device_name(self):
        return self.controls_object.manager.name

    def clear_state(self):
        """Reset all state variables (for use after the setting has been released)."""
        self.control_load = 0.0
        self.control_time = None
        self.control_value = None
        self.revert_value = None

    def modify_load(self):
        # TODO: This block regarding the dictionary does not always successfully find a scalar value.
        if isinstance(self.load, dict):
            load_equation = self.load["load_equation"]
            load_point_values = []
            for load_arg in self.load["load_equation_args"]:
                point_to_get = self.agent.base_rpc_path(path=load_arg[1])
                try:
                    # TODO: This should be a get_multiple outside the loop that calls this function or a subscription.
                    value = self.agent.vip.rpc.call(self.device_actuator, "get_point", point_to_get).get(timeout=30)
                except RemoteError as ex:
                    _log.warning("Failed get point for load calculation {point_to_get} (RemoteError): {str(ex)}")
                    self.control_load = 0.0
                    break
                load_point_values.append((load_arg[0], value))
                try:
                    self.control_load = sympy_evaluate(load_equation, load_point_values)
                except:
                    _log.debug(f"Could not convert expression for load estimation: {load_equation} --"
                               f" {load_point_values}")
                    self.control_load = 0.0
        error = False
        if self.revert_value is None:
            try:
                self.revert_value = self.agent.vip.rpc.call(self.device_actuator, "get_point",
                                                            self.control_point_topic).get(timeout=30)
            except (RemoteError, gevent.Timeout) as ex:
                error = True
                _log.warning(f"Failed get point for revert value storage {self.control_point_topic}"
                             f" (RemoteError): {str(ex)}")
                self.control_value = None  # TODO: Should control_value be altered here?
                return error

        self._determine_control_value()
        self.control_time = get_aware_utc_now()
        self._actuate()
        return error

    def release(self, trigger=False):
        if self.revert_value is None:
            # If we don't have a value to release to. Revert instead.
            result = self.agent.vip.rpc.call(self.device_actuator, "revert_point", "ilc", self.point).get(timeout=30)
            _log.debug("Reverted point: {} - Result: {}".format(self.point, result))
        else:
            self._actuate(release=True)

    # @abc.abstractmethod
    # def _release(self, release=False):
    #     # Implementations may just call super if this is sufficient, or may override this.
    #     target_value = self.revert_value if release else self.control_value
    #     publish_point = 'Release' if release else 'Actuate'
    #     self.agent.vip.rpc.call(self.device_actuator, "set_point", "ilc_agent", self.control_point_topic,
    #                             target_value).get(timeout=30)
    #     prefix = self.agent.update_base_topic.split("/")[0]
    #     topic = "/".join([prefix, self.control_point_topic, publish_point])
    #     message = {"Value": self.control_value, "PreviousValue": self.control_value}
    #     self.agent.publish_record(topic, message)
    #     if release and self.finalize_release_with_revert:
    #         # Release with revert_point to cede control.
    #         result = self.agent.vip.rpc.call(self.device_actuator, "revert_point", "ilc", self.point).get(timeout=30)
    #         _log.debug("Reverted point: {} - Result: {}".format(self.point, result))

    @abc.abstractmethod
    def _determine_control_value(self):
        # Implementations should typically call super when finished with their own logic to run this:
        if None not in [self.minimum, self.maximum]:
            self.control_value = max(self.minimum, min(self.control_value, self.maximum))
        elif self.minimum is not None and self.maximum is None:
            self.control_value = max(self.minimum, self.control_value)
        elif self.maximum is not None and self.minimum is None:
            self.control_value = min(self.maximum, self.control_value)

    @abc.abstractmethod
    def _actuate(self, release=False, trigger=False, **kwargs):
        # Implementations may just call super if this is sufficient, or may override this.
        target_value = self.revert_value if release else self.control_value
        publish_point = 'Release' if release else 'Actuate'
        try:
            self.agent.vip.rpc.call(self.device_actuator, "set_point", "ilc_agent", self.control_point_topic,
                                    target_value).get(timeout=30)
            prefix = self.agent.update_base_topic.split("/")[0]
            topic = "/".join([prefix, self.control_point_topic, publish_point])
            message = {"Value": self.control_value, "PreviousValue": self.revert_value}
            self.agent.publish_record(topic, message)
            if release and self.finalize_release_with_revert:
                # Release with revert_point to cede control.
                result = self.agent.vip.rpc.call(self.device_actuator, "revert_point", "ilc", self.point).get(
                    timeout=30)
                _log.debug("Reverted point: {} - Result: {}".format(self.point, result))
        except (Exception, gevent.Timeout) as e:
            _log.warning(f'Exception encountered during {publish_point}:')
            _log.warning(e)

    @staticmethod
    def _setup_equation_args(default_device, equation_args):
        arg_list = []
        for arg in equation_args:
            point, point_device = fix_up_point_name(arg, default_device)
            if isinstance(arg, list):
                token = arg[0]
            else:
                token = arg
            arg_list.append([token, point])
        return arg_list

    def get_point_device(self):
        return self.point_device

    def get_control_info(self):
        return {
            'point': self.point,
            'load': self.load,
            'revert_priority': self.revert_priority,
            'maximum': self.maximum,
            'minimum': self.minimum,
            'control_mode': self.control_mode
        }

    def check_condition(self):
        # If we don't have a condition then we are always true.
        if self.conditional_control is None:
            return True

        if self.conditional_points:
            value = sympy_evaluate(self.conditional_control, self.conditional_points)
            _log.debug('{} (conditional_control) evaluated to {}'.format(self.conditional_control, value))
        else:
            value = False
        return value

    def ingest_data(self, time_stamp, data):
        for topic, point in self.device_topic_map.items():
            if topic in data:
                self.current_device_values[point] = data[topic]

        # bail if we are missing values.
        if len(self.current_device_values) < len(self.device_topic_map):
            return

        self.conditional_points = self.current_device_values.items()

    @classmethod
    def make_setting(cls, control_method, **kwargs):
        if control_method.lower() == "equation":
            return EquationControlSetting(**kwargs)
        elif control_method.lower() == "offset":
            return OffsetControlSetting(**kwargs)
        elif control_method.lower() == "ramp":
            return RampControlSetting(**kwargs)
        elif control_method.lower() == "value":
            return ValueControlSetting(**kwargs)
        else:
            raise ValueError(f"Missing valid 'control_method' configuration parameter! Received: '{control_method}'")


class EquationControlSetting(ControlSetting):
    def __init__(self, default_device, equation, **kwargs):
        super(EquationControlSetting, self).__init__(**kwargs)
        equation_args = equation['equation_args']
        self.equation_args = self._setup_equation_args(default_device, equation_args)
        self.control_value_formula = equation['operation']
        self.maximum = equation['maximum']
        self.minimum = equation['minimum']

    def get_control_info(self):
        control_info = super(EquationControlSetting, self).get_control_info()
        control_info.update({
            'control_equation': self.control_value_formula,
            'control_method': 'equation',
            'equation_args': self.equation_args,
        })

    def _determine_control_value(self):
        equation = self.control_value_formula
        equation_point_values = []

        for eq_arg in self.equation_args:
            point_get = self.agent.base_rpc_path(path=eq_arg[1])
            value = self.agent.vip.rpc.call(self.device_actuator, "get_point", point_get).get(timeout=30)
            equation_point_values.append((eq_arg[0], value))

        self.control_value = sympy_evaluate(equation, equation_point_values)
        super(EquationControlSetting, self)._determine_control_value()

    def _actuate(self, release=False):
        super(EquationControlSetting, self)._actuate(release)

class OffsetControlSetting(ControlSetting):
    def __init__(self, offset, **kwargs):
        super(OffsetControlSetting, self).__init__(**kwargs)
        self.offset = offset

    def get_control_info(self):
        control_info = super(OffsetControlSetting, self).get_control_info()
        control_info.update({'control_method': 'offset', 'offset': self.offset})
        return control_info

    def _determine_control_value(self):
        self.control_value = self.revert_value + self.offset
        super(OffsetControlSetting, self)._determine_control_value()

    def _actuate(self, release=False):
        super(OffsetControlSetting, self)._actuate(release)


class RampControlSetting(ControlSetting):
    def __init__(self, destination_value, increment_time, increment_value, **kwargs):
        super(RampControlSetting, self).__init__(**kwargs)
        self.control_method = 'ramp'
        self.destination_value = destination_value
        self.increment_time = increment_time
        self.increment_value = increment_value
        self.greenlet = None

    def get_control_info(self):
        control_info = super(RampControlSetting, self).get_control_info()
        control_info.update({'control_method': 'ramp',
                             'destination_value': self.destination_value,
                             'increment_time': self.increment_time,
                             'increment_value': self.increment_value
                             })
        return control_info

    def _determine_control_value(self):
        self.control_value = self.destination_value
        super(RampControlSetting, self)._determine_control_value()

    def _actuate(self, release=False, trigger=False):
        target_value = self.revert_value if release else self.control_value
        publish_point = 'Release' if release else 'Actuate'
        try:
            if self.greenlet:
                _log.debug(f'##### KILLING RAMP {publish_point} GREENLET FOR {self.point}')
                self.greenlet.kill()
            last_loop_value = self.greenlet.get(timeout=1) if self.greenlet else None
            start_value = self.agent.vip.rpc.call(self.device_actuator, "get_point", self.control_point_topic
                                                  ).get(timeout=30)
            steps = int(abs((start_value - target_value) / self.increment_value))
            _log.debug(f"####### IN RAMP {publish_point} FOR {self.point}"
                       f" GOT {steps} STEPS FROM START_VALUE: {start_value}"
                       f" to TARGET_VALUE: {target_value}, CONTROL_VALUE IS: {self.control_value},"
                       f" INCREMENT_VALUE: {self.increment_value}. LAST_LOOP_VALUE WAS: {last_loop_value}")
            sign = 1 if start_value >= target_value else -1

            def ramp():
                final = None
                try:
                    final = self._ramping_loop(publish_point=publish_point, steps=steps, sign=sign,
                                               start_value=start_value) if not trigger else start_value
                    if release and self.finalize_release_with_revert:
                        # Release with revert_point to cede control.
                        final = self.agent.vip.rpc.call(self.device_actuator, "revert_point", "ilc", self.point).get(
                            timeout=30)
                        _log.debug("##### Reverted point: {} - Result: {}".format(self.point, final))
                    elif final != target_value:
                        final = self.agent.vip.rpc.call(self.device_actuator, "set_point", "ilc_agent",
                                                        self.control_point_topic, target_value).get(timeout=30)
                except (Exception, gevent.Timeout) as ex:
                    _log.warning(f'##### Exception encountered in Ramp {publish_point}:')
                    _log.warning(ex)
                finally:
                    return final

            self.greenlet = gevent.spawn(ramp)
        except (Exception, gevent.Timeout) as e:
            _log.warning(f'##### Exception encountered in Ramp {publish_point}:')
            _log.warning(e)

    def _ramping_loop(self, publish_point, steps, sign, start_value):
        _log.debug(
            f"######## IN RAMPING LOOP FOR {self.point}, received: publish_point: {publish_point}, steps: {steps}, sign: {sign},"
            f" start_value: {start_value}")
        _log.debug(
            f"##### IN RAMPING LOOP FOR {self.point}, SIGN IS: {sign}, self.increment_value is: {self.increment_value}")
        current_value = start_value
        try:
            for _ in range(steps):
                previous_value = current_value
                current_value -= sign * self.increment_value
                _log.debug(
                    f"#### IN RAMPING LOOP FOR {self.point}, CURRENT_VALUE is: {current_value}, PREVIOUS_VALUE: {previous_value}")
                self.agent.vip.rpc.call(self.device_actuator, "set_point", "ilc_agent", self.control_point_topic,
                                        current_value).get(timeout=30)
                prefix = self.agent.update_base_topic.split("/")[0]
                topic = "/".join([prefix, self.control_point_topic, publish_point])
                message = {"Value": current_value, "PreviousValue": previous_value}
                self.agent.publish_record(topic, message)
                gevent.sleep(self.increment_time)
        finally:
            return current_value


class ValueControlSetting(ControlSetting):
    def __init__(self, value, **kwargs):
        super(ValueControlSetting, self).__init__(**kwargs)
        self.value = value

    def get_control_info(self):
        control_info = super(ValueControlSetting, self).get_control_info()
        control_info.update({'control_method': 'value', 'value': self.value})

    def _determine_control_value(self):
        self.control_value = self.value
        super(ValueControlSetting, self)._determine_control_value()

    def _actuate(self, release=False):
        super(ValueControlSetting, self)._actuate(release)
