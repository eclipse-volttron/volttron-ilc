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

import dateutil.tz
import gevent
import logging
import math
import sys
import time

from datetime import timedelta as td, datetime as dt
from dateutil import parser
from transitions import Machine
from weakref import WeakSet
# from transitions.extensions import GraphMachine as Machine

from importlib.metadata import distribution, PackageNotFoundError

try:
    distribution('volttron-core')
    from volttron.client.logs import setup_logging
    from volttron.client.vip.agent import Agent, Core, RPC
    from volttron.client.messaging import topics, headers as headers_mod
    from volttron.utils import format_timestamp, get_aware_utc_now, parse_timestamp_string, vip_main
    from volttron.utils.jsonrpc import RemoteError
    from volttron.utils.math_utils import mean
except PackageNotFoundError:
    from volttron.platform.vip.agent import Agent, Core, RPC
    from volttron.platform.messaging import topics, headers as headers_mod
    from volttron.platform.agent.utils import (
        format_timestamp, get_aware_utc_now, load_config, parse_timestamp_string, setup_logging, vip_main
    )
    from volttron.platform.jsonrpc import RemoteError
    from volttron.platform.agent.math_utils import mean

from ilc.control_handler import ControlCluster, ControlContainer
from ilc.criteria_handler import CriteriaContainer, CriteriaCluster
from ilc.ilc_matrices import calc_column_sums, extract_criteria, normalize_matrix, validate_input
from ilc.utils import sympy_evaluate

__version__ = "2.2.1"

setup_logging()
_log = logging.getLogger(__name__)
APP_CAT = "LOAD CONTROL"
APP_NAME = "ILC"


class ILCAgent(Agent):
    states = ['inactive', 'curtail', 'curtail_holding', 'curtail_releasing', 'augment', "augment_holding",
              'augment_releasing']
    transitions = [
        {
            'trigger': 'curtail_load',
            'source': 'inactive',
            'dest': 'curtail'
        },
        {
            'trigger': 'curtail_load',
            'source': 'curtail',
            'dest': '='
        },
        {
            'trigger': 'hold',
            'source': 'curtail',
            'dest': 'curtail_holding'
        },
        {
            'trigger': 'curtail_load',
            'source': 'curtail_holding',
            'dest': 'curtail',
            'conditions': 'confirm_elapsed'
        },
        {
            'trigger': 'release',
            'source': 'curtail_holding',
            'dest': 'curtail_releasing',
            'conditions': 'confirm_start_release',
            "after": "reset_devices"
        },
        {
            'trigger': 'release',
            'source': 'curtail',
            'dest': 'curtail_releasing',
            'conditions': 'confirm_start_release',
            'after': 'reset_devices'
        },
        {
            'trigger': 'release',
            'source': ['curtail_releasing', 'augment_releasing'],
            'dest': None,
            'after': 'reset_devices',
            'conditions': 'confirm_next_release'
        },
        {
            'trigger': 'curtail_load',
            'source': 'curtail_releasing',
            'dest': 'curtail',
            'conditions': 'confirm_next_release'
        },
        {
            'trigger': 'augment_load',
            'source': 'inactive',
            'dest': 'augment'
        },
        {
            'trigger': 'augment_load',
            'source': 'augment',
            'dest': '='
        },
        {
            'trigger': 'hold',
            'source': 'augment',
            'dest': 'augment_holding'
        },
        {
            'trigger': 'augment_load',
            'source': 'augment_holding',
            'dest': 'augment',
            'conditions': 'confirm_elapsed'
        },
        {
            'trigger': 'release',
            'source': 'augment_holding',
            'dest': 'augment_releasing',
            'conditions': 'confirm_start_release',
            "after": "reset_devices"
        },
        {
            'trigger': 'release',
            'source': 'augment',
            'dest': 'augment_releasing',
            'conditions': 'confirm_start_release',
            'after': 'reset_devices'
        },
        {
            'trigger': 'augment_load',
            'source': 'augment_releasing',
            'dest': 'augment',
            'conditions': 'confirm_next_release'
        },
        {
            'trigger': 'curtail_load',
            'source': ['augment', 'augment_holding', 'augment_releasing'],
            'dest': 'curtail_holding',
            'after': 'reinitialize_release'
        },
        {
            'trigger': 'augment_load',
            'source': ['curtail', 'curtail_holding', 'curtail_releasing'],
            'dest': 'augment_holding',
            'after': 'reinitialize_release'
        },
        {
            'trigger': 'finished',
            'source': ['curtail_releasing', 'augment_releasing'],
            'dest': 'inactive',
            "after": 'reinitialize_release'
        },
        {
            'trigger': 'no_target',
            'source': '*',
            'dest': 'inactive',
            "after": 'reinitialize_release'
        }
    ]

    def __init__(self, config_path, **kwargs):
        super(ILCAgent, self).__init__(**kwargs)
        self.state = None
        self.state_machine = Machine(model=self, states=ILCAgent.states,
                                     transitions=ILCAgent.transitions, initial='inactive', queued=True)
        # self.get_graph().draw('my_state_diagram.png', prog='dot')
        self.state_machine.on_enter_curtail('modify_load')
        self.state_machine.on_enter_augment('modify_load')
        self.state_machine.on_enter_curtail_releasing('setup_release')
        self.state_machine.on_enter_augment_releasing('setup_release')

        self.default_config = {
            "campus": "CAMPUS",
            "building": "BUILDING",
            "power_meter": {},
            "agent_id": "ILC",
            "demand_limit": 30.0,
            "control_time": 20.0,
            "curtailment_confirm": 5.0,
            "curtailment_break": 20.0,
            "average_building_power_window": 15.0,
            "stagger_release": True,
            "stagger_off_time": True,
            "simulation_running": False,
            "confirm_time": 5,
            "clusters": []
        }
        # TODO: Why are self.confirm_time and self.current_time defined as timedeltas, but only used a datetimes?
        self.confirm_time = td(minutes=self.default_config.get("confirm_time"))
        self.current_time = td(minutes=0)
        self.state_machine = Machine(model=self, states=ILCAgent.states,
                                     transitions=ILCAgent.transitions, initial='inactive', queued=True)
        # self.get_graph().draw('my_state_diagram.png', prog='dot')
        self.state_machine.on_enter_curtail('modify_load')
        self.state_machine.on_enter_augment('modify_load')
        self.state_machine.on_enter_curtail_releasing('setup_release')
        self.state_machine.on_enter_augment_releasing('setup_release')

        self.vip.config.set_default("config", self.default_config)
        self.vip.config.subscribe(self.configure_main,
                                  actions=["NEW", "UPDATE"],
                                  pattern="config")

        self.next_confirm = None
        self.action_end = None
        self.kill_signal_received = False
        self.scheduled_devices = set()
        self.devices = WeakSet()
        self.bldg_power = []
        self.avg_power = None
        self.device_group_size = None
        self.current_stagger = None
        self.next_release = None
        self.power_meta = None
        self.tasks = {}
        self.tz = None
        self.lock = False
        self.sim_time = 0
        self.config_reload_needed = False
        self.saved_config = None
        self.power_meter_topic = None
        self.kill_device_topic = None
        self.load_control_modes = ["curtail"]
        self.schedule = {}
        self.agent_id = APP_NAME
        self.record_topic = "record"
        self.target_agent_subscription = "record/target_agent"
        self.update_base_topic = self.record_topic
        self.ilc_start_topic = f"{self.agent_id}/ilc/start"
        self.base_rpc_path = topics.RPC_DEVICE_PATH(campus="",
                                                    building="",
                                                    unit="",
                                                    path=None,
                                                    point="")
        self.device_topic_list = []
        self.power_point = None
        self.demand_limit = None

        self.demand_schedule = None
        self.action_time = td(minutes=15)
        self.average_window = td(minutes=15)
        self.confirm_time = td(minutes=5)

        self.actuator_schedule_buffer = td(minutes=15) + self.action_time
        self.longest_possible_curtail = 0.0

        self.stagger_release_time = self.action_time
        self.stagger_release = False
        self.need_actuator_schedule = False
        self.demand_threshold = 5.0
        self.sim_running = False
        self.demand_expr = None
        self.demand_args = None
        self.calculate_demand = False
        self.criteria_container = CriteriaContainer()
        self.control_container = ControlContainer()

    def configure_main(self, config_name, action, contents):
        config = self.default_config.copy()
        config.update(contents)
        if action == "NEW" or "UPDATE":
            _log.debug("CONFIG NAME: {}, ACTION: {}, STATE: {}".format(config_name, action, self.state))
            if self.state not in ['curtail', 'curtail_holding', 'curtail_releasing', 'augment', 'augment_holding',
                                  'augment_releasing']:
                self.reset_parameters(config)
            else:
                _log.debug("ENTER CONFIG UPDATE..CURTAIL IN ACTION, UPDATE DEFERRED")
                # Defer reloading of parameters after curtailment operation
                self.config_reload_needed = True
                # self.new_config = self.default_config.copy()
                self.saved_config = self.default_config.copy()
                self.saved_config.update(contents)

    @RPC.export
    def update_configurations(self, data):
        """
        Update configuration for ILC via RPC.
        :param data: dictionary of all ILC configurations.
        :type data: Dict[Dict]
        :return: None
        """
        try:
            config = data.pop('config')
        except KeyError as ex:
            config = {}
            _log.debug(f'Cannot remotely update configurations!  Main config is not in payload!: {ex}')
        for name, data in data.items():
            self.vip.config.set(name, data)
        self.vip.config.set('config', config, send_update=True, trigger_callback=True)
        return True

    def reset_parameters(self, config=None):
        """
        Reset all parameters based on configuration change
        :param config: config
        :return:
        """

        self.agent_id = config.get("agent_id", APP_NAME)
        self.load_control_modes = config.get("load_control_modes", ["curtail"])

        campus = config.get("campus", "")
        building = config.get("building", "")
        self.agent_id = config.get("agent_id", self.agent_id)
        ilc_start_topic = self.agent_id
        # --------------------------------------------------------------------------------

        # For Target agent updates...
        self.record_topic = config.get("analysis_prefix_topic", self.record_topic)
        self.target_agent_subscription = "{}/target_agent".format(self.record_topic)
        # --------------------------------------------------------------------------------

        update_base_topic = self.record_topic
        if campus:
            update_base_topic = "/".join([update_base_topic, campus])
            ilc_start_topic = "/".join([self.agent_id, campus])

        if building:
            update_base_topic = "/".join([update_base_topic, building])
            ilc_start_topic = "/".join([ilc_start_topic, building])

        self.update_base_topic = update_base_topic
        self.ilc_start_topic = "/".join([ilc_start_topic, "ilc/start"])

        cluster_configs = config["clusters"]
        self.criteria_container = CriteriaContainer()
        self.control_container = ControlContainer()

        for cluster_config in cluster_configs:
            _log.debug("CLUSTER CONFIG: {}".format(cluster_config))
            pairwise_criteria_config = cluster_config["pairwise_criteria_config"]

            criteria_config = cluster_config["device_criteria_config"]
            control_config = cluster_config["device_control_config"]

            cluster_priority = cluster_config["cluster_priority"]
            cluster_actuator = cluster_config.get("cluster_actuator", "platform.actuator")
            # Check that all three parameters are not None
            if pairwise_criteria_config and criteria_config and control_config:
                criteria_labels, criteria_array, self.load_control_modes = extract_criteria(pairwise_criteria_config)
                col_sums = calc_column_sums(criteria_array)
                row_average = normalize_matrix(criteria_array, col_sums)
                _log.debug("VALIDATE - criteria_array {} - col_sums {}".format(criteria_array, col_sums))
                if not validate_input(criteria_array, col_sums):
                    _log.debug("Inconsistent pairwise configuration. Check "
                               "configuration in: {}".format(pairwise_criteria_config))
                    sys.exit()

                criteria_cluster = CriteriaCluster(cluster_priority, criteria_labels, row_average, criteria_config,
                                                   self.record_topic, self)
                self.criteria_container.add_criteria_cluster(criteria_cluster)
                _log.debug("CONTROL config: {}, ------------------- CRITERIA config: {}".format(control_config, criteria_config))
                control_cluster = ControlCluster(control_config, cluster_actuator, self.record_topic, self)
                self.control_container.add_control_cluster(control_cluster)

        all_devices = self.control_container.get_device_topic_set()
        for device_name in all_devices:
            device_topic = topics.DEVICES_VALUE(campus="",
                                                building="",
                                                unit="",
                                                path=device_name,
                                                point="all")

            self.device_topic_list.append(device_topic)

        power_meter_info = config.get("power_meter", {})
        power_meter = power_meter_info.get("device_topic", None)
        self.power_point = power_meter_info.get("point", None)
        demand_formula = power_meter_info.get("demand_formula")
        self.calculate_demand = False

        if demand_formula is not None:
            self.calculate_demand = True
            try:
                self.demand_expr = demand_formula["operation"]
                self.demand_args = demand_formula["operation_args"]
                _log.debug("Demand calculation - expression: {}".format(self.demand_expr))
            except (KeyError, ValueError):
                _log.debug("Missing 'operation_args' or 'operation' for setting demand formula!")
                self.calculate_demand = False
            except:
                _log.debug("Unexpected error when reading demand formula parameters!")
                self.calculate_demand = False

        self.power_meter_topic = topics.DEVICES_VALUE(campus="",
                                                      building="",
                                                      unit="",
                                                      path=power_meter,
                                                      point="all")
        self.kill_device_topic = None
        kill_token = config.get("kill_switch")
        if kill_token is not None:
            kill_device = kill_token["device"]
            self.kill_pt = kill_token[
                "point"]  # TODO: This may not be initialized, and would throw an error where used.
            self.kill_device_topic = topics.DEVICES_VALUE(campus=campus,
                                                          building=building,
                                                          unit=kill_device,
                                                          path="",
                                                          point="all")
        demand_limit = config["demand_limit"]
        if isinstance(demand_limit, (int, float)):
            self.demand_limit = float(demand_limit)
        else:
            try:
                self.demand_limit = float(demand_limit)
            except ValueError:
                self.demand_limit = None

        self.demand_schedule = config.get("demand_schedule", self.demand_schedule)
        self.action_time = td(minutes=config.get("control_time", self.action_time.seconds / 60))
        self.average_window = td(minutes=config.get("average_building_power_window", self.average_window.seconds / 60))
        self.confirm_time = td(minutes=config.get("confirm_time", self.confirm_time.seconds / 60))

        self.actuator_schedule_buffer = td(minutes=config.get("actuator_schedule_buffer", 15)) + self.action_time
        self.longest_possible_curtail = len(all_devices) * self.action_time * 2

        self.stagger_release_time = td(minutes=config.get("release_time", self.action_time.seconds / 60))
        self.stagger_release = config.get("stagger_release", self.stagger_release)
        self.need_actuator_schedule = config.get("need_actuator_schedule", self.need_actuator_schedule)
        self.demand_threshold = config.get("demand_threshold", self.demand_threshold)
        self.sim_running = config.get("simulation_running", self.sim_running)
        self.starting_base('core')
        self.config_reload_needed = False

    #    @Core.receiver("onstart")
    def starting_base(self, sender, **kwargs):
        """
        Startup method:
         - Setup subscriptions to curtailable devices.
         - Setup subscription to building power meter.
        :param sender:
        :param kwargs:
        :return:
        """
        for device_topic in self.device_topic_list:
            _log.debug("Subscribing to " + device_topic)
            self.vip.pubsub.subscribe(peer="pubsub",
                                      prefix=device_topic,
                                      callback=self.new_data)
        if self.power_meter_topic is not None:
            _log.debug("Subscribing to " + self.power_meter_topic)
            self.vip.pubsub.subscribe(peer="pubsub",
                                      prefix=self.power_meter_topic,
                                      callback=self.load_message_handler)

        if self.kill_device_topic is not None:
            _log.debug("Subscribing to " + self.kill_device_topic)
            self.vip.pubsub.subscribe(peer="pubsub",
                                      prefix=self.kill_device_topic,
                                      callback=self.handle_agent_kill)

        demand_limit_handler = self.demand_limit_handler if not self.sim_running else self.simulation_demand_limit_handler

        if self.demand_schedule is not None and not self.sim_running:
            self.setup_demand_schedule()
        elif self.demand_schedule is not None and self.sim_running:
            self.setup_demand_schedule_sim()

        self.vip.pubsub.subscribe(peer="pubsub",
                                  prefix=self.target_agent_subscription,
                                  callback=demand_limit_handler)
        _log.debug("Target agent subscription: " + self.target_agent_subscription)
        self.vip.pubsub.publish("pubsub", self.ilc_start_topic, headers={}, message={})
        self.setup_topics()

    def setup_topics(self):
        self.criteria_topics = self.criteria_container.get_ingest_topic_dict()
        self.control_topics = self.control_container.get_ingest_topic_dict()
        self.all_criteria_topics = []
        self.all_control_topics = []
        for lst in self.criteria_topics.values():
            self.all_criteria_topics.extend(lst)
        for lst in self.control_topics.values():
            self.all_control_topics.extend(lst)

    @Core.receiver("onstop")
    def shutdown(self, sender, **kwargs):
        _log.debug("Shutting down ILC, releasing all controls!")
        self.reinitialize_release()

    def confirm_elapsed(self):
        if self.current_time > self.next_confirm:
            return True
        else:
            return False

    def confirm_end(self):
        if self.action_end is not None and self.current_time >= self.action_end:
            return True
        else:
            return False

    def confirm_next_release(self):
        if self.next_release is not None and self.current_time >= self.next_release:
            return True
        else:
            return False

    def confirm_start_release(self):
        if self.action_end is not None and self.current_time >= self.action_end:
            self.lock = True
            return True
        else:
            return False

    def setup_demand_schedule_sim(self):
        if self.demand_schedule:
            for day_str, schedule_info in self.demand_schedule.items():
                _day = parser.parse(day_str).weekday()
                if schedule_info not in ["always_on", "always_off"]:
                    start = parser.parse(schedule_info["start"]).time()
                    end = parser.parse(schedule_info["end"]).time()
                    target = schedule_info.get("target", None)
                    self.schedule[_day] = {"start": start, "end": end, "target": target}
                else:
                    self.schedule[_day] = schedule_info

    def setup_demand_schedule(self):
        self.tasks = {}
        current_time = dt.now()
        demand_goal = self.demand_schedule[0]

        start = parser.parse(self.demand_schedule[1])
        end = parser.parse(self.demand_schedule[2])

        start = current_time.replace(hour=start.hour, minute=start.minute) + td(days=1)
        end = current_time.replace(hour=end.hour, minute=end.minute) + td(days=1)
        _log.debug("Setting demand goal target {} -  start: {} - end: {}".format(demand_goal, start, end))
        self.tasks[start] = {
            "schedule": [
                self.core.schedule(start, self.demand_limit_update, demand_goal, start),
                self.core.schedule(end, self.demand_limit_update, None, start)
            ]
        }

    def demand_limit_update(self, demand_goal, task_id):
        """
        Sets demand_goal based on schedule and corresponding demand_goal value received from TargetAgent.
        :param demand_goal:
        :param task_id:
        :return:
        """
        _log.debug("Updating demand limit: {}".format(demand_goal))
        self.demand_limit = demand_goal
        if demand_goal is None and self.tasks and task_id in self.tasks:
            self.tasks.pop(task_id)
            if self.demand_schedule is not None:
                self.setup_demand_schedule()

    def demand_limit_handler(self, peer, sender, bus, topic, headers, message):
        self.sim_time = 0
        if isinstance(message, list):
            target_info = message[0]["value"]
            tz_info = message[1]["value"]["tz"]
        else:
            target_info = message
            tz_info = "US/Pacific"

        self.tz = to_zone = dateutil.tz.gettz(tz_info)
        start_time = parser.parse(target_info["start"]).astimezone(to_zone)
        end_time = parser.parse(
            target_info.get("end", start_time.replace(hour=23, minute=59, second=45).isoformat())).astimezone(to_zone)
        target = target_info["target"]
        demand_goal = float(target) if target is not None else target
        task_id = target_info["id"]
        _log.debug("TARGET - id: {} - start: {} - goal: {}".format(task_id, start_time, demand_goal))
        task_list = []
        for key, value in self.tasks.items():
            if start_time == value["end"]:
                start_time += td(seconds=15)
            if (start_time < value["end"] and end_time > value["start"]) or value["start"] <= start_time <= value[
                "end"]:
                task_list.append(key)
        for task in task_list:
            sched_tasks = self.tasks.pop(task)["schedule"]
            for current_task in sched_tasks:
                current_task.cancel()

        current_task_exists = self.tasks.get(target_info["id"])
        if current_task_exists is not None:
            _log.debug("TARGET: duplicate task - {}".format(target_info["id"]))
            for item in self.tasks.pop(target_info["id"])["schedule"]:
                item.cancel()
        _log.debug("TARGET: create schedule - ID: {}".format(target_info["id"]))
        self.tasks[target_info["id"]] = {
            "schedule": [
                self.core.schedule(start_time,
                                   self.demand_limit_update,
                                   demand_goal,
                                   task_id),
                self.core.schedule(end_time,
                                   self.demand_limit_update,
                                   None,
                                   task_id)
            ],
            "start": start_time,
            "end": end_time,
            "target": demand_goal
        }
        return

    def breakout_all_publish(self, topic, message):
        values_map = {}
        meta_map = {}
        topic_parts = topic.split('/')

        start_index = int(topic_parts[0] == "devices")
        end_index = -int(topic_parts[-1] == "all")

        topic = "/".join(topic_parts[start_index:end_index])
        values, meta = message

        for point in values:
            values_map[topic + "/" + point] = values[point]
            if point in meta:
                meta_map[topic + "/" + point] = meta[point]

        return values_map, meta_map

    def sync_status(self):
        # TODO: as data comes in it loops through all criteria for each device.  This causes near continuous execution of these loop.
        for device_name, device_criteria in self.criteria_container.devices.items():
            for (subdevice, state), criteria in device_criteria.criteria.items():
                if not self.devices:
                    status = False
                    device_criteria.criteria_status((subdevice, state), status)
                else:
                    status = False
                    for control_setting in self.devices:
                        if subdevice == control_setting.device_id and device_name == control_setting.device_name:
                            status = True
                            break
                    device_criteria.criteria_status((subdevice, state), status)
                    _log.debug(
                        "Device: {} -- subdevice: {} -- curtail status: {}".format(device_name, subdevice, status))

    def new_criteria_data(self, data_topics, now):
        data_t = list(data_topics.keys())
        device_topics = {}
        device_criteria_topics = self.intersection(self.all_criteria_topics, data_t)
        for topic, values in data_topics.items():
            if topic in device_criteria_topics:
                device_topics[topic] = values
        device_set = set(list(device_topics.keys()))
        for device, topic_lst in self.criteria_topics.items():
            topic_set = set(topic_lst)
            needed_topics = self.intersection(topic_set, device_set)
            if needed_topics:
                device.ingest_data(now, device_topics)

    def new_control_data(self, data_topics, now):
        data_t = list(data_topics.keys())
        device_topics = {}
        device_control_topics = self.intersection(self.all_control_topics, data_t)
        for topic, values in data_topics.items():
            if topic in device_control_topics:
                device_topics[topic] = values
        device_set = set(list(device_topics.keys()))
        for device, topic_lst in self.control_topics.items():
            topic_set = set(topic_lst)
            needed_topics = self.intersection(topic_set, device_set)
            if needed_topics:
                device.ingest_data(now, device_topics)

    def new_data(self, peer, sender, bus, topic, header, message):
        """
        Call back method for curtailable device data subscription.
        :param peer:
        :param sender:
        :param bus:
        :param topic:
        :param header:
        :param message:
        :return:
        """
        start = time.time()
        if self.kill_signal_received:
            return
        _log.info("Data Received for {}".format(topic))
        self.sync_status()
        data, meta = message
        now = parse_timestamp_string(header[headers_mod.TIMESTAMP])
        data_topics, meta_topics = self.breakout_all_publish(topic, message)
        self.new_criteria_data(data_topics, now)
        self.new_control_data(data_topics, now)
        end = time.time()
        duration = end - start
        _log.debug("TIME: {} -- {}".format(topic, duration))

    @Core.periodic(60)
    def create_device_status_publish(self):
        """
        Publish device status.
        :param current_time_str:
        :param device_name:
        :param data:
        :param meta:
        :return:
        """

        #  self.devices == [[], [a,b,c,d,e,f,g,h], []]
        #     [
        #       0  control_setting.device_name,
        #       1  control_setting.device_id,
        #       2  control_setting.control_point_topic,
        #       3  control_setting.revert_value,
        #       4  control_setting.control_load,
        #       5  control_setting.revert_priority,
        #       6  format_timestamp(self.current_time),
        #       7  control_setting.device_actuator,
        #       8  control_setting.control_mode
        #      ]
        # )
        # try:
        for control_setting in self.devices:
            # this is not correct UCSD configs are incorrect so this works
            device_update_topic = "/".join([self.update_base_topic, self.agent_id, control_setting.device_name])

            headers = {
                "Date": format_timestamp(get_aware_utc_now()),
                "TimeStamp": format_timestamp(get_aware_utc_now())
            }

            device_message = [
                {
                    "ControlMode": control_setting.control_mode,
                    "PreviousValue": control_setting.revert_value if control_setting.revert_value is not None else "None",
                    "Active": 1
                }
            ]
            self.vip.pubsub.publish("pubsub", device_update_topic, headers=headers, message=device_message
                                    ).get(timeout=4.0)
        # except Exception as e:
        #    _log.debug(f"Unable to publish device status message: {e}.")

    @staticmethod
    def intersection(topics, data):
        topics = set(topics)
        data = set(data)
        return topics.intersection(data)

    def check_schedule(self, current_time):
        """
        Simulation cannot use clock time, this function handles the CBP target scheduling for
        Energy simulations and updating target based on pubsub message for transactive type
        simulation.
        :param current_time:
        :return:
        """
        # Handles load scheduling in configuration file
        if self.schedule:
            current_time = current_time.replace(tzinfo=self.tz)
            current_schedule = self.schedule[current_time.weekday()]
            if "always_off" in current_schedule:
                self.demand_limit = None
                return
            _start = current_schedule["start"]
            _end = current_schedule["end"]
            _target = current_schedule["target"]
            if _start <= current_time.time() < _end:
                self.demand_limit = _target
            else:
                self.demand_limit = None
        # Handles updating the target that is sent via pub-sub by transactive type application
        # and stored in tasks in simulation_demand_limit_handler
        if self.tasks:
            task_list = []
            current_time = current_time.replace(tzinfo=self.tz)
            for key, value in self.tasks.items():
                if value["start"] <= current_time < value["end"]:
                    self.demand_limit = value["target"]
                elif current_time >= value["end"]:
                    self.demand_limit = None
                    task_list.append(key)
            for key in task_list:
                self.tasks.pop(key)

    def handle_agent_kill(self, peer, sender, bus, topic, headers, message):
        """
        Locally implemented override for ILC application.
        When an override is detected the ILC application will return
        operations for all units to normal.
        :param peer:
        :param sender:
        :param bus:
        :param topic:
        :param headers:
        :param message:
        :return:
        """
        data = message[0]
        _log.info("Checking kill signal")
        kill_signal = bool(data[self.kill_pt])
        _now = parser.parse(headers["Date"])
        if kill_signal:
            _log.info("Kill signal received, shutting down")
            self.kill_signal_received = True
            gevent.sleep(8)
            self.device_group_size = [len(self.devices)]
            self.reset_devices()
            sys.exit()

    def calculate_average_power(self, current_power, current_time):
        """
        Calculate the average power.
        :param current_power:
        :param current_time:
        :return:
        """
        if self.sim_running:
            self.check_schedule(current_time)

        if self.bldg_power:
            average_time = self.bldg_power[-1][0] - self.bldg_power[0][0] + td(seconds=15)
        else:
            average_time = td(minutes=0)

        if average_time >= self.average_window and current_power > 0:
            self.bldg_power.append((current_time, current_power))
            self.bldg_power.pop(0)
        elif current_power > 0:
            self.bldg_power.append((current_time, current_power))

        smoothing_constant = 2.0 / (len(self.bldg_power) + 1.0) * 2.0 if self.bldg_power else 1.0
        smoothing_constant = smoothing_constant if smoothing_constant <= 1.0 else 1.0
        power_sort = list(self.bldg_power)
        power_sort.sort(reverse=True)
        exp_power = 0

        for n in range(len(self.bldg_power)):
            exp_power += power_sort[n][1] * smoothing_constant * (1.0 - smoothing_constant) ** n

        exp_power += power_sort[-1][1] * (1.0 - smoothing_constant) ** (len(self.bldg_power))

        norm_list = [float(i[1]) for i in self.bldg_power]
        average_power = mean(norm_list) if norm_list else 0.0

        _log.debug("Reported time: {} - instantaneous power: {}".format(current_time,
                                                                        current_power))
        _log.debug("{} minute average power: {} - exponential power: {}".format(average_time,
                                                                                average_power,
                                                                                exp_power))
        return exp_power, average_power, average_time

    def load_message_handler(self, peer, sender, bus, topic, headers, message):
        """
        Call back method for building power meter. Calculates the average
        building demand over a configurable time and manages the curtailment
        time and curtailment break times.
        :param peer:
        :param sender:
        :param bus:
        :param topic:
        :param headers:
        :param message:
        :return:
        """
        try:
            self.sim_time += 1
            if self.kill_signal_received:
                return
            data = message[0]
            meta = message[1]

            _log.debug("Reading building power data.")
            if self.calculate_demand:
                try:
                    demand_point_list = []
                    for point in self.demand_args:
                        _log.debug("Demand calculation - point: {} - value: {}".format(point, data[point]))
                        demand_point_list.append((point, data[point]))
                    current_power = sympy_evaluate(self.demand_expr, demand_point_list)
                    _log.debug("Demand calculation - calculated power: {}".format(current_power))
                except:
                    current_power = float(data[self.power_point])
                    _log.debug("Demand calculation - exception using meter value: {}".format(current_power))
            else:
                current_power = float(data[self.power_point])
            self.current_time = parser.parse(headers["Date"])
            self.avg_power, average_power, average_time = self.calculate_average_power(current_power,
                                                                                       self.current_time)

            if self.power_meta is None:
                try:
                    self.power_meta = meta[self.power_point]
                except:
                    self.power_meta = {
                        "tz": "UTC", "units": "kiloWatts", "type": "float"
                    }

            if self.lock:
                return

            if len(self.bldg_power) < 5:
                return
            self.check_load()

        finally:
            try:
                if self.sim_running:
                    headers = {
                        headers_mod.DATE: format_timestamp(self.current_time)
                    }
                else:
                    headers = {
                        headers_mod.DATE: format_timestamp(get_aware_utc_now())
                    }
                load_topic = "/".join([self.update_base_topic, self.agent_id, "BuildingPower"])
                demand_limit = "None" if self.demand_limit is None else self.demand_limit
                power_message = [
                    {
                        "AverageBuildingPower": float(average_power),
                        "AverageTimeLength": int(average_time.total_seconds() / 60),
                        "LoadControlPower": float(self.avg_power),
                        "Timestamp": format_timestamp(self.current_time),
                        "Target": demand_limit
                    },
                    {
                        "AverageBuildingPower": {
                            "tz": self.power_meta["tz"],
                            "type": "float",
                            "units": self.power_meta["units"]
                        },
                        "AverageTimeLength": {
                            "tz": self.power_meta["tz"],
                            "type": "integer",
                            "units": "minutes"
                        },
                        "LoadControlPower": {
                            "tz": self.power_meta["tz"],
                            "type": "float",
                            "units": self.power_meta["units"]
                        },
                        "Timestamp": {"tz": self.power_meta["tz"], "type": "timestamp", "units": "None"},
                        "Target": {"tz": self.power_meta["tz"], "type": "float", "units": self.power_meta["units"]}
                    }
                ]
                self.vip.pubsub.publish("pubsub", load_topic, headers=headers, message=power_message).get(timeout=30.0)
            except:
                _log.debug("Unable to publish average power information.  Input data may not contain metadata.")
            # TODO: Refactor this code block.  Disparate code paths for simulation and real devices is undesireable
            if self.sim_running:
                gevent.sleep(0.1)
                self.vip.pubsub.publish("pubsub", "applications/ilc/advance", headers={}, message={})

    def check_load(self):
        """
        Check whole building power and manager to this goal.
        """
        _log.debug("Checking building load: {}".format(self.demand_limit))

        if self.demand_limit is not None:
            if "curtail" in self.load_control_modes and self.avg_power > self.demand_limit + self.demand_threshold:
                result = "Current load of {} kW exceeds demand limit of {} kW.".format(self.avg_power,
                                                                                       self.demand_limit + self.demand_threshold)
                self.curtail_load()
            elif "augment" in self.load_control_modes and self.avg_power < self.demand_limit - self.demand_threshold:
                result = "Current load of {} kW is below demand limit of {} kW.".format(self.avg_power,
                                                                                        self.demand_limit - self.demand_threshold)
                self.augment_load()
            else:
                result = "ILC is not active  - Current load: {} kW -- demand goal: {}".format(self.avg_power,
                                                                                              self.demand_limit)
                if self.state != 'inactive':
                    result = "Current load of {} kW meets demand goal of {} kW.".format(self.avg_power,
                                                                                        self.demand_limit)
                    self.release()
        else:
            result = "Demand goal has not been set. Current load: ({load}) kW.".format(load=self.avg_power)
            if self.state != 'inactive':
                self.no_target()
        _log.debug("Result: {}".format(result))
        # self.lock = False
        self.create_application_status(result)

    def modify_load(self):
        """
        Curtail loads by turning off device (or device components).
        """
        _log.debug("***** ENTERING MODIFY LOADS *****************{}".format(self.state))

        scored_devices = self.criteria_container.get_score_order(self.state)
        _log.debug("SCORED devices: {}".format(list(scored_devices)))

        # Actuate devices contains tuples of (device_name, device_id, actuator).
        active_devices = self.control_container.get_devices_status(self.state)
        _log.debug("ACTIVE devices: {}".format(active_devices))

        score_order = [device for scored in scored_devices for device in active_devices if scored
                       in [(device[0], device[1])]]  # [0] is device_name, [1] is device_id
        _log.debug("SCORED AND ACTIVE devices: {}".format(score_order))

        score_order = self.actuator_request(score_order)

        need_curtailed = abs(self.avg_power - self.demand_limit)
        est_curtailed = 0.0
        remaining_devices = score_order[:]

        for device in self.devices:
            if device.control_mode != "dollar":
                current_tuple = (device.device_name, device.device_id, device.device_actuator)
                if current_tuple in remaining_devices:
                    remaining_devices.remove(current_tuple)

        if not remaining_devices:
            _log.debug("Everything available has already been curtailed")
            self.lock = False
            return

        self.lock = True
        self.state_at_actuation = self.state
        self.action_end = self.current_time + self.action_time
        self.next_confirm = self.current_time + self.confirm_time

        for device in remaining_devices:
            if self.kill_signal_received:
                break
            device_name, device_id, actuator = device
            control_manager = self.control_container.get_device((device_name, actuator))
            control_setting = control_manager.get_control_setting(device_id, self.state)
            _log.debug(f"State: {self.state} - action info: {control_setting.get_control_info()} - device "
                       f"{device_name}, {device_id} -- remaining {remaining_devices}")
            if control_setting is None:
                continue
            try:
                error = control_setting.modify_load()
            except (RemoteError, gevent.Timeout) as ex:
                _log.warning(f"Failed to set {control_setting.control_point_topic} to {control_setting.control_value}:"
                             f" {str(ex)}")
                continue
            if error:
                gevent.sleep(1)
                continue

            est_curtailed += control_setting.control_load
            control_manager.increment_control(device_id)
            if self.update_devices(device_name, device_id):
                self.devices.add(control_setting)
                # TODO: Remove deprecated code block after confirmed working.
                #  self.devices == [[], [a,b,c,d,e,f,g,h], []]
                #     [
                #       0  control_setting.device_name,
                #       1  control_setting.device_id,
                #       2  control_setting.control_point_topic,
                #       3  control_setting.revert_value,
                #       4  control_setting.control_load,
                #       5  control_setting.revert_priority,
                #       6  format_timestamp(self.current_time),
                #       7  control_setting.device_actuator,
                #       8  control_setting.control_mode
                #      ]
                # )
            if est_curtailed >= need_curtailed:
                break
        self.lock = False
        self.hold()

    def update_devices(self, device_name, device_id):
        """
        Update devices list with only newly controlled devices.
        """
        for device in self.devices:
            if device_name == device.device_name and device_id == device.device_id:
                return False
        return True

    def actuator_request(self, score_order):
        """
        Request schedule to interact with devices via rpc call to actuator agent.
        :param score_order: ahp priority for devices (curtailment priority).
        :return:
        """
        current_time = get_aware_utc_now()
        start_time_str = format_timestamp(current_time)
        end_curtail_time = current_time + self.longest_possible_curtail + self.actuator_schedule_buffer
        end_time_str = format_timestamp(end_curtail_time)
        control_devices = []

        already_handled = dict((device[0], True) for device in self.scheduled_devices)

        for item in score_order:

            device, token, device_actuator = item
            point_device = self.control_container.get_device((device, device_actuator)).get_point_device(token,
                                                                                                         self.state)
            if point_device is None:
                continue

            control_device = self.base_rpc_path(path=point_device)
            if not self.need_actuator_schedule:
                self.scheduled_devices.add((device, device_actuator, control_device))
                control_devices.append(item)
                continue

            _log.debug("Reserving device: {}".format(device))
            if device in already_handled:
                if already_handled[device]:
                    _log.debug("Skipping reserve device (previously reserved): " + device)
                    control_devices.append(item)
                continue

            schedule_request = [[control_device, start_time_str, end_time_str]]
            try:
                if self.kill_signal_received:
                    break
                result = self.vip.rpc.call(device_actuator, "request_new_schedule",
                                           self.agent_id, control_device, "HIGH", schedule_request).get(timeout=30)
            except RemoteError as ex:
                _log.warning("Failed to schedule device {} (RemoteError): {}".format(device, str(ex)))
                continue

            if result is not None and result["result"] == "FAILURE":
                _log.warning("Failed to schedule device (unavailable) " + device)
                already_handled[device] = False
            else:
                already_handled[device] = True
                self.scheduled_devices.add((device, device_actuator, control_device))
                control_devices.append(item)

        return control_devices

    def setup_release(self):
        if self.stagger_release and self.devices:
            _log.debug("Number or controlled devices: {}".format(len(self.devices)))

            release_steps = int(max(1, math.floor(self.stagger_release_time / self.confirm_time + 1)))
            _log.debug(
                f'In setup_release -- self.stagger_release_time: {self.stagger_release_time}, confirm time: {self.confirm_time}, release_steps: {release_steps}')
            _log.debug(f'Length of self.devices: {len(self.devices)}')
            self.device_group_size = [int(math.floor(len(self.devices) / release_steps))] * release_steps
            _log.debug("On creation, current group size:  {}".format(self.device_group_size))

            if len(self.devices) > release_steps:
                for group in range(len(self.devices) % release_steps):
                    self.device_group_size[group] += 1
            else:
                self.device_group_size = [0] * release_steps
                interval = int(math.ceil(float(release_steps) / len(self.devices)))
                _log.debug("Release interval offset: {}".format(interval))
                for group in range(0, len(self.device_group_size), interval):
                    self.device_group_size[group] = 1
                unassigned = len(self.devices) - sum(self.device_group_size)
                for group, value in enumerate(self.device_group_size):
                    if value == 0:
                        self.device_group_size[group] = 1
                        unassigned -= 1
                    if unassigned <= 0:
                        break

            self.current_stagger = [math.floor((self.stagger_release_time / (release_steps - 1)).seconds / 60)
                                    ] * (release_steps - 1)
            for group in range(int(self.stagger_release_time.seconds / 60 % (release_steps - 1))):
                self.current_stagger[group] += 1
        else:
            self.device_group_size = [len(self.devices)]
            self.current_stagger = []

        _log.debug("Current stagger time:  {}".format(self.current_stagger))
        _log.debug("A end of setup_release, current group size:  {}".format(self.device_group_size))

    def reset_devices(self):
        """
        Release control of devices.
        :return:
        """
        scored_devices = self.criteria_container.get_score_order(self.state_at_actuation)
        controlled = [device for scored in scored_devices for device in self.devices if
                      scored in [(device.device_name, device.device_id)]]
        # THIS SORTED self.devices by their order in sorted_devices.
        _log.debug("Controlled devices: {}".format(self.devices))

        currently_controlled = controlled[::-1]  # reverse order of scored devices.
        controlled_iterate = currently_controlled[:]
        index_counter = 0
        _log.debug("Controlled devices for release reverse sort: {}".format(currently_controlled))

        for item in range(self.device_group_size.pop(0)):
            dev = controlled_iterate[item]

            # If we do not have the highest priority setting with this device_name, work with that one instead.
            if dev.revert_priority is not None:
                current_device_list = [c for c in self.devices if c.device_name == dev.device_name]
                dev = max(current_device_list, key=lambda t: t.revert_priority)

            try:
                dev.release()
                if currently_controlled:
                    _log.debug("Removing from controlled list: {} ".format(controlled_iterate[item]))
                    self.control_container.get_device((dev.device_name, dev.device_actuator)).reset_control_status(
                        dev.device_id)
                    index = controlled_iterate.index(controlled_iterate[item]) - index_counter
                    currently_controlled[index].clear_state()
                    currently_controlled.pop(index)
                    index_counter += 1
            except RemoteError as ex:
                _log.warning("Failed to revert point {} (RemoteError): {}".format(dev.point, str(ex)))
                continue
        self.devices = WeakSet(currently_controlled)
        if self.current_stagger:
            self.next_release = self.current_time + td(minutes=self.current_stagger.pop(0))
        elif self.state not in ['curtail_holding', 'augment_holding', 'augment', 'curtail', 'inactive']:
            self.finished()
        self.lock = False

    def reinitialize_release(self):
        if self.devices:
            self.device_group_size = [len(self.devices)]
            self.reset_devices()
        self.devices = WeakSet()
        self.device_group_size = None
        self.next_release = None
        self.action_end = None
        self.next_confirm = self.current_time + self.confirm_time
        # self.reset_all_devices()
        if self.state == 'inactive':
            _log.debug("**********TRYING TO RELOAD CONFIG PARAMETERS*********")
            if self.config_reload_needed:
                self.reset_parameters(self.saved_config)

    def reset_all_devices(self):
        for device in self.scheduled_devices:
            try:
                release_all = self.vip.rpc.call(device[1], "revert_device", "ilc", device[2]).get(timeout=30)
                _log.debug("Revert device: {} with return value {}".format(device[2], release_all))
            except RemoteError as ex:
                _log.warning("Failed revert all on device {} (RemoteError): {}".format(device[2], str(ex)))
            result = self.vip.rpc.call(device[1], "request_cancel_schedule", self.agent_id, device[2]).get(timeout=30)
        self.scheduled_devices = set()

    def create_application_status(self, result):
        """
        Publish application status.
        :param result:
        :return:
        """
        try:
            topic = "/".join([self.update_base_topic, self.agent_id])
            application_state = "Inactive"
            if self.devices:
                application_state = "Active"
            if self.sim_running:
                headers = {
                    headers_mod.DATE: format_timestamp(self.current_time)
                }
            else:
                headers = {
                    headers_mod.DATE: format_timestamp(get_aware_utc_now()),
                }

            application_message = [
                {
                    "Timestamp": format_timestamp(self.current_time),
                    "Result": result,
                    "ApplicationState": application_state
                },
                {
                    "Timestamp": {"tz": self.power_meta["tz"], "type": "timestamp", "units": "None"},
                    "Result": {"tz": self.power_meta["tz"], "type": "string", "units": "None"},
                    "ApplicationState": {"tz": self.power_meta["tz"], "type": "string", "units": "None"}
                }
            ]
            self.vip.pubsub.publish("pubsub", topic, headers=headers, message=application_message).get(timeout=30.0)
        except:
            _log.debug("Unable to publish application status message.")

    # TODO: create_device_status_publish function was unused. Should it be used somewhere?
    # def create_device_status_publish(self, device_time, device_name, data, topic, meta):
    #     """
    #     Publish device status.
    #     :param device_time:
    #     :param device_name:
    #     :param data:
    #     :param topic:
    #     :param meta:
    #     :return:
    #     """
    #     try:
    #         device_tokens = self.control_container.devices[device_name].command_status.keys()
    #         for subdevice in device_tokens:
    #             control = self.control_container.get_device(device_name).get_control_info(subdevice)
    #             control_pt = control["point"]
    #             device_update_topic = "/".join([self.base_rpc_path, device_name[0], subdevice, control_pt])
    #             previous_value = data[control_pt]
    #             control_time = None
    #             device_state = "Inactive"
    #             for item in self.devices:
    #                 if device_name[0] == item.device_name:
    #                     previous_value = item.control_point_topic
    #                     control_time = item.control_time
    #                     device_state = "Active"
    #
    #             if self.sim_running:
    #                 headers = {
    #                     headers_mod.DATE: format_timestamp(self.current_time),
    #                     "ApplicationName": self.agent_id,
    #                 }
    #             else:
    #                 headers = {
    #                     headers_mod.DATE: format_timestamp(get_aware_utc_now()),
    #                     "ApplicationName": self.agent_id,
    #                 }
    #
    #             device_msg = [
    #                 {
    #                     "DeviceState": device_state,
    #                     "PreviousValue": previous_value,
    #                     "Timestamp": format_timestamp(device_time),
    #                     "TimeChanged": control_time
    #                 },
    #                 {
    #                     "PreviousValue": meta[control_pt],
    #                     "TimeChanged": {
    #                         "tz": meta[control_pt]["tz"],
    #                         "type": "datetime"
    #                     },
    #                     "DeviceState": {"tz": meta[control_pt]["tz"], "type": "string"},
    #                     "Timestamp": {"tz": self.power_meta["tz"], "type": "timestamp", "units": "None"},
    #                 }
    #             ]
    #             self.vip.pubsub.publish("pubsub",
    #                                     device_update_topic,
    #                                     headers=headers,
    #                                     message=device_msg).get(timeout=4.0)
    #     except:
    #         _log.debug("Unable to publish device status message.")

    def simulation_demand_limit_handler(self, peer, sender, bus, topic, headers, message):
        """
        Simulation handler for TargetAgent.
        :param peer:
        :param sender:
        :param bus:
        :param topic:
        :param headers:
        :param message:
        :return:
        """
        self.sim_time = 0
        if isinstance(message, list):
            target_info = message[0]["value"]
            tz_info = message[1]["value"]["tz"]
        else:
            target_info = message
            tz_info = "US/Pacific"

        self.tz = to_zone = dateutil.tz.gettz(tz_info)
        start_time = parser.parse(target_info["start"]).astimezone(to_zone)
        end_time = parser.parse(target_info.get("end", start_time.replace(hour=23, minute=59, second=59))).astimezone(
            to_zone)

        demand_goal = target_info["target"]
        task_id = target_info["id"]

        _log.debug("TARGET: Simulation running.")
        key_list = []
        for key, value in self.tasks.items():
            if (start_time < value["end"] and end_time > value["start"]) or (
                    value["start"] <= start_time < value["end"]):
                key_list.append(key)
        for key in key_list:
            self.tasks.pop(key)

        _log.debug("TARGET: received demand goal schedule - start: {} - end: {} - target: {}.".format(start_time,
                                                                                                      end_time,
                                                                                                      demand_goal))
        self.tasks[target_info["id"]] = {"start": start_time, "end": end_time, "target": demand_goal}
        return

    def publish_record(self, topic_suffix, message):
        if self.sim_running:
            headers = {headers_mod.DATE: format_timestamp(self.current_time)}
        else:
            headers = {headers_mod.DATE: format_timestamp(get_aware_utc_now())}
        message["TimeStamp"] = format_timestamp(self.current_time)
        topic = "/".join([self.record_topic, topic_suffix])
        self.vip.pubsub.publish("pubsub", topic, headers, message).get()


def main():
    """Main method called by the aip."""
    try:
        vip_main(ILCAgent)
    except Exception as exception:
        _log.exception("unhandled exception")
        _log.error(repr(exception))


if __name__ == "__main__":
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
