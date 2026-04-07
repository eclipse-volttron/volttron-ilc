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

import re
import logging

from typing import ItemsView, List, Set, Dict, Tuple, Union
from sympy.parsing.sympy_parser import parse_expr
from sympy.logic.boolalg import Boolean

_log = logging.getLogger(__name__)


def clean_text(text: str, rep: dict = {}) -> str:
    """
    Removes special characters associated with mathematics from a string.

    :param text: string with special characters
    :type text: str
    :param rep: dictionary of special character replacements.
    :type rep: dict
    :return: string where special characters have been removed (replaced).
    :rtype: str
    """
    rep = rep if rep else {".": "_", "-": "_", "+": "_", "/": "_", ":": "_"}
    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))
    new_key = pattern.sub(lambda m: rep[re.escape(m.group(0))], text)
    return new_key


def sympy_evaluate(condition: str, points: Union[List[Tuple[str, float]], ItemsView[str, float]]) -> Union[bool, float]:
    """
    Calls clean_text to remove special characters from string in points,
    does string replace to for cleaned point in condition, and evaluates symbolic math
    condition.

    :param condition: string equation or condition.
    :type condition: str
    :param points: list of tuples with - [(point_name, value)] =
    :type points: list[tuples]
    :return: evaluated sympy expression
    :rtype: float or bool
    """
    cleaned_points = []
    cleaned_condition = condition
    for point, value in points:
        cleaned = clean_text(point)
        cleaned_condition = cleaned_condition.replace(point, cleaned)
        cleaned_points.append((cleaned, value))
    _log.debug(f"Sympy debug condition: {condition} -- {cleaned_condition}")
    _log.debug(f"Sympy debug points: {points} -- {cleaned_points}")
    equation = parse_expr(cleaned_condition)
    return_value = equation.subs(cleaned_points)
    _log.debug(f"Sympy debug return value for condition: {condition} -- {return_value}")
    if return_value.is_infinite:
        return 0.0
    elif isinstance(return_value, Boolean):
        return bool(return_value)
    else:
        return float(return_value)


def parse_sympy(data: List[str]|str) -> str:
    """
    Creates conditional from list of conditional components.

    :param data: List of conditional parts
    :type data: list

    :return: string of constructed condition for sympy
    :rtype: str
    """
    if isinstance(data, list):
        return_data = ""
        for item in data:
            parsed_string = "(" + item + ")" if item not in ("&", "|") else item
            return_data += parsed_string
    else:
        return_data = data
    return return_data


def create_device_topic_map(arg_list: Union[List[str], List[Tuple[str, str]]],
                            default_topic: str = ""
                            ) -> Tuple[Dict[str, str], Set[str]]:
    """
    Create device topic map for ingestion of data.

    :param arg_list: list of point names or point name, device topic pairs.
    :type arg_list: list
    :param default_topic: full topic for device
    :type default_topic: str
    :return result: dictionary of full point path: point
    :rtype result: dict
    :return topics: set of device topic strings
    :rtype topics: set
    """
    result = {}
    topics = set()
    for item in arg_list:
        if isinstance(item, str):
            point = item
            result[default_topic + '/' + point] = point
            topics.add(default_topic)
        elif isinstance(item, (list, tuple)):
            device, point = item
            result[device+'/'+point] = point
            topics.add(device)
    return result, topics


def fix_up_point_name(point: Union[str, List[str]], default_topic: str = "") -> Tuple[str, str]:
    """
    Create full point path from point and device topic.

    :param point: point name from device
    :type point: str
    :param default_topic: full topic for device
    :type default_topic: str
    :return: tuple with full point path and device topic
    :rtype: tuple
     """
    if isinstance(point, list):
        device, point = point
        return device + '/' + point, device
    elif isinstance(point, str):
        return default_topic + '/' + point, default_topic
