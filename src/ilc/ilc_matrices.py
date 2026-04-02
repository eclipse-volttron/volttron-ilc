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

import logging
import math
import operator

from collections import defaultdict
from functools import reduce

_log = logging.getLogger(__name__)


def extract_criteria(filename):
    """
    Extract pairwise criteria parameters
    :param filename:
    :return:
    """
    criteria_labels = {}
    criteria_matrix = {}
    # config_matrix = load_config(filename)
    config_matrix = filename
    # check if file has been updated or uses old format
    _log.debug("CONFIG_MATRIX: {}".format(config_matrix))
    if "curtail" not in config_matrix.keys() and "augment" not in config_matrix.keys():
        config_matrix = {"curtail": config_matrix}

    _log.debug("CONFIG_MATRIX: {}".format(config_matrix))
    for state in config_matrix:
        index_of = dict([(a, i) for i, a in enumerate(config_matrix[state].keys())])

        criteria_labels[state] = []
        for label, index in index_of.items():
            criteria_labels[state].insert(index, label)

        criteria_matrix[state] = [[0.0 for _ in config_matrix[state]] for _ in config_matrix[state]]
        for j in config_matrix[state]:
            row = index_of[j]
            criteria_matrix[state][row][row] = 1.0

            for k in config_matrix[state][j]:
                col = index_of[k]
                criteria_matrix[state][row][col] = float(config_matrix[state][j][k])
                criteria_matrix[state][col][row] = float(1.0 / criteria_matrix[state][row][col])

    return criteria_labels, criteria_matrix, list(config_matrix.keys())


def calc_column_sums(criteria_matrix):
    """
    Calculate the column sums for the criteria matrix.
    :param criteria_matrix:
    :return:
    """
    cumsum = {}
    for state in criteria_matrix:
        j = 0
        cumsum[state] = []
        while j < len(criteria_matrix[state][0]):
            col = [float(row[j]) for row in criteria_matrix[state]]
            cumsum[state].append(sum(col))
            j += 1
    return cumsum


def normalize_matrix(criteria_matrix, col_sums):
    """
    Normalizes the members of criteria matrix using the vector
    col_sums. Returns sums of each row of the matrix.
    :param criteria_matrix:
    :param col_sums:
    :return:
    """
    normalized_matrix = {}
    row_sums = {}
    for state in criteria_matrix:
        normalized_matrix[state] = []
        row_sums[state] = []
        i = 0
        while i < len(criteria_matrix[state]):
            j = 0
            norm_row = []
            while j < len(criteria_matrix[state][0]):
                norm_row.append(criteria_matrix[state][i][j]/(col_sums[state][j] if col_sums[state][j] != 0 else 1))
                j += 1
            row_sum = sum(norm_row)
            norm_row.append(row_sum/j)
            row_sums[state].append(row_sum/j)
            normalized_matrix[state].append(norm_row)
            i += 1
    return row_sums


def validate_input(pairwise_matrix, col_sums):
    """
    Validates the criteria matrix to ensure that the inputs are

    internally consistent. Returns a True if the matrix is valid,
    and False if it is not.
    :param pairwise_matrix:
    :param col_sums:
    :return:
    """
    # Calculate row products and take the 5th root
    _log.info("Validating matrix")
    consistent = True
    for state in pairwise_matrix:
        random_index = [0, 0, 0, 0.58, 0.9, 1.12, 1.24, 1.32, 1.41, 1.45, 1.49]
        roots = []
        for row in pairwise_matrix[state]:
            roots.append(math.pow(reduce(operator.mul, row, 1), 1.0/5))
        # Sum the vector of products
        root_sum = sum(roots)
        # Calculate the priority vector
        priority_vec = []
        for item in roots:
            priority_vec.append(item / root_sum)

        # Calculate the priority row
        priority_row = []
        for i in range(0, len(col_sums[state])):
            priority_row.append(col_sums[state][i] * priority_vec[i])

        # Sum the priority row
        priority_row_sum = sum(priority_row)

        # Calculate the consistency index
        ncols = max(len(col_sums[state]) - 1, 1)
        consistency_index = \
            (priority_row_sum - len(col_sums[state]))/ncols

        # Calculate the consistency ratio
        if len(col_sums[state]) < 4:
            consistency_ratio = consistency_index
        else:
            rindex = random_index[len(col_sums[state])]
            consistency_ratio = consistency_index / rindex

        _log.debug("Pairwise comparison: {} - CR: {}".format(state, consistency_index))
        if consistency_ratio > 0.2:
            consistent = False
            _log.debug("Inconsistent pairwise comparison: {} - CR: {}".format(state, consistency_ratio))

    return consistent


def build_score(_matrix, weight, priority):
    """
    Calculates the curtailment score using the normalized matrix
    and the weights vector. Returns a sorted vector of weights for each
    device that is a candidate for curtailment.
    :param _matrix:
    :param weight:
    :param priority:
    :return:
    """
    input_keys, input_values = _matrix.keys(), _matrix.values()
    scores = []

    for input_array in input_values:
        criteria_sum = sum(i*w for i, w in zip(input_array, weight))

        scores.append(criteria_sum*priority)

    return zip(scores, input_keys)


def input_matrix(builder, criteria_labels):
    """
    Construct input normalized input matrix.
    :param builder:
    :param criteria_labels:
    :return:
    """
    sum_mat = defaultdict(float)
    inp_mat = {}
    label_check = list(list(builder.values())[-1].keys())
    if set(label_check) != set(criteria_labels):
        raise Exception('Input criteria and data criteria do not match.')
    for device_data in builder.values():
        for k, v in device_data.items():
            sum_mat[k] += v
    for key in builder:
        inp_mat[key] = mat_list = []
        for tag in criteria_labels:
            builder_value = builder[key][tag]
            if builder_value:
                mat_list.append(builder_value/sum_mat[tag])
            else:
                mat_list.append(0.0)

    return inp_mat
