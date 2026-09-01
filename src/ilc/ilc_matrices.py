# -*- coding: utf-8 -*- {{{
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


import math
import operator
from functools import reduce

def validate_input(pairwise_matrix, col_sums):
    """
    Validates AHP pairwise comparison matrices using the consistency ratio.

    pairwise_matrix format:
        {
            "state1": [
                [1,   3,   5],
                [1/3, 1,   2],
                [1/5, 1/2, 1]
            ]
        }

    col_sums is still accepted as input, but the consistency calculation
    uses the full pairwise matrix because that is more reliable.

    Returns:
        True if all matrices are consistent enough, otherwise False.
    """

    _log.info("Validating matrix")

    random_index = {
        1: 0.00,
        2: 0.00,
        3: 0.58,
        4: 0.90,
        5: 1.12,
        6: 1.24,
        7: 1.32,
        8: 1.41,
        9: 1.45,
        10: 1.49,
    }

    consistent = True

    for state in pairwise_matrix:
        matrix = pairwise_matrix[state]
        n = len(matrix)

        if n == 0:
            raise ValueError(f"{state}: matrix is empty")

        if state not in col_sums:
            raise ValueError(f"{state}: missing column sums")

        if len(col_sums[state]) != n:
            raise ValueError(
                f"{state}: col_sums length does not match matrix size"
            )

        if n not in random_index:
            raise ValueError(
                f"{state}: random index not available for matrix size {n}"
            )

        # Validate square matrix and positive values
        for row in matrix:
            if len(row) != n:
                raise ValueError(f"{state}: matrix must be square")

            for value in row:
                if value <= 0:
                    raise ValueError(
                        f"{state}: AHP pairwise comparison values must be positive"
                    )

        # 1x1 and 2x2 reciprocal AHP matrices are always consistent
        if n <= 2:
            consistency_index = 0.0
            consistency_ratio = 0.0

            _log.debug(
                "Pairwise comparison: {} - CI: {} - CR: {}".format(
                    state, consistency_index, consistency_ratio
                )
            )

            continue

        # Calculate row geometric means.
        # Original code used 1.0 / 5, which is only correct for 5x5 matrices.
        roots = []
        for row in matrix:
            row_product = reduce(operator.mul, row, 1)
            roots.append(math.pow(row_product, 1.0 / n))

        # Normalize to get priority vector
        root_sum = sum(roots)
        priority_vec = [item / root_sum for item in roots]

        # Calculate A * w
        weighted_sum_vec = []
        for i in range(n):
            weighted_sum = 0.0
            for j in range(n):
                weighted_sum += matrix[i][j] * priority_vec[j]
            weighted_sum_vec.append(weighted_sum)

        # Calculate lambda values: (A*w)_i / w_i
        lambda_values = []
        for i in range(n):
            lambda_values.append(weighted_sum_vec[i] / priority_vec[i])

        # Average lambda values to estimate lambda_max
        lambda_max = sum(lambda_values) / n

        # Calculate consistency index
        consistency_index = (lambda_max - n) / (n - 1)

        # Avoid tiny negative values caused by floating point precision
        if consistency_index < 0 and abs(consistency_index) < 1e-12:
            consistency_index = 0.0

        # Calculate consistency ratio
        consistency_ratio = consistency_index / random_index[n]

        _log.debug(
            "Pairwise comparison: {} - lambda_max: {} - CI: {} - CR: {}".format(
                state, lambda_max, consistency_index, consistency_ratio
            )
        )

        if consistency_ratio > 0.2:
            consistent = False
            _log.debug(
                "Inconsistent pairwise comparison: {} - CR: {}".format(
                    state, consistency_ratio
                )
            )

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
