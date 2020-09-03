#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# celery_controlled_singleton - celery-controlled singleton task detection.
# It can detect and help you discard same tasks which are already running.
#
# Copyright (C) 2020-present Himawari Tachibana <fieliapm@gmail.com>
#
# This file is part of celery_controlled_singleton
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
################################################################################


import time


__all__ = ['get_task_start_rank', 'is_first_started_task']


INSPECT_DELAY = 0.1


# celery tasks inspect
# CAUTION: if no active task can pass task_filter_func(task), we just assume: current task is the only one which is running


def __inspect_active_task_list(celery_app, timeout=None):
    time.sleep(INSPECT_DELAY)

    kwargs = {}
    if timeout is not None:
        kwargs['timeout'] = timeout
    task_list = celery_app.control.inspect(**kwargs)
    return task_list.active()


def __filter_task(task_filter_func, task):
    return task_filter_func(task['name'], task['args'], task['kwargs'])


def __find_current_task(current_task_id, task_list):
    for (node, tasks) in task_list.items():
        for task in tasks:
            if task['id'] == current_task_id:
                return task
    return None


def __find_current_task_start_rank(current_task, task_filter_func, task_list):
    rank = 0
    for (node, tasks) in task_list.items():
        for task in tasks:
            if ___filter_task(task_filter_func, task):
                if task['time_start'] <= current_task['time_start']:
                    rank += 1
    return rank


def get_task_start_rank(celery_app, current_task_id, task_filter_func, timeout=None):
    active_task_list = __inspect_active_task_list(celery_app, timeout)
    if active_task_list is None:
        return 0

    current_task = __find_current_task(current_task_id, active_task_list)
    if current_task is None:
        return 0

    return __find_current_task_start_rank(current_task, task_filter_func, active_task_list)


def __find_first_started_task(task_filter_func, task_list):
    first_started_task = None
    for (node, tasks) in task_list.items():
        for task in tasks:
            if __filter_task(task_filter_func, task):
                if first_started_task is None or task['time_start'] < first_started_task['time_start']:
                    first_started_task = task
    return first_started_task


def is_first_started_task(celery_app, current_task_id, task_filter_func, timeout=None):
    active_task_list = __inspect_active_task_list(celery_app, timeout)
    if active_task_list is None:
        return True

    first_started_task = __find_first_started_task(task_filter_func, active_task_list)
    return first_started_task is None or first_started_task['id'] == current_task_id

