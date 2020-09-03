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

# CAUTION: if no active task has self task name and can pass task_filter_func(args, kwargs),
# we just assume: self task is the only one which is running


def __inspect_active_task_list(celery_app, timeout=None):
    time.sleep(INSPECT_DELAY)

    kwargs = {}
    if timeout is not None:
        kwargs['timeout'] = timeout
    inspect_obj = celery_app.control.inspect(**kwargs)
    return inspect_obj.active()


def iter_task_list(task_list):
    for (node, task_status_list) in task_list.items():
        for task_status in task_status_list:
            yield task_status


def __filter_task(task_status, self_task_name, task_filter_func):
    #return task_status['name'] == self_task_name and task_filter_func is None or task_filter_func(task_status['args'], task_status['kwargs'])
    if task_status['name'] == self_task_name:
        if task_filter_func is None:
            return True
        else:
            return task_filter_func(task_status['args'], task_status['kwargs'])
    else:
        return False


def __find_task_status(task_list, task_id):
    for task_status in iter_task_list(task_list):
        if task_status['id'] == task_id:
            return task_status
    return None


def __find_task_start_rank(task_list, self_task_status, task_filter_func):
    rank = 0
    for task_status in iter_task_list(task_list):
        if __filter_task(task_status, self_task_status['name'], task_filter_func):
            if task_status['time_start'] <= self_task_status['time_start']:
                rank += 1
    return rank


def get_task_start_rank(celery_app, self, task_filter_func=None, timeout=None):
    active_task_list = __inspect_active_task_list(celery_app, timeout)
    if active_task_list is None:
        return 0

    self_task_status = __find_task_status(active_task_list, self.request.id)
    if self_task_status is None:
        return 0

    return __find_task_start_rank(active_task_list, self_task_status, task_filter_func)


def __find_first_started_task_status(task_list, task_name, task_filter_func):
    first_started_task_status = None
    for task_status in iter_task_list(task_list):
        if __filter_task(task_status, task_name, task_filter_func):
            if first_started_task_status is None or task_status['time_start'] < first_started_task_status['time_start']:
                first_started_task_status = task_status
    return first_started_task_status


def is_first_started_task(celery_app, self, task_filter_func=None, timeout=None):
    active_task_list = __inspect_active_task_list(celery_app, timeout)
    if active_task_list is None:
        return True

    first_started_task_status = __find_first_started_task_status(active_task_list, self.name, task_filter_func)
    return first_started_task_status is None or first_started_task_status['id'] == self.request.id

