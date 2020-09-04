#!/usr/bin/env python
# -*- coding: utf-8 -*-


import time

import celery

import celery_controlled_singleton


celery_app_name = 'my_app'

celery_app = celery.Celery(celery_app_name, backend='rpc://', broker='pyamqp://guest@localhost//')
#celery_app = celery.Celery(celery_app_name, backend='redis://', broker='redis://localhost/')


def print_info(self, operation, fmt, *args):
    info = fmt % args
    print('%s[%s](%s%s) %s: %s' % (self.name, self.request.id, repr(self.request.args), repr(self.request.kwargs), operation, info))


@celery_app.task(bind=True)
def add(self, x, y, use_rank=False):
    def __add_task_filter(args, kwargs):
        return True

    print_info(self, 'start', '%f', time.time())

    if use_rank:
        rank = celery_controlled_singleton.get_task_start_rank(celery_app, self, __add_task_filter)
        print_info(self, 'rank', '%d', rank)
        if rank > 1:
            print_info(self, 'skip', '%f', time.time())
            return None
    else:
        t = celery_controlled_singleton.is_first_started_task(celery_app, self)
        print_info(self, 'first?', '%s', t)
        if not t:
            print_info(self, 'skip', '%f', time.time())
            return None

    time.sleep(5.0)

    print_info(self, 'end', '%f', time.time())
    return x + y


@celery_app.task(bind=True)
def sub(self, x, y, use_rank=False):
    def __sub_task_filter(args, kwargs):
        return args[0] == x and args[1] == y

    print_info(self, 'start', '%f', time.time())

    if use_rank:
        rank = celery_controlled_singleton.get_task_start_rank(celery_app, self, __sub_task_filter, timeout=2.0)
        print_info(self, 'rank', '%d', rank)
        if rank > 1:
            print_info(self, 'skip', '%f', time.time())
            return None
    else:
        t = celery_controlled_singleton.is_first_started_task(celery_app, self, __sub_task_filter, timeout=2.0)
        print_info(self, 'first?', '%s', t)
        if not t:
            print_info(self, 'skip', '%f', time.time())
            return None

    time.sleep(5.0)

    print_info(self, 'end', '%f', time.time())
    return x - y

