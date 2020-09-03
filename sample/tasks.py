#!/usr/bin/env python
# -*- coding: utf-8 -*-


import time

import celery

import celery_controlled_singleton


celery_app_name = 'my_app'

celery_app = celery.Celery(celery_app_name, backend='rpc://', broker='pyamqp://guest@localhost//')
#celery_app = celery.Celery(celery_app_name, backend='redis://', broker='redis://localhost/')


@celery_app.task(bind=True)
def add(self, x, y):
    def __add_task_filter(args, kwargs):
        return True

    print('add start %s %f' % (self.request.id, time.time()))

    rank = celery_controlled_singleton.get_task_start_rank(celery_app, self, __add_task_filter)
    print("rank %s %d" % (self.request.id, rank))
    t = celery_controlled_singleton.is_first_started_task(celery_app, self)
    print("first? %s %s" % (self.request.id, t))
    if not t:
        print('add skip %s %f' % (self.request.id, time.time()))
        return None

    time.sleep(5.0)

    print('add end %s %f' % (self.request.id, time.time()))
    return x + y


@celery_app.task(bind=True)
def sub(self, x, y):
    def __sub_task_filter(args, kwargs):
        return args[0] == x and args[1] == y

    print('sub start %s %f' % (self.request.id, time.time()))

    rank = celery_controlled_singleton.get_task_start_rank(celery_app, self, __sub_task_filter, timeout=2.0)
    print("rank %s %d" % (self.request.id, rank))
    t = celery_controlled_singleton.is_first_started_task(celery_app, self, __sub_task_filter, timeout=2.0)
    print("first? %s %s" % (self.request.id, t))
    if not t:
        print('sub skip %s %f' % (self.request.id, time.time()))
        return None

    time.sleep(5.0)

    print('sub end %s %f' % (self.request.id, time.time()))
    return x - y

