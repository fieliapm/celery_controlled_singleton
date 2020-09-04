#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import time

import celery

import tasks


def delay_run(task):
    result_list = []
    for i in range(30):
        result = task.apply_async(args=(i, 1), kwargs={'use_rank': False})
        result_list.append(result)
        #time.sleep(1.0)
    return result_list


def collect_result(name, result_list):
    count = 0
    for result in result_list:
        r = result.get()
        if r is not None:
            count += 1
        print('%s result: %s' % (name, str(r)))
    print('%s run count: %d' % (name, count))


def main(argv=sys.argv[:]):
    add_result_list = delay_run(tasks.add)
    sub_result_list = delay_run(tasks.sub)
    time.sleep(5.0)
    collect_result('add', add_result_list)
    collect_result('sub', sub_result_list)
    return 0


if __name__ == '__main__':
    sys.exit(main())

