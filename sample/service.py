#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import time

import celery

import tasks


def run(task):
    result_list = []
    for i in range(6):
        result = task.apply_async(args=(i, 1))
        result_list.append(result)
        time.sleep(1.0)

    count = 0
    for result in result_list:
        r = result.get()
        if r is not None:
            count += 1
        print('result: %s' % (str(r),))
    print('run count: %d' % (count,))


def main(argv=sys.argv[:]):
    run(tasks.add)
    time.sleep(5.0)
    run(tasks.sub)
    return 0


if __name__ == '__main__':
    sys.exit(main())
