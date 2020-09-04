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


from distutils.core import setup


setup(name='celery_controlled_singleton',
    version='0.1.0',
    description='celery-controlled singleton task detection',
    author='Himawari Tachibana',
    author_email='fieliapm@gmail.com',
    url='https://github.com/fieliapm/celery_controlled_singleton',
    py_modules=['celery_controlled_singleton'],
    install_requires=[
        'six',
        'celery>=4.2.0rc1', # first version support absolute time_start of task
    ],
)

