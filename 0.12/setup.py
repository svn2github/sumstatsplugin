#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2011-2012 Rob Guttman <guttman@alum.mit.edu>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
#

from setuptools import setup

PACKAGE = 'TracSumStats'
VERSION = '0.9.0'

setup(
    name=PACKAGE, version=VERSION,
    description='Sums a field for Roadmap/Milestone stats',
    author="Rob Guttman", author_email="guttman@alum.mit.edu",
    license='3-Clause BSD', url='http://trac-hacks.org/wiki/SumStatsPlugin',
    packages = ['sumstats'],
    package_data = {'sumstats':['templates/*.css']},
    entry_points = {'trac.plugins':['sumstats.web_ui = sumstats.web_ui']}
)
