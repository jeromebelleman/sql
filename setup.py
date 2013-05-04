#!/usr/bin/env python
# coding=utf-8

import os
from distutils.core import setup

delattr(os, 'link')

setup(
    name='sql',
    version='1.0',
    author='Jerome Belleman',
    author_email='Jerome.Belleman@gmail.com',
    url='http://cern.ch/jbl',
    description="SQL command-line interface",
    long_description="A Python expression of what SQL*Plus should have been.",
    scripts=['sql'],
    data_files=[('share/man/man1', ['sql.1'])],
)
