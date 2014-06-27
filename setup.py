# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from setuptools import setup, find_packages

setup(
    name='aiocouchdb',
    version='0.0',
    packages=find_packages(),
    url='https://github.com/kxepal/aiocouchdb',
    license='BSD',
    author='Alexander Shorin',
    author_email='kxepal@gmail.com',
    description='CouchDB client built on top of aiohttp (asyncio)',
    install_requires=[
        'aiohttp>=0.8'
    ]
)
