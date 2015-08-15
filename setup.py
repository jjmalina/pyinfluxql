# -*- coding: utf-8 -*-
"""
    setup
    ~~~~~

    setup.py for PyInfluxQL
"""

from setuptools import setup, find_packages


def get_requirements(suffix=''):
    with open('requirements%s.txt' % suffix) as f:
        rv = f.read().splitlines()
    return rv


def get_long_description():
    with open('README.md') as f:
        rv = f.read()
    return rv

setup(
    name='pyinfluxql',
    version='0.0.1',
    url='https://github.com/jjmalina/pyinfluxql',
    author='Jeremiah Malina',
    author_email='me@jerem.io',
    description='',
    long_description=get_long_description(),
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    tests_require=get_requirements('-dev')
)
