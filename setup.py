# -*- coding: utf-8 -*-
"""
    setup
    ~~~~~

    setup.py for PyInfluxQL
"""

from setuptools import setup, find_packages


def get_long_description():
    with open('README.rst') as f:
        readme = f.read()
    return readme

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
    tests_require=find_packages(include=['*-dev'])
)
