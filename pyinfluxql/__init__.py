# -*- coding: utf-8 -*-
"""
    pyinfluxql
    ~~~~~~~~~~

    InfluxDB SQL generator
"""

__version__ = '0.0.1'

from .query import Query


class Engine(object):
    def __init__(self, client):
        self.client = client

    def execute(self, query):
        return self.client.query(str(query))

    def query(self, *expressions):
        return Query(*expressions)
