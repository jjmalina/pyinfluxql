# -*- coding: utf-8 -*-
"""
    tests.test_server
    ~~~~~~~~~~~~~~~~~

    Integration tests
"""

import pytest
from pyinfluxql import Query
from pyinfluxql.functions import Mean
from datetime import timedelta


@pytest.mark.integration
def test_query(engine, date_range):
    start, end = date_range
    query = Query(Mean('value')).from_('deliciousness')
    result = list(engine.execute(query).get_points())
    result == [
        {u'mean': 2.063999999999999, u'time': u'1970-01-01T00:00:00Z'}]

    query = Query(
        Mean('value')).from_('deliciousness').where(
        time__gt=start +
        timedelta(
            hours=1))
    result = list(engine.execute(query).get_points())
    result == [
        {u'mean': 2.056451612903225, u'time': u'2015-06-06T01:00:00.000000001Z'}]
    query = Query(Mean('value')).from_('deliciousness') \
        .date_range(start, end - timedelta(hours=50))
    result = list(engine.execute(query).get_points())
    result == [
        {u'mean': 2.1507537688442198, u'time': u'2015-06-06T00:00:00.000000001Z'}]
    query = Query(Mean('value')).from_('deliciousness') \
        .where(dish="pie", time__lt=end)
    result = list(engine.execute(query).get_points())
    result == [
        {u'mean': 1.9120000000000004, u'time': u'1970-01-01T00:00:00Z'}]
    query = Query(Mean('value')).from_('deliciousness') \
        .where(dish="pie").date_range(start, end) \
        .group_by(time=timedelta(hours=50))
    result = list(engine.execute(query).get_points())
    result == [{u'mean': None,
                u'time': u'2015-06-05T16:00:00Z'},
               {u'mean': 1.4000000000000001,
                u'time': u'2015-06-07T18:00:00Z'},
               {u'mean': 1.8399999999999999,
                u'time': u'2015-06-09T20:00:00Z'},
               {u'mean': 2.24,
                u'time': u'2015-06-11T22:00:00Z'},
               {u'mean': 1.7999999999999998,
                u'time': u'2015-06-14T00:00:00Z'},
               {u'mean': 2.5,
                u'time': u'2015-06-16T02:00:00Z'}]
