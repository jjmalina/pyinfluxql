# -*- coding: utf-8 -*-
"""
    test_utils
    ~~~~~~~~~~

    tests utils
"""

import pytest
from datetime import timedelta

from pyinfluxql.utils import format_timedelta, format_boolean, parse_interval


@pytest.mark.unit
def test_format_timedelta():
    """_format_timedelta should correctly format a timedelta

    anything >= than 24 hours (days, weeks) should convert to hours
    anything >= than 60 minutes should convert to hours
    anything >= than 60 seconds should convert to to minutes
    anything >= than 1000 milliseconds should convert to seconds
    anything >= than 1000 microseconds should convert to milliseconds
    anything < should convert to microseconds
    """
    assert format_timedelta(timedelta(weeks=2)) == "2w"
    assert format_timedelta(timedelta(weeks=2, minutes=2)) == "20162m"
    assert format_timedelta(timedelta(days=30)) == "30d"
    assert format_timedelta(timedelta(days=2)) == "2d"
    assert format_timedelta(timedelta(days=2, hours=2)) == "50h"
    assert format_timedelta(timedelta(days=2, hours=2, minutes=1)) == "3001m"
    assert format_timedelta(timedelta(hours=36)) == "36h"
    assert format_timedelta(timedelta(hours=24)) == "1d"
    assert format_timedelta(timedelta(hours=16)) == "16h"
    assert format_timedelta(timedelta(hours=1)) == "1h"
    assert format_timedelta(timedelta(minutes=120)) == "2h"
    assert format_timedelta(timedelta(minutes=90)) == "90m"
    assert format_timedelta(timedelta(minutes=59)) == "59m"
    assert format_timedelta(timedelta(minutes=30)) == "30m"
    assert format_timedelta(timedelta(minutes=1)) == "1m"
    assert format_timedelta(timedelta(seconds=90)) == "90s"
    assert format_timedelta(timedelta(seconds=59)) == "59s"
    assert format_timedelta(timedelta(seconds=1)) == "1s"
    assert format_timedelta(timedelta(milliseconds=2000)) == "2s"
    assert format_timedelta(timedelta(milliseconds=999)) == "999ms"
    assert format_timedelta(timedelta(milliseconds=1)) == "1ms"
    assert format_timedelta(timedelta(microseconds=2000)) == "2ms"
    assert format_timedelta(timedelta(microseconds=2500)) == "2500us"
    assert format_timedelta(timedelta(microseconds=999)) == "999us"
    assert format_timedelta(timedelta(microseconds=1)) == "1us"


@pytest.mark.unit
def test_parse_interval():
    assert parse_interval('1s') == timedelta(seconds=1)
    assert parse_interval('1m') == timedelta(minutes=1)
    assert parse_interval('1h') == timedelta(hours=1)
    assert parse_interval('24h') == timedelta(hours=24)
    assert parse_interval('1d') == timedelta(days=1)


@pytest.mark.unit
def test_format_boolean():
    assert format_boolean(True) == 'true'
    assert format_boolean(False) == 'false'
