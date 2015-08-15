# -*- coding: utf-8 -*-
"""
    pyinfluxql.utils
    ~~~~~~~~~~~~~~~~

    Utility functions
"""

from datetime import timedelta


def parse_interval(interval):
    unit = interval[-1]
    scalar = int(interval[:-1])
    if unit == 's':
        key = 'seconds'
    elif unit == 'm':
        key = 'minutes'
    elif unit == 'h':
        key = 'hours'
    elif unit == 'd':
        key = 'days'
    else:
        key = 's'
    return timedelta(**{key: scalar})


def format_timedelta(td):
    """formats a timedelta into the largest unit possible
    """
    total_seconds = td.total_seconds()
    units = [(604800, 'w'), (86400, 'd'), (3600, 'h'), (60, 'm'), (1, 's')]
    for seconds, unit in units:
        if total_seconds >= seconds and total_seconds % seconds == 0:
            return "%r%s" % (int(total_seconds / seconds), unit)
    if total_seconds >= 0.001:
        if (total_seconds / 0.001) % 1 == 0:
            return "%r%s" % (int(total_seconds * 1000), 'ms')
        else:
            micro = int(total_seconds / 0.000001)
            micro += int(total_seconds % 0.000001)
            return "%r%s" % (micro, 'us')
    return "%r%s" % (int(total_seconds * 1000000), 'us')


def format_boolean(value):
    return 'true' if value else 'false'
