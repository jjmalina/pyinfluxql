# -*- coding: utf-8 -*-
"""
    pyinfluxql.query
    ~~~~~~~~~~~~~~~~

    PyInfluxQL query generator
"""

import re
import six
import datetime
from copy import copy, deepcopy
from dateutil.tz import tzutc
from .functions import Func
from .utils import format_timedelta, format_boolean

UTC_TZ = tzutc()


class Query(object):
    binary_op = {
        'eq': '=',
        'ne': '!=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<='
    }
    _numeric_types = (int, float)
    _order_identifiers = {'asc', 'desc'}

    def __init__(self, *expressions):
        self._select_expressions = list(expressions)
        self._measurement = None
        self._is_delete = False
        self._limit = None
        self._where = {}
        self._start_time = None
        self._end_time = None
        self._group_by_fill = False
        self._group_by_time = None
        self._group_by = []
        self._into_series = None
        self._order = None
        self._order_by = []

    def clone(self):
        query = Query().from_(self._measurement)
        query._select_expressions = deepcopy(self._select_expressions)
        query._is_delete = self._is_delete
        query._limit = self._limit
        query._where = deepcopy(self._where)
        query._group_by_time = copy(self._group_by_time)
        query._group_by = deepcopy(self._group_by)
        return query

    def _format_select_expression(self, expr):
        formatted = expr
        if issubclass(type(expr), Func):
            formatted = expr.format()
        return formatted

    def _format_select_expressions(self, *select_expressions):
        """Format the function stack to form a function clause
        If it's empty and there are no functions we just return the column name
        """
        return ", ".join([
            self._format_select_expression(s) for s in select_expressions
        ])

    def _format_select(self):
        return "SELECT %s" % self._format_select_expressions(
            *self._select_expressions)

    def _format_measurement(self, measurement):
        enquote = (
            not (measurement[0] == '/' and measurement[-1] == '/')
            and (" " in measurement or "-" in measurement))
        if enquote:
            return '"%s"' % measurement
        return measurement

    def _format_from(self):
        clause = "FROM %s" % self._format_measurement(self._measurement)
        return clause

    def _format_value(self, value):
        if isinstance(value, six.string_types):
            if value[0] == '/':
                return value
            return "'%s'" % value
        elif type(value) is bool:
            return format_boolean(value)
        elif isinstance(value, self._numeric_types):
            return "%r" % value
        elif isinstance(value, datetime.datetime):
            if value.tzinfo:
                value = value.astimezone(UTC_TZ)
            dt = datetime.datetime.strftime(value, "%Y-%m-%d %H:%M:%S.%f")
            return "'%s'" % dt[:-3]

    def _format_where_expression(self, identifiers, comparator, value):
        return '%s %s %s' % ('.'.join(identifiers),
                             self.binary_op[comparator],
                             self._format_value(value))

    def _format_where(self):
        if not self._where:
            return ''

        formatted = []
        for expression in sorted(self._where.keys()):
            if '__' not in expression:
                comparator = 'eq'
                identifiers = [expression]
            else:
                identifiers = expression.split('__')
                if identifiers[-1] not in self.binary_op:
                    comparator = 'eq'
                else:
                    comparator = identifiers[-1]
                    identifiers = identifiers[:-1]

            formatted.append(self._format_where_expression(
                identifiers, comparator, self._where[expression]))

        return "WHERE %s" % (" AND ".join(formatted))

    def _format_group_by(self):
        if self._group_by or self._group_by_time:
            time_format = []
            if self._group_by_time:
                time_fmt = None
                if isinstance(self._group_by_time, datetime.timedelta):
                    time_fmt = "time(%s)" % format_timedelta(self._group_by_time)
                elif isinstance(self._group_by_time, six.string_types):
                    time_fmt = "time(%s)" % self._group_by_time
                if time_fmt is not None:
                    time_format.append(time_fmt)
            clause = "GROUP BY " + ", ".join(time_format + self._group_by)
            if self._group_by_fill:
                clause += ' fill(0)'
            return clause
        return ''

    def _format_into(self):
        if self._into_series:
            return 'INTO %s' % self._into_series
        return ''

    def _format_limit(self):
        clause = ''
        if self._limit:
            clause = "LIMIT %i" % self._limit
        return clause

    def _format_order(self):
        clause = ''
        if self._order:
            clause = 'ORDER BY %s %s' % (' '.join(self._order_by), self._order)
        return clause

    def _format_query(self, query):
        """Trims extra spaces and inserts a semicolon at the end
        """
        query = re.sub(r' +', ' ', query)
        if query[-1] == ' ':
            query = query[:len(query) - 1]
        return query + ';'

    def _format_delete_query(self):
        query = "DELETE %s %s" % (self._format_from(), self._format_where())
        return self._format_query(query)

    def _format_select_query(self):
        query = "%s %s %s %s %s %s %s" % (self._format_select(),
                                          self._format_from(),
                                          self._format_where(),
                                          self._format_group_by(),
                                          self._format_limit(),
                                          self._format_into(),
                                          self._format_order())
        return self._format_query(query)

    def _format(self):
        if self._is_delete:
            return self._format_delete_query()
        return self._format_select_query()

    def from_(self, measurement):
        self._measurement = measurement
        return self

    def select(self, *expressions):
        """Could be a one or more column names or expressions composed of
        functions from http://influxdb.org/docs/query_language/functions.html
        """
        if not expressions:
            raise TypeError("Select takes at least one expression")
        self._select_expressions.extend(expressions)
        return self

    def where(self, **clauses):
        """
        .where(something=something,
               something__ne=something,
               something__lt=somethingelse,
               something__gt=somethingelseelse)

        See "The Where Clause" at http://influxdb.org/docs/query_language/
        OR operations are not supported

        TODO:
            support OR operations by adding in some kind of _Or function
            see http://docs.sqlalchemy.org/en/rel_0_9/orm/tutorial.html#common-filter-operators
        """
        self._where.update(clauses)
        return self

    def date_range(self, start=None, end=None):
        """Insert where clauses to filter by date
        """
        if not start and not end:
            raise ValueError("date_range requires either a start or end")
        elif start and end and start > end:
            raise ValueError(
                "date_range boundaries should have start <= end, got %r > %r" % (
                    start, end))
        if start:
            self._where['time__gt'] = start
            self._start_time = start
        if end:
            self._where['time__lt'] = end
            self._end_time = end
        return self

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    def group_by(self, *columns, **kwargs):
        if 'time' in kwargs and kwargs['time']:
            self._group_by_time = kwargs['time']
        if 'fill' in kwargs and kwargs['fill']:
            self._group_by_fill = True
        if columns:
            self._group_by.extend(columns)
        return self

    def group_by_time(self, time, **kwargs):
        return self.group_by(time=time, **kwargs)

    def into(self, series):
        self._into_series = series
        return self

    def limit(self, n):
        self._limit = n
        return self

    def order(self, field, order):
        """Allows you to order by time ascending or descending.
        Time is the only way to order from InfluxDB itself.
        """
        if order.lower() not in self._order_identifiers:
            raise ValueError("order must either be 'asc' or 'desc'")
        self._order_by = [field]
        self._order = order.upper()
        return self

    def __str__(self):
        return self._format()

    def __unicode__(self):
        return six.u(self._format())


class ContinuousQuery(object):
    def __init__(self, name, database, query):
        self.name = name
        self.database = database
        self.query = query

    def _format(self):
        return six.u("""CREATE CONTINUOUS QUERY "{name}" ON {database} BEGIN {query} END""".format(
            name=self.name,
            database=self.database,
            query=str(self.query)).replace(';', ''))

    def __str__(self):
        return self._format()

    def __unicode__(self):
        return self._format()
