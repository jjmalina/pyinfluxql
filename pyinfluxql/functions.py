# -*- coding: utf-8 -*-
"""
    pyinfluxql.functions
    ~~~~~~~~~~~~~~~~~~~~

    InfluxDB functions
    https://influxdb.com/docs/v0.9/query_language/functions.html
"""


class Expression(object):
    def __init__(self, expression):
        self._expression = expression
        self._as = None

    def as_(self, alias):
        self._as = alias
        return self

    def _format_as(self):
        return "%s" % " AS %s" % (self._as) if self._as else ''

    def format(self):
        if isinstance(self._expression, Expression):
            formatted = "(%s)" % self._expression.format()
        formatted = "%s" % self._expression
        return "%s%s" % (formatted, self._format_as())


class Func(Expression):
    """Base class for an InfluxDB function
    """
    identifier = None
    _valid_arg_types = {str}

    def __init__(self, *args):
        super(Func, self).__init__(None)
        self.validate_args(*args)
        self._args = args

    def validate_arg_length(self, args, length):
        if len(args) != length:
            raise ValueError(u"Function %s takes %i arguments" % (
                self.identifier, length))

    def validate_args(self, *args):
        self.validate_arg_length(args, 1)

    def format(self):
        formatted_args = []
        for arg in self._args:
            if issubclass(type(arg), Func):
                formatted_args.append(arg.format())
            elif type(arg) in self._valid_arg_types:
                formatted_args.append(arg)
            else:
                formatted_args.append(u"%r" % arg)
        return u"%s(%s)%s" % (self.identifier, ", ".join(formatted_args), self._format_as())


class Count(Func):
    identifier = 'COUNT'


class Min(Func):
    identifier = 'MIN'


class Max(Func):
    identifier = 'MAX'


class Mean(Func):
    identifier = 'MEAN'


class Median(Func):
    identifier = 'MEDIAN'


class Distinct(Func):
    identifier = 'DISTINCT'


class Percentile(Func):
    identifier = 'PERCENTILE'
    _valid_first_arg_types = {int, float}

    def validate_args(self, *args):
        self.validate_arg_length(args, 2)
        if type(args[1]) not in self._valid_first_arg_types:
            raise TypeError(
                "Second argument to %s must be int or float" % self.identifier)
        if args[1] <= 0 or args[1] >= 100.0:
            raise ValueError(
                "Second argument to %s must be between 0 and 100" % self.identifier)


class Derivative(Func):
    identifier = 'DERIVATIVE'


class Sum(Func):
    identifier = 'SUM'


class Stddev(Func):
    identifier = 'STDDEV'


class First(Func):
    identifier = 'FIRST'


class Last(Func):
    identifier = 'LAST'
