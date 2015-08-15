# -*- coding: utf-8 -*-
"""
    test_functions
    ~~~~~~~~~~~~~~


"""

import pytest

from pyinfluxql.functions import (Expression, Func, Sum, Min, Max, Mean, Count,
                                  Median, Derivative, Distinct, Stddev, First,
                                  Last, Percentile)


@pytest.mark.unit
def test_expression():
    assert Expression('hi')
    assert Expression('hi').format() == 'hi'
    assert Expression('hi').as_('x')
    assert Expression('hi').as_('x').format() == 'hi AS x'


@pytest.mark.unit
def test_func():
    assert Func(1)._args == (1,)


@pytest.mark.unit
def test_validate_arg_length_fails():
    with pytest.raises(ValueError):
        args = [1, 2, 3]
        func = Func('a')
        func.validate_arg_length(args, 4)


@pytest.mark.unit
def test_validate_args():
    Func('a').validate_args(1)


@pytest.mark.unit
def test_validate_args_fails():
    with pytest.raises(ValueError):
        Func('a').validate_args(1, 2)


@pytest.mark.unit
def test_format(monkeypatch):
    monkeypatch.setattr(Func, 'identifier', 'TEST')
    assert Func(1).format() == 'TEST(1)'
    assert Func('hi').format() == 'TEST(hi)'
    assert Func(Func('hi')).format() == 'TEST(TEST(hi))'
    assert Func(Func('hi')).as_('x').format() == 'TEST(TEST(hi)) AS x'


@pytest.mark.unit
def test_count():
    assert Count('col').format() == 'COUNT(col)'


@pytest.mark.unit
def test_sum():
    assert Sum('col').format() == 'SUM(col)'


@pytest.mark.unit
def test_min():
    assert Min('col').format() == 'MIN(col)'


@pytest.mark.unit
def test_max():
    assert Max('col').format() == 'MAX(col)'


@pytest.mark.unit
def test_mean():
    assert Mean('col').format() == 'MEAN(col)'


@pytest.mark.unit
def test_median():
    assert Median('col').format() == 'MEDIAN(col)'


@pytest.mark.unit
def test_derivative():
    assert Derivative('col').format() == 'DERIVATIVE(col)'


@pytest.mark.unit
def test_distinct():
    assert Distinct('col').format() == 'DISTINCT(col)'


@pytest.mark.unit
def test_stddev():
    assert Stddev('col').format() == 'STDDEV(col)'


@pytest.mark.unit
def test_first():
    assert First('col').format() == 'FIRST(col)'


@pytest.mark.unit
def test_last():
    assert Last('col').format() == 'LAST(col)'


@pytest.mark.unit
def test_percentile():
    assert Percentile('col', 99).format() == 'PERCENTILE(col, 99)'
    assert Percentile('col', 99.0).format() == 'PERCENTILE(col, 99.0)'


@pytest.mark.unit
def test_percentile_fails_missing_args():
    with pytest.raises(ValueError):
        Percentile('col')


@pytest.mark.unit
def test_percentile_fails_invalid_args():
    with pytest.raises(TypeError):
        Percentile('col', 's')


@pytest.mark.unit
def test_percentile_fails_extra_args():
    with pytest.raises(ValueError):
        Percentile('col', 's', 1)


@pytest.mark.unit
def test_percentile_fails_lower_range():
    with pytest.raises(ValueError):
        Percentile('col', 0)


@pytest.mark.unit
def test_percentile_fails_upper_range():
    with pytest.raises(ValueError):
        Percentile('col', 100)


@pytest.mark.unit
def test_composed_functions_format():
    percentile = Percentile('a', 99)
    last = Last(percentile)
    first = First(last)
    stddev = Stddev(first)
    distinct = Distinct(stddev)
    derivative = Derivative(distinct)
    median = Median(derivative)
    mean = Mean(median)
    max_ = Max(mean)
    min_ = Min(max_)
    count = Count(min_)
    composed = Sum(count)
    assert composed.format() == 'SUM(COUNT(MIN(MAX(MEAN(MEDIAN(DERIVATIVE(DISTINCT(STDDEV(FIRST(LAST(PERCENTILE(a, 99))))))))))))'
