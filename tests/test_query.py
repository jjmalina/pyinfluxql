# -*- coding: utf-8 -*-
"""
    test_query
    ~~~~~~~~~~

    Tests the query generator
"""

import six
import pytest
from datetime import datetime, timedelta
import dateutil
from pyinfluxql.functions import Sum, Min, Max, Count, Distinct, Percentile
from pyinfluxql.query import Query, ContinuousQuery


@pytest.mark.unit
def test_clone():
    """Cloning a query instance should return a new query instance with the
    same data but different references
    """
    query = Query(Count(Distinct('col'))).from_('measurement')
    query._limit = 100
    query._group_by_time = timedelta(hours=24)
    query._group_by.append('col2')

    new_query = query.clone()
    assert new_query._measurement == query._measurement
    assert len(new_query._select_expressions) == len(query._select_expressions)
    assert new_query._select_expressions != query._select_expressions
    assert new_query._limit == query._limit
    assert new_query._group_by_time == query._group_by_time
    assert new_query._group_by == query._group_by

    new_query._select_expressions.append(Count('blah'))
    new_query._limit = 10
    new_query._group_by_time = timedelta(days=7)
    new_query._group_by.append('col3')

    assert len(new_query._select_expressions) != len(query._select_expressions)
    assert len(new_query._select_expressions) == 2
    assert len(query._select_expressions) == 1
    assert new_query._limit != query._limit
    assert new_query._limit == 10
    assert query._limit == 100
    assert new_query._group_by_time != query._group_by_time
    assert new_query._group_by_time == timedelta(days=7)
    assert query._group_by_time == timedelta(hours=24)
    assert new_query._group_by != query._group_by
    assert new_query._group_by == ['col2', 'col3']
    assert query._group_by == ['col2']


@pytest.mark.unit
def test_select():
    """Selecting should be chainable and add to the `_select_expressions`
    list.
    """
    q = Query('colname')
    query = q.from_('test_measurement')
    assert isinstance(query, Query)
    assert len(query._select_expressions) == 1
    query.select('colname2').select('colname3')
    assert isinstance(query, Query)
    assert len(query._select_expressions) == 3
    query.select('colname4', 'colname5')
    assert len(query._select_expressions) == 5


@pytest.mark.unit
def test_format_select():
    q = Query().from_('test_measurement')
    q._select_expressions = ['hello']
    assert q._format_select() == 'SELECT hello'
    q._select_expressions = ['hello', 'goodbye']
    assert q._format_select() == 'SELECT hello, goodbye'
    q = Query().from_('test_measurement')
    q._select_expressions = [Sum('hello')]
    assert q._format_select() == 'SELECT SUM(hello)'
    q._select_expressions = [Sum('hello'), Min('bye')]
    assert q._format_select() == 'SELECT SUM(hello), MIN(bye)'
    q = Query().from_('1').select(Max(Min('hello')))
    assert q._format_select() == 'SELECT MAX(MIN(hello))'


@pytest.mark.unit
def test_format_select_expressions():
    """_format_select_expressions should take multiple arguments and
    format functions correctly
    """
    q = Query()
    assert q._format_select_expressions('1 + 1') == '1 + 1'
    assert q._format_select_expressions('1 + 1', 'BLAH') == '1 + 1, BLAH'
    assert q._format_select_expressions('1 + 1', 'BLAH', '2') == \
        '1 + 1, BLAH, 2'
    assert q._format_select_expressions(*[Distinct('a'), 'BLAH', '2']) == \
        'DISTINCT(a), BLAH, 2'


@pytest.mark.unit
def test_format_select_rexpression():
    """_format_select_expression should take one argument and if a function
    format it correctly
    """
    q = Query()
    assert q._format_select_expression('a') == 'a'
    assert q._format_select_expression(Sum('a')) == 'SUM(a)'
    assert q._format_select_expression(Sum(Max('a'))) == 'SUM(MAX(a))'


@pytest.mark.unit
def test_format_measurement():
    q = Query().from_('test_measurement')
    assert q._format_measurement('test_measurement') == 'test_measurement'
    assert q._format_measurement('test series') == '"test series"'
    assert q._format_measurement('test-series') == '"test-series"'
    assert q._format_measurement('/test series*/') == '/test series*/'
    assert q._format_measurement('/test-series*/') == '/test-series*/'


@pytest.mark.unit
def test_format_from():
    """_format_from should format correctly
    """
    assert Query().from_('test_measurement')._format_from() == 'FROM test_measurement'
    assert Query().from_('test series')._format_from() == 'FROM "test series"'

    assert Query().from_('a_series')._format_from() == 'FROM a_series'
    assert Query().from_('a series')._format_from() == 'FROM "a series"'


@pytest.mark.unit
def test_where():
    """where should insert into the _where dict and be chainable
    """
    q = Query('test_measurement').where(a=1, b=3, c__gt=3)
    assert q._where['a'] == 1
    assert q._where['b'] == 3
    assert q._where['c__gt'] == 3
    assert isinstance(q, Query)


@pytest.mark.unit
def test_format_value():
    """_format_value should format strings, ints, floats, bools and
    datetimes correctly
    """
    q = Query('test_measurement')
    assert q._format_value('hello') == "'hello'"
    assert q._format_value(1) == "1"
    assert q._format_value(1.0) == "1.0"
    assert q._format_value(True) == "true"
    assert q._format_value(False) == "false"
    assert q._format_value('/stats.*/') == "/stats.*/"
    assert q._format_value(datetime(2014, 2, 10, 18, 4, 53, 834825)) == \
        "'2014-02-10 18:04:53.834'"
    assert q._format_value(
        datetime(2014, 2, 10, 18, 4, 53, 834825)
            .replace(tzinfo=dateutil.tz.gettz('US/Eastern'))) == \
        "'2014-02-10 23:04:53.834'"


@pytest.mark.unit
def test_date_range():
    q = Query()
    start = datetime.utcnow() - timedelta(hours=1)
    end = datetime.utcnow() - timedelta(minutes=1)
    q.date_range(start)
    assert q._where['time__gt'] == start

    q = Query()
    q.date_range(start, end)
    assert q._where['time__gt'] == start
    assert q._where['time__lt'] == end

    q = Query()
    q.date_range(start=start, end=end)
    assert q._where['time__gt'] == start
    assert q._where['time__lt'] == end

    q = Query()
    q.date_range(start=10, end=100)
    assert q._where['time__gt'] == 10
    assert q._where['time__lt'] == 100

    with pytest.raises(ValueError):
        Query().date_range(end, start)
    with pytest.raises(ValueError):
        Query().date_range()


@pytest.mark.unit
def test_format_where():
    """_format_where should format an entire where clause correctly
    """
    q = Query().where(foo=4)
    assert q._format_where() == 'WHERE foo = 4'

    q = Query().where(foo__bar=4)
    assert q._format_where() == 'WHERE foo.bar = 4'

    q = Query().where(foo__bar__lt=4)
    assert q._format_where() == 'WHERE foo.bar < 4'

    q = Query().where(foo__bar__baz__lt=4)
    assert q._format_where() == 'WHERE foo.bar.baz < 4'

    query = Query().where(
        col1='a',
        col2__ne='b',
        col3__lt=5,
        col4__gt=7.0)
    assert query._format_where() == \
        "WHERE col1 = 'a' AND col2 != 'b' AND col3 < 5 AND col4 > 7.0"


@pytest.mark.unit
def test_format_where_eq():
    """equals expressions should be formatted correctly in a where clause
    """
    q = Query()
    assert q._format_where_expression(['col'], 'eq', 'hi') == "col = 'hi'"


@pytest.mark.unit
def test_format_where_ne():
    """not equals expressions should be formatted correctly
    in a where clause
    """
    q = Query()
    assert q._format_where_expression(['col'], 'ne', False) == "col != false"
    assert q._format_where_expression(['col'], 'ne', True) == "col != true"


@pytest.mark.unit
def test_format_where_lt():
    """less than expressions should be formatted correctly
    in a where clause
    """
    q = Query()
    assert q._format_where_expression(['col'], 'lt', 1.0) == "col < 1.0"
    assert q._format_where_expression(['col'], 'lt', 50) == "col < 50"


@pytest.mark.unit
def test_format_where_gt():
    """greater than expressions should be formatted correctly
    in a where clause
    """
    q = Query()
    assert q._format_where_expression(['col'], 'gt', 1.0) == "col > 1.0"
    assert q._format_where_expression(['col'], 'gt', 50) == "col > 50"


@pytest.mark.unit
def test_group_by():
    """group_by should correctly set the query's group by arguments and
    be chainable
    """
    td = timedelta(hours=1)
    q = Query().group_by('col1', 'col2', time=td)
    assert isinstance(q, Query)
    assert q._group_by_time == td
    assert q._group_by == ['col1', 'col2']
    q = Query().group_by(time=td, fill=True)
    assert q._group_by_time == td
    assert q._group_by_fill
    q = Query().group_by(time=td, fill=False)
    assert not q._group_by_fill


@pytest.mark.unit
def test_group_by_time():
    td = timedelta(hours=1)
    q = Query().group_by_time(td)
    assert q._group_by_time == td
    td = timedelta(hours=2)
    q.group_by_time(td)
    assert q._group_by_time == td
    q.group_by_time('1h')
    assert q._group_by_time == '1h'


@pytest.mark.unit
def test_format_group_by():
    """_format_group_by should correctly format one or more
    group by statements
    """
    q = Query().group_by('col1')
    assert q._format_group_by() == 'GROUP BY col1'
    q.group_by('col2')
    assert q._format_group_by() == 'GROUP BY col1, col2'
    q.group_by(time=timedelta(days=1))
    assert q._format_group_by() == 'GROUP BY time(1d), col1, col2'
    q = Query().group_by(time=timedelta(hours=5))
    assert q._format_group_by() == 'GROUP BY time(5h)'
    q = Query().group_by(time=timedelta(hours=5), fill=True)
    assert q._format_group_by() == 'GROUP BY time(5h) fill(0)'
    q = Query().group_by(time=timedelta(hours=5), fill=False)
    assert q._format_group_by() == 'GROUP BY time(5h)'
    q = Query().group_by(time='1h', fill=False)
    assert q._format_group_by() == 'GROUP BY time(1h)'
    q = Query().group_by_time('1h', fill=True)
    assert q._format_group_by() == 'GROUP BY time(1h) fill(0)'


@pytest.mark.unit
def test_limit():
    """limit should set the query's limit argument and be chainable
    """
    q = Query().limit(1000)
    assert isinstance(q, Query)
    assert q._limit == 1000


@pytest.mark.unit
def test_format_limit():
    """_format_lmit should correctly format the limit clause
    """
    q = Query().limit(1000)
    assert q._format_limit() == 'LIMIT 1000'


@pytest.mark.unit
def test_order():
    q = Query().order('time', 'asc')
    assert q._order == 'ASC'
    q = Query().order('time', 'ASC')
    assert q._order == 'ASC'
    q = Query().order('time', 'desc')
    assert q._order == 'DESC'
    q = Query().order('time', 'DESC')
    assert q._order == 'DESC'
    with pytest.raises(TypeError):
        Query().order('-time')


@pytest.mark.unit
def test_format_order():
    """_format_order should correctly format the order clause
    """
    q = Query().order('time', 'asc')
    assert q._format_order() == 'ORDER BY time ASC'
    q.order('time', 'desc')
    assert q._format_order() == 'ORDER BY time DESC'
    q = Query().order('time', 'ASC')
    assert q._format_order() == 'ORDER BY time ASC'
    q.order('time', 'DESC')
    assert q._format_order() == 'ORDER BY time DESC'


@pytest.mark.unit
def test_into():
    q = Query().into('another_series')
    assert q._into_series == 'another_series'


@pytest.mark.unit
def test_format_into():
    q = Query().into('another_series')
    assert q._format_into() == 'INTO another_series'
    q = Query()
    assert q._format_into() == ''


@pytest.mark.unit
def test_format_query():
    q = Query().from_('x')
    expected = "SELECT * FROM x;"
    assert q._format_query("SELECT   *   FROM    x    ") == expected
    expected = 'DELETE FROM x;'
    assert q._format_query('DELETE     FROM     x   ') == expected


@pytest.mark.unit
def test_format_select_query():
    """_format should correctly format the entire query
    """
    # Test simple selects
    assert Query('*').from_('x')._format_select_query() == \
        "SELECT * FROM x;"

    assert Query('a', 'b').from_('x')._format_select_query() == \
        "SELECT a, b FROM x;"

    # Test limit
    assert Query('*').from_('x').limit(100) \
        ._format_select_query() == "SELECT * FROM x LIMIT 100;"

    # Test order
    assert Query('*').from_('x').order('time', 'asc') \
        ._format_select_query() == "SELECT * FROM x ORDER BY time ASC;"

    assert Query('*').from_('x').order('time', 'desc') \
        ._format_select_query() == "SELECT * FROM x ORDER BY time DESC;"

    # Test functions
    assert Query(Count('a')).from_('x') \
        ._format_select_query() == "SELECT COUNT(a) FROM x;"

    assert Query(Sum(Count('a'))).from_('x') \
        ._format_select_query() == "SELECT SUM(COUNT(a)) FROM x;"

    # Test where, comparators and value formatting
    assert Query('*').from_('x').where(a='something') \
        ._format_select_query() == "SELECT * FROM x WHERE a = 'something';"

    assert Query('*').from_('x').where(a='something', b=1) \
        ._format_select_query() == \
        "SELECT * FROM x WHERE a = 'something' AND b = 1;"

    assert Query('*').from_('x').where(a__ne='something') \
        ._format_select_query() == "SELECT * FROM x WHERE a != 'something';"

    assert Query('*').from_('x').where(a=True, b=False) \
        ._format_select_query() == \
        "SELECT * FROM x WHERE a = true AND b = false;"

    assert Query('*').from_('x').where(a=True, b=False) \
        ._format_select_query() == \
        "SELECT * FROM x WHERE a = true AND b = false;"

    assert Query('*').from_('x').where(a__lt=4, b__gt=6.0) \
        ._format_select_query() == "SELECT * FROM x WHERE a < 4 AND b > 6.0;"

    # Test group by
    assert Query('*').from_('x').group_by('a') \
        ._format_select_query() == "SELECT * FROM x GROUP BY a;"

    assert Query('*').from_('x').group_by('a', 'b') \
        ._format_select_query() == "SELECT * FROM x GROUP BY a, b;"

    q = Query('*').from_('x') \
        .group_by(time=timedelta(hours=1))
    assert q._format_select_query() == "SELECT * FROM x GROUP BY time(1h);"

    q = Query('*').from_('x') \
        .group_by('a', 'b', time=timedelta(hours=1))
    assert q._format_select_query() == "SELECT * FROM x GROUP BY time(1h), a, b;"

    # Test something really crazy
    fmt = "SELECT COUNT(a), SUM(b), PERCENTILE(d, 99) FROM x "
    fmt += "WHERE e = false AND f != true AND g < 4 AND h > 5 "
    fmt += "GROUP BY time(1h), a, b fill(0) "
    fmt += "LIMIT 100 ORDER BY time ASC;"

    q = Query(Count('a'), Sum('b'), Percentile('d', 99)) \
        .from_('x') \
        .where(e=False, f__ne=True, g__lt=4, h__gt=5) \
        .group_by('a', 'b', time=timedelta(minutes=60), fill=True) \
        .limit(100).order('time', 'asc')

    assert q._format_select_query() == fmt


@pytest.mark.unit
def test_format_delete_query():
    q = Query().from_('series')
    q._is_delete = True
    assert q._format_delete_query() == 'DELETE FROM series;'

    q.date_range(start=20, end=40)
    expected = 'DELETE FROM series WHERE time > 20 AND time < 40;'
    assert q._format_delete_query() == expected

    q = Query().from_('series')
    q.date_range(end=40)
    expected = 'DELETE FROM series WHERE time < 40;'
    assert q._format_delete_query() == expected


@pytest.mark.unit
def test_format():
    q = Query('blah').from_('series')
    q._is_delete = True
    assert q._format() == 'DELETE FROM series;'

    q.date_range(start=20, end=40)
    q._is_delete = False
    expected = 'SELECT blah FROM series WHERE time > 20 AND time < 40;'
    assert q._format() == expected


@pytest.mark.unit
def test_str():
    q = Query('blah').from_('series')
    q._is_delete = True
    assert str(q) == 'DELETE FROM series;'

    q.date_range(start=20, end=40)
    q._is_delete = False
    expected = 'SELECT blah FROM series WHERE time > 20 AND time < 40;'
    assert str(q) == expected


@pytest.mark.unit
def test_unicode():
    q = Query('blah').from_('series')
    q._is_delete = True
    assert six.u(str(q)) == u'DELETE FROM series;'

    q.date_range(start=20, end=40)
    q._is_delete = False
    expected = u'SELECT blah FROM series WHERE time > 20 AND time < 40;'
    assert six.u(str(q)) == expected


@pytest.mark.unit
def test_format_continuous_query():
    q = Query(Count('col')).from_('clicks') \
        .group_by(time=timedelta(hours=1)).into('clicks.count.1h')
    cq = ContinuousQuery("1h_clicks_count", "test", q)
    expected = 'CREATE CONTINUOUS QUERY "1h_clicks_count" ON test BEGIN SELECT COUNT(col) FROM clicks GROUP BY time(1h) INTO clicks.count.1h END'
    assert cq._format() == expected
