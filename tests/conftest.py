# -*- coding: utf-8 -*-
"""
    test.confest
    ~~~~~~~~~~~~

    Fixtures for pyinfluxql tests
"""

import pytest
from influxdb import InfluxDBClient
from datetime import datetime, timedelta
import random
from pyinfluxql import Engine

influxdb_settings = {
    'INFLUXDB_HOST': 'localhost',
    'INFLUXDB_PORT': 8086,
    'INFLUXDB_USER': 'root',
    'INFLUXDB_PASSWORD': 'root',
    'INFLUXDB_DB': 'pyinfluxsql_test'
}


@pytest.yield_fixture(scope='module')
def influx_db():
    _influxdb = InfluxDBClient(
        influxdb_settings['INFLUXDB_HOST'],
        influxdb_settings['INFLUXDB_PORT'],
        influxdb_settings['INFLUXDB_USER'],
        influxdb_settings['INFLUXDB_PASSWORD'],
        influxdb_settings['INFLUXDB_DB'])

    db_name = influxdb_settings['INFLUXDB_DB']

    def _del():
        _influxdb.drop_database(db_name)

    try:
        _del()
    except:
        pass

    _influxdb.create_database(db_name)
    yield _influxdb

    _del()


@pytest.fixture(scope='module')
def date_range():
    start = datetime(2015, 6, 6)
    return start, start + timedelta(hours=250)


@pytest.fixture(scope='module')
def engine(influx_db, date_range):
    start = date_range[0]
    series = []
    random.seed(5)
    for i in range(250):
        time = start + timedelta(hours=i)
        series.append({
            "measurement": "deliciousness",
            "tags": {
                "dish": "pie" if i % 2 == 0 else "pizza"
            },
            "time": time,
            "fields": {
                "value": int(random.random() * 5)
            }
        })

    influx_db.write_points(series)
    return Engine(influx_db)
