# -*- coding: utf-8 -*-
"""
    test.confest
    ~~~~~~~~~~~~

    Fixtures for pyinfluxql tests
"""

import pytest
from influxdb import InfluxDBClient

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
