# PyInfluxQL

A query generator for the [InfluxDB SQL query syntax](https://influxdb.com/docs/v0.9/query_language/query_syntax.html). Like SQLAlchemy but for InfluxDB. Consider this an experimental WIP. 

# Example

```
from influxdb import InfluxDBClient
from pyinfluxql import Engine, Query, Mean

client = InfluxDBClient('localhost', 8086, 'root', 'root', 'example')
engine = Engine(client)
query = Query(Mean('value')).from_('cpu_load') \
    .where(time__gt=datetime.now() - timedelta(1))
    .group_by(time=timedelta(hours=1))
engine.execute(query)
```

# TODO

- [] integration tests against an InfluxDB server
- [] travis
- [] tox to test python versions
- [] support for select expression aliases
- [] support for create statements
- [] support for show statements
- [] support for drop statements
- [] support for grant/revoke statements
- [] support for alter statements
