# Install
```shell script
pip install -U namekox-sqlalchemy
```

# Warning
You may use any database [driver compatible with SQLAlchemy](http://docs.sqlalchemy.org/en/rel_0_9/dialects/index.html) provided it is safe to use with [eventlet](http://eventlet.net). This will include all pure This will inc-python drivers. Known safe drivers are:
* [pysqlite](http://docs.sqlalchemy.org/en/rel_0_9/dialects/sqlite.html#module-sqlalchemy.dialects.sqlite.pysqlite)
* [pymysql](http://docs.sqlalchemy.org/en/rel_0_9/dialects/mysql.html#module-sqlalchemy.dialects.mysql.pymysql)
* [pyodbc](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15)

# Example
```python
# ! -*- coding: utf-8 -*-
#
# author: forcemain@163.com


import random
import sqlalchemy as sa
import sqlalchemy_utils as su


from marshmallow import Schema, fields
from namekox_webserver.core.entrypoints.app import app
from namekox_timer.core.entrypoints.timer import timer
from sqlalchemy.ext.declarative import declarative_base
from namekox_sqlalchemy.core.dependencies import Database


Model = declarative_base(name='ping_service')


class Result(Model, su.Timestamp):
    __tablename__ = 'result'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    ip = sa.Column(su.IPAddressType, nullable=False)
    alive = sa.Column(sa.Boolean, nullable=False)


class ResultSchema(Schema):
    ip = fields.String(required=True)
    alive = fields.Boolean(required=True)
    created = fields.DateTime(required=True)


def generate_ip():
    return '.'.join([str(random.randint(1, 255)) for _ in range(4)])


class Ping(object):
    name = 'ping'
    db = Database(name, Model, engine_options={'pool_pre_ping': True})

    @app.api('/api/ping/<ip>/', methods=['GET'])
    def ping_res(self, request, ip=None):
        results = self.db.query(Result).filter(
            Result.ip == ip
        ).order_by(
            sa.desc(Result.created)
        )
        return ResultSchema(many=True).dump(results).data

    @timer(5)
    def ip_ping(self):
        result = Result(ip=generate_ip(), alive=random.choice([True, False]))
        self.db.add(result)
        self.db.commit()
```

# Migration
> config.yaml
```yaml
COMMAND:
  - namekox_sqlalchemy.cli.subcmd.migration:Alembic
DATABASE:
  ping: sqlite:///{dbname}.db
```
> namekox alembic --help
```shell script
usage: namekox alembic [-h] [-c CONFIG] [-n NAME] [-x X] [--raiseerr]
                       {branches,current,downgrade,edit,heads,history,init,list_templates,merge,revision,show,stamp,upgrade}
                       ...

manage sqlalchemy migration

positional arguments:
  {branches,current,downgrade,edit,heads,history,init,list_templates,merge,revision,show,stamp,upgrade}
    branches            Show current branch points.
    current             Display the current revision for a database.
    downgrade           Revert to a previous version.
    edit                Edit revision script(s) using $EDITOR.
    heads               Show current available heads in the script directory.
    history             List changeset scripts in chronological order.
    init                Initialize a new scripts directory.
    list_templates      List available templates.
    merge               Merge two revisions together. Creates a new migration
                        file.
    revision            Create a new revision file.
    show                Show the revision(s) denoted by the given symbol.
    stamp               'stamp' the revision table with the given revision;
                        don't run any migrations.
    upgrade             Upgrade to a later version.

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Alternate config file; defaults to value of
                        ALEMBIC_CONFIG environment variable, or "alembic.ini"
  -n NAME, --name NAME  Name of section in .ini file to use for Alembic config
  -x X                  Additional arguments consumed by custom env.py scripts
                        e.g. -x setting1=somesetting -x setting2=somesetting
  --raiseerr            Raise a full stack trace on error
```
> namekox alembic init alembic
- `sqlalchemy.url` required in alembic.ini
- `target_metadata` required in alembic/env.py
- `import sqlalchemy_utils` required in alembic/script.py.mako
```shell script
Creating directory namekox_service/alembic ...  done
Creating directory namekox_service/alembic/versions ...  done
Generating namekox_service/alembic/script.py.mako ...  done
Generating namekox_service/alembic/env.py ...  done
Generating namekox_service/alembic/env.pyc ...  done
Generating namekox_service/alembic/README ...  done
Generating namekox_service/alembic.ini ...  done
Please edit configuration/connection/logging settings in 'namekox_service/alembic.ini' before proceeding.
```
> namekox alembic revision --autogenerate -m "init"
```shell script
INFO  [alembic.runtime.migration] Context impl SQLiteImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
INFO  [alembic.autogenerate.compare] Detected added table 'result'
Generating namekox_service/alembic/versions/e2003d1ff397_init.py ...  done
```
> namekox alembic upgrade head
```shell script
INFO  [alembic.runtime.migration] Context impl SQLiteImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
INFO  [alembic.runtime.migration] Running upgrade  -> 2178367b7697, init
```

# Running
> namekox run ping
```shell script
2020-11-04 11:27:07,948 DEBUG load container class from namekox_core.core.service.container:ServiceContainer
2020-11-04 11:27:07,949 DEBUG starting services ['ping']
2020-11-04 11:27:07,949 DEBUG starting service ping entrypoints [ping:namekox_timer.core.entrypoints.timer.Timer:ip_ping, ping:namekox_webserver.core.entrypoints.app.server.WebServer:server, ping:namekox_webserver.core.entrypoints.app.handler.ApiServerHandler:ping_res]
2020-11-04 11:27:07,950 DEBUG spawn manage thread handle ping:namekox_timer.core.entrypoints.timer:_run(args=(), kwargs={}, tid=_run)
2020-11-04 11:27:07,951 DEBUG spawn manage thread handle ping:namekox_webserver.core.entrypoints.app.server:handle_connect(args=(), kwargs={}, tid=handle_connect)
2020-11-04 11:27:07,952 DEBUG service ping entrypoints [ping:namekox_timer.core.entrypoints.timer.Timer:ip_ping, ping:namekox_webserver.core.entrypoints.app.server.WebServer:server, ping:namekox_webserver.core.entrypoints.app.handler.ApiServerHandler:ping_res] started
2020-11-04 11:27:07,952 DEBUG starting service ping dependencies [ping:namekox_sqlalchemy.core.dependencies.Database:db]
2020-11-04 11:27:07,958 DEBUG service ping dependencies [ping:namekox_sqlalchemy.core.dependencies.Database:db] started
2020-11-04 11:27:07,959 DEBUG services ['ping'] started
2020-11-04 11:27:12,952 DEBUG spawn worker thread handle ping:ip_ping(args=(), kwargs={}, context=None)
2020-11-04 11:27:17,582 DEBUG spawn manage thread handle ping:namekox_webserver.core.entrypoints.app.server:handle_request(args=(<eventlet.greenio.base.GreenSocket object at 0x10eb28e10>, ('127.0.0.1', 55137)), kwargs={}, tid=handle_request)
2020-11-04 11:27:17,585 DEBUG spawn worker thread handle ping:ping_res(args=(<Request 'http://127.0.0.1/api/ping/229.157.217.181/' [GET]>,), kwargs={'ip': u'229.157.217.181'}, context={})
127.0.0.1 - - [04/Nov/2020 11:27:17] "GET /api/ping/229.157.217.181/ HTTP/1.1" 200 276 0.006447
2020-11-04 11:27:17,954 DEBUG spawn worker thread handle ping:ip_ping(args=(), kwargs={}, context=None)
2020-11-04 11:27:22,957 DEBUG spawn worker thread handle ping:ip_ping(args=(), kwargs={}, context=None)
^C2020-11-04 11:27:26,222 DEBUG stopping services ['ping']
2020-11-04 11:27:26,223 DEBUG stopping service ping entrypoints [ping:namekox_timer.core.entrypoints.timer.Timer:ip_ping, ping:namekox_webserver.core.entrypoints.app.server.WebServer:server, ping:namekox_webserver.core.entrypoints.app.handler.ApiServerHandler:ping_res]
2020-11-04 11:27:26,224 DEBUG wait service ping entrypoints [ping:namekox_timer.core.entrypoints.timer.Timer:ip_ping, ping:namekox_webserver.core.entrypoints.app.server.WebServer:server, ping:namekox_webserver.core.entrypoints.app.handler.ApiServerHandler:ping_res] stop
2020-11-04 11:27:26,224 DEBUG service ping entrypoints [ping:namekox_timer.core.entrypoints.timer.Timer:ip_ping, ping:namekox_webserver.core.entrypoints.app.server.WebServer:server, ping:namekox_webserver.core.entrypoints.app.handler.ApiServerHandler:ping_res] stopped
2020-11-04 11:27:26,225 DEBUG stopping service ping dependencies [ping:namekox_sqlalchemy.core.dependencies.Database:db]
2020-11-04 11:27:26,226 DEBUG service ping dependencies [ping:namekox_sqlalchemy.core.dependencies.Database:db] stopped
2020-11-04 11:27:26,230 DEBUG services ['ping'] stopped
2020-11-04 11:27:26,230 DEBUG killing services ['ping']
2020-11-04 11:27:26,230 DEBUG service ping already stopped
2020-11-04 11:27:26,231 DEBUG services ['ping'] killed
```
> curl http://127.0.0.1/api/ping/229.157.217.181/
```json
{
    "errs": "",
    "code": "Request:Success",
    "data": [
        {
            "ip": "229.157.217.181",
            "alive": false,
            "created": "2020-11-04T03:27:12.214048+00:00"
        }
    ],
    "call_id": "c1901120-cf4d-4e5b-a546-f8415abd166d"
}
```
