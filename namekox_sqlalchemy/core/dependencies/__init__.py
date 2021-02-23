#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals


from sqlalchemy import create_engine
from namekox_core.core.friendly import AsLazyProperty
from sqlalchemy.orm import sessionmaker, scoped_session
from namekox_core.core.service.dependency import Dependency
from namekox_sqlalchemy.constants import DATABASE_CONFIG_KEY


class Database(Dependency):
    def __init__(self, dbname, dbbase, session_wrapper=None, engine_options=None, session_options=None):
        self.engine = None
        self.dbname = dbname
        self.dbbase = dbbase
        self.session_map = {}
        self.session_cls = None
        self.session_wrapper = session_wrapper
        self.engine_options = engine_options or {}
        self.session_options = session_options or {}
        super(Database, self).__init__(dbbase, engine_options=engine_options, session_options=session_options)

    @AsLazyProperty
    def uris(self):
        return self.container.config.get(DATABASE_CONFIG_KEY, {})

    def setup(self):
        duri = self.uris[self.dbname].format(dbname=self.dbbase.__name__)
        self.engine = create_engine(duri, **self.engine_options)
        self.session_cls = scoped_session(sessionmaker(bind=self.engine, **self.session_options))
        self.session_cls = self.session_wrapper(self.session_cls) if self.session_wrapper else self.session_cls

    def stop(self):
        self.engine and self.engine.dispose()
        del self.engine

    def get_instance(self, context):
        self.session_map[context.call_id] = self.session_cls()
        return self.session_map[context.call_id]

    def worker_teardown(self, context):
        session = self.session_map.pop(context.call_id, None)
        session and session.close()
