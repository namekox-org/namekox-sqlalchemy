#! -*- coding: utf-8 -*-

# author: forcemain@163.com


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from namekox_core.core.friendly import AsLazyProperty
from namekox_core.core.service.dependency import Dependency
from namekox_sqlalchemy.constants import DATABASE_CONFIG_KEY


class Database(Dependency):
    def __init__(self, dbname, dbbase, engine_options=None, session_options=None):
        self.engine = None
        self.session = None
        self.dbname = dbname
        self.dbbase = dbbase
        self.session_cls = None
        self.engine_options = engine_options or {}
        self.session_options = session_options or {}
        super(Database, self).__init__(dbbase, engine_options=engine_options, session_options=session_options)

    @AsLazyProperty
    def uris(self):
        return self.container.config.get(DATABASE_CONFIG_KEY, {})

    def setup(self):
        duri = self.uris[self.dbname].format(dbname=self.dbbase.__name__)
        self.engine = create_engine(duri, **self.engine_options)
        self.session_cls = sessionmaker(bind=self.engine, **self.session_options)

    def stop(self):
        self.engine and self.engine.dispose()
        del self.engine

    def get_instance(self, context):
        self.session = self.session_cls()
        return self.session

    def worker_teardown(self, context):
        self.session.close()
