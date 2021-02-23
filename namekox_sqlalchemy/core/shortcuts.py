#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals

import six


def select_or_create(session, model, defaults=None, **kwargs):
    defaults = defaults or {}
    instance = session.query(model).filter_by(**kwargs).first()
    defaults.update(kwargs)
    if not instance:
        instance = model(**defaults)
        session.add(instance)
        session.commit()
    return instance


def update_or_create(session, model, defaults=None, **kwargs):
    defaults = defaults or {}
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        [setattr(instance, k, v) for k, v in six.iteritems(defaults)]
    else:
        defaults.update(kwargs)
        instance = model(**defaults)
        session.add(instance)
    session.commit()
    return instance
