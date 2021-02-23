#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals

import inspect
import sqlalchemy as sa


from sqlalchemy.sql.elements import BooleanClauseList


from .. import exceptions


class DynamicCondition(object):
    """ Dynamic generate orm condition from dict
    doc:
    https://docs.sqlalchemy.org/en/13/core/sqlelement.html?highlight=columnoperators#sqlalchemy.sql.operators.ColumnOperators
    """
    def __init__(self, models, default_model):
        self.models = models
        self.default_model = default_model

    @staticmethod
    def re_get_attribute(obj, dotpath=''):
        if not dotpath:
            return obj
        for attr in dotpath.split('.'):
            obj = getattr(obj, attr)
        return obj

    def to_orm_condition(self, condition):
        orm_condition = sa.and_()
        if isinstance(condition, dict):
            condition_data = []
            default_model_name = self.default_model.__name__
            for sk in condition:
                md = getattr(self.models, sk.split('.')[0], None)
                if md and inspect.isclass(md):
                    dk = sk
                else:
                    dk = '{0}.{1}'.format(default_model_name, sk)
                dk = self.re_get_attribute(self.models, dk)
                if not callable(dk):
                    dv = (dk == condition[sk])
                else:
                    dv = dk(*condition[sk] if isinstance(condition[sk], (tuple, list)) else condition[sk])
                condition_data.append(dv)
            orm_condition = sa.and_(*condition_data)
        elif isinstance(condition, BooleanClauseList):
            orm_condition = condition
        elif isinstance(condition, (list, tuple)):
            orm_condition = self.as_orm_condition(condition)
        return orm_condition

    def as_orm_condition(self, condition):
        if isinstance(condition, dict):
            return self.to_orm_condition(condition)
        relation_map = {'or': sa.or_, 'and': sa.and_}
        if not isinstance(condition, list) or len(condition) != 3:
            errs = 'required [a, and, b] or [a or b]'
            raise exceptions.BadCondition(errs)
        relation_key = condition[1]
        if relation_key not in relation_map:
            errs = 'relation only supported and / or'
            raise exceptions.BadCondition(errs)
        condition_a = condition[0]
        condition_b = condition[2]
        relation = relation_map[relation_key]
        condition_a = self.to_orm_condition(condition_a)
        condition_b = self.to_orm_condition(condition_b)
        return relation(condition_a, condition_b)
