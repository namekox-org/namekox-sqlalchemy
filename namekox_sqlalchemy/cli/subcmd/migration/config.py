#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals

import os


from alembic.config import CommandLine
from alembic.util import memoized_property
from alembic.config import Config as BaseConfig
from namekox_core.core.parsers.patterns import ENV_VAR_MATCHER
from namekox_core.core.parsers.base import recursive_replace_env_var
from alembic.util.compat import SafeConfigParser as BaseSafeConfigParser


class SafeConfigParser(BaseSafeConfigParser):
    @staticmethod
    def _recursive_replace_env_var(val):
        return ENV_VAR_MATCHER.sub(recursive_replace_env_var, val)

    def _read(self, fp, fpname):
        BaseSafeConfigParser._read(self, fp, fpname)
        all_sections = [self._defaults]
        all_sections.extend(self._sections.values())
        for options in all_sections:
            for name, val in options.items():
                options[name] = self._recursive_replace_env_var(val)


class Config(BaseConfig):
    @memoized_property
    def file_config(self):
        if self.config_file_name:
            here = os.path.abspath(os.path.dirname(self.config_file_name))
        else:
            here = ''
        self.config_args['here'] = here
        file_config = SafeConfigParser(self.config_args)
        if self.config_file_name:
            file_config.read([self.config_file_name])
        else:
            file_config.add_section(self.config_ini_section)
        return file_config
