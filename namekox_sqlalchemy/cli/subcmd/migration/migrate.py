#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals

import os
import inspect


from alembic import command
from alembic.util import compat
from namekox_core.cli.subcmd.base import BaseCommand


from .config import Config, CommandLine


class Alembic(BaseCommand):
    """ manage sqlalchemy migration """
    @classmethod
    def name(cls):
        return 'alembic'

    @classmethod
    def add_options(cls, fn, sub_parser, positional, positional_translations, kwargs):
        kwargs_opts = {
            'template': (
                '-t', '--template',
                dict(
                    default='generic',
                    type=str,
                    help='Setup template for use with "init"'
                )),
            'message': (
                '-m', '--message',
                dict(
                    type=str,
                    help='Message string to use with "revision"'
                )),
            'sql': (
                '--sql',
                dict(
                    action='store_true',
                    help='Dont emit SQL to database - dump to standard output/file instead. See docs on offline mode.'
                )),
            'tag': ('--tag',
                    dict(
                        type=str,
                        help='Arbitrary "tag" name - can be used by custom env.py scripts.',
                    )),
            'head': (
                '--head',
                dict(
                    type=str,
                    help='Specify head revision or <branchname>@head to base new revision on.',
                )),
            'splice': (
                '--splice',
                dict(
                    action='store_true',
                    help='Allow a non-head revision as the head to splice onto',
                )),
            'depends_on': (
                '--depends-on',
                dict(
                    action='append',
                    help='Specify one or more revision identifiers which this revision should depend on.',
                )),
            'rev_id': (
                '--rev-id',
                dict(
                    type=str,
                    help='Specify a hardcoded revision id instead of generating one',
                )),
            'version_path': (
                '--version-path',
                dict(
                    type=str,
                    help='Specify specific path from config for version file',
                )),
            'branch_label': (
                '--branch-label',
                dict(
                    type=str,
                    help='Specify a branch label to apply to the new revision',
                )),
            'verbose': (
                '-v', '--verbose',
                dict(
                    action='store_true',
                    help='Use more verbose output'
                )),
            'resolve_dependencies': (
                '--resolve-dependencies',
                dict(
                    action='store_true',
                    help='Treat dependency versions as down revisions',
                )),
            'autogenerate': (
                '--autogenerate',
                dict(
                    action='store_true',
                    help='Populate revision script with candidate migration operations, based on comparison of database to model.',
                )),
            'head_only': (
                '--head-only',
                dict(
                    action='store_true',
                    help='Deprecated.  Use --verbose for additional output',
                )),
            'rev_range': (
                '-r', '--rev-range',
                dict(
                    action='store',
                    help='Specify a revision range; format is [start]:[end]',
                )),
            'indicate_current': (
                '-i', '--indicate-current',
                dict(
                    action='store_true',
                    help='Indicate the current revision',
                )),
            'purge': (
                '--purge',
                dict(
                    action='store_true',
                    help='Unconditionally erase the version table before stamping',
                )),
            'package': (
                '--package',
                dict(
                    action='store_true',
                    help='Write empty __init__.py files to the environment and version locations',
                )),
        }
        positional_help = {
            'directory': 'location of scripts directory',
            'revision': 'revision identifier',
            'revisions': 'one or more revisions, or "heads" for all heads',
        }
        for arg in kwargs:
            if arg in kwargs_opts:
                args = kwargs_opts[arg]
                args, kw = args[0:-1], args[-1]
                sub_parser.add_argument(*args, **kw)
        for arg in positional:
            if arg == 'revisions' or fn in positional_translations and positional_translations[fn][arg] == 'revisions':
                sub_parser.add_argument('revisions', nargs='+', help=positional_help.get('revisions'))
            else:
                sub_parser.add_argument(arg, help=positional_help.get(arg))

    @classmethod
    def init_parser(cls, parser, config=None):
        parser.add_argument('-c', '--config', type=str,
                            default=os.environ.get('ALEMBIC_CONFIG', 'alembic.ini'),
                            help='Alternate config file; defaults to value of '
                                 'ALEMBIC_CONFIG environment variable, or "alembic.ini"')
        parser.add_argument('-n', '--name', type=str, default='alembic',
                            help='Name of section in .ini file to ' 'use for Alembic config')
        parser.add_argument('-x', action='append',
                            help='Additional arguments consumed by custom env.py scripts '
                                 'e.g. -x setting1=somesetting -x setting2=somesetting')
        parser.add_argument('--raiseerr', action='store_true',
                            help='Raise a full stack trace on error')
        positional_translations = {command.stamp: {'revision': 'revisions'}}
        sub_parsers = parser.add_subparsers()
        for fn in [getattr(command, n) for n in dir(command)]:
            if (
                inspect.isfunction(fn)
                and fn.__name__[0] != '_'
                and fn.__module__ == 'alembic.command'
            ):
                spec = compat.inspect_getargspec(fn)
                if spec[3]:
                    positional = spec[0][1: -len(spec[3])]
                    kwarg = spec[0][-len(spec[3]):]
                else:
                    positional = spec[0][1:]
                    kwarg = []
                if fn in positional_translations:
                    positional = [
                        positional_translations[fn].get(name, name)
                        for name in positional
                    ]
                # parse first line(s) of helptext without a line break
                help_ = fn.__doc__
                if help_:
                    help_text = []
                    for line in help_.split('\n'):
                        if not line.strip():
                            break
                        else:
                            help_text.append(line.strip())
                else:
                    help_text = ''
                sub_parser = sub_parsers.add_parser(
                    fn.__name__, help=' '.join(help_text)
                )
                cls.add_options(fn, sub_parser, positional, positional_translations, kwarg)
                sub_parser.set_defaults(cmd=(fn, positional, kwarg))
        return parser

    @classmethod
    def main(cls, args, config=None):
        if not hasattr(args, 'cmd'):
            cls.parser.error('too few arguments')
        cfg = Config(
            file_=args.config,
            ini_section=args.name,
            cmd_opts=args,
        )
        CommandLine().run_cmd(cfg, args)
