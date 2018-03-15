import argparse

from .__details__ import *

from comoda import a_logger, LOG_LEVELS
from importlib import import_module

SUBMOD_NAMES = [
    "check_rundirs",
    "delivery",
    "proc_rundir",
    "qc",
    "sync_lims",
    "dictionaries",
]
SUBMODULES = [import_module("%s.%s" % (__package__, n)) for n in SUBMOD_NAMES]


class App(object):
    def __init__(self):
        self.supported_submodules = []
        for m in SUBMODULES:
            m.do_register(self.supported_submodules)

    def make_parser(self):
        parser = argparse.ArgumentParser(prog=__appname__,
                                         description='Preprocessing of sequencing data')
        parser.add_argument('--config_file', type=str, metavar='PATH',
                            help='configuration file',
                            default=None)
        parser.add_argument('--logfile', type=str, metavar='PATH',
                            help='log file (default=stderr).')
        parser.add_argument('--loglevel', type=str, help='logger level.',
                            choices=LOG_LEVELS, default='INFO')
        parser.add_argument('--batch_queuing', dest='batch_queuing',
                            action='store_true',
                            help='Submit jobs to the batch system (default)')
        parser.add_argument('--no_batch_queuing', dest='batch_queuing',
                            action='store_false',
                            help="Do not submit jobs to the batch system")
        parser.set_defaults(batch_queuing=True)
        parser.add_argument('-v', '--version', action='version',
                            version='%(prog)s {}'.format(__version__))

        subparsers = parser.add_subparsers(dest='subparser_name',
                                           title='subcommands',
                                           description='valid subcommands',
                                           help='sub-command description')

        for k, h, addarg, impl in self.supported_submodules:
            subparser = subparsers.add_parser(k, help=h)
            addarg(subparser)
            subparser.set_defaults(func=impl)

        return parser


def main():
    app = App()
    parser = app.make_parser()
    args = parser.parse_args()
    logger = a_logger('main', level=args.loglevel, filename=args.logfile)

    args.func(logger, args) if hasattr(args, 'func') else parser.print_help()
