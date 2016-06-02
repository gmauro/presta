import os

from presta.utils import path_exists, get_conf
from presta.app.tasks import rd_completed


class RundirsRootpath(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.root_path = ''
        if args.root_path:
            self.root_path = args.root_path
        else:
            conf = get_conf(logger, args.config_file)
            io_conf = conf.get_io_section()
            if 'rundirs_root_path' in io_conf:
                self.root_path = io_conf['rundirs_root_path']

    def check(self):
        path_exists(self.root_path, self.logger)
        localroot, dirnames, filenames = os.walk(self.root_path).next()
        running = []
        completed = []
        for d in dirnames:
            if rd_completed(os.path.join(self.root_path, d)):
                completed.append(d)
            else:
                running.append(d)
        self.logger.info('Checking rundirs in: {}'.format(self.root_path))
        self.logger.info('Rundir running:')
        for d in running:
            self.logger.info('{}'.format(d))
        self.logger.info('Rundir completed:')
        for d in completed:
            self.logger.info('{}'.format(d))


help_doc = """
Starting from a root path, print the state of all the rundirs found.
"""


def make_parser(parser):
    parser.add_argument('--root_path', metavar="PATH",
                        help="alternative rundirs root path")


def implementation(logger, args):
    rr = RundirsRootpath(logger=logger, args=args)
    rr.check()


def do_register(registration_list):
    registration_list.append(('check', help_doc, make_parser,
                              implementation))
