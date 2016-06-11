import os

from presta.utils import path_exists, get_conf
from presta.app.tasks import seq_completed, check_ownership


class RundirsRootpath(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger

        conf = get_conf(logger, args.config_file)

        if args.root_path:
            self.root_path = args.root_path
        else:
            io_conf = conf.get_io_section()
            self.root_path = io_conf.get('rundirs_root_path')

        do_conf = conf.get_section('data_ownership')
        self.user = do_conf.get('user')
        self.group = do_conf.get('group')

    def check(self):
        path_exists(self.root_path, self.logger)
        localroot, dirnames, filenames = os.walk(self.root_path).next()
        running = []
        completed = []
        for d in dirnames:
            d_path = os.path.join(self.root_path, d)
            if seq_completed(d_path) and \
                check_ownership(user=self.user, group=self.group, dir=d_path):
                completed.append(d)
            else:
                running.append(d)
        self.logger.info('Checking rundirs in: {}'.format(self.root_path))
        self.logger.info('Rundirs running:')
        for d in running:
            self.logger.info('{}'.format(d))
        self.logger.info('Rundirs done:')
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
