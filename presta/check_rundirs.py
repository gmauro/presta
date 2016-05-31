import os

from presta.utils import path_exists
from presta.app.tasks import rd_completed


class RundirsRootpath(object):
    def __init__(self, root_path=None, logger=None):
        self.root_path = root_path
        self.logger = logger

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
        self.logger.info('Rundir running:')
        for d in running:
            self.logger.info('{}'.format(d))
        self.logger.info('Rundir completed:')
        for d in completed:
            self.logger.info('{}'.format(d))


help_doc = """
Print the state of rundirs from a root path
"""


def make_parser(parser):
    parser.add_argument('--root_path', metavar="PATH",
                        help="rundirs root path", required=True)


def implementation(logger, args):
    rr = RundirsRootpath(root_path=args.root_path, logger=logger)
    rr.check()


def do_register(registration_list):
    registration_list.append(('check_rundirs', help_doc, make_parser,
                              implementation))
