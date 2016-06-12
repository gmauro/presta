import os

from presta.utils import path_exists, get_conf
from presta.app.tasks import rd_ready_to_be_preprocessed


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
        ownership = []
        for d in dirnames:
            d_path = os.path.join(self.root_path, d)
            checks = rd_ready_to_be_preprocessed(user=self.user,
                                                 group=self.group, path=d_path)
            if checks[0]:
                if checks[1]:
                    completed.append(d)
                else:
                    ownership.append(d)
            else:
                running.append(d)
        self.logger.info('Checking rundirs in: {}'.format(self.root_path))
        self.logger.info('Rundirs running:')
        for d in running:
            self.logger.info('{}'.format(d))
        self.logger.info("Rundirs waiting for ownership's modification:")
        for d in ownership:
                self.logger.info('{}'.format(d))
        self.logger.info('Rundirs ready to be pre-processed:')
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
