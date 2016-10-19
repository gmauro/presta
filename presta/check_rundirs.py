import os

from presta.utils import path_exists, get_conf
from presta.app.tasks import rd_ready_to_be_preprocessed
from presta.app.events import emit_event


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

        self.ir_conf = conf.get_irods_section()
        self.emit_events = args.emit_events

    def check(self):
        def flatten(l):
            out = []
            for item in l:
                if isinstance(item, (list, tuple)):
                    out.extend(flatten(item))
                else:
                    out.append(item)
            return out

        path_exists(self.root_path, self.logger)
        localroot, dirnames, filenames = os.walk(self.root_path).next()

        positive_labels = ['finished', "ownership ok" ,
                           'SampleSheet found', 'Barcodes have the same size', 'Metadata found']
        negative_labels = ['running ', "waiting for ownership's modification",
                           'SampleSheet not found',
                           "Barcodes don't have the same size", 'Metadata not found']

        dir_dict = dict()
        for d in dirnames:
            dir_dict[d] = []
            d_path = os.path.join(self.root_path, d)
            checks = rd_ready_to_be_preprocessed(user=self.user,
                                                 group=self.group,
                                                 path=d_path,
                                                 rd_label=d,
                                                 ir_conf=self.ir_conf)

            ready_to_be_preprocessed = checks[0] and checks[1] and checks[2][0]

            if self.emit_events and ready_to_be_preprocessed:
                emit_event.si(event='rd_ready',
                              params=dict(rd_path=d_path,
                                          rd_label=d)).delay()

            checks = flatten(checks)
            for i in range(len(checks)):
                if checks[i]:
                    dir_dict[d].append(positive_labels[i])
                else:
                    dir_dict[d].append(negative_labels[i])

        self.logger.info('Checking rundirs in: {}'.format(self.root_path))

        for d, labels in dir_dict.iteritems():
            self.logger.info(' ')
            self.logger.info('Rundir {}'.format(d))
            self.logger.info('{}'.format(labels))


help_doc = """
Starting from a root path, print the state of all the rundirs found.
"""


def make_parser(parser):
    parser.add_argument('--root_path', metavar="PATH",
                        help="alternative rundirs root path")
    parser.add_argument('--emit_events', action='store_true',
                        help='sends event to router')


def implementation(logger, args):
    rr = RundirsRootpath(logger=logger, args=args)
    rr.check()


def do_register(registration_list):
    registration_list.append(('check', help_doc, make_parser,
                              implementation))
