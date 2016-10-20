import os

from alta.utils import ensure_dir
from presta.utils import path_exists, get_conf
from presta.app.events import emit_event

from client import Client
from celery import chain

class UpdateLimsWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        conf = get_conf(logger, args.config_file)

    def run(self):
        pass

help_doc = """
Synchronize bika lims
"""

def make_parser(parser):
    samples = parser.add_subparsers('samples')
    samples.add_argument('--ids', action='store_true',
                        help='sends events to router')

    batches = parser.add_subparsers('batches')
    batches.add_argument('--ids', action='store_true',
                         help='sends events to router')

    parser.add_argument('--emit_events', action='store_true',
                        help='sends events to router')


def implementation(logger, args):
    workflow = UpdateLimsWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('sync', help_doc, make_parser,
                              implementation))


