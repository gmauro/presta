import os, sys

from alta.utils import ensure_dir
from presta.utils import get_conf
from presta.app.router import dispatch_event
from presta.app.tasks import rd_collect_samples
from presta.app.lims import sync_samples

from celery import chain


class SyncLimsWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger

        conf = get_conf(logger, args.config_file)
        self.conf = conf

        self.irods_conf = self.conf.get_irods_section()
        self.bika_conf = self.conf.get_section('bika')

        self.io_conf = self.conf.get_io_section()
        self.rundir_label = args.rundir_label
        self.samplesheet_filename = 'SampleSheet.csv'

        self.emit_events = args.emit_events
        self.force = args.force
        self.sync_all_analyses = args.sync_all_analyses

    def run(self):
        qc_path = os.path.join(self.io_conf.get('qc_export_basepath'), self.rundir_label)
        if not self.force and not os.path.exists(qc_path):
            self.logger.error('{} pre-processing is not yet completed.'
                              ' Use --force option to bypass this check'.format(self.rundir_label))
            sys.exit()

        self.logger.info('Synchronizing {}'.format(self.rundir_label))

        sync_task = chain(rd_collect_samples.si(conf=self.irods_conf,
                                                samplesheet_filename=self.samplesheet_filename,
                                                rd_label=self.rundir_label),
                          sync_samples.s(conf=self.bika_conf,
                                         sync_all_analyses=self.sync_all_analyses),
                          )

        sync_task.delay()


help_doc = """
Synchronize bika lims
"""


def make_parser(parser):
    parser.add_argument('--rundir_label', '-r', metavar="STRING", required=True,
                        help='Label of the rundir to synchronize')
    parser.add_argument('--force', '-f', action='store_true',
                        help='force the synchronization even if the qc files are missing')
    parser.add_argument('--sync_all_analyses', '-a', action='store_true',
                        help='synchronizes all analysises: pre and post processing')
    parser.add_argument('--emit_events', action='store_true',
                        help='sends events to router')


def implementation(logger, args):
    workflow = SyncLimsWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('sync', help_doc, make_parser,
                              implementation))



