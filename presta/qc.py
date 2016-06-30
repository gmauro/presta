import os.path

from alta.utils import ensure_dir
from celery import chain
from presta.app.tasks import copy_qc_dirs, rd_collect_fastq, qc_runner
from presta.utils import path_exists, get_conf


class qcWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        conf = get_conf(logger, args.config_file)
        self.dspath = args.ds_path
        self.exportpath = args.export_path
        self.batch_queuing = args.batch_queuing
        self.queues_conf = conf.get_section('queues')

    def run(self):
        fqc_path = os.path.join(self.dspath, 'fastqc')
        copy_task = copy_qc_dirs.si(self.dspath, self.exportpath)
        if not path_exists(fqc_path, self.logger):
            self.logger.info("Generating Fastqc reports")
            ensure_dir(fqc_path)
            qc_task = chain(rd_collect_fastq.si(ds_path=self.dspath),
                            qc_runner.s(outdir=fqc_path,
                                        batch_queuing=self.batch_queuing,
                                        queue_spec=self.queues_conf.get('q_fastqc')),
                            copy_task
                            ).delay()
        else:
            self.logger.info('Coping qc dirs from {} to {}'.format(self.dspath,
                                                                   self.exportpath))
            copy_task.delay()


help_doc = """
Generate (if needed) and export quality control reports
"""


def make_parser(parser):
    parser.add_argument('--ds_path', metavar="PATH",
                        help="datasets path", required=True)
    parser.add_argument('--export_path', type=str, help='export path',
                        default='', required=True)


def implementation(logger, args):
    workflow = qcWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('qc', help_doc, make_parser,
                              implementation))
