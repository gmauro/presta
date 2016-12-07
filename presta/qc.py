import os.path
import sys

from alta.utils import ensure_dir
from celery import chain
from presta.app.tasks import copy_qc_dirs, rd_collect_fastq, qc_runner
from presta.utils import path_exists, get_conf


class QcWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        conf = get_conf(logger, args.config_file)
        self.batch_queuing = args.batch_queuing
        self.queues_conf = conf.get_section('queues')

        r_dir_label = args.rundir_label
        ds_dir_label = 'datasets'
        fqc_dir_label = 'fastqc'

        input_path = args.ds_path
        output_path = args.export_path


        if r_dir_label or (input_path and output_path):
            pass
        else:
            logger.error("You must provide the rundir_label or both ds_path "
                         "and export_path")
            sys.exit()

        # input path must exists as parser argument or as config file argument
        if not input_path:
            io_conf = conf.get_io_section()
            input_path = os.path.join(io_conf.get('archive_root_path'),
                                      r_dir_label,
                                      ds_dir_label)
        path_exists(input_path, logger)
        self.input_path = input_path

        # export path must exists as parser argument or as config file argument
        if not output_path:
            io_conf = conf.get_io_section()
            output_path = os.path.join(io_conf.get('qc_export_basepath'),
                                       r_dir_label)
        # FIXME: this is a local path, must be checked that run on right node
        if not path_exists(output_path, logger, force=False):
            ensure_dir(output_path)
        path_exists(output_path, logger)
        self.output_path = output_path

        self.fqc_path = os.path.join(self.input_path, fqc_dir_label)
        self.rerun = args.rerun

    def run(self):

        copy_task = copy_qc_dirs.si(self.input_path, self.output_path)
        msgs = ["Generating Fastqc reports",
                "Coping qc dirs from {} to {}".format(self.input_path,
                                                      self.output_path)]

        if path_exists(self.fqc_path, self.logger, force=False) and len(os.listdir(self.fqc_path)) > 0 \
                and not self.rerun:

            self.logger.info(msgs[1])
            copy_task.delay()
        else:
            self.logger.info("{} and {}".format(msgs[0], msgs[1]))
            ensure_dir(self.fqc_path)
            qc_task = chain(rd_collect_fastq.si(ds_path=self.input_path),
                            qc_runner.s(outdir=self.fqc_path,
                                        batch_queuing=self.batch_queuing,
                                        queue_spec=self.queues_conf.get('q_fastqc')),
                            copy_task
                            ).delay()


help_doc = """
Generate (if needed) and export quality control reports
"""


def make_parser(parser):
    parser.add_argument('--rundir_label', '-r', metavar="STRING",
                        help='Label of the rundir to process')
    parser.add_argument('--ds_path', metavar="PATH",
                        help="Where datasets are stored")
    parser.add_argument('--export_path', type=str, metavar="PATH",
                        help='Where qc reports have to be stored')
    parser.add_argument('--rerun', action='store_true',
                        help='force generating Fastqc reports')
    parser.add_argument('--emit_events', action='store_true',
                        help='sends event to router')


def implementation(logger, args):
    workflow = QcWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('qc', help_doc, make_parser,
                              implementation))
