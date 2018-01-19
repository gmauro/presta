import os.path
import sys

from alta.utils import ensure_dir
from celery import chain
from presta.app.tasks import copy_qc_dirs, rd_collect_fastq, qc_runner
from presta.app.router import trigger_event, dispatch_event
from presta.utils import path_exists, get_conf
from client import Client

ALIGNER = ['bowtie', 'bowtie2', 'bwa']


class QcWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.conf = get_conf(logger, args.config_file)
        self.io_conf = self.conf.get_io_section()
        self.batch_queuing = args.batch_queuing
        self.queues_conf = self.conf.get_section('queues')

        rd_label = args.rd_label
        ds_path = args.ds_path if args.ds_path \
            else os.path.join(self.io_conf.get('archive_root_path'),
                              rd_label,
                              self.io_conf.get('ds_folder_name'))

        qc_path = args.qc_path if args.qc_path \
            else os.path.join(ds_path,
                              self.io_conf.get('qc_folder_name'))

        qc_export_path = args.qc_export_path if args.qc_export_path \
            else os.path.join(self.io_conf.get('qc_export_basepath'),
                              rd_label)

        fastqscreen_output_path = args.fastqscreen_output_path if args.fastqscreen_output_path \
            else os.path.join(qc_path,
                              self.io_conf.get('fastqscreen_folder_name'))

        multiqc_output_path = args.multiqc_output_path if args.multiqc_output_path \
            else os.path.join(qc_path,
                              self.io_conf.get('multiqc_folder_name'))

        fastqscreen_config_file = args.fastqscreen_config_file if args.fastqscreen_config_file \
            else self.io_conf.get('fastqscreen_config_file')

        multiqc_samplesmap_file = args.multiqc_samplesmap_file if args.multiqc_samplesmap_file \
            else  os.path.join(multiqc_output_path,
                               self.io_conf.get('multiqc_samplesmap_file'))

        # FIXME: this is a local path, must be checked that run on right node
        if not path_exists(qc_export_path, logger, force=False):
            ensure_dir(qc_export_path)

        path_exists(ds_path, logger)
        path_exists(qc_export_path, logger)
        path_exists(fastqscreen_config_file, logger)

        c = Client(conf=self.conf, logger=self.logger)
        c.init_bika()
        run_info = c.bk.get_run_info(run_label=rd_label)

        self.rd_label = rd_label
        self.input_path = ds_path
        self.output_path = qc_export_path
        self.qc_path = qc_path
        self.fastqscreen_output_path = fastqscreen_output_path
        self.fastqscreen_config_file = fastqscreen_config_file
        self.multiqc_output_path = multiqc_output_path
        self.multiqc_samplesmap_file = multiqc_samplesmap_file
        self.aligner = args.aligner
        self.rerun = args.rerun
        self.started = os.path.join(self.qc_path,
                                    self.io_conf.get('quality_check_started_file'))
        self.completed = os.path.join(self.qc_path,
                                      self.io_conf.get('quality_check_completed_file'))

        self.run_info = run_info

    def dump_samplesmap(self, source, destination):
        try:
            with open(destination, 'w') as fp:
                for sample_id,info in source.iteritems():
                    client_sample_id = info['client_sample_id']
                    fp.write("{}\t{}".format(sample_id, client_sample_id))

        except OSError as e:
            self.logger.error('I have retrieved any information about this run')
            return False

        return destination

    def run(self):
        msgs = ["Generating Fastqc reports",
                "Coping qc dirs from {} to {}".format(self.input_path,
                                                      self.output_path)]

        if path_exists(self.qc_path, self.logger, force=False) and len(os.listdir(self.qc_path)) > 0 \
                and not self.rerun:

            self.logger.info(msgs[1])
            copy_task = dispatch_event.si(event='copy_qc_folders',
                                          params=dict(src=self.input_path,
                                                      dest=self.output_path)
                                          )
            copy_task.delay()

        else:
            self.logger.info("{} and {}".format(msgs[0], msgs[1]))
            ensure_dir(self.qc_path, force=True)
            ensure_dir(self.fastqscreen_output_path, force=True)
            ensure_dir(self.multiqc_output_path, force=True)

            qc_task = chain(dispatch_event.si(event='qc_started',
                                              params=dict(progress_status_file=self.started)),
                            rd_collect_fastq.si(ds_path=self.input_path),
                            qc_runner.s(outdir=self.qc_path,
                                        fastqscreen_conf=self.fastqscreen_config_file,
                                        fastqscreen_outdir=self.fastqscreen_output_path,
                                        aligner=self.aligner,
                                        batch_queuing=self.batch_queuing,
                                        queue_spec=self.queues_conf.get('q_fastqc')),
                            ).apply_async()

            multiqc_task = trigger_event.si(event='multiqc_runner',
                                            params=dict(
                                                qc_dir=self.qc_path,
                                                fastqscreen_dir=self.fastqscreen_output_path,
                                                multiqc_output_path=self.multiqc_output_path,
                                                rd_label=self.rd_label,
                                                samplesmap_file=self.dump_samplesmap(source=self.run_info,
                                                                                     destination=self.multiqc_samplesmap_file),
                                                batch_queuing=self.batch_queuing,
                                                queue_spec=self.queues_conf.get('q_fastqc')
                                            ),
                                            tasks=qc_task.get()
                                            ).apply_sync()

            copy_task = trigger_event.si(event='copy_qc_folders',
                                         params=dict(src=self.input_path,
                                                     dest=self.output_path),
                                         tasks=multiqc_task.get())
            copy_task.apply_async()

            trigger_event.si(event='qc_completed',
                             params=dict(progress_status_file=self.completed),
                             tasks=qc_task.get()).apply_async()


help_doc = """
Generate (if needed) and export quality control reports
"""


def make_parser(parser):
    parser.add_argument('--rd_label', '-r', metavar="STRING",
                        help='Label of the rundir to process', required=True)

    parser.add_argument('--ds_path', '-d',  metavar="PATH",
                        help="Where datasets are stored")

    parser.add_argument('--qc_path', '-q', metavar="PATH",
                        help="Fastq Quality Check output path")

    parser.add_argument('--qc_export_path', '-e', metavar="PATH",
                        help="Fastq Quality Check export path")

    parser.add_argument('--fastqscreen_config_file', metavar="PATH",
                        help="FastqScreen config file")

    parser.add_argument('--fastqscreen_output_path', metavar="PATH",
                        help="FastqScreen Output Path")

    parser.add_argument('--fastqscreen_aligner', choices=ALIGNER,
                        default="bowtie2", help='Specify the aligner to use for the mapping')

    parser.add_argument('--multiqc_output_path', metavar="PATH",
                        help="MultiQC Output Path")

    parser.add_argument('--multiqc_samplesmap_file', metavar="PATH",
                        help="MultiQC sample map file")

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
