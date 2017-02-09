import os.path
import sys

from alta.utils import ensure_dir
from presta.utils import path_exists, get_conf
from presta.app.tasks import bcl2fastq, move,  \
    rd_ready_to_be_preprocessed, \
    copy_samplesheet_from_irods, copy_run_info_to_irods, copy_run_parameters_to_irods, \
    replace_values_into_samplesheet, sanitize_metadata, replace_index_cycles_into_run_info
from presta.app.router import dispatch_event
from celery import chain


class PreprocessingWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger

        conf = get_conf(logger, args.config_file)
        self.conf = conf
        self.io_conf = self.conf.get_io_section()
        self.ir_conf = self.conf.get_irods_section()
        self.do_conf = self.conf.get_section('data_ownership')
        self.queues_conf = self.conf.get_section('queues')

        rd_label = args.rd_label

        rd_path = args.rd_path if args.rd_path \
            else os.path.join(self.io_conf.get('rundirs_root_path'),
                              rd_label)

        ds_path = args.ds_path if args.ds_path \
            else os.path.join(self.io_conf.get('archive_root_path'),
                              rd_label,
                              self.io_conf.get('ds_folder_name'))

        ssheet_path = args.ssheet_path if args.ssheet_path \
            else os.path.join(self.io_conf.get('archive_root_path'),
                              rd_label,
                              self.io_conf.get('ssheet_filename'))

        qc_path = args.qc_path if args.qc_path \
            else os.path.join(ds_path,
                              self.io_conf.get('qc_folder_name'))

        qc_export_path = args.qc_export_path if args.qc_export_path \
            else os.path.join(self.io_conf.get('qc_export_basepath'),
                              rd_label)

        run_info_path = os.path.join(
            rd_path,
            self.io_conf.get('run_info_filename')
        )
        run_parameters_path = os.path.join(
            rd_path,
            self.io_conf.get('run_parameters_filename')
        )

        self.rd = dict(
            path=rd_path,
            label=rd_label
        )
        self.ds = dict(
            path=ds_path
        )
        self.qc = dict(
            path=qc_path,
            export_path=qc_export_path
        )
        self.samplesheet = dict(
            path=ssheet_path,
            filename=os.path.basename(ssheet_path)
        )
        self.run_info = dict(
            path=run_info_path,
            filename=os.path.basename(run_info_path)
        )
        self.run_parameters = dict(
            path=run_parameters_path,
            filename=os.path.basename(run_parameters_path)
        )

        self.user = self.do_conf.get('user')
        self.group = self.do_conf.get('group')

        self.no_lane_splitting = args.no_lane_splitting

        self.barcode_mismatches = args.barcode_mismatches

        self.overwrite_samplesheet = not os.path.isfile(ssheet_path)

        self.emit_events = args.emit_events

        self.batch_queuing = args.batch_queuing

        self.started_file = os.path.join(self.rd['path'],
                                         self.io_conf.get('preprocessing_started_file'))

        self.completed_file = os.path.join(self.rd['path'],
                                           self.io_conf.get('preprocessing_completed_file'))

    def run(self):
        path_exists(self.rd['path'], self.logger)

        rd_status_checks = rd_ready_to_be_preprocessed(
            user=self.user,
            group=self.group,
            path=self.rd['path'],
            rd_label=self.rd['label'],
            ssht_filename=self.samplesheet['filename'],
            ir_conf=self.ir_conf,
            io_conf=self.io_conf)

        check = rd_status_checks[0] and rd_status_checks[1] and \
                rd_status_checks[2][0]

        check_sanitize_metadata = not rd_status_checks[3]

        if not check:
            self.logger.error("{} is not ready to be preprocessed".format(
                self.rd['label']))
            sys.exit()

        self.logger.info('Processing {}'.format(self.rd['label']))
        self.logger.info('running path {}'.format(self.rd['path']))
        self.logger.info('datasets path {}'.format(self.ds['path']))
        self.logger.info('samplesheet path {}'.format(self.samplesheet['path']))

        if self.emit_events:
            self.logger.info('quality check output path {}'.format(self.qc['path']))
            self.logger.info('quality check export path {}'.format(self.qc['export_path']))

        ensure_dir(self.ds['path'])

        irods_task = chain(
            sanitize_metadata.si(conf=self.ir_conf,
                                 ssht_filename=self.samplesheet['filename'],
                                 rd_label=self.rd['label'],
                                 sanitize=check_sanitize_metadata
                                 ),

            copy_run_info_to_irods.si(conf=self.ir_conf,
                                      run_info_path=self.run_info['path'],
                                      rd_label=self.rd['label']
                                      ),

            copy_run_parameters_to_irods.si(conf=self.ir_conf,
                                            run_parameters_path=self.run_parameters['path'],
                                            rd_label=self.rd['label']
                                            ),
        )

        samplesheet_task = chain(

            copy_samplesheet_from_irods.si(conf=self.ir_conf,
                                           ssht_path=self.samplesheet['path'],
                                           rd_label=self.rd['label'],
                                           overwrite_samplesheet=self.overwrite_samplesheet
                                           ),

            replace_values_into_samplesheet.si(conf=self.ir_conf,
                                               ssht_path=self.samplesheet['path'],
                                               rd_label=self.rd['label'],
                                               overwrite_samplesheet=self.overwrite_samplesheet
                                               ),

        )

        # full pre-processing sequencing rundir pipeline
        pipeline = chain(
            dispatch_event.si(event='preprocessing_started',
                              params=dict(ds_path=self.ds['path'],
                                          rd_label=self.rd['label'],
                                          progress_status_file=self.started_file,
                                          emit_events=self.emit_events)),

            irods_task,
            samplesheet_task,

            replace_index_cycles_into_run_info.si(conf=self.ir_conf,
                                                  ssht_path=self.samplesheet['path'],
                                                  run_info_path=self.run_info['path'],
                                                  rd_label=self.rd['label']),

            bcl2fastq.si(rd_path=self.rd['path'],
                         ds_path=self.ds['path'],
                         ssht_path=self.samplesheet['path'],
                         run_info_path=self.run_info['path'],
                         no_lane_splitting=self.no_lane_splitting,
                         barcode_mismatches=self.barcode_mismatches,
                         batch_queuing=self.batch_queuing,
                         queue_spec=self.queues_conf.get('low')),

            replace_index_cycles_into_run_info.si(conf=self.ir_conf,
                                                  ssht_path=self.samplesheet['path'],
                                                  run_info_path=self.run_info['path'],
                                                  rd_label=self.rd['label']),

            dispatch_event.si(event='fastq_ready',
                              params=dict(ds_path=self.ds['path'],
                                          qc_path=self.qc['path'],
                                          qc_export_path=self.qc['export_path'],
                                          force=True,
                                          rd_label=self.rd['label'],
                                          progress_status_file=self.completed_file,
                                          emit_events=self.emit_events)),
        )
        pipeline.delay()


help_doc = """
Process a rundir
"""


def make_parser(parser):
    parser.add_argument('--rd_label', '-l', metavar="STRING",
                        help="Label of the rundir to process", required=True)

    parser.add_argument('--rd_path', '-r', metavar="PATH",
                        help="Rundir (raw data) input path")

    parser.add_argument('--ds_path', '-d', metavar="PATH",
                        help="Dataset output path"
                             "(Where datasets are stored)")

    parser.add_argument('--ssheet_path', '-s', metavar="PATH",
                        help="SampleSheet.csv input path")

    parser.add_argument('--qc_path', '-q', metavar="PATH",
                        help="Fastq Quality Check output path")

    parser.add_argument('--qc_export_path', '-e', metavar="PATH",
                        help="Fastq Quality Check export path")

    parser.add_argument('--no_lane_splitting', action='store_true',
                        help='Do not split fastq by lane')

    parser.add_argument("--barcode_mismatches", type=int, choices=[0, 1, 2],
                        default=1, help='Number of allowed mismatches per index')

    parser.add_argument('--emit_events', action='store_true',
                        help='sends events to router')


def implementation(logger, args):
    workflow = PreprocessingWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('proc', help_doc, make_parser,
                              implementation))
