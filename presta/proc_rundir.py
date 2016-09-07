import os.path
import sys

from alta.utils import ensure_dir
from presta.utils import path_exists, get_conf
from presta.app.tasks import bcl2fastq, rd_collect_fastq, move, qc_runner, \
    rd_ready_to_be_preprocessed, \
    copy_samplesheet_from_irods, copy_run_info_from_irods, copy_run_parameters_from_irods, \
    replace_values_into_samplesheet
from celery import chain


class PreprocessingWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        rpath = args.rd_path
        cpath = args.rd_path.replace('running', 'completed')
        apath = os.path.join(cpath, 'raw')
        self.rd = {'rpath': rpath,
                   'cpath': cpath,
                   'apath': apath,
                   'label': os.path.basename(args.rd_path)
                   }
        conf = get_conf(logger, args.config_file)
        self.conf = conf

        dspath = os.path.join(cpath, 'datasets')
        self.ds = {'path': dspath}

        fqc_basepath = os.path.join(dspath, 'fastqc')
        self.fqc = dict(path=fqc_basepath)

        io_conf = conf.get_io_section()
        export_path = os.path.join(io_conf.get('qc_export_basepath'),
                                   self.rd['label'])
        self.qc = {'export_path':  export_path}

        ssheet = {'basepath': os.path.join(cpath),
                  'filename': 'SampleSheet.csv'}
        ssheet['file_path'] = os.path.join(ssheet['basepath'],
                                           ssheet['filename'])
        self.samplesheet = ssheet

        run_info = {'basepath': os.path.join(cpath),
                    'filename': 'RunInfo.xml'}
        run_info['file_path'] = os.path.join(run_info['basepath'],
                                             run_info['filename'])
        self.run_info = run_info

        run_parameters = {'basepath': os.path.join(cpath),
                          'filename': 'runParameters.xml'}
        run_parameters['file_path'] = os.path.join(run_parameters['basepath'],
                                                   run_parameters['filename'])
        self.run_parameters = run_parameters

        do_conf = conf.get_section('data_ownership')
        self.user = do_conf.get('user')
        self.group = do_conf.get('group')

        self.no_lane_splitting = args.no_lane_splitting
        self.no_barcode_trimming = args.no_barcode_trimming

        self.barcode_mismatches = args.barcode_mismatches

        self.overwrite_samplesheet = args.overwrite_samplesheet

        self.batch_queuing = args.batch_queuing
        self.queues_conf = conf.get_section('queues')

        self._add_config_from_cli(args)

    def _add_config_from_cli(self, args):
        if args.output:
            self.ds['path'] = args.output

        if args.fastqc_outdir:
            self.fqc['path'] = args.fastq_outdir

    def run(self):
        path_exists(self.rd['rpath'], self.logger)
        rd_status_checks = rd_ready_to_be_preprocessed(
            user=self.user,
            group=self.group,
            path=self.rd['rpath'],
            rd_label=self.rd['label'],
            ssht_filename=self.samplesheet['filename'],
            ir_conf=self.conf.get_irods_section())

        check = rd_status_checks[0] and rd_status_checks[1] and \
                rd_status_checks[2][0]

        check_barcode_trimming = not rd_status_checks[2][1] and not self.no_barcode_trimming

        if not check:
            self.logger.error("{} is not ready to be preprocessed".format(
                self.rd['label']))
            sys.exit()

        self.logger.info('Processing {}'.format(self.rd['label']))
        self.logger.info('running path {}'.format(self.rd['rpath']))
        self.logger.info('completed path {}'.format(self.rd['cpath']))
        self.logger.info('archive path {}'.format(self.rd['apath']))
        self.logger.info('samplesheet path {}'.format(self.samplesheet['file_path']))
        self.logger.info('run info path {}'.format(self.run_info['file_path']))
        self.logger.info('run parameters path {}'.format(self.run_parameters['file_path']))

        ensure_dir(self.ds['path'])
        ensure_dir(self.fqc['path'])

        samplesheet_task = chain(

            copy_run_info_from_irods.si(conf=self.conf.get_irods_section(),
                                        run_info_path=self.run_info['file_path'],
                                        rd_label=self.rd['label']),

            copy_run_parameters_from_irods.si(conf=self.conf.get_irods_section(),
                                              run_parameters_path=self.run_parameters['file_path'],
                                              rd_label=self.rd['label']),

            copy_samplesheet_from_irods.si(conf=self.conf.get_irods_section(),
                                           ssht_path=self.samplesheet['file_path'],
                                           rd_label=self.rd['label']),

            replace_values_into_samplesheet.si(ssht_path=self.samplesheet['file_path'],
                                               run_info_path=self.run_info['file_path'],
                                               trim_barcodes=check_barcode_trimming),

        )

        qc_task = chain(rd_collect_fastq.si(ds_path=self.ds['path']),
                        qc_runner.s(outdir=self.fqc['path'],
                                    batch_queuing=self.batch_queuing,
                                    queue_spec=self.queues_conf.get('low'))
                        )

        # full pre-processing sequencing rundir pipeline
        pipeline = chain(
            samplesheet_task,
            move.si(self.rd['rpath'], self.rd['apath']),
            bcl2fastq.si(rd_path=self.rd['apath'],
                         ds_path=self.ds['path'],
                         ssht_path=self.samplesheet['file_path'],
                         no_lane_splitting=self.no_lane_splitting,
                         barcode_mismatches = self.barcode_mismatches,
                         batch_queuing=self.batch_queuing,
                         queue_spec=self.queues_conf.get('low')),
            qc_task).delay()


help_doc = """
Process a rundir
"""


def make_parser(parser):
    parser.add_argument('--rd_path', metavar="PATH",
                        help="rundir path", required=True)
    parser.add_argument('--output', type=str, help='output path', default='')
    parser.add_argument('--overwrite_samplesheet', action='store_true',
                        help='Overwrite the samplesheet '
                             'if already present into the filesystem')
    parser.add_argument('--no_barcode_trimming', action='store_true',
                        help='Do not trim barcode')
    parser.add_argument('--fastqc_outdir', type=str, help='fastqc output path')
    parser.add_argument('--no_lane_splitting', action='store_true',
                        help='Do not split fastq by lane')
    parser.add_argument("--barcode_mismatches", type=int, choices=[0, 1, 2],
                        default=1, help='Number of allowed mismatches per index')


def implementation(logger, args):
    workflow = PreprocessingWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('proc', help_doc, make_parser,
                              implementation))
