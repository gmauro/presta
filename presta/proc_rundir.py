import os.path
import sys

from alta.utils import ensure_dir
from presta.utils import path_exists, get_conf
from presta.app.tasks import bcl2fastq, rd_collect_fastq, move, fastqc, copy_qc_dirs, \
     rd_ready_to_be_preprocessed, copy_samplesheet_from_irods, replace_values_into_samplesheet
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

        fqc_path_prefix = os.path.join(dspath, 'fastqc')
        self.fqc = dict(path=fqc_path_prefix)

        ssheet = {'basepath': os.path.join(rpath),
                  'filename': 'SampleSheet.csv'}
        ssheet['file_path'] = os.path.join(ssheet['basepath'],
                                           ssheet['filename'])
        self.samplesheet = ssheet

        do_conf = conf.get_section('data_ownership')
        self.user = do_conf.get('user')
        self.group = do_conf.get('group')

        self._add_config_from_cli(args)

    def _add_config_from_cli(self, args):
        if args.output:
            self.ds['path'] = args.output

        io_conf = self.conf.get_io_section()
        if args.fastqc_outdir:
            self.fqc['path'] = args.fastq_outdir
        self.fqc['path'] = os.path.join(self.fqc['path'], self.rd['label'])
        self.fqc['export_path'] = io_conf.get('qc_export_path')

    def run(self):
        path_exists(self.rd['rpath'], self.logger)
        rd_status_checks = rd_ready_to_be_preprocessed(user=self.user,
                                                       group=self.group,
                                                       path=self.rd['rpath'])
        check = rd_status_checks[0] and rd_status_checks[1]
        if not check:
            self.logger.error("{} is not ready to be preprocessed".format(
                self.rd['label']))
            sys.exit()

        self.logger.info('Processing {}'.format(self.rd['label']))
        self.logger.info('running path {}'.format(self.rd['rpath']))
        self.logger.info('completed path {}'.format(self.rd['cpath']))
        self.logger.info('archive path {}'.format(self.rd['apath']))

        ensure_dir(self.ds['path'])
        ensure_dir(self.fqc['path'])

        ssht_task = chain(
            copy_samplesheet_from_irods.si(conf=self.conf.get_irods_section(),
                                           ssht_path=self.samplesheet['file_path'],
                                           rd_label=self.rd['label']),
            replace_values_into_samplesheet.s()
        )

        qc_task = chain(rd_collect_fastq.si(ds_path=self.ds['path']),
                        fastqc.s(self.fqc['path']),
                        copy_qc_dirs.si(self.ds['path'], os.path.join(
                            self.fqc['export_path'], self.rd['label'])))

        # full pre-processing sequencing rundir pipeline
        pipeline = chain(
            ssht_task,
            move.si(self.rd['rpath'], self.rd['apath']),
            bcl2fastq.si(self.rd['apath'], self.ds['path'],
                         self.samplesheet['file_path']),
            qc_task).delay()


help_doc = """
Process a rundir
"""


def make_parser(parser):
    parser.add_argument('--rd_path', metavar="PATH",
                        help="rundir path", required=True)
    parser.add_argument('--output', type=str, help='output path', default='')
    parser.add_argument('--samplesheet', type=str, help='samplesheet path')
    parser.add_argument('--fastqc_outdir', type=str, help='fastqc output path')


def implementation(logger, args):
    workflow = PreprocessingWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('proc', help_doc, make_parser,
                              implementation))
