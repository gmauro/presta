import os.path
import sys

from alta.utils import ensure_dir
from alta.objectstore import build_object_store
from presta.utils import path_exists, get_conf, IEMSampleSheetReader
from presta.app.tasks import bcl2fastq, rd_collect_fastq, rd_completed, \
     rd_move, fastqc
from celery import chain


class PreprocessingWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.rd = {'rpath': args.run_dir,
                   'cpath': args.run_dir.replace('running', 'completed'),
                   'apath': os.path.join(self.rd['cpath'], 'raw'),
                   'label': os.path.basename(args.run_dir)
                   }
        self.conf = get_conf(logger, args.config_file)
        self.ds = {'path': os.path.join(self.rd['cpath'], 'datasets')}
        if args.output:
            self.ds['path'] = args.output

        io_conf = self.conf.get_io_section()
        self.fqc = {'path': os.path.join(self.ds['path'], 'fastqc'),
                    'export_path': io_conf.get('fastqc_outdir')
                    }
        if args.fastqc_outdir:
            self.fqc['path'] = args.fastq_outdir
        self.fqc['path'] = os.path.join(self.fqc['path'], self.rd['label'])

        self.samplesheet = {'path': os.path.join(self.rd['rpath']),
                            'filename': 'SampleSheet.csv'}
        self.samplesheet['file_path'] = os.path.join(self.samplesheet['path'],
                                                     self.samplesheet['filename'])
        if not path_exists(self.samplesheet['file_path'], self.logger,
                           force=False):
            self.copy_samplesheet_from_irods()

    def copy_samplesheet_from_irods(self):
        ir_conf = self.conf.get_irods_section()
        ir = build_object_store(store='irods',
                                host=ir_conf['host'],
                                port=ir_conf['port'],
                                user=ir_conf['user'],
                                password=ir_conf['password'],
                                zone=ir_conf['zone'])

        ipath = os.path.join(ir_conf['runs_collection'], self.rd['label'],
                             self.samplesheet['filename'])
        self.logger.info('Coping samplesheet from iRODS {}'.format(
            self.samplesheet['file_path']))
        ir.get_object(ipath, dest_path=self.samplesheet['file_path'])

    def replace_values_into_samplesheet(self):
        with open(self.samplesheet['file_path'], 'r') as f:
            samplesheet = IEMSampleSheetReader(f)

        with open(self.samplesheet['file_path'], 'w') as f:
            for row in samplesheet.header:
                f.write(row)
            for row in samplesheet.get_body():
                f.write(row)

    def run(self):
        path_exists(self.rd['path'], self.logger)
        if not rd_completed(self.rd['path']):
            self.logger.error("{} is not ready to be preprocessed".format(
                self.rd['label']))
            sys.exit()
        self.logger.info('Processing {}'.format(self.rd['label']))
        ensure_dir(self.ds['path'])
        ensure_dir(self.fqc['path'])

        self.replace_values_into_samplesheet()

        # chain(bcl2fastq.si(self.rd['rpath'], self.ds['path'],
        #                    self.samplesheet['file_path']),
        #       rd_move.si(self.rd['rpath'], self.rd['cpath']),
        #       rd_collect_fastq.si(ds_path=self.ds['path']),
        #       fastqc.s(self.fqc['path'])).delay()


help_doc = """
Process a rundir
"""


def make_parser(parser):
    parser.add_argument('--run_dir', metavar="PATH",
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
