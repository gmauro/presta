import os.path
import string

from alta.utils import ensure_dir
from alta.objectstore import build_object_store
from presta.utils import path_exists, get_conf, IEMSampleSheetReader
from presta.app.tasks import bcl2fastq, rd_completed, rd_move
from celery import chain

help_doc = """
Process a rundir
"""


def make_parser(parser):
    parser.add_argument('--run_dir', metavar="PATH",
                        help="rundir path", required=True)
    parser.add_argument('--output', type=str, help='output path')
    parser.add_argument('--samplesheet', type=str, help='samplesheet path')


def implementation(logger, args):
    if args.run_dir:
            path_exists(args.run_dir, logger)
    rd_path = args.run_dir

    if rd_completed(rd_path):
        rd_label = os.path.basename(rd_path)
        logger.info('Processing {} '.format(rd_label))
        if args.output:
            ds_path = args.output
        else:
            ds_path = os.path.join(rd_path.replace('running', 'completed'),
                                   'datasets')
        ensure_dir(ds_path)
        if args.samplesheet:
            ss_file = args.samplesheet
        else:
            ss_file = os.path.join(rd_path.replace('running', 'completed'),
                                   'samplesheet.csv')
        if not path_exists(ss_file, logger, force=False):
            conf = get_conf(logger, args.config_file)
            ir_conf = conf.get_irods_section()

            ir = build_object_store(store='irods',
                                    host=ir_conf['host'],
                                    port=ir_conf['port'],
                                    user=ir_conf['user'],
                                    password=ir_conf['password'],
                                    zone=ir_conf['zone'])
            runs_c = ir_conf['runs_collection']
            ipath = os.path.join(runs_c, rd_label,
                                 'samplesheet.csv')
            logger.info('Coping samplesheet from iRODS {}'.format(ipath))
            ss_file_orig = ''.join([ss_file, '.orig'])
            obj = ir.get_object(ipath, dest_path=ss_file_orig)

            with open(ss_file_orig, 'r') as f:
                samplesheet = IEMSampleSheetReader(f)

            with open(ss_file, 'w') as f2:
                for row in samplesheet.get_body():
                    f2.write(row)

        logger.debug('Rundir path : {}'.format(rd_path))
        logger.debug('Output path: {}'.format(ds_path))
        logger.debug('Samplesheet path: {}'.format(ss_file))

        running_path = rd_path
        completed_path = os.path.join(rd_path.replace('running','completed'),
                                      'raw')

        logger.debug("{} {}".format(completed_path, running_path))

        chain(bcl2fastq.si(rd_path, ds_path, ss_file).delay())
        #       rd_move.si(running_path, completed_path).delay())


def do_register(registration_list):
    registration_list.append(('proc_rundir', help_doc, make_parser,
                              implementation))
