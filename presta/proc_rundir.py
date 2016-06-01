import os.path

from alta.utils import ensure_dir
from alta.objectstore import build_object_store
from presta.utils import path_exists, get_conf
from presta.app.tasks import rd_completed

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
            print ir_conf

            ir = build_object_store(store='irods',
                                    host=ir_conf['host'],
                                    port=ir_conf['port'],
                                    user=ir_conf['user'],
                                    password=ir_conf['password'],
                                    zone=ir_conf['zone'])
            ipath = os.path.join('/CRS4HUB/home/sequencing/runs', rd_label,
                                 'samplesheet.csv')
            logger.info('Coping samplesheet from iRODS {}'.format(ipath))
            obj = ir.get_object(ipath, dest_path=ss_file)


def do_register(registration_list):
    registration_list.append(('proc_rundir', help_doc, make_parser,
                              implementation))
