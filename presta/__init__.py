import argparse
import os.path
import sys
from app.celery.tasks import bcl2fastq
from alta.utils import a_logger, ensure_dir, LOG_LEVELS


def make_parser():
    parser = argparse.ArgumentParser(description='presta')
    parser.add_argument('--run_dir', type=str, help='rundir path',
                        required=True)
    parser.add_argument('--output', type=str, help='output path')
    parser.add_argument('--samplesheet', type=str, help='samplesheet path')
    parser.add_argument('--logfile', type=str, help='log file ('
                                                    'default=stderr).')
    parser.add_argument('--loglevel', type=str, help='logger level.',
                        choices=LOG_LEVELS, default='DEBUG')
    return parser


def main(argv):
    def check_if_path_exists(path, logger):
        if not os.path.exists(path):
            logger.error("{} doesn't exists".format(path))
            sys.exit()

    parser = make_parser()
    args = parser.parse_args(argv)
    logger = a_logger('main', level=args.loglevel, filename=args.logfile)

    if args.run_dir:
        check_if_path_exists(args.run_dir, logger)
    rd_path = args.run_dir

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



    logger.debug('Rundir path : {}'.format(rd_path))
    logger.debug('Output path: {}'.format(ds_path))
    logger.debug('Samplesheet path: {}'.format(ss_file))

    res = bcl2fastq.delay(rd_path, ds_path, ss_file)

