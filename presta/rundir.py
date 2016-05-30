import argparse
import os.path
import sys

from alta import ConfigurationFromYamlFile
from alta.utils import a_logger, ensure_dir, LOG_LEVELS
from presta.app.tasks import bcl2fastq, rd_completed, rd_move
from celery import chain
from pkg_resources import resource_filename


class WeightedPath(object):
    def __init__(self, path, weight):
        self.path = path
        self.weight = weight

    def __repr__(self):
        return '{}: {} {}'.format(self.__class__.__name__,
                                  self.path,
                                  self.weight)

    def __cmp__(self, other):
        if hasattr(other, 'weight'):
            return self.weight.__cmp__(other.weight)


def make_parser():
    parser = argparse.ArgumentParser(prog='presta',
                                     description='Preprocessing of sequencing data')
    parser.add_argument('--config_file', type=str, help='configuration file',
                        default=None)
    parser.add_argument('--logfile', type=str, help='log file ('
                                                    'default=stderr).')
    parser.add_argument('--loglevel', type=str, help='logger level.',
                        choices=LOG_LEVELS, default='DEBUG')

    subparsers = parser.add_subparsers(dest='subparser_name',
                                       title='subcommands',
                                       description='valid subcommands',
                                       help='sub-command help')
    parser_a = subparsers.add_parser('check', help='check help')
    parser_a.add_argument('--path', type=str, help='rundirs path',
                          required=True)

    parser_b = subparsers.add_parser('run', help='run help')
    parser_b.add_argument('--run_dir', type=str, help='rundir path',
                          required=True)
    parser_b.add_argument('--output', type=str, help='output path')
    parser_b.add_argument('--samplesheet', type=str, help='samplesheet path')

    return parser


def paths_setup(logger, cf_from_cli=None):
    home = os.path.expanduser("~")
    presta_config_from_home = os.path.join(home, 'presta',
                                           'presta_config.yml')
    presta_config_from_package = resource_filename('presta',
                                                   'config/presta_config.yml')
    config_file_paths = []
    if cf_from_cli and path_exists(cf_from_cli, logger, force=False):
        config_file_paths.append(WeightedPath(cf_from_cli, 0))
    if path_exists(presta_config_from_home, logger, force=False):
        config_file_paths.append(WeightedPath(presta_config_from_home, 1))
    if path_exists(presta_config_from_package, logger, force=False):
        config_file_paths.append(WeightedPath(presta_config_from_package, 2))
    logger.debug("config file paths: {}".format(config_file_paths))
    return sorted(config_file_paths).pop().path


def path_exists(path, logger, force=True):
    def file_missing(path, logger, force):
        if force:
            logger.error("{} doesn't exists".format(path))
            sys.exit()
        return False

    return True if os.path.exists(path) else file_missing(path, logger,
                                                          force)


def main(argv):

    parser = make_parser()
    args = parser.parse_args(argv)
    logger = a_logger('main', level=args.loglevel, filename=args.logfile)

    config_file_path = paths_setup(logger, args.config_file)

    # Load YAML configuration file
    conf = ConfigurationFromYamlFile(config_file_path)

    if args.subparser_name == 'check':
        path = args.path
        path_exists(path, logger)
        localroot, dirnames, filenames = os.walk(path).next()

        running = []
        completed = []
        for d in dirnames:
            if rd_completed(os.path.join(path, d)):
                completed.append(d)
            else:
                running.append(d)
        logger.info('Rundir running:')
        for d in running:
            logger.info('{}'.format(d))
        logger.info('Rundir completed:')
        for d in completed:
            logger.info('{}'.format(d))

    if args.subparser_name == 'run':
        if args.run_dir:
            path_exists(args.run_dir, logger)
        rd_path = args.run_dir

        if rd_completed(rd_path):
            logger.info('Processing {} '.format(os.path.basename(rd_path)))
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
            path_exists(ss_file, logger)
            logger.debug('Rundir path : {}'.format(rd_path))
            logger.debug('Output path: {}'.format(ds_path))
            logger.debug('Samplesheet path: {}'.format(ss_file))

            completed_path = os.path.join(rd_path.replace(
                'running','completed'), 'raw')
            running_path = rd_path

            logger.debug("{} {}".format(completed_path, running_path))

            chain(bcl2fastq(rd_path, ds_path, ss_file),
                  rd_move(running_path, completed_path))
            # chain(rd_move(running_path, completed_path),
            #       rd_move(rd_path.replace('running', 'completed'), archive_path))
            #bcl2fastq.delay(rd_path, ds_path, ss_file)
        else:
            logger.info('Skipping uncompleted run dir {} '.format(
                os.path.basename(rd_path)))


