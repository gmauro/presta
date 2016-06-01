"""
Utilities used by other modules.
"""

import csv
import os
import sys

from alta import ConfigurationFromYamlFile
from pkg_resources import resource_filename


class IEMSampleSheetReader(csv.DictReader):
    """
    Illumina Experimental Manager SampleSheet reader.
    """
    def __init__(self, f):
        csv.DictReader.__init__(self, f, delimiter=',')
        self.header = None
        self.body = None
        magic = f.readline()
        if not magic.startswith('[Header]'):
            raise ValueError('%s is not an IEM SampleSheet' % f.name)
        header = []
        l = f.readline()
        while not l.startswith('[Data]'):
            header.append(l.strip())  # ms-dos sanitation
            l = f.readline()
        else:
            self.header = header
        self.body = csv.DictReader(f.readlines(), delimiter=',')

    def replace_column(self, label='Sample_Name', new_value=''):
        for r in self.body:
            r[label] = new_value


def get_conf(logger, config_file):
    config_file_path = paths_setup(logger, config_file)

    # Load YAML configuration file
    return ConfigurationFromYamlFile(config_file_path)


def path_exists(path, logger, force=True):
    def file_missing(path, logger, force):
        if force:
            logger.error("path - {} - doesn't exists".format(path))
            sys.exit()
        return False

    return True if os.path.exists(path) else file_missing(path, logger,
                                                         force)


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

    return sorted(config_file_paths)[0].path


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

