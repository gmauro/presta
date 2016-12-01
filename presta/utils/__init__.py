"""
Utilities used by other modules.
"""

import csv
import os
import re
import string
import sys
import subprocess
import uuid

import xml.etree.ElementTree as ET
from alta import ConfigurationFromYamlFile
from pkg_resources import resource_filename


SAMPLES_WITHOUT_BARCODES = [2, 8]
DEFAULT_INDEX_CYCLES = dict(index='8', index1='8')


class IEMRunInfoReader:
    """
    Illumina Experimental Manager RunInfo xml reader.
    """

    def __init__(self, f):
        self.xml_file = f
        self.tree = ET.parse(self.xml_file)
        self.root = self.tree.getroot()

    def get_reads(self):
        reads = [r.attrib for r in self.root.iter('Read')]
        return reads

    def get_indexed_reads(self):
        reads = self.get_reads()
        return filter(lambda item: item["IsIndexedRead"] == "Y", reads)

    def get_index_cycles(self):
        indexed_reads = self.get_indexed_reads()
        return dict(
            index=next((item['NumCycles'] for item in indexed_reads
                        if item["IsIndexedRead"] == "Y" and item['Number'] == "2"), None),
            index1=next((item['NumCycles'] for item in indexed_reads
                         if item["IsIndexedRead"] == "Y" and item['Number'] != "2"), None))

    def get_default_index_cycles(self):
        return DEFAULT_INDEX_CYCLES

    def set_index_cycles(self, index_cycles, write=True):

        for read in self.root.iter('Read'):
            if read.attrib["IsIndexedRead"] == "Y":
                if read.attrib['Number'] == '2':
                    read.attrib.update(NumCycles=index_cycles.get('index', DEFAULT_INDEX_CYCLES['index']))
                else:
                    read.attrib.update(NumCycles=index_cycles.get('index', DEFAULT_INDEX_CYCLES['index']))
        if write:
            self.tree.write(self.xml_file)


class IEMSampleSheetReader(csv.DictReader):
    """
    Illumina Experimental Manager SampleSheet reader.
    """

    def __init__(self, f):
        csv.DictReader.__init__(self, f, delimiter=',')
        self.header = ''
        self.data = ''

        first_line = f.readline()
        if not first_line.startswith('[Header]'):
            raise ValueError('%s is not an IEM samplesheet'.format(f.name))
        header = [first_line.strip()]
        l = f.readline()
        while not l.startswith('[Data]'):
            header.append(l.strip())  # ms-dos
            l = f.readline()
        else:
            header.append(l.strip())
            self.header = header

        self.data = csv.DictReader(f.readlines(), delimiter=',')

    def barcodes_have_the_same_size(self):
        def mean(data):
            """Return the sample arithmetic mean of data."""
            n = len(data)
            if n < 1:
                raise ValueError('mean requires at least one data point')
            return sum(data) / float(n)

        def _ss(data):
            """Return sum of square deviations of sequence data."""
            c = mean(data)
            ss = sum((x - c) ** 2 for x in data)
            return ss

        def pstdev(data):
            """Calculates the population standard deviation."""
            n = len(data)

            if n < 2:
                raise ValueError('variance requires at least two data points')
            ss = _ss(data)
            pvar = ss / n  # the population variance
            return pvar ** 0.5

        lengths = []
        to_be_verified = ['index']

        for row in self.data:
            for f in self.data.fieldnames:
                if f in to_be_verified:
                    lengths.append(len(row[f]))

        if len(lengths) == 0:
            return True

        return True if pstdev(lengths) == float(0) else False

    def get_body(self, label='Sample_Name', new_value='', replace=True):
        def sanitize(mystr):
            """
            Sanitize string in accordance with Illumina's documentation
            bcl2fastq2 Conversion Software v2.17 Guide
            """
            retainlist = "_-"
            return re.sub(r'[^\w' + retainlist + ']', '_', mystr)

        body = []
        for i in self.header:
            body.append(i)
            body.append('\n')
        body.append(string.join(self.data.fieldnames, ','))
        body.append('\n')

        to_be_sanitized = ['Sample_Project', 'Sample_Name']

        for row in self.data:
            for f in self.data.fieldnames:
                if replace and f == label:
                    body.append(new_value)
                else:
                    if f in to_be_sanitized:
                        body.append(sanitize(row[f]))
                    else:
                        body.append(row[f])
                body.append(',')
            body.append('\n')

        return body

    def get_barcode_mask(self):
        barcodes_mask = dict()

        for row in self.data:
            if row['Lane'] not in barcodes_mask:
                barcodes_mask[row['Lane']] = dict(
                    index=len(row['index']) if 'index' in row else None,
                    index1=len(row['index1']) if 'index1' in row else None,
                )

        return barcodes_mask


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

    return True if os.path.exists(os.path.expanduser(path)) else file_missing(path,
                                                                              logger,
                                                                              force)


def sanitize_filename(filename):
    valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in filename if c in valid_chars)


def format_dataset_filename(sample_label, lane=None, read=None, ext=None, uid=False):
    filename = sanitize_filename(sample_label)

    if read:
        filename = '_'.join(
            [filename, lane, read]) if lane else '_'.join(
            [filename, read])

    if uid:
        filename = '.'.join([filename, str(uuid.uuid4())])

    if ext:
        filename = '.'.join([filename, ext])



    return sanitize_filename(filename)


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


def runJob(cmd, logger):
    try:
        # subprocess.check_output(cmd)
        process = subprocess.Popen(cmd,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        output = process.communicate()[0]
        ret = process.wait()
        return True
    except subprocess.CalledProcessError as e:
        logger.info(e)
        if e.output:
            logger.info("command output: %s", e.output)
        else:
            logger.info("no command output available")
        return False


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
