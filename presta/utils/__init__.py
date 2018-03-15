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
import hashlib
import datetime
import json

import xml.etree.ElementTree as ET
from alta import ConfigurationFromYamlFile
from pkg_resources import resource_filename
from ..__details__ import __appname__


SAMPLES_WITHOUT_BARCODES = [2, 8]
DEFAULT_INDEX_CYCLES = dict(index='8', index1='8')
PROGRESS_STATUS = dict(COMPLETED='completed', STARTED='started', TODO='todo')


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

    def is_paired_end_sequencing(self):
        reads = self.get_reads()
        reads = filter(lambda item: item["IsIndexedRead"] == "N", reads)

        if len(reads) == 1:
            return False

        return True


class LogBook:
    """
    Logbook manager
    """

    def __init__(self, filename):
        self.filename = filename
        self.logfile = None
        self.logbook = dict()

    def dump(self):
        a = []
        if not os.path.isfile(self.filename):
            a.append(self.logbook)
            with open(self.filename, mode='w') as f:
                f.write(json.dumps(a, indent=4, sort_keys=True, default=str))
        else:
            with open(self.filename) as feedsjson:
                feeds = json.load(feedsjson)

            feeds.append(self.logbook)
            with open(self.filename, mode='w') as f:
                f.write(json.dumps(feeds, indent=4, sort_keys=True, default=str))

    def start(self, task_name, args=None):
        self.logbook.update(task_name=task_name)
        self.logbook.update(args=args)
        self.logbook.update(start_time=datetime.datetime.now())

    def end(self):
        self.logbook.update(end_time=datetime.datetime.now())
        execution_time = self.logbook.get('end_time') - self.logbook.get('start_time')
        self.logbook.update(execution_time=execution_time)
        self.dump()


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
        return False if self.get_barcode_mask() is None else True

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
                    if f in to_be_sanitized and row[f]:
                        body.append(sanitize(row[f]))
                    else:
                        body.append(row[f])
                body.append(',')
            body.append('\n')

        return body

    def get_barcode_mask(self):
        barcodes_mask = dict()

        for row in self.data:
            index = len(row['index']) if 'index' in row else None
            index1 = None

            if 'index1' in row or 'index2' in row:
                index1 = len(row['index2']) if 'index2' in row else len(row['index1'])

            if row['Lane'] not in barcodes_mask:
                barcodes_mask[row['Lane']] = dict(
                    index=index,
                    index1=index1,
                )
            else:
                if index != barcodes_mask[row['Lane']]['index'] or index1 != barcodes_mask[row['Lane']]['index1']:
                    return None

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
    presta_config_from_home = os.path.join(home, __appname__,
                                           'presta_config.yml')
    presta_config_from_package = resource_filename(__appname__,
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


def touch(path, logger):
    try:

        with open(path, 'a'):
            os.utime(path, None)
    except IOError as e:
        logger.error("While touching {} file: {}".format(path, e.strerror))


def read_chunks(file_handle, chunk_size=8192):
    while True:
        data = file_handle.read(chunk_size)
        if not data:
            break
        yield data


def get_md5(file_handle):
    hasher = hashlib.md5()
    for chunk in read_chunks(file_handle):
        hasher.update(chunk)
    return hasher.hexdigest()


def check_progress_status(root_path, started_file, completed_file):
    localroot, dirnames, filenames = os.walk(root_path).next()

    if started_file not in filenames:
        return PROGRESS_STATUS.get('TODO')
    elif completed_file not in filenames:
        return PROGRESS_STATUS.get('STARTED')
    else:
        started_file = os.path.join(root_path, started_file)
        completed_file = os.path.join(root_path, completed_file)

        if os.path.getmtime(started_file) > os.path.getmtime(completed_file):
            return PROGRESS_STATUS.get('STARTED')

    return PROGRESS_STATUS.get('COMPLETED')


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
