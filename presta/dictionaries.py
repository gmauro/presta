import os
import sys
import textwrap
import json
import yaml

from client import Client
from datasets import DatasetsManager

from presta.utils import path_exists, get_conf

OUTPUT_FORMAT = ['json', 'yaml', 'tsv']
SAMPLE_TYPES_TOSKIP = ['FLOWCELL', 'POOL']


#
# input_data file example
#
# default_paths: # default path where to look for files
#   - "/default/path/1"
#   - "/default/path/2"
# batches:
#   0: # from this batchid, retrieve only these samples
#     bid: 01
#     samples:
#       - sid1
#       - sid2
#   1: # from this batchid, retrieve all the samples
#     bid: 02
#


class DictWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.output_format = args.output_format

        self.conf = get_conf(logger, args.config_file)
        self.io_conf = self.conf.get_io_section()

        c = Client(conf=self.conf, logger=self.logger)
        c.init_bika()
        self.bika = c.bk

        path_exists(args.input_file, self.logger)
        with open(args.input_file, 'r') as stream:
            input_data = yaml.safe_load(stream)

        self.input_paths = input_data.get('default_paths',
                                          [self.io_conf.get('archive_root_path')])
        for _ in self.input_paths:
            path_exists(_, self.logger)

        output_file = args.output_file if args.output_file else None
        if output_file != os.path.realpath(output_file):
            self.logger.error('{} is not a valid path. Please use absolute path'.format(output_file))
            sys.exit()
        self.output_file = output_file

        batches = input_data.get('batches', None)
            
        self.batches_info = dict()
        self.sids = list()

        for _, batch in batches.items():
            bid = batch.get('bid', None)
            samples = batch.get('samples', [])

            if bid:
                self.logger.info("Retrieving info for batch {}".format(bid))
                batch_info = self.bika.get_batch_info(bid, samples)
                if batch_info:
                    sids = [_ for _ in batch_info.keys() if batch_info[
                        _].get('type') not in SAMPLE_TYPES_TOSKIP]
                    self.sids.extend(sids)
                    self.batches_info.update(batch_info)
                else:
                    self.logger.error('No samples information found for the '
                                      'batch {}'.format(bid))

        if not self.sids:
            self.logger.error('I have not retrieve any information for the '
                              'batches {}'.format(" ".join(self.sids)))
            sys.exit()

    def dump(self, source, destination, ext):
        try:
            if ext == 'json':
                with open(destination, 'w+') as fp:
                    json.dump(source, fp)
            elif ext == 'yaml':
                with open(destination, 'w+') as fp:
                    yaml.safe_dump(source, fp, default_flow_style=False,
                                   allow_unicode=True)
            elif ext == 'tsv':
                with open('.'.join([destination, 'samples', ext]), 'w+') as fp:
                    fp.write("sample\todp\tunits\n")
                    for k, v in source['samples'].items():
                        fp.write("{}\t{}\t{}\n".format(k, 2500, ",".join(v)))
                with open('.'.join([destination, 'units', ext]), 'w+') as fp:
                    fp.write("sample\tunit\tfq1\tfq2\n")
                    for k, v in source['units'].items():
                        fp.write("{}\t{}\t{}\n".format(k.split('.')[2], k,
                                                       "\t".join(v)))

        except OSError as e:
            self.logger.error('Data not dumped. Error: {}'.format(e))
            return False
        return True

    def run(self):
        dm = DatasetsManager(self.logger, self.sids)
        samples = dict()
        units = dict()
        for ipath in self.input_paths:
            self.logger.info("searching for datasets in {}".format(ipath))
            datasets_info, count = dm.collect_fastq_from_fs(ipath)

            self.logger.info("found {} files".format(count))

            for bid in self.sids:
                request_id = None
                if bid in datasets_info:
                    request_id = self.batches_info[bid].get('sample_label')
                    if request_id not in samples:
                        samples[request_id] = list()
                    for f in datasets_info[bid]:
                        src = f.get('filepath')
                        lane = f.get('lane')
                        fc = f.get('fc_label')

                        if lane is None:
                            lane = 'L0000'

                        unit = ".".join([fc, lane, request_id])

                        if unit not in samples[request_id]:
                            samples[request_id].append(unit)

                        if unit not in units:
                            units[unit] = list()

                        if src not in units[unit]:
                            units[unit].append(src)

                if request_id in samples and len(samples[request_id]) == 0:
                    del samples[request_id]

        self.logger.info("Writing in output file: {} ".format(self.output_file))
        self.logger.info('Found {} samples'.format(len(samples.keys())))
        self.logger.info('Found {} units'.format(len(units.keys())))

        dictionary = dict(samples=samples, units=units)

        self.dump(dictionary, self.output_file, self.output_format)


help_doc = """
Prepare dictionaries file to start post-processing
"""

input_file_doc = 'presta dict --example print an example of input file'

input_file_example = '''                                                                                 
input file example:                                                                                      

 default_paths: # default path where to look for files                                                     
   - "/default/path/1"                                                                                     
   - "/default/path/2"                                                                                     
 batches:                                                                                                  
   0: # from this batchid, retrieve only these samples                                                     
     bid: 01                                                                                               
     samples:                                                                                              
       - sid1                                                                                              
       - sid2                                                                                              
   1: # from this batchid, retrieve all the samples                                                        
     bid: 02                                                                                               
'''


def make_parser(parser):

    parser.add_argument('--input_file', '-i', metavar="PATH",
                        help="Yaml file with a list of batch ids and/or of "
                             "sample ids from Bikalims")

    parser.add_argument('--output_file', '-o', metavar="PATH",
                        help="Absolute path to the output dictionary file")

    parser.add_argument('--output_format', '-f', type=str, choices=OUTPUT_FORMAT,
                        default="json", help='Output file format')

    parser.add_argument('--example', '-x', dest='show_example',
                        action='store_true', help='print input file example')

    parser.epilog = input_file_doc


def implementation(logger, args):
    if args.show_example:
        print(textwrap.dedent(input_file_example))
        sys.exit()

    if not args.input_file or not args.output_file:
        print('****')
        print(' Please provide input and output files')
        print(' see: presta dict -h')
        print('****')
        sys.exit()
        
    workflow = DictWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('dict', help_doc, make_parser,
                              implementation))
