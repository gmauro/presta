import sys
import json
import yaml

from client import Client
from datasets import DatasetsManager

from presta.utils import path_exists, get_conf

OUTPUT_FORMAT = ['json', 'yaml']
SAMPLE_TYPES_TOSKIP = ['FLOWCELL', 'POOL']


class DictWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.output_format = args.output_format

        self.conf = get_conf(logger, args.config_file)
        self.io_conf = self.conf.get_io_section()

        # input path must exists as parser argument or as config file argument
        input_path = args.input_path if args.input_path else self.io_conf.get('archive_root_path')
        path_exists(input_path, self.logger)
        self.input_path = input_path

        output_file = args.output_file if args.output_file else None
        self.output_file = output_file

        self.batch_ids = args.batch_ids if args.batch_ids else list()

        c = Client(conf=self.conf, logger=self.logger)
        c.init_bika()
        self.bika = c.bk

        self.batches_info = dict()
        self.bids = list()

        for batch_id in self.batch_ids:
            batch_info = self.bika.get_batch_info(batch_id)
            if batch_info:
                bids = [_ for _ in batch_info.keys() if batch_info[_].get('type') not in SAMPLE_TYPES_TOSKIP]
                self.bids.extend(bids)
                self.batches_info.update(batch_info)
            else:
                logger.error('I have retrieved any information of the samples '
                             'owned by the batch {}'.format(batch_id))

        if len(self.bids) == 0:
            logger.error('I have retrieved any information of the batches '
                         '{}'.format(" ".join(self.batch_ids)))

            sys.exit()

    def dump(self, source, destination, ext):
        try:
            with open(destination, 'w+') as fp:
                if ext == 'json':
                    json.dump(source, fp)
                elif ext == 'yaml':
                    yaml.safe_dump(source, fp, default_flow_style=False, allow_unicode=True)
        except OSError as e:
            self.logger.error('Data not dumped. Error: {}'.format(e))
            return False
        return True

    def run(self):
        self.logger.info("searching for datasets in {}".format(self.input_path))
        dm = DatasetsManager(self.logger, self.bids)
        datasets_info, count = dm.collect_fastq_from_fs(self.input_path)

        self.logger.info("found {} files".format(count))

        samples = dict()
        units = dict()

        for bid in self.bids:
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

            if len(samples[request_id]) == 0:
                del samples[request_id]

        self.logger.info('Found {} samples'.format(len(samples.keys())))
        self.logger.info('Found {} units'.format(len(units.keys())))

        dictionary = dict(samples=samples,
                          units=units)

        self.dump(dictionary, self.output_file, self.output_format)


help_doc = """
Prepare dictionaries file to start post-processing
"""


def make_parser(parser):

    parser.add_argument('--batch_ids', '-b', nargs='+', type=str, required=True,
                        help='Batch id from BikaLims (List of)')

    parser.add_argument('--input_path', '-i', metavar="PATH",
                        help="Where input datasets are stored")

    parser.add_argument('--output_file', '-o', metavar="PATH", required=True,
                        help="Where output dictionary file have to be stored")

    parser.add_argument('--output_format', '-f', type=str, choices=OUTPUT_FORMAT,
                        default="json",
                        help='Output file format')


def implementation(logger, args):
    workflow = DictWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('dict', help_doc, make_parser,
                              implementation))
