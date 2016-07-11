"""
Handle the delivery of NGS data obtained from the pre-processing step

 inputs: datasets in fastq.gz format retrieved from a filesystem path
 destinations:
     an iRODS collection
     a different filesystem path
     a library of a Galaxy Server
     a folder of a FTP server
"""

import os
from alta.utils import ensure_dir
from client import Client
from datasets import DatasetsManager
from presta.utils import path_exists, get_conf
from shutil import copyfile

DESTINATIONS = ['collection', 'path', 'library', 'ftp_folder']


class DeliveryWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.batch_id = args.batch_id
        self.destination = args.destination
        self.dry_run = args.dry_run

        conf = get_conf(logger, args.config_file)
        self.conf = conf

        # input path must exists as parser argument or as config file argument
        if args.input_path:
            input_path = args.input_path
        else:
            io_conf = conf.get_io_section()
            input_path = io_conf.get('archive_root_path')
        path_exists(input_path, logger)
        self.input_path = input_path

        # output path must exists as parser argument or as config file argument
        if args.output_path:
            output_path = args.output_path
        else:
            io_conf = conf.get_io_section()
            output_path = io_conf.get('ds_export_path')

        if not path_exists(output_path, logger, force=False):
            ensure_dir(output_path)
        path_exists(output_path, logger)
        self.output_path = output_path

    def __fs_carrier(self, ipath, opath):
        c = Client(conf=self.conf, logger=self.logger)
        c.init_bika()
        batch_info = c.bk.get_batch_info(self.batch_id)
        bids = [_ for _ in batch_info.keys() if batch_info[_].get('type') in
                ['SAMPLE-IN-FLOWCELL']]
        self.logger.info('Looking for files related to {} Bika ids'.format(
            len(bids)))
        self.logger.info('Starting from {}'.format(ipath))
        if len(bids) > 0:
            ensure_dir(os.path.join(opath, self.batch_id))

        dm = DatasetsManager(self.logger, bids)
        datasets_info, count = dm.collect_fastq_from_fs(ipath)
        self.logger.info("found {} files".format(count))

        for bid in bids:
            if bid in datasets_info:
                for f in datasets_info[bid]:
                    src = f.get('filepath')
                    read = f.get('read_label')
                    ext = f.get('file_ext')
                    sample_label = batch_info[bid].get('client_sample_id')
                    sample_label = '_'.join(
                        [sample_label.replace(' ', '_'), read])
                    sample_label = '.'.join([sample_label, ext])
                    dst = os.path.join(opath, self.batch_id, sample_label)

                    self.logger.info("Coping {} into {}".format(src, dst))
                    if os.path.isfile(dst):
                        self.logger.info('{} skipped'.format(os.path.basename(
                            dst)))
                    else:
                        if not self.dry_run:
                            copyfile(src, dst)
                            self.logger.info(
                                '{} copied'.format(os.path.basename(dst)))
            else:
                msg = 'I have not found any file related to this ' \
                      'Bika id: {}'.format(bid)
                self.logger.warning(msg)
                self.logger.info('{} skipped'.format(bid))

    def run(self):
        if self.destination == 'path':
            self.__fs_carrier(self.input_path, self.output_path)


help_doc = """
Handle the delivery of NGS data obtained from the pre-processing step
"""


def make_parser(parser):
    parser.add_argument('--batch_id', metavar="STRING",
                        help="Batch id from BikaLims", required=True)
    parser.add_argument('--destination', '-d', type=str, choices=DESTINATIONS,
                        help='where datasets have to be delivered',
                        required=True)
    parser.add_argument('--dry_run', action='store_true', default=False,
                        help='Delivery will be only described.')
    parser.add_argument('--input_path', '-i', metavar="PATH",
                        help="Where input datasets are stored")
    parser.add_argument('--output_path', '-o', metavar="PATH",
                        help="Where output datasets have to be stored")


def implementation(logger, args):
    workflow = DeliveryWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('delivery', help_doc, make_parser,
                              implementation))
