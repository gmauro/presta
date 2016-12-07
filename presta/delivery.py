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
import sys

from ansible.parsing.dataloader import DataLoader
from ansible.vars import VariableManager
from ansible.inventory import Inventory
from ansible.executor.playbook_executor import PlaybookExecutor
from alta.utils import ensure_dir
from collections import namedtuple
from client import Client
from datasets import DatasetsManager
from presta.app.tasks import copy
from presta.app.router import trigger_event, dispatch_event

from presta.utils import path_exists, get_conf, format_dataset_filename
from celery import chain


DESTINATIONS = ['collection', 'path', 'library', 'ftp']
SAMPLE_TYPES_TOSKIP = ['FLOWCELL', 'POOL']


class DeliveryWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.destination = args.destination
        self.dry_run = args.dry_run

        conf = get_conf(logger, args.config_file)
        self.conf = conf

        self.batch_id = batch_id = args.batch_id
        c = Client(conf=conf, logger=logger)
        c.init_bika()
        batch_info = c.bk.get_batch_info(batch_id)
        if batch_info:
            self.batch_info = batch_info
        else:
            logger.error('I have retrieved any information of the samples '
                         'owned by the batch {}'.format(batch_id))
            sys.exit()

        # input path must exists as parser argument or as config file argument
        if args.input_path:
            input_path = args.input_path
        else:
            io_conf = conf.get_io_section()
            input_path = io_conf.get('archive_root_path')
        path_exists(input_path, logger)
        self.input_path = input_path

        output_path = args.output_path if args.output_path else None
        self.output_path = output_path

        inventory = args.inventory if args.inventory else None
        self.inventory = inventory

        playbook_path = args.playbook_path if args.playbook_path else None
        self.playbook_path = playbook_path

        self.merge = args.merge

    def __fs2fs_carrier(self, ipath, opath):

        bids = [_ for _ in self.batch_info.keys() if self.batch_info[_].get(
            'type') not in SAMPLE_TYPES_TOSKIP]
        self.logger.info('Looking for files related to {} Bika ids'.format(
            len(bids)))
        self.logger.info('Starting from {}'.format(ipath))
        if len(bids) > 0:
            ensure_dir(os.path.join(opath, self.batch_id))

        dm = DatasetsManager(self.logger, bids)
        datasets_info, count = dm.collect_fastq_from_fs(ipath)

        self.logger.info("found {} files".format(count))

        to_be_merged = dict()

        for bid in bids:
            sample_label = self.batch_info[bid].get('client_sample_id')

            if bid not in to_be_merged:
                to_be_merged[bid] = dict()

            if bid in datasets_info:
                for f in datasets_info[bid]:
                    src = f.get('filepath')
                    read = f.get('read_label')
                    lane = f.get('lane')
                    ext = f.get('file_ext')

                    filename = format_dataset_filename(sample_label=sample_label,
                                                       lane=lane,
                                                       read=read,
                                                       ext=ext,
                                                       uid=True)

                    dst = os.path.join(opath, self.batch_id, filename)

                    self.logger.info("Coping {} into {}".format(src, dst))

                    if os.path.isfile(dst):
                        self.logger.info('{} skipped'.format(os.path.basename(
                            dst)))
                    else:
                        if not self.dry_run:
                            tsk = copy.si(src, dst).delay()
                            self.logger.info(
                                '{} copied'.format(os.path.basename(dst)))

                        if self.merge:
                            to_be_merged[bid][ext] = dict() if ext not in to_be_merged[bid] else to_be_merged[bid][ext]

                            if read not in to_be_merged[bid][ext]:
                                to_be_merged[bid][ext][read] = dict(src=list(), dst=list(), tsk=list())

                            to_be_merged[bid][ext][read]['src'].append(src)
                            to_be_merged[bid][ext][read]['dst'].append(dst)

                            if not self.dry_run and tsk:
                                to_be_merged[bid][ext][read]['tsk'].append(tsk.task_id)

            else:
                msg = 'I have not found any file related to this ' \
                      'Bika id: {}'.format(bid)

                self.logger.warning(msg)
                self.logger.info('{} skipped'.format(bid))
                del to_be_merged[bid]

        if self.merge:

            for bid, file_ext in to_be_merged.iteritems():
                sample_label = self.batch_info[bid].get('client_sample_id')
                for ext, reads in file_ext.iteritems():
                    for read, datasets in reads.iteritems():

                        filename = format_dataset_filename(sample_label=sample_label,
                                                           read=read,
                                                           ext=ext)
                        src = datasets['dst']
                        dst = os.path.join(opath, self.batch_id, filename)
                        tsk = datasets['tsk']

                        self.logger.info("Merging {} into {}".format(" ".join(src), dst))
                        if not self.dry_run:
                            merge_task = trigger_event.si(event='merge_datasets',
                                                          params=dict(src=src,
                                                                      dst=dst,
                                                                      remove_src=True),
                                                          tasks=tsk)
                            merge_task.delay()

    def __execute_playbook(self, playbook, inventory_file,
                           random_user, random_clear_text_password):
        path_exists(playbook, self.logger)
        path_exists(inventory_file, self.logger)

        variable_manager = VariableManager()
        loader = DataLoader()

        inventory = Inventory(loader=loader,
                              variable_manager=variable_manager,
                              host_list=inventory_file)

        Options = namedtuple('Options', ['listtags', 'listtasks',
                                         'listhosts', 'syntax', 'connection',
                                         'module_path', 'forks',
                                         'remote_user', 'private_key_file',
                                         'ssh_common_args', 'ssh_extra_args',
                                         'sftp_extra_args', 'scp_extra_args',
                                         'become', 'become_method',
                                         'become_user', 'verbosity', 'check'])

        options = Options(listtags=False, listtasks=False, listhosts=False,
                          syntax=False, connection='ssh', module_path=None,
                          forks=1, remote_user=None,
                          private_key_file=None, ssh_common_args=None,
                          ssh_extra_args=None, sftp_extra_args=None,
                          scp_extra_args=None, become=True,
                          become_method='sudo', become_user='root',
                          verbosity=None, check=False)

        variable_manager.extra_vars = {'r_user': random_user,
                                       'r_password': random_clear_text_password}
        passwords = {}

        pbex = PlaybookExecutor(playbooks=[playbook],
                                inventory=inventory,
                                variable_manager=variable_manager,
                                loader=loader, options=options,
                                passwords=passwords)
        results = pbex.run()
        return results

    def run(self):
        if self.destination == 'path':
            io_conf = self.conf.get_io_section()
            if self.output_path:
                output_path = self.output_path
            else:
                output_path = io_conf.get('ds_export_path')

            # if not path_exists(output_path, logger, force=False):
            #     ensure_dir(output_path)
            # path_exists(output_path, logger)
            self.__fs2fs_carrier(self.input_path, output_path)

        if self.destination == 'ftp':
            def pass_gen(length):
                import string
                import random

                ascii = string.ascii_letters + string.digits + '@-_'

                return "".join([list(set(ascii))[random.randint(0, len(list(set(
                    ascii))) - 1)] for i in range(length)])

            random_user = pass_gen(8)
            random_clear_text_password = pass_gen(12)

            self.logger.info('Creating random account into the ftp server')
            self.logger.info('user: {}'.format(random_user))
            self.logger.info('password: {}'.format(random_clear_text_password))

            playbook_label = 'create_ftp_user.yml'
            if self.playbook_path:
                playbook_path = self.playbook_path
            else:
                io_conf = self.conf.get_io_section()
                playbook_path = os.path.expanduser(io_conf.get('playbooks_path'))
            playbook = os.path.join(playbook_path, playbook_label)
            path_exists(playbook, self.logger)

            inventory_label = 'inventory'
            if self.inventory:
                inventory = self.inventory
            else:
                io_conf = self.conf.get_io_section()
                inventory_path = os.path.expanduser(io_conf.get('playbooks_path'))
                inventory = os.path.join(inventory_path,
                                         inventory_label)
            path_exists(inventory, self.logger)

            results = self.__execute_playbook(playbook,
                                              inventory,
                                              random_user,
                                              random_clear_text_password)
            self.logger.info('Playbook result: {}'.format(results))

            if self.output_path:
                output_path = self.output_path
            else:
                io_conf = self.conf.get_io_section()
                output_path = os.path.join(io_conf.get('ftp_export_path'),
                                           random_user)
            path_exists(output_path, self.logger)

            self.__fs2fs_carrier(self.input_path, output_path)


help_doc = """
Handle the delivery of NGS data obtained from the pre-processing step
"""


def make_parser(parser):
    parser.add_argument('--batch_id', metavar="STRING",
                        help="Batch id from BikaLims", required=True)
    parser.add_argument('--destination', '-d', type=str, choices=DESTINATIONS,
                        help='where datasets have to be delivered',
                        required=True)
    parser.add_argument('--merge', action='store_true', default=False,
                        help='Merge fastq sample from different lanes.')
    parser.add_argument('--dry_run', action='store_true', default=False,
                        help='Delivery will be only described.')
    parser.add_argument('--input_path', '-i', metavar="PATH",
                        help="Where input datasets are stored")
    parser.add_argument('--output_path', '-o', metavar="PATH",
                        help="Where output datasets have to be stored")
    parser.add_argument('--playbook_path', metavar="PATH",
                        help="Path to playbooks dir")
    parser.add_argument('--inventory', metavar="PATH",
                        help="Path to inventory file")


def implementation(logger, args):
    workflow = DeliveryWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('delivery', help_doc, make_parser,
                              implementation))
