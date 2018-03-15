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
from comoda import ensure_dir
from collections import namedtuple
from client import Client
from datasets import DatasetsManager
from .app.tasks import copy
from .app.lims import update_delivery_details
from .app.router import trigger_event, dispatch_event
from .utils import path_exists, get_conf, format_dataset_filename


DESTINATIONS = ['collection', 'path', 'library', 'ftp']
SAMPLE_TYPES_TOSKIP = ['FLOWCELL', 'POOL']
REVIEW_STATES_4_DELIVERY = ['ready']


class DeliveryWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger

        self.dry_run = args.dry_run
        self.md5_check = args.md5_check

        self.conf = get_conf(logger, args.config_file)
        self.io_conf = self.conf.get_io_section()

        self.delivery_id = args.delivery_id
        
        c = Client(conf=self.conf, logger=self.logger)
        c.init_bika()
        delivery_info = c.bk.get_delivery_info(delivery_id=self.delivery_id)
        if not delivery_info or not isinstance(delivery_info, dict):
            self.logger.error('No information retrieved about the '
                         'delivery {}'.format(self.delivery_id))
            sys.exit()
                
        self.delivery = delivery_info
        self.details = self.delivery.get('details', dict())
        self.samples_info = self.delivery.get('samples_info', dict())
        
        if self.delivery.get('review_state') not in REVIEW_STATES_4_DELIVERY:
            self.logger.error('Delivery {} is not ready to be '
                              'processed '.format(self.delivery_id))
            sys.exit()

        # input path must exists as config file argument
        input_paths = [self.io_conf.get('archive_root_path'),
                       self.io_conf.get('staging_root_path')]
        for input_path in input_paths:
            path_exists(input_path, self.logger)
        self.input_paths = input_paths

        inventory = args.inventory if args.inventory else None
        self.inventory = inventory

        playbook_path = args.playbook_path if args.playbook_path else None
        self.playbook_path = playbook_path

        self.destination = self.details.get('mode')
        self.merge = bool(self.details.get('merge'))
        self.runs = self.details.get('runs', [])
        self.output_path = self.details.get('path')

    def __fs2fs_carrier(self, input_paths, opath):

        self.delivery_started = os.path.join(opath,
                                             self.io_conf.get('delivery_started_file'))
        self.delivery_completed = os.path.join(opath,
                                               self.io_conf.get('delivery_completed_file'))

        self.merge_started = os.path.join(opath,
                                          self.io_conf.get('merge_started_file'))
        self.merge_completed = os.path.join(opath,
                                            self.io_conf.get('merge_completed_file'))

        bids = [_ for _ in self.delivery['samples_info'].keys() if self.delivery['samples_info'][_].get(
            'type') not in SAMPLE_TYPES_TOSKIP]

        if len(bids) > 0:
            for id, info in self.delivery['samples_info'].iteritems():
                batch_id = info.get('batch_id')
                path = os.path.join(opath, batch_id)
                if not self.dry_run and not os.path.exists(path):
                    ensure_dir(path)

        self.logger.info('Looking for files related to {} Bika ids'.format(len(bids)))

        dm = DatasetsManager(self.logger, bids)
        for path in input_paths:
            if self.runs and isinstance(self.runs, list) and len(self.runs) > 0:
                for run in self.runs:
                    ipath = os.path.join(path, run)
                    if os.path.exists(ipath):
                        self.logger.info('Searching in {}'.format(ipath))
                        datasets_info, count = dm.collect_fastq_from_fs(ipath)
                        self.logger.info("found {} files in {}".format(count, ipath))
            else:
                ipath = path
                if os.path.exists(ipath):
                    self.logger.info('Searching in  {}'.format(ipath))
                    datasets_info, count = dm.collect_fastq_from_fs(ipath)
                    self.logger.info("found {} files in {}".format(count, ipath))

        datasets_info = dm.fastq_collection
        count = dm.fastq_counter

        self.logger.info("found {} files".format(count))

        to_be_merged = dict()

        if not self.dry_run:
            dispatch_event.si(event='delivery_started',
                              params=dict(progress_status_file=self.delivery_started, delivery_id=self.delivery_id)
                              ).delay()

        for bid in bids:
            sample_label = self.samples_info[bid].get('client_sample_id')

            if bid not in to_be_merged:
                to_be_merged[bid] = dict()

            if bid in datasets_info:
                for f in datasets_info[bid]:
                    src = f.get('filepath')
                    read = f.get('read_label')
                    lane = f.get('lane')
                    ext = f.get('file_ext')
                    
                    batch_id = self.samples_info[bid].get('batch_id')

                    filename = format_dataset_filename(sample_label=sample_label,
                                                       lane=lane,
                                                       read=read,
                                                       ext=ext,
                                                       uid=True)

                    dst = os.path.join(opath, batch_id, filename)

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
                            if self.md5_check:
                                # MD5 CHECKSUM
                                self.logger.info("Getting MD5 hash of {}".format(dst))
                                if not self.dry_run:
                                    md5_task = trigger_event.si(event='get_md5_checksum',
                                                                params=dict(src=dst,
                                                                            dst=".".join([dst, 'md5'])),
                                                                tasks=[tsk.task_id]).delay()
                                    task_id = md5_task.get()

            else:
                msg = 'I have not found any file related to this ' \
                      'Bika id: {}'.format(bid)

                self.logger.warning(msg)
                self.logger.info('{} skipped'.format(bid))
                del to_be_merged[bid]

        if self.merge:
            if not self.dry_run:
                dispatch_event.si(event='merge_started',
                                  params=dict(progress_status_file=self.merge_started)
                                  ).delay()

            for bid, file_ext in to_be_merged.iteritems():
                sample_label = self.samples_info[bid].get('client_sample_id')
                for ext, reads in file_ext.iteritems():
                    for read, datasets in reads.iteritems():

                        filename = format_dataset_filename(sample_label=sample_label,
                                                           read=read,
                                                           ext=ext)
                        src = datasets['dst']
                        dst = os.path.join(opath, batch_id, filename)
                        tsk = datasets['tsk']

                        self.logger.info("Merging {} into {}".format(" ".join(src), dst))
                        if not self.dry_run:
                            merge_task = trigger_event.si(event='merge_datasets',
                                                          params=dict(src=src,
                                                                      dst=dst,
                                                                      remove_src=True),
                                                          tasks=tsk).delay()
                            task_id = merge_task.get()
                            if self.md5_check:
                                # MD5 CHECKSUM
                                self.logger.info("Getting MD5 hash of {}".format(dst))
                                md5_task = trigger_event.si(event='get_md5_checksum',
                                                            params=dict(src=dst,
                                                                        dst=".".join([dst, 'md5'])),
                                                            tasks=[task_id]).delay()
                                task_id = md5_task.get()

                            to_be_merged[bid][ext][read]['tsk'] = [task_id]

        if not self.dry_run:
            task_ids = list()
            for bid, file_ext in to_be_merged.iteritems():
                for ext, reads in file_ext.iteritems():
                    for read, datasets in reads.iteritems():
                        task_ids.extend(datasets['tsk'])

            trigger_event.si(event='delivery_completed',
                             params=dict(progress_status_file=self.delivery_completed, delivery_id=self.delivery_id),
                             tasks=task_ids).delay()

            if self.merge:
                trigger_event.si(event='merge_completed',
                                 params=dict(progress_status_file=self.merge_completed, delivery_id=self.delivery_id),
                                 tasks=task_ids).delay()

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
        if self.destination == 'MOUNT':
            output_path = self.output_path
            self.__fs2fs_carrier(self.input_paths, output_path)

        if self.destination == 'FTP':
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

            playbook_label = 'create_user.yml'
            playbook_path = self.playbook_path if self.playbook_path \
                else os.path.expanduser(self.io_conf.get('playbooks_path'))
            playbook = os.path.join(playbook_path, playbook_label)

            path_exists(playbook, self.logger)

            inventory_label = 'inventory'

            inventory = self.inventory if self.inventory \
                else os.path.join(os.path.expanduser(self.io_conf.get('playbooks_path')),
                                  inventory_label)

            path_exists(inventory, self.logger)

            results = self.__execute_playbook(playbook,
                                              inventory,
                                              random_user,
                                              random_clear_text_password)
            self.logger.info('Playbook result: {}'.format(results))

            output_path = os.path.join(self.io_conf.get('ftp_export_path'),
                                       random_user)
            path_exists(output_path, self.logger)

            if not self.dry_run:
                update_delivery_details.si(delivery_id=self.delivery_id,
                                           user=random_user,
                                           password=random_clear_text_password,
                                           path=output_path).delay()

            self.__fs2fs_carrier(self.input_paths, output_path)


help_doc = """
Handle the delivery of NGS data obtained from the pre-processing step
"""


def make_parser(parser):
    parser.add_argument('--delivery_id', '-d', metavar="STRING",
                        help="Delivery id from BikaLims", required=True)

    parser.add_argument('--dry_run', action='store_true', default=False,
                        help='Delivery will be only described.')

    parser.add_argument('--playbook_path', metavar="PATH",
                        help="Path to playbooks dir")
    parser.add_argument('--inventory', metavar="PATH",
                        help="Path to inventory file")
    parser.add_argument('--md5_checksum', dest='md5_check',
                        action='store_true',
                        help='Get MD5 hash of each file')
    parser.add_argument('--no_md5_checksum', dest='md5_check',
                        action='store_false',
                        help="Do not get MD5 hash of each file")
    parser.set_defaults(md5_check=True)


def implementation(logger, args):
    workflow = DeliveryWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('delivery', help_doc, make_parser,
                              implementation))
