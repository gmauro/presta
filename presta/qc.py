from presta.app.tasks import copy_qc_dirs


class qcWorkflow(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.dspath = args.ds_path
        self.exportpath = args.export_path

    def run(self):
        self.logger.info('Coping qc dirs from {} to {}'.format(self.dspath,
                                                               self.exportpath))
        copy_qc_dirs(self.dspath, self.exportpath)


help_doc = """
export quality control directories
"""


def make_parser(parser):
    parser.add_argument('--ds_path', metavar="PATH",
                        help="datasets path", required=True)
    parser.add_argument('--export_path', type=str, help='export path',
                        default='', required=True)


def implementation(logger, args):
    workflow = qcWorkflow(args=args, logger=logger)
    workflow.run()


def do_register(registration_list):
    registration_list.append(('qc', help_doc, make_parser,
                              implementation))
