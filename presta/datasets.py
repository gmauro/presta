"""

"""

import os


class DatasetsManager(object):
    def __init__(self, logger):
        self.logger = logger

    @staticmethod
    def collect_fastq_from_fs(path):
        results = dict()
        file_ext = 'fastq.gz'
        dir_label = 'datasets'
        for (localroot, dirnames, filenames) in os.walk(path):
            if dir_label in localroot.split('/'):
                for fname in filenames:
                    if '.'.join(fname.split('.')[1:]) == file_ext:
                        extended_id = fname.split('_')[0]
                        id = '-'.join(extended_id.split('-')[:-1])
                        list_item = {'extended_id': extended_id,
                                     'filename': fname,
                                     'filepath': os.path.join(localroot,
                                                              fname),
                                     'file_ext': file_ext,
                                     'id': id,
                                     'read_label': fname.split('_')[2],
                                     }
                        if not id in results:
                            results[id] = []
                        results[id].append(list_item)

        return results

    @staticmethod
    def collect_fastq_from_irods(ipath):
        pass
