"""

"""

import os


class DatasetsManager(object):
    def __init__(self, logger, ids):
        self.ids = ids
        self.logger = logger

    def collect_fastq_from_fs(self, path):
        results = dict()
        file_ext = 'fastq.gz'
        dir_label = 'datasets'
        for (localroot, dirnames, filenames) in os.walk(path):
            if dir_label in localroot.split('/'):
                for fname in filenames:
                    extended_id = fname.split('_')[0]
                    _id = '-'.join(extended_id.split('-')[:-1])
                    ext = '.'.join(fname.split('.')[1:])
                    if ext == file_ext and _id in self.ids:
                        list_item = {'extended_id': extended_id,
                                     'filename': fname,
                                     'filepath': os.path.join(localroot,
                                                              fname),
                                     'file_ext': file_ext,
                                     '_id': _id,
                                     'read_label': fname.split('_')[2],
                                     }
                        if _id not in results:
                            results[_id] = []
                        results[_id].append(list_item)

        return results

    @staticmethod
    def collect_fastq_from_irods(ipath):
        pass

