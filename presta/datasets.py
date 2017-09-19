"""

"""

import glob
import os


class DatasetsManager(object):
    def __init__(self, logger, ids):
        self.ids = ids
        self.logger = logger
        self.fastq_collection = dict()
        self.fastq_counter = 0

    def collect_fastq_from_fs(self, base_path):
        results = dict()
        count = 0
        file_ext = 'fastq.gz'
        ds_dir_label = 'datasets'
        filesDepth = []
        for depth in ['*', '*/*', '*/*/*', '*/*/*/*']:
            filesGlob = glob.glob(os.path.join(base_path, depth))
            filesDepth.extend(filter(lambda f: os.path.isfile(f) and
                                     ds_dir_label in f.split('/') and
                                     file_ext in f.split('/')[-1],
                                     filesGlob))
        for path in filesDepth:
            fname = os.path.basename(path)
            extended_id = fname.split('_')[0]
            _id = '-'.join(extended_id.split('-')[:-1])
            if _id in self.ids:
                rd_label_index = path.split('/').index(ds_dir_label) - 1
                rd_label = path.split('/')[rd_label_index]
                fc_label = rd_label.split('_')[-1][1:]

                list_item = dict(extended_id=extended_id,
                                 filename=fname,
                                 filepath=path,
                                 file_ext=file_ext,
                                 _id=_id,
                                 lane=fname.split('_')[2] if fname.split('_')[2].startswith('L') else None,
                                 read_label=fname.split('_')[2] if fname.split('_')[2].startswith('R') else fname.split('_')[3],
                                 rd_label=rd_label,
                                 fc_label=fc_label,
                                 )
                if _id not in results:
                    results[_id] = []
                if _id not in self.fastq_collection:
                    self.fastq_collection[_id] = []

                results[_id].append(list_item)
                self.fastq_collection[_id].append(list_item)

                count += 1
                self.fastq_counter += 1

        return results, count

    @staticmethod
    def collect_fastq_from_irods(ipath):
        pass
