from __future__ import absolute_import

from . import app
from alta.objectstore import build_object_store
from alta.utils import ensure_dir
from celery import group
import drmaa
from grp import getgrgid
from presta.utils import IEMSampleSheetReader
from presta.utils import IEMRunInfoReader
from presta.utils import runJob
from presta.utils import get_conf
from presta.utils import touch
from presta.utils import check_progress_status, PROGRESS_STATUS

from pwd import getpwuid
import errno
import os
import shlex
import shutil

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@app.task(name='presta.app.tasks.run_presta_check')
def run_presta_check(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    root_path = kwargs.get('root_path')
    cmd_line = ['presta', 'check']

    if emit_events:
        cmd_line.append('--emit_events')
    if root_path:
        cmd_line.extend(['--root_path', root_path])

    result = runJob(cmd_line, logger)
    return True if result else False


@app.task(name='presta.app.tasks.run_presta_proc')
def run_presta_proc(**kwargs):
    emit_events = kwargs.get('emit_events', False)

    rd_label = kwargs.get('rd_label')
    rd_path = kwargs.get('rd_path')
    ds_path = kwargs.get('ds_path')

    cmd_line = ['presta', 'proc']
    if rd_label:
        cmd_line.extend(['--rd_label', rd_label])

        if rd_path:
            cmd_line.extend(['--rd_path', rd_path])
        if ds_path:
            cmd_line.extend(['--ds_path', ds_path])
        if emit_events:
            cmd_line.append('--emit_events')

        result = runJob(cmd_line, logger)
        return True if result else False

    return False


@app.task(name='presta.app.tasks.run_presta_qc')
def run_presta_qc(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    rd_label = kwargs.get('rd_label')
    ds_path = kwargs.get('ds_path')
    qc_path = kwargs.get('qc_path')
    qc_export_path = kwargs.get('qc_export_path')
    rerun = kwargs.get('force')

    cmd_line = ['presta', 'qc']

    if rd_label:
        cmd_line.extend(['--rd_label', rd_label])
    if qc_export_path:
        cmd_line.extend(['--qc_export_path', qc_export_path])
    if ds_path:
        cmd_line.extend(['--ds_path', ds_path])
    if qc_path:
        cmd_line.extend(['--qc_path', qc_path])
    if rerun:
        cmd_line.append('--rerun')
    if emit_events:
        cmd_line.append('--emit_events')

    result = runJob(cmd_line, logger)
    return True if result else False


@app.task(name='presta.app.tasks.run_presta_sync')
def run_presta_sync(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    rundir_label = kwargs.get('rd_label')
    force = kwargs.get('force')

    cmd_line = ['presta', 'sync']

    if emit_events:
        cmd_line.append('--emit_events')
    if rundir_label:
        cmd_line.extend(['--rundir_label', rundir_label])
    if force:
        cmd_line.append('--force')

    result = runJob(cmd_line, logger)
    return True if result else False


@app.task(name='presta.app.tasks.rd_collect_fastq')
def rd_collect_fastq(**kwargs):
    path = kwargs.get('ds_path')
    results = []
    for (localroot, dirnames, filenames) in os.walk(path):
        for f in filenames:
            if f[-3:] == '.gz':
                logger.info('FASTQ = {}'.format(f))
                results.append(os.path.join(localroot, f))
    return results


@app.task(name='presta.app.tasks.rd_ready_to_be_preprocessed')
def rd_ready_to_be_preprocessed(**kwargs):
    """
    Verify if sequencer has ended to write, if rundir's ownership is
    correct and if samplesheet has been uploaded into iRODS
    """
    path = kwargs.get('path')
    user = kwargs.get('user')
    grp = kwargs.get('group')
    rundir_label = kwargs.get('rd_label')
    samplesheet_filename = kwargs.get('ssht_filename', 'SampleSheet.csv')
    ir_conf = kwargs.get('ir_conf')
    io_conf = kwargs.get('io_conf')
    ipath = os.path.join(ir_conf['runs_collection'],
                         rundir_label,
                         samplesheet_filename)

    task0 = seq_completed.si(path)
    task1 = check_ownership.si(user=user, group=grp, dir=path)
    task2 = samplesheet_ready.si(ir_conf, ipath)
    task3 = check_metadata.si(ir_conf, os.path.dirname(ipath))
    task4 = check_preprocessing_status.si(rd_path=path, io_conf=io_conf)

    pipeline = group([task0, task1, task2, task3, task4])

    result = pipeline.apply_async()
    return result.join()


@app.task(name='presta.app.tasks.samplesheet_ready')
def samplesheet_ready(ir_conf, ipath):

    ir = build_object_store(store='irods',
                            host=ir_conf['host'],
                            port=ir_conf['port'],
                            user=ir_conf['user'],
                            password=ir_conf['password'].encode('ascii'),
                            zone=ir_conf['zone'])
    try:
        exists, iobj = ir.exists(ipath, delivery=True)
        ir.sess.cleanup()
    except:
        ir.sess.cleanup()

    if exists:
        with iobj.open('r') as f:
            samplesheet = IEMSampleSheetReader(f)

        return exists, samplesheet.barcodes_have_the_same_size()
    else:
        return False, False


@app.task(name='presta.app.tasks.check_metadata')
def check_metadata(ir_conf, ipath, get_metadata=False):

    def retrieve_imetadata(iobj):
        return [dict(name=m.name,
                     value=m.value,
                     units=m.units)
                for m in iobj.metadata.items()]

    ir = build_object_store(store='irods',
                            host=ir_conf['host'],
                            port=ir_conf['port'],
                            user=ir_conf['user'],
                            password=ir_conf['password'].encode('ascii'),
                            zone=ir_conf['zone'])
    try:
        exists, iobj = ir.exists(ipath, delivery=True)
        ir.sess.cleanup()
    except:
        ir.sess.cleanup()

    if get_metadata:
        return exists and len(iobj.metadata.items()) > 0, retrieve_imetadata(iobj)

    return exists and len(iobj.metadata.items()) > 0


@app.task(name='presta.app.tasks.seq_completed')
def seq_completed(rd_path):
    illumina_last_file = 'RTAComplete.txt'
    localroot, dirnames, filenames = os.walk(rd_path).next()
    return True if illumina_last_file in filenames else False


@app.task(name='presta.app.task.check_ownership')
def check_ownership(**kwargs):
    user = kwargs.get('user')
    grp = kwargs.get('group')
    d = kwargs.get('dir')

    def find_owner(directory):
        try:
            return getpwuid(os.stat(directory).st_uid).pw_name
        except:
            return ''

    def find_group(directory):
        try:
            return getgrgid(os.stat(directory).st_gid).gr_name
        except:
            return ''

    return True if user == find_owner(d) and grp == find_group(d) else False


@app.task(name='presta.app.task.check_preprocessing_status')
def check_preprocessing_status(**kwargs):
    rd_path = kwargs.get('rd_path')
    io_conf = kwargs.get('io_conf')

    rd_progress_status = check_rd_progress_status(rd_path=rd_path, io_conf=io_conf)

    return rd_progress_status in PROGRESS_STATUS.get('TODO')


@app.task(name='presta.app.tasks.copy')
def copy(src, dest):
    result = False
    try:
        if os.path.exists(dest):
            shutil.rmtree(dest)

        shutil.copytree(src, dest)
        result = True
    except OSError as e:
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            logger.error('Source not copied. Error: {}'.format(e))
    return result


@app.task(name='presta.app.tasks.remove')
def remove(files=list()):
    result = False
    try:
        for f in files:
            if os.path.exists(f):
                os.remove(f)
        result = True
    except OSError as e:
        logger.error('Source not copied. Error: {}'.format(e))
    return result


@app.task(name='presta.app.tasks.copy_qc_dirs', ignore_result=True)
def copy_qc_dirs(trigger=None,  **kwargs):

    if trigger is False:
        return trigger

    src = kwargs.get('src')
    dest = kwargs.get('dest')

    dirs = ['Stats', 'Reports', 'fastqc']
    ensure_dir(dest)
    task0 = copy.si(os.path.join(src, dirs[0]), os.path.join(dest, dirs[0]))
    task1 = copy.si(os.path.join(src, dirs[1]), os.path.join(dest, dirs[1]))
    task2 = copy.si(os.path.join(src, dirs[2]), os.path.join(dest, dirs[2]))

    job = group([task0, task1, task2])

    result = job.apply_async()

    return result
    #return result.join()


@app.task(name='presta.app.tasks.merge')
def merge(**kwargs):

    src = kwargs.get('src', list())
    dst = kwargs.get('dst', None)
    remove_src = kwargs.get('remove_src', False)

    if isinstance(src, list) and len(src) > 0:

        try:
            with open(dst, 'wb') as outfile:
                for infile in src:
                    shutil.copyfileobj(open(infile), outfile)

            if not os.path.exists(dst):
                return False

            if remove_src:
                task = remove.si(src)
                task.delay()

            return True

        except OSError as e:
            logger.error('Sources not merged. Error: {}'.format(e))
            return list()

    return True


@app.task(name='presta.app.tasks.set_progress_status')
def set_progress_status(**kwargs):
    progress_status_file = kwargs.get('progress_status_file')
    return touch(progress_status_file)


@app.task(name='presta.app.tasks.sanitize_metadata', ignore_result=True)
def sanitize_metadata(**kwargs):
    ir_conf = kwargs.get('conf')
    rundir_label = kwargs.get('rd_label')
    samplesheet_filename = kwargs.get('ssht_filename')
    sanitize = kwargs.get('sanitize')

    if sanitize:
        rundir_ipath = os.path.join(ir_conf['runs_collection'],
                                    rundir_label)

        samplesheet_ipath = os.path.join(ir_conf['runs_collection'],
                                         rundir_label,
                                         samplesheet_filename)

        samplesheet_has_metadata, imetadata = check_metadata(ir_conf=ir_conf,
                                                             ipath=samplesheet_ipath,
                                                             get_metadata=True)
        if samplesheet_has_metadata:
            _set_imetadata(ir_conf=ir_conf,
                           ipath=rundir_ipath,
                           imetadata=imetadata)


@app.task(name='presta.app.tasks.copy_samplesheet_from_irods',
          ignore_result=True)
def copy_samplesheet_from_irods(**kwargs):
    ir_conf = kwargs.get('conf')
    samplesheet_file_path = kwargs.get('ssht_path')
    samplesheet_filename = os.path.basename(samplesheet_file_path)
    rundir_label = kwargs.get('rd_label')
    overwrite_samplesheet = kwargs.get('overwrite_samplesheet')

    if overwrite_samplesheet:
        ir = build_object_store(store='irods',
                                host=ir_conf['host'],
                                port=ir_conf['port'],
                                user=ir_conf['user'],
                                password=ir_conf['password'].encode('ascii'),
                                zone=ir_conf['zone'])

        ipath = os.path.join(ir_conf['runs_collection'],
                             rundir_label,
                             samplesheet_filename)
        logger.info('Coping samplesheet from iRODS {} to FS {}'.format(
            ipath, samplesheet_file_path))
        try:
            ir.get_object(ipath, dest_path=samplesheet_file_path)
            ir.sess.cleanup()
        except:
            ir.sess.cleanup()
    return samplesheet_file_path


@app.task(name='presta.app.tasks.copy_run_info_to_irods',
          ignore_result=True)
def copy_run_info_to_irods(**kwargs):
    ir_conf = kwargs.get('conf')
    run_info_file_path = kwargs.get('run_info_path')
    run_info_filename = os.path.basename(run_info_file_path)
    rundir_label = kwargs.get('rd_label')

    irods_path = os.path.join(ir_conf['runs_collection'],
                              rundir_label,
                              run_info_filename)

    _copy_file_into_irods(conf=ir_conf,
                          file_path=run_info_file_path,
                          irods_path=irods_path)

    return run_info_file_path


@app.task(name='presta.app.tasks.copy_run_parameters_to_irods',
          ignore_result=True)
def copy_run_parameters_to_irods(**kwargs):
    ir_conf = kwargs.get('conf')
    run_parameters_file_path = kwargs.get('run_parameters_path')
    run_parameters_filename = os.path.basename(run_parameters_file_path)
    rundir_label = kwargs.get('rd_label')

    irods_path = os.path.join(ir_conf['runs_collection'],
                              rundir_label,
                              run_parameters_filename)

    _copy_file_into_irods(conf=ir_conf,
                          file_path=run_parameters_file_path,
                          irods_path=irods_path)

    return run_parameters_file_path


@app.task(name='presta.app.tasks.replace_values_into_samplesheet',
          ignore_result=True)
def replace_values_into_samplesheet(**kwargs):

    samplesheet_file_path = kwargs.get('ssht_path')
    overwrite_samplesheet = kwargs.get('overwrite_samplesheet')

    if overwrite_samplesheet:
        with open(samplesheet_file_path, 'r') as f:
            samplesheet = IEMSampleSheetReader(f)

        with open(samplesheet_file_path, 'w') as f:
            for row in samplesheet.get_body(replace=True):
                f.write(row)


@app.task(name='presta.app.tasks.replace_index_cycles_into_run_info',
          ignore_result=True)
def replace_index_cycles_into_run_info(**kwargs):
    ir_conf = kwargs.get('conf')
    overwrite_run_info_file = not kwargs.get('barcodes_have_same_size')
    run_info_file_path = kwargs.get('run_info_path')
    rundir_label = kwargs.get('rd_label')

    if overwrite_run_info_file:
        index_cycles_from_metadata = _get_index_cycles_from_metadata(ir_conf=ir_conf,
                                                                     rundir_label=rundir_label)

        index_cycles_from_run_info_file, default_index_cycles = _get_index_cycles_from_run_info_file(
            run_info_file_path=run_info_file_path,
            get_default_values=True)

        index_cycles = default_index_cycles \
            if index_cycles_from_metadata == index_cycles_from_run_info_file\
            else index_cycles_from_metadata

        logger.info('Editing index cycles on: {}\n'
                    'Old values:{}\n'
                    'New values: {}'.format(run_info_file_path,
                                            index_cycles_from_run_info_file,
                                            index_cycles))

        run_info_file = IEMRunInfoReader(run_info_file_path)
        run_info_file.set_index_cycles(index_cycles)


@app.task(name='presta.app.tasks.move', ignore_result=True)
def move(src, dest):
    try:
        shutil.move(src, dest)
    except shutil.Error as e:
        logger.error('Source not moved. Error: {}'.format(e))


@app.task(name='presta.app.tasks.bcl2fastq')
def bcl2fastq(**kwargs):
    rd_path = kwargs.get('rd_path')
    ds_path = kwargs.get('ds_path')
    ssht_path = kwargs.get('ssht_path')
    run_info_file_path = kwargs.get('run_info_path')
    no_lane_splitting = kwargs.get('no_lane_splitting', False)
    barcode_mismatches = kwargs.get('barcode_mismatches', 1)
    submit_to_batch_scheduler = kwargs.get('batch_queuing', True)
    queue_spec = kwargs.get('queue_spec')

    command = 'bcl2fastq'
    rd_arg = '-R {}'.format(rd_path)
    output_arg = '-o {}'.format(ds_path)
    samplesheet_arg = '--sample-sheet {}'.format(ssht_path)
    options = ['--ignore-missing-bcls',
               '--ignore-missing-filter',
               '--ignore-missing-positions',
               '--find-adapters-with-sliding-window',
               '--barcode-mismatches {}'.format(barcode_mismatches)]

    if no_lane_splitting:
        options.append('--no-lane-splitting')

    with open(ssht_path, 'r') as f:
        samplesheet = IEMSampleSheetReader(f)

    barcode_mask = samplesheet.get_barcode_mask()
    is_paired_end = _is_paired_end(run_info_file_path=run_info_file_path)

    for lane, barcode_length in barcode_mask.items():
        if barcode_length['index'] is None or barcode_length['index'] in ['None']:
            continue

        elif barcode_length['index1'] is None or barcode_length['index1'] in ['None']:
            mask = "{}:Y*,I{}n*,Y*".format(lane, barcode_length['index']) if is_paired_end \
                else "{}:Y*,I{}n*".format(lane, barcode_length['index'])
        else:
            mask = "{}:Y*,I{}n*,I{}n*,Y*".format(lane, barcode_length['index'], barcode_length['index1']) \
                if is_paired_end else "{}:Y*,I{}n*,I{}n*".format(lane, barcode_length['index'], barcode_length['index1'])

        options.append("--use-bases-mask {}".format(mask))

    cmd_line = shlex.split(' '.join([command, rd_arg, output_arg,
                                    samplesheet_arg, ' '.join(options)]))
    logger.info('Executing {}'.format(cmd_line))

    if submit_to_batch_scheduler:
        home = os.path.expanduser("~")
        launcher = kwargs.get('launcher', 'launcher')

        jt = {'jobName': command,
              'nativeSpecification': queue_spec,
              'remoteCommand': os.path.join(home, launcher),
              'args': cmd_line
              }
        try:
            output = runGEJob(jt)
        except:
            output = runJob(cmd_line, logger)
    else:
        output = runJob(cmd_line, logger)

    return True if output else False


@app.task(name='presta.app.tasks.qc_runner')
def qc_runner(file_list, **kwargs):
    def chunk(lis, n):
        return [lis[i:i + n] for i in range(0, len(lis), n)]

    chunk_size = kwargs.get('chunk_size', 6)
    tasks = list()
    for f in chunk(file_list, chunk_size):
        task = fastqc.s(f, outdir=kwargs.get('outdir'),
                        threads=chunk_size,
                        batch_queuing=kwargs.get('batch_queuing'),
                        queue_spec=kwargs.get('queue_spec')
                        ).delay()
        tasks.append(task.task_id)
    return tasks


@app.task(name='presta.app.tasks.fastqc')
def fastqc(fq_list, **kwargs):
    command = 'fastqc'
    output_arg = '--outdir {}'.format(kwargs.get('outdir'))
    options = ['--format fastq',
               '--threads {}'.format(kwargs.get('threads', 1))]
    fq_list_arg = ' '.join(fq_list)
    submit_to_batch_scheduler = kwargs.get('batch_queuing', True)
    queue_spec = kwargs.get('queue_spec')

    cmd_line = shlex.split(' '.join([command, output_arg, ' '.join(options),
                                     fq_list_arg]))
    logger.info('Executing {}'.format(cmd_line))

    if submit_to_batch_scheduler:
        home = os.path.expanduser("~")
        launcher = kwargs.get('launcher', 'launcher')

        jt = {'jobName': command,
              'nativeSpecification': queue_spec,
              'remoteCommand': os.path.join(home, launcher),
              'args': cmd_line
              }
        try:
            output = runGEJob(jt)
        except:
            output = runJob(cmd_line, logger)

    else:
        output = runJob(cmd_line, logger)

    return True if output else False


@app.task(name='presta.app.tasks.rd_collect_samples')
def rd_collect_samples(**kwargs):
    ir_conf = kwargs.get('conf')
    rundir_label = kwargs.get('rd_label')
    samplesheet_filename = kwargs.get('samplesheet_filename', 'SampleSheet.csv')

    samples = []
    ir = build_object_store(store='irods',
                            host=ir_conf['host'],
                            port=ir_conf['port'],
                            user=ir_conf['user'],
                            password=ir_conf['password'].encode('ascii'),
                            zone=ir_conf['zone'])

    ipath = os.path.join(ir_conf['runs_collection'],
                         rundir_label,
                         samplesheet_filename)

    try:
        exists, iobj = ir.exists(ipath, delivery=True)
        ir.sess.cleanup()
    except:
        ir.sess.cleanup()

    if exists:
        with iobj.open('r') as f:
            samplesheet = IEMSampleSheetReader(f)

        samples = [dict(
            id=r['Sample_ID'],
            name=r['Sample_Name']
        ) for r in samplesheet.data]

    return samples


@app.task(name='presta.app.tasks.search_rd_to_archive')
def search_rd_to_archive(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    conf = get_conf(logger, None)
    io_conf = conf.get_io_section()

    rd_root_path = kwargs.get('rd_root_path') if kwargs.get('rd_root_path') \
        else io_conf.get('rundirs_root_path')
    archive_root_path = kwargs.get('archive_root_path') if kwargs.get('archive_root_path') \
        else io_conf.get('archive_root_path')

    logger.info('Checking rundirs in: {}'.format(rd_root_path))
    localroot, dirnames, filenames = os.walk(rd_root_path).next()

    for rd_label in dirnames:
        rd_path = os.path.join(localroot,
                               rd_label)

        if check_rd_to_archive(rd_path=rd_path,
                               io_conf=io_conf):

            logger.info('{} processed. Ready to be archived'.format(rd_label))
            if emit_events:
                archive_task = archive_rd.si(rd_path=rd_path,
                                             archive_path=os.path.join(archive_root_path,
                                                                       rd_label,
                                                                       io_conf.get('rawdata_folder_name')))
                archive_task.delay()


@app.task(name='presta.app.tasks.search_rd_to_stage')
def search_rd_to_stage(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    conf = get_conf(logger, None)
    io_conf = conf.get_io_section()

    archive_root_path = kwargs.get('archive_root_path') if kwargs.get('archive_root_path') \
        else io_conf.get('archive_root_path')

    stage_root_path = kwargs.get('staging_root_path') if kwargs.get('staging_root_path') \
        else io_conf.get('staging_root_path')

    logger.info('Checking rundirs in: {}'.format(archive_root_path))
    localroot, dirnames, filenames = os.walk(archive_root_path).next()

    for rd_label in dirnames:
        rd_path = os.path.join(localroot,
                               rd_label)

        if check_rd_to_stage(rd_path=rd_path,
                             io_conf=io_conf):

            logger.info('{} backuped. Ready to be staged'.format(rd_label))
            if emit_events:
                stage_task = stage_rd.si(rd_path=rd_path,
                                         stage_path=os.path.join(stage_root_path,
                                                                 rd_label))
                stage_task.delay()


@app.task(name='presta.app.tasks.archive_rd')
def archive_rd(**kwargs):
    rd_path = kwargs.get('rd_path')
    archive_path = kwargs.get('archive_path')

    src = rd_path
    dest = archive_path

    logger.info('Archiving {} in {}'.format(src, dest))
    mv_task = move.si(src=src, dest=dest)
    mv_task.apply_async()


@app.task(name='presta.app.tasks.stage_rd')
def stage_rd(**kwargs):
    rd_path = kwargs.get('rd_path')
    stage_path = kwargs.get('stage_path')

    src = rd_path
    dest = stage_path

    logger.info('Staging {} in {}'.format(src, dest))
    mv_task = move.si(src=src, dest=dest)
    mv_task.apply_async()


@app.task(name='presta.app.tasks.check_rd_to_archive')
def check_rd_to_archive(**kwargs):
    rd_path = kwargs.get('rd_path')
    io_conf = kwargs.get('io_conf')

    rd_progress_status = check_rd_progress_status(rd_path=rd_path, io_conf=io_conf)

    return rd_progress_status in PROGRESS_STATUS.get('COMPLETED')


@app.task(name='presta.app.tasks.check_rd_to_stage')
def check_rd_to_stage(**kwargs):
    rd_path = kwargs.get('rd_path')
    io_conf = kwargs.get('io_conf')

    rd_backup_status = check_rd_backup_status(rd_path=rd_path, io_conf=io_conf)

    return rd_backup_status in PROGRESS_STATUS.get('COMPLETED')


@app.task(name='presta.app.tasks.check_rd_progress_status')
def check_rd_progress_status(**kwargs):
    rd_path = kwargs.get('rd_path')
    io_conf = kwargs.get('io_conf')

    started_file = io_conf.get('preprocessing_started_file')
    completed_file = io_conf.get('preprocessing_completed_file')

    return check_progress_status(rd_path, started_file, completed_file)


@app.task(name='presta.app.tasks.check_rd_backup_status')
def check_rd_backup_status(**kwargs):
    rd_path = kwargs.get('rd_path')
    io_conf = kwargs.get('io_conf')

    started_file = io_conf.get('backup_started_file')
    completed_file = io_conf.get('backup_completed_file')

    return check_progress_status(rd_path, started_file, completed_file)


def _set_imetadata(ir_conf, ipath, imetadata):

    ir = build_object_store(store='irods',
                            host=ir_conf['host'],
                            port=ir_conf['port'],
                            user=ir_conf['user'],
                            password=ir_conf['password'].encode('ascii'),
                            zone=ir_conf['zone'])
    for m in imetadata:
        ir.add_object_metadata(path=ipath,
                               meta=(m.get('name'),
                                     m.get('value') if len(m.get('value')) > 0 else None,
                                     m.get('units')))
        ir.sess.cleanup()


def _copy_file_into_irods(**kwargs):
    ir_conf = kwargs.get('conf')
    file_path = kwargs.get('file_path')
    irods_path = kwargs.get('irods_path')

    ir = build_object_store(store='irods',
                            host=ir_conf['host'],
                            port=ir_conf['port'],
                            user=ir_conf['user'],
                            password=ir_conf['password'].encode('ascii'),
                            zone=ir_conf['zone'])

    logger.info('Coping from FS {} to iRODS {}'.format(file_path, irods_path))
    try:
        ir.put_object(source_path=file_path, dest_path=irods_path, force=True)
        ir.sess.cleanup()
    except:
        ir.sess.cleanup()


def _get_index_cycles_from_metadata(ir_conf, rundir_label):
    ipath = os.path.join(ir_conf['runs_collection'],
                         rundir_label)
    rundir_has_metadata, imetadata = check_metadata(ir_conf=ir_conf,
                                                    ipath=ipath,
                                                    get_metadata=True)
    if rundir_has_metadata:
        return dict(index=next((m['value'] for m in imetadata
                                if m["name"] == "index1_cycles" and m['value'] != "None"), None),
                    index1=next((m['value'] for m in imetadata
                                 if m["name"] == "index2_cycles" and m['value'] != "None"), None),
                    )

    return dict(index=None, index1=None)


def _get_index_cycles_from_run_info_file(run_info_file_path, get_default_values=False):
    with open(run_info_file_path, 'r') as f:
        run_info_file = IEMRunInfoReader(f)

    if get_default_values:
        return run_info_file.get_index_cycles(), run_info_file.get_default_index_cycles()

    return run_info_file.get_index_cycles()


def _is_paired_end(run_info_file_path):
    run_info_file = IEMRunInfoReader(run_info_file_path)
    return run_info_file.is_paired_end_sequencing()

def runGEJob(jt_attr):
    def init_job_template(jt, attr):
        jt.jobName = '_'.join(['presta', attr['jobName']])
        jt.nativeSpecification = attr['nativeSpecification']
        jt.remoteCommand = attr['remoteCommand']
        jt.args = attr['args']
        return jt

    with drmaa.Session() as s:
        jt = init_job_template(s.createJobTemplate(), jt_attr)
        jobid = s.runJob(jt)
        logger.info('Your job has been submitted with ID %s' % jobid)

        retval = s.wait(jobid, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        logger.info('Job: {0} finished with status {1}'.format(retval.jobId,
                                                               retval.exitStatus))

        logger.info('Cleaning up')
        s.deleteJobTemplate(jt)

    return retval.hasExited


