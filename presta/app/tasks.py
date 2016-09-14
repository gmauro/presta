from __future__ import absolute_import

from . import app
from alta.objectstore import build_object_store
from alta.utils import ensure_dir
from celery import group
import drmaa
from grp import getgrgid
from presta.utils import IEMSampleSheetReader
from presta.utils import IEMRunInfoReader
from pwd import getpwuid
import errno
import os
import shlex
import shutil
import subprocess

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@app.task(name='presta.app.tasks.rd_collect_fastq')
def rd_collect_fastq(**kwargs):
    path = kwargs.get('ds_path')
    results = []
    for (localroot, dirnames, filenames) in os.walk(path):
        for f in filenames:
            if f[-3:] == '.gz':
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
    ipath = os.path.join(ir_conf['runs_collection'],
                         rundir_label,
                         samplesheet_filename)

    task0 = seq_completed.si(path)
    task1 = check_ownership.si(user=user, group=grp, dir=path)
    task2 = samplesheet_ready.si(ir_conf, ipath)
    task3 = check_metadata.si(ir_conf, os.path.dirname(ipath))

    pipeline = group(task0, task1, task2, task3)()

    while pipeline.waiting():
         pass
    return pipeline.join()


@app.task(name='presta.app.tasks.samplesheet_ready')
def samplesheet_ready(ir_conf, ipath):
    ir = build_object_store(store='irods',
                            host=ir_conf['host'],
                            port=ir_conf['port'],
                            user=ir_conf['user'],
                            password=ir_conf['password'].encode('ascii'),
                            zone=ir_conf['zone'])

    exists, iobj = ir.exists(ipath, delivery=True)
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

    exists, iobj = ir.exists(ipath, delivery=True)

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
        return getpwuid(os.stat(directory).st_uid).pw_name

    def find_group(directory):
        return getgrgid(os.stat(directory).st_gid).gr_name

    return True if user == find_owner(d) and grp == find_group(d) else False


@app.task(name='presta.app.tasks.copy')
def copy(src, dest):
    result = False
    try:
        shutil.copytree(src, dest)
        result = True
    except OSError as e:
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            logger.error('Source not copied. Error: {}'.format(e))
    return result


@app.task(name='presta.app.tasks.copy_qc_dirs', ignore_result=True)
def copy_qc_dirs(src, dest):
    dirs = ['Stats', 'Reports', 'fastqc']
    ensure_dir(dest)
    task0 = copy.si(os.path.join(src, dirs[0]), os.path.join(dest, dirs[0]))
    task1 = copy.si(os.path.join(src, dirs[1]), os.path.join(dest, dirs[1]))
    task2 = copy.si(os.path.join(src, dirs[2]), os.path.join(dest, dirs[2]))

    job = group(task0, task1, task2)()
    while job.waiting():
        pass
    return job.join()


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
            __set_imetadata(ir_conf=ir_conf,
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
        ir.get_object(ipath, dest_path=samplesheet_file_path)

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

    __copy_file_into_irods(conf=ir_conf,
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

    __copy_file_into_irods(conf=ir_conf,
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
        index_cycles_from_metadata = __get_index_cycles_from_metadata(ir_conf=ir_conf,
                                                                      rundir_label=rundir_label)

        index_cycles_from_run_info_file, default_index_cycles = __get_index_cycles_from_run_info_file(
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
    for lane, barcode_length in barcode_mask.items():
        if barcode_length['index1'] is None or barcode_length['index1'] in ['None']:
            options.append("--use-bases-mask {}:Y*,I{}n*,Y*".format(lane, barcode_length['index']))
        else:
            options.append(
                "--use-bases-mask {}:Y*,I{}n*,I{}n*,Y*".format(lane, barcode_length['index'], barcode_length['index1']))

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
        output = runGEJob(jt)
    else:
        output = runJob(cmd_line)

    return True if output else False


@app.task(name='presta.app.tasks.qc_runner', ignore_result=True)
def qc_runner(file_list, **kwargs):
    def chunk(lis, n):
        return [lis[i:i + n] for i in range(0, len(lis), n)]

    chunk_size = kwargs.get('chunk_size', 6)
    for f in chunk(file_list, chunk_size):
        fastqc.s(f, outdir=kwargs.get('outdir'),
                 threads=chunk_size,
                 batch_queuing=kwargs.get('batch_queuing'),
                 queue_spec=kwargs.get('queue_spec')
                 ).delay()


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
        output = runGEJob(jt)
    else:
        output = runJob(cmd_line)

    return True if output else False


def __set_imetadata(ir_conf, ipath, imetadata):

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


def __copy_file_into_irods(**kwargs):
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

    ir.put_object(source_path=file_path, dest_path=irods_path, force=True)


def __get_index_cycles_from_metadata(ir_conf, rundir_label):
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


def __get_index_cycles_from_run_info_file(run_info_file_path, get_default_values=False):
    with open(run_info_file_path, 'r') as f:
        run_info_file = IEMRunInfoReader(f)

    if get_default_values:
        return run_info_file.get_index_cycles(), run_info_file.get_default_index_cycles()

    return run_info_file.get_index_cycles()


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


def runJob(cmd):
    try:
        subprocess.check_output(cmd)
        return True
    except subprocess.CalledProcessError as e:
        logger.info(e)
        if e.output:
            logger.info("command output: %s", e.output)
        else:
            logger.info("no command output available")
        return False
