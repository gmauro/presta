from __future__ import absolute_import

from . import app
from alta.objectstore import build_object_store
from alta.utils import ensure_dir
from celery import group
import drmaa
from grp import getgrgid
from presta.utils import IEMSampleSheetReader
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
    task2 = iexists.si(ir_conf, ipath)

    pipeline = group(task0, task1, task2)()
    while pipeline.waiting():
        pass
    return pipeline.join()


@app.task(name='presta.app.tasks.iexists')
def iexists(ir_conf, ipath):
    ir = build_object_store(store='irods',
                            host=ir_conf['host'],
                            port=ir_conf['port'],
                            user=ir_conf['user'],
                            password=ir_conf['password'].encode('ascii'),
                            zone=ir_conf['zone'])

    return ir.exists(ipath)


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


@app.task(name='presta.app.tasks.copy', ignore_result=True)
def copy(src, dest):
    try:
        shutil.copytree(src, dest)
    except OSError as e:
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            logger.error('Source not copied. Error: {}'.format(e))


@app.task(name='presta.app.tasks.copy_qc_dirs', ignore_result=True)
def copy_qc_dirs(src, dest):
    dirs = ['Stats', 'Reports', 'fastqc']
    ensure_dir(dest)
    task0 = copy.s(os.path.join(src, dirs[0]), os.path.join(dest, dirs[0]))
    task1 = copy.s(os.path.join(src, dirs[1]), os.path.join(dest, dirs[1]))
    task2 = copy.s(os.path.join(src, dirs[2]), os.path.join(dest, dirs[2]))

    job = group(task0, task1, task2)


@app.task(name='presta.app.tasks.copy_samplesheet_from_irods',
          ignore_result=True)
def copy_samplesheet_from_irods(**kwargs):
    ir_conf = kwargs.get('conf')
    samplesheet_file_path = kwargs.get('ssht_path')
    samplesheet_filename = os.path.basename(samplesheet_file_path)
    rundir_label = kwargs.get('rd_label')

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


@app.task(name='presta.app.tasks.replace_values_into_samplesheet',
          ignore_result=True)
def replace_values_into_samplesheet(file_path):
    with open(file_path, 'r') as f:
        samplesheet = IEMSampleSheetReader(f)

    with open(file_path, 'w') as f:
        for row in samplesheet.get_body(replace=True):
            f.write(row)


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
    submit_to_queuing_system = kwargs.get('queue', True)
    queue_spec = kwargs.get('queue_spec')

    command = 'bcl2fastq'
    rd_arg = '-R {}'.format(rd_path)
    output_arg = '-o {}'.format(ds_path)
    samplesheet_arg = '--sample-sheet {}'.format(ssht_path)
    options = ['--barcode-mismatches 1',
               '--ignore-missing-bcls',
               '--ignore-missing-filter',
               '--ignore-missing-positions',
               '--find-adapters-with-sliding-window']

    if no_lane_splitting:
        options.append('--no-lane-splitting')

    cmd_line = shlex.split(' '.join([command, rd_arg, output_arg,
                                    samplesheet_arg, ' '.join(options)]))
    logger.info('Executing {}'.format(cmd_line))

    if submit_to_queuing_system:
        home = os.path.expanduser("~")
        launcher = kwargs.get('launcher', 'launcher')

        jt = {'jobName': '_'.join(['presta', command]),
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
                 queue=kwargs.get('queue'),
                 queue_spec=kwargs.get('queue_spec')
                 ).delay()


@app.task(name='presta.app.tasks.fastqc')
def fastqc(fq_list, **kwargs):
    command = 'fastqc'
    output_arg = '--outdir {}'.format(kwargs.get('outdir'))
    options = ['--format fastq',
               '--threads {}'.format(kwargs.get('threads', 1))]
    fq_list_arg = ' '.join(fq_list)
    submit_to_queuing_system = kwargs.get('queue', True)
    queue_spec = kwargs.get('queue_spec')

    cmd_line = shlex.split(' '.join([command, output_arg, ' '.join(options),
                                     fq_list_arg]))
    logger.info('Executing {}'.format(cmd_line))

    if submit_to_queuing_system:
        home = os.path.expanduser("~")
        launcher = kwargs.get('launcher', 'launcher')

        jt = {'jobName': '_'.join(['presta', command]),
              'nativeSpecification': queue_spec,
              'remoteCommand': os.path.join(home, launcher),
              'args': cmd_line
              }
        output = runGEJob(jt)
    else:
        output = runJob(cmd_line)

    return True if output else False


def runGEJob(jt_attr):
    def init_job_template(jt, attr):
        jt.jobName = attr['jobName']
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
