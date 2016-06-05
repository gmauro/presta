from __future__ import absolute_import

from . import app
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


@app.task(name='presta.app.tasks.rd_completed')
def rd_completed(rd_path):
    illumina_last_file = 'RTAComplete.txt'
    localroot, dirnames, filenames = os.walk(rd_path).next()
    return True if illumina_last_file in filenames else False


@app.task(name='presta.app.tasks.rd_move', ignore_result=True)
def rd_move(src, dest):
    return shutil.move(src, dest)


@app.task(name='presta.app.tasks.bcl2fastq')
def bcl2fastq(rd_path, output_path, samplesheet_path):
    command = 'bcl2fastq'
    rd_arg = '-R {}'.format(rd_path)
    output_arg = '-o {}'.format(output_path)
    samplesheet_arg = '--sample-sheet {}'.format(samplesheet_path)
    args = ['--no-lane-splitting',
            '--barcode-mismatches 1',
            '--ignore-missing-bcls',
            '--ignore-missing-filter',
            '--ignore-missing-positions',
            '--find-adapters-with-sliding-window',
            '--loading-threads 4',
            '--demultiplexing-threads 4',
            '--processing-threads 4',
            '--writing-threads 4']

    cmd_line = shlex.split(' '.join([command, rd_arg, output_arg,
                                    samplesheet_arg, ' '.join(args)]))
    logger.info('Executing {}'.format(cmd_line))
    output = runJob(cmd_line)

    return True if output else False


@app.task(name='presta.app.tasks.fastqc')
def fastqc(fq_list):
    command = 'fastqc'
    output_arg = '--outdir {}'.format('tmp/fqc')
    args = ['--threads 4',
            '--format fastq']
    fq_list_arg = ' '.join(fq_list)

    cmd_line = shlex.split(' '.join([command, output_arg, ' '.join(args),
                                     fq_list_arg]))
    logger.info('Executing {}'.format(cmd_line))
    output = runJob(cmd_line)

    return True if output else False


def runJob(cmd):
    try:
        subprocess.check_output(cmd)
        return True
    except subprocess.CalledProcessError as e:
        logger.debug(e)
        if e.output:
            logger.debug("command output: %s", e.output)
        else:
            logger.debug("no command output available")
        return False


