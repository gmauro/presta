from __future__ import absolute_import

from . import app
import drmaa
import os
import shlex, subprocess
import shutil

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


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
            '--find-adapters-with-sliding-window']

    cmd_line = shlex.split(' '.join([command, rd_arg, output_arg,
                                    samplesheet_arg, ' '.join(args)]))
    logger.info('Executing {}'.format(cmd_line))
    output = runJob(cmd_line)

    return True if output else False


def runJob(cmd):
    try:
        output = subprocess.check_output(cmd)
        return output
    except subprocess.CalledProcessError as e:
        logger.debug(e)
        if e.output:
            logger.debug("command output: %s", e.output)
        else:
            logger.debug("no command output available")
        return False


def bcl2fastqGE(rd, ss_filename='samplesheet.csv', ds_dirname='datasets'):
    home = os.path.expanduser("~")

    jt = {'jobname': '_'.join(['bcl2fastq', rd]),
          'nativespecification': '-q eolo -l eolo=1 -l exclusive=True',
          'remotecommand': os.path.join(home, 'ge_scripts', 'run_bcl2fastq.sh'),
          'args': [rd, ss_filename, ds_dirname]
          }

    return runGEJob(jt)


@app.task
def testGE():
    home = os.path.expanduser("~")

    jt = {'jobname': 'testGE',
          'nativespecification': '-q eolo -l eolo=1',
          'remotecommand': os.path.join(home, 'ge_scripts', 'test_ge.sh')
          }

    return runGEJob(jt)


def runGEJob(jt):
    with drmaa.Session() as s:
        jobid = s.runJob(**jt)
        print('Your job has been submitted with ID %s' % jobid)

        print('Cleaning up')
        s.deleteJobTemplate(jt)

        return jobid
