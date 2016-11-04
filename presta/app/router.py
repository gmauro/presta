from __future__ import absolute_import

from . import app
from presta.app.tasks import run_presta_check, run_presta_proc, run_presta_qc, run_presta_sync
from presta.app.lims import search_batches_to_sync, search_worksheets_to_sync, search_samples_to_sync

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


tasks = {}
task = lambda f: tasks.setdefault(f.__name__, f)


@task
def check_rd(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        check_rd.__name__,
        run_presta_check.__name__)
    )

    run_presta_check.si(**params).delay()


@task
def rd_ready(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        rd_ready.__name__,
        run_presta_proc.__name__)
    )

    run_presta_proc.si(**params).delay()


@task
def fastq_ready(params):
    logger.info('Received event "{}". Run tasks "{}" - "{}"'.format(
        fastq_ready.__name__,
        run_presta_qc.__name__,
        run_presta_sync.__name__)
    )

    run_presta_qc.si(**params).delay()
    run_presta_sync.si(**params).delay()


@task
def check_batches(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        check_batches.__name__,
        search_batches_to_sync.__name__)
    )

    batches, samples = search_batches_to_sync.si(**params).get()
    logger.info('ready: {}'.format(batches))

@task
def check_worksheets(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        check_worksheets.__name__,
        search_worksheets_to_sync.__name__)
    )

    search_worksheets_to_sync.si(**params).delay()


@task
def check_samples(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        check_samples.__name__,
        search_samples_to_sync.__name__)
    )

    search_samples_to_sync.si(**params).delay()


@app.task(name='presta.app.router.dispatch_event')
def dispatch_event(event, params):
    tasks[event](params)
