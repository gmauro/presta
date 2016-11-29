from __future__ import absolute_import

from . import app
from presta.app.tasks import run_presta_check, run_presta_proc, run_presta_qc, run_presta_sync, merge
from presta.app.lims import search_batches_to_sync, search_worksheets_to_sync, search_samples_to_sync

from celery.result import AsyncResult
from celery.states import SUCCESS, FAILURE

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

    search_batches_to_sync.si(**params).delay()


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

@task
def merge_datasets(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        merge_datasets.__name__,
        merge.__name__)
    )

    merge.si(**params).delay()

@task
def nothing_to_do(params):
    pass


@app.task(name='presta.app.router.trigger_event')
def trigger_event(event, params,
                  tasks=list(),
                  event_failure='nothing_to_do', params_failure=dict()):

    trigger = wait_for_jobs_to_complete(tasks=tasks)

    # If all task is successfully completed
    if trigger:
        dispatch_event(event, params)

    # If at least one task is failed
    dispatch_event(event_failure, params_failure)


@app.task(name='presta.app.router.dispatch_event')
def dispatch_event(event, params):
    tasks[event](params)


def wait_for_jobs_to_complete(tasks=list()):

    status = dict()
    task_ids = [t.id for t in tasks]

    while len(status) < len(task_ids):
        for id in task_ids:
            result = AsyncResult(id)
            if result.status in [SUCCESS, FAILURE] and id not in status:
                status[id] = result.status

    if FAILURE in status.values():
        return False

    return True
