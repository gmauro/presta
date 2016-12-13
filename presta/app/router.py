from __future__ import absolute_import

from . import app
from presta.app.tasks import run_presta_check, run_presta_proc, run_presta_qc, run_presta_sync, merge, copy_qc_dirs, \
    set_progress_status, search_rd_to_archive, search_rd_to_stage
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
def check_rd_to_stage(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        check_rd_to_stage.__name__,
        search_rd_to_stage.__name__)
    )

    search_rd_to_stage.si(**params).delay()

@task
def check_rd_to_archive(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        check_rd_to_archive.__name__,
        search_rd_to_archive.__name__)
    )

    search_rd_to_archive.si(**params).delay()

@task
def rd_ready(params):
    dispatch = params.get('emit_events')
    if dispatch:
        logger.info('Received event "{}". Run task "{}"'.format(
            rd_ready.__name__,
            run_presta_proc.__name__)
        )

        run_presta_proc.si(**params).delay()
    else:
        logger.info('Event "{}". Nothing to dispatch'.format(rd_ready.__name__))


@task
def preprocessing_started(params):
    logger.info('Received event "{}". Run tasks "{}" '.format(
        preprocessing_started.__name__,
        set_progress_status.__name__)
    )
    set_progress_status.si(**params).delay()


@task
def fastq_ready(params):
    dispatch = params.get('emit_events')
    if dispatch:
        logger.info('Received event "{}". Run tasks "{}" - "{}"'.format(
            fastq_ready.__name__,
            run_presta_qc.__name__,
            run_presta_sync.__name__)
        )
        run_presta_qc.si(**params).delay()
        run_presta_sync.si(**params).delay()
    else:
        logger.info('Event "{}". Nothing to dispatch'.format(fastq_ready.__name__))

    set_progress_status.si(**params).delay()


@task
def qc_started(params):
    logger.info('Received event "{}". Run tasks "{}" '.format(
        qc_started.__name__,
        set_progress_status.__name__)
    )
    set_progress_status.si(**params).delay()


@task
def qc_completed(params):
    logger.info('Received event "{}". Run tasks "{}" '.format(
        qc_completed.__name__,
        set_progress_status.__name__)
    )
    set_progress_status.si(**params).delay()


@task
def delivery_started(params):
    logger.info('Received event "{}". Run tasks "{}" '.format(
        delivery_started.__name__,
        set_progress_status.__name__)
    )
    set_progress_status.si(**params).delay()


@task
def delivery_completed(params):
    logger.info('Received event "{}". Run tasks "{}" '.format(
        delivery_completed.__name__,
        set_progress_status.__name__)
    )
    set_progress_status.si(**params).delay()


@task
def merge_started(params):
    logger.info('Received event "{}". Run tasks "{}" '.format(
        merge_started.__name__,
        set_progress_status.__name__)
    )
    set_progress_status.si(**params).delay()


@task
def merge_completed(params):
    logger.info('Received event "{}". Run tasks "{}" '.format(
        merge_completed.__name__,
        set_progress_status.__name__)
    )
    set_progress_status.si(**params).delay()


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
def copy_qc_folders(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        copy_qc_folders.__name__,
        copy_qc_dirs.__name__)
    )

    copy_qc_dirs.si(**params).delay()


@task
def merge_datasets(params):
    logger.info('Received event "{}". Run task "{}"'.format(
        merge_datasets.__name__,
        merge.__name__)
    )

    merge_task = merge.si(**params).delay()
    return merge_task.task_id


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
        return dispatch_event(event, params)

    # If at least one task is failed
    return dispatch_event(event_failure, params_failure)


@app.task(name='presta.app.router.dispatch_event')
def dispatch_event(event, params):
    return tasks[event](params)


def wait_for_jobs_to_complete(tasks=list()):

    status = dict()
    task_ids = [t for t in tasks]

    while len(status) < len(task_ids):
        for id in task_ids:
            result = AsyncResult(id)
            if result.status in [SUCCESS, FAILURE] and id not in status:
                status[id] = result.status

    if FAILURE in status.values():
        return False

    return True
