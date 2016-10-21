from __future__ import absolute_import

from . import app
from presta.app.tasks import run_presta_check, run_presta_proc, run_presta_qc

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


tasks = {}
task = lambda f: tasks.setdefault(f.__name__, f)


@task
def check_rd(params):
    logger.info('Received "{}" event. Run "{}" task'.format(
        check_rd.__name__,
        run_presta_check.__name__)
    )

    run_presta_check.si(**params).delay()


@task
def rd_ready(params):
    logger.info('Received "{}" event. Run "{}" task'.format(
        rd_ready.__name__,
        run_presta_proc.__name__)
    )

    run_presta_proc.si(**params).delay()

@task
def fastq_ready(params):
    logger.info('Received "{}" event. Run "{}" task'.format(
        fastq_ready.__name__,
        run_presta_qc.__name__)
    )

    run_presta_qc.si(**params).delay()


@app.task(name='presta.app.events.dispatch_event')
def dispatch_event(event, params):
    tasks[event](params)
