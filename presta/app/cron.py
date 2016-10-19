from __future__ import absolute_import

from . import app
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)
from presta.utils import runJob
from presta.app.events import emit_event


@app.task(name='presta.app.cron.check_rd_ready_to_be_preprocessed')
def check_rd_ready_to_be_preprocessed(**kwargs):
    logger.info('Cron Task: searching for run ready to be preprocessed...')
    emit_event(event='check_rd').apply_async()
    return True