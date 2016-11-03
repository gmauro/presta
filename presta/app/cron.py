from __future__ import absolute_import

from . import app
from presta.app.router import dispatch_event

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@app.task(name='presta.app.cron.check_rd_ready_to_be_preprocessed')
def check_rd_ready_to_be_preprocessed():
    logger.info('Cron Task: searching for run ready to be preprocessed...')
    dispatch_event.si(event='check_rd',
                      params=dict(emit_events=True),
                      ).delay()
    return True


@app.task(name='presta.app.cron.check_batches_ready_to_be_closed')
def check_batches_ready_to_be_closed():
    logger.info('Cron Task: searching for batches ready to be closed...')
    dispatch_event.si(event='check_batches',
                      params=dict(emit_events=True),
                      ).delay()
    return True


@app.task(name='presta.app.cron.check_worksheets_ready_to_be_closed')
def check_worksheets_ready_to_be_closed():
    logger.info('Cron Task: searching for worksheets ready to be closed...')
    dispatch_event.si(event='check_worksheets',
                      params=dict(emit_events=True),
                      ).delay()
    return True


@app.task(name='presta.app.cron.check_samples_ready_to_be_published')
def check_samples_ready_to_be_published():
    logger.info('Cron Task: searching for samples ready to be published...')
    dispatch_event.si(event='check_samples',
                      params=dict(emit_events=True),
                      ).delay()
    return True
