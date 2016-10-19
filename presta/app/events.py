from __future__ import absolute_import

from . import app
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)
from presta.utils import runJob


@app.task(name='presta.app.events.emit_event')
def emit_event(event=None, **kwargs):
    if event in ['check_rd_ready_to_be_preprocessed']:
        cmd_line = ['presta', 'check', '--emit_events']
        output = runJob(cmd_line, logger)
        return True if output else False

    elif event in ['check_rd_ready_to_be_preprocessed']:
        rd_path = kwargs.get('rd_path')
        rd_label = kwargs.get('rd_label')
        logger.info('{} is ready to be processed. Start preprocessing...'.format(rd_label))
        cmd_line = ['presta', 'proc', '--rd_path', rd_path, '--export_qc']
        output = runJob(cmd_line, logger)
        return True if output else False