from __future__ import absolute_import

from . import app
from celery.utils.log import get_task_logger
from presta.utils import runJob

logger = get_task_logger(__name__)


tasks = {}
task = lambda f: tasks.setdefault(f.__name__, f)


@task
def check_rd(**kwargs):
    cmd_line = ['presta', 'check', '--emit_events']
    output = runJob(cmd_line, logger)
    return True if output else False


@task
def rd_ready(**kwargs):
    rd_path = kwargs.get('rd_path')
    rd_label = kwargs.get('rd_label')
    logger.info('{} is ready to be processed. Start preprocessing...'.format(rd_label))
    cmd_line = ['presta', 'proc', '--rd_path', rd_path, '--export_qc']
    output = runJob(cmd_line, logger)
    return True if output else False


@app.task(name='presta.app.events.emit_event')
def emit_event(event=None, **kwargs):
    tasks[event](**kwargs)