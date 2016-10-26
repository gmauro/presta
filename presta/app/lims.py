from __future__ import absolute_import

from . import app
from alta.bims import Bims
from presta.app.router import dispatch_event

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@app.task(name='presta.app.lims.sync_samples')
def sync_samples(samples, **kwargs):
    if samples:
        logger.info('Samples: {}'.format(samples))

    return True