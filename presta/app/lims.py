from __future__ import absolute_import

from . import app
from alta.bims import Bims
from presta.app.router import dispatch_event

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

ACTIONS = ['open', 'close', 'submit', 'verify', 'publish']
def _do_action_for_many(bika_conn, action, obj_paths):
    params = {}


    return True