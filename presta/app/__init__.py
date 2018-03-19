from __future__ import absolute_import
from celery import Celery

from comoda import a_logger
from ..utils import get_conf


class Config:
    def __init__(self):
        logger = a_logger('app')
        conf = get_conf(logger, config_file_from_cli=None, profile='celery')
        celery_conf = conf.get_section('celery')

        self.broker_url = celery_conf.get('broker_url',
                                          'amqp://guest:guest@localhost:5672')
        self.result_backend = celery_conf.get('result_backend',
                                              'amqp://guest:guest@localhost:5672')
        self.task_serializer = celery_conf.get('task_serializer', 'json')
        self.result_serializer = celery_conf.get('result_serializer', 'json')
        self.enable_utc = celery_conf.get('enable_utc', False)
        self.timezone = celery_conf.get('timezone', 'Europe/Rome')
        self.task_publish_retry = celery_conf.get('task_publish_retry', True)
        retry_policy = {
            'max_retries': 10,
            'interval_start': 5,
            'interval_step': 5,
            'interval_max': 5,
        }
        self.task_publish_retry_policy = celery_conf.get(
            'task_publish_retry_policy', retry_policy)


app = Celery('app',
             include=['presta.app.tasks',
                      'presta.app.cron',
                      'presta.app.router',
                      'presta.app.lims',
                      ])

app.config_from_object(Config())

if __name__ == '__main__':
    app.start()
