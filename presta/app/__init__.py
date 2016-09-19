from __future__ import absolute_import
import os
from celery import Celery
from celery.schedules import crontab

app = Celery('app',
             include=['app.tasks'])

# Set default configuration module name
os.environ.setdefault('CELERY_CONFIG_MODULE', 'app.celeryconfig')

app.config_from_envvar('CELERY_CONFIG_MODULE')
app.conf.update(
    CELERYBEAT_SCHEDULE={

        # Execute every three hours: midnight, 3am, 6am, 9am, noon, 3pm, 6pm, 9pm.

        'check-every-three-minutes': {
            'task': 'presta.app.tasks.check_rd_ready_to_be_preprocessed',
            'schedule': crontab(minute='*/1'),
            'kwargs': dict(rd_path='MY_SEQ_RUNNING'),
        },
    }
)

if __name__ == '__main__':
    app.start()
