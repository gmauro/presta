from __future__ import absolute_import
import os
from celery import Celery

app = Celery('app',
             include=['app.tasks',
                      'app.cron',
                      'app.router',
                      'app.lims',
                      ])

# Set default configuration module name
os.environ.setdefault('CELERY_CONFIG_MODULE', 'app.celeryconfig')

app.config_from_envvar('CELERY_CONFIG_MODULE')

if __name__ == '__main__':
    app.start()
