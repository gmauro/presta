from __future__ import absolute_import

from celery import Celery

app = Celery('app',
             include=['app.celery.tasks'])


app.config_from_object('app.celery.celeryconfig')

if __name__ == '__main__':
    app.start()
