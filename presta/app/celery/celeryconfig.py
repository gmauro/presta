BROKER_URL = 'amqp://guest:guest@localhost:5672'
CELERY_RESULT_BACKEND = 'amqp://guest:guest@localhost:5672'

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT=['json']
CELERY_TIMEZONE = 'Europe/Rome'
CELERY_ENABLE_UTC = True
