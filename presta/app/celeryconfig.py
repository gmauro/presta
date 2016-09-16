from celery.schedules import crontab

BROKER_URL = 'amqp://guest:guest@biobank06.crs4.it:5672'
CELERY_RESULT_BACKEND = 'amqp://guest:guest@biobank06.crs4.it:5672'

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT=['json']
CELERY_TIMEZONE = 'Europe/Rome'
CELERY_ENABLE_UTC = True

# CELERYBEAT_SCHEDULE = {                                                                                                                                                
#     # Execute every three hours: midnight, 3am, 6am, 9am, noon, 3pm, 6pm, 9pm.                                                                                         
#     'check-every-three-hours': {                                                                                                                                       
#         'task': 'presta.app.tasks.check_rd_ready_to_be_preprocessed',                                                                                                  
#         'schedule': crontab(minute=0, hour='*/3'),                                                                                                                     
#         'args': '$SEQ_RUNNING',                                                                                                                                        
#     },                                                                                                                                                                 
# }                                                                                                                                                                      

CELERYBEAT_SCHEDULE = {
    # Execute every three hours: midnight, 3am, 6am, 9am, noon, 3pm, 6pm, 9pm.                                                                                           
    'check-every-three-minutes': {
        'task': 'presta.app.tasks.check_rd_ready_to_be_preprocessed',
        'schedule': crontab(minute='*/1'),
        'args': '$SEQ_RUNNING',
    },
}