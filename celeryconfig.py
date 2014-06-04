BROKER_URL = ['amqp://observium:observium@nyforeman1.x.pdtpartners.com:5672//observium',
              'amqp://observium:observium@caforeman1.x.pdtpartners.com:5672//observium']
CELERY_RESULT_BACKEND = 'amqp'
CELERY_TASK_RESULT_EXPIRES = 3600

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'America/New_York'
CELERY_ENABLE_UTC = True

CELERY_DISABLE_RATE_LIMITS = True

# CELERY_ROUTES = {
#    'tasks.poll': 'low-priority',
#    'tasks.discover': 'low-priority',
#}

#CELERY_ANNOTATIONS = {
#    'tasks.poll': {'rate_limit': '10/m'},
#    'tasks.discover': {'rate_limit': '10/m'},
#}

BROKER_FAILOVER_STRATEGY = 'round-robin'