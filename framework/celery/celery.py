from __future__ import absolute_import
from celery import Celery

observium = Celery('queue', include=['framework.observium.tasks'])

observium.config_from_object('celeryconfig')

if __name__ == '__main__':
    observium.start()
