# coding: utf-8

import os
import sys
import logging.config

_LOG_PATH = os.getenv("log.path", "log")
_LOG_LEVEL = os.getenv("log.level", "DEBUG").upper()
_LOG_FORMATTER_STR = '%(asctime)s %(levelname)s %(processName)s '\
                  '%(filename)s line:%(lineno)d %(message)s'

if not os.path.exists(_LOG_PATH):
    sys.stdout.write(
        "LOG_PATH: %s does not exist, create it\n" % _LOG_PATH)
    os.makedirs(_LOG_PATH)

_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format':
                '%(asctime)s,%(msecs)d [%(name)s-%(process)d] %(levelname)s '
                '%(module)s %(lineno)d - %(message)s'
        },
        'module': {
            'format': _LOG_FORMATTER_STR
        },
        'simple': {
            'format': '%(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            'stream': sys.stdout,
        },
        'main': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'D',
            'interval': 1,
            'backupCount': 30,
            'filename': os.path.join(_LOG_PATH, 'main.log'),
            'formatter': 'module',
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        'main': {
            'handlers': ['main'],
            'level': 'DEBUG',
            'propagate': False,
            'formatter': 'module'
        },
    }
}


logging.config.dictConfig(_CONFIG)
LOG = logging.getLogger('main')
