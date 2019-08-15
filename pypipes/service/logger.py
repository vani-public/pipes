import logging
import logging.config
from copy import deepcopy

from pypipes.context import context, apply_context_to_kwargs

from pypipes.context.factory import LazyContext

PIPE_LOG_FORMAT = '%(asctime)s|%(levelname)s %(program_id)s.%(processor_id)s:%(message)s'

DEFAULT_LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'pipe': {
            'format': PIPE_LOG_FORMAT
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout'
        },
        'pipe_console': {
            'class': 'logging.StreamHandler',
            'formatter': 'pipe',
            'stream': 'ext://sys.stdout'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    },
    'loggers': {
        'pipe': {
            'level': 'INFO',
            'handlers': ['pipe_console'],
            'propagate': False,
        },
    }
}

# json log config uses `pythonjsonlogger.jsonlogger.JsonFormatter` formatter class
# from python-json-logger package that you have to install
JSON_LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'json': {
            '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': '%(asctime)s|%(levelname)s: %(message)s'
        },
        'pipe': {
            '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': PIPE_LOG_FORMAT
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'json',
            'stream': 'ext://sys.stdout'
        },
        'pipe_console': {
            'class': 'logging.StreamHandler',
            'formatter': 'pipe',
            'stream': 'ext://sys.stdout'
        }
    },
    'root': {
        'level': 'WARNING',
        'handlers': ['console']
    },
    'loggers': {
        'pipe': {
            'level': 'INFO',
            'handlers': ['pipe_console'],
            'propagate': False,
        },
    }
}


def logger_context(logger_name='pipe', extras=None):
    """
    Create logger context factory
    :param logger_name: log name
    :param extras: extras dict for logger adapter. IContextLookup may be used as value.
    :param extras: dict, list, tuple
    :return: IContextFactory
    """
    if not extras:
        extras = {'program_id': context, 'processor_id': context}
    elif isinstance(extras, (list, tuple)):
        # get all extras from context root
        extras = {extra: context for extra in extras}
    logger = logging.getLogger(logger_name)

    def create_logger_adapter(injections):
        return logging.LoggerAdapter(
            logger, extra=apply_context_to_kwargs(extras, injections))
    return LazyContext(create_logger_adapter)


def init_pipe_log(log_file=None, level=logging.INFO, fmt=None, pipe_logger_name='pipe'):
    """
    initialize log file handler for pipe logger
    :param log_file: log file location
    :param level: log level
    :param fmt: log format
    :param pipe_logger_name: log name
    """
    if not fmt:
        fmt = PIPE_LOG_FORMAT

    logging.basicConfig(level=level)
    logger = logging.getLogger(pipe_logger_name)
    logger.propagate = False
    if log_file:
        ch = logging.FileHandler(log_file)
    else:
        ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt))
    logger.addHandler(ch)


def init_logging(config=None, default=None):
    source = (config and config.logging) or default or DEFAULT_LOG_CONFIG
    logging.config.dictConfig(source)


def pipe_logger(format, loglevel=None, base=None):
    """
    Update pipe logger format and log level in logging config
    :param format: logger format
    :param base: base logger config
    :return: logger config
    :rtype: dict
    """
    base = base or DEFAULT_LOG_CONFIG
    config = deepcopy(base)
    config['formatters']['pipe']['format'] = format
    if loglevel:
        config['loggers']['pipe']['level'] = loglevel
    return config
