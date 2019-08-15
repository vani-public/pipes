from logging import LoggerAdapter

from pypipes.config import Config
from pypipes.service.logger import logger_context, init_pipe_log, init_logging, \
    DEFAULT_LOG_CONFIG, pipe_logger
from mock import patch

from pypipes.context import context


def test_logger_context():
    lazy_context = logger_context(logger_name='test')
    logger = lazy_context({'program_id': 'program_id1', 'processor_id': 'processor_id1',
                           'key': 'value'})
    assert isinstance(logger, LoggerAdapter)
    logger.info('test log message')
    assert logger.extra == {'program_id': 'program_id1', 'processor_id': 'processor_id1'}
    assert logger.logger.name == 'test'


def test_logger_custom_extras_context():
    lazy_context = logger_context(extras=('extra1', 'extra2'))
    logger = lazy_context({'extra1': 'value1', 'extra2': 'value2',
                           'key': 'value'})
    logger.info('test log message')
    assert logger.extra == {'extra1': 'value1', 'extra2': 'value2'}

    lazy_context = logger_context(extras={'extra1': context, 'extra2': 'fixed_value'})
    logger = lazy_context({'extra1': 'value1', 'extra2': 'value2',
                           'key': 'value'})
    assert logger.extra == {'extra1': 'value1', 'extra2': 'fixed_value'}


@patch('pypipes.service.logger.logging')
def test_init_pipe_log(logging_mock):
    # just verify that the func has no error inside
    init_pipe_log()
    logging_mock.getLogger.assert_called_once_with('pipe')
    logging_mock.StreamHandler.assert_called_once_with()


@patch('pypipes.service.logger.logging')
def test_init_pipe_log_file(logging_mock):
    # just verify that the func has no error inside
    init_pipe_log(pipe_logger_name='test', log_file='test.log')
    logging_mock.getLogger.assert_called_once_with('test')
    logging_mock.FileHandler.assert_called_once_with('test.log')


@patch('pypipes.service.logger.logging')
def test_init_logging(logging_mock):
    init_logging()
    logging_mock.config.dictConfig.assert_called_once_with(DEFAULT_LOG_CONFIG)


@patch('pypipes.service.logger.logging')
def test_init_logging_config(logging_mock):
    logging_config = {'version': '1.0'}
    config = Config({'logging': logging_config})
    init_logging(config)
    logging_mock.config.dictConfig.assert_called_once_with(logging_config)


def test_pipe_logger():
    log_config = pipe_logger(format='custom log format', loglevel='DEBUG')
    assert log_config
