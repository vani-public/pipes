"""
This example demonstrate a usage of:
* RunInline - infrastructure based of recursively inline execution
* Scheduler - scheduler doesn't work in RunInline
* Sync.wait_all - pipeline synchronization
* LazyContext - lazy context initialization
* ContextPool - context pool initialization
* custom message_mapping for Program
* global context managers for Program
* metrics context - collecting of processor metrics and annotation sending
* @pagination - managing of processing pages
"""
import logging

from pypipes.context import message
from pypipes.context.manager import pipe_contextmanager
from pypipes.context.metrics import collect_metrics
from pypipes.context.pagination import pagination
from pypipes.context.pool import ContextPool
from pypipes.context.sync import Sync
from pypipes.infrastructure.inline import RunInline
from pypipes.processor import pipe_processor
from pypipes.processor.scheduler import Scheduler
from pypipes.program import Program
from pypipes.service.counter import MemCounter
from pypipes.service.metric import log_metrics_context

from pypipes.context.factory import LazyContext

logging.basicConfig(level=logging.INFO, format='%(program_id)s.%(processor_id)s: > %(message)s')


# -------------------------------------------------------------------
# pipeline processors

@pipe_processor
def start_notification(logger, metrics):
    """
    :type logger: logging.LogAdapter
    :type metrics: pypipes.service.metric.IMetrics
    """
    logger.info('Job started')
    metrics.annotation('Job started')
    return {}  # emit an empty message for next processor


@pipe_processor
def processor0():
    # processor func could be a generator
    for i in range(10):
        yield {'total_page': i}


@pagination
@pipe_processor
def processor1(upper_level_page, response, logger, page=0):
    """
    :param upper_level_page: page index received from previous processor
    :param response: response handler
    :type response: pypipes.infrastructure.response.IResponseHandler
    :param page: page index, emited by @pagination contextmanager
    :type logger: logging.LogAdapter
    """
    if page < 9:
        response.page = page + 1

    logger.info('process page %s', page)
    response.message.total_page = upper_level_page * 10 + (page or 0)


@pipe_processor
def processor2(message, logger):
    """
    :param message: input message
    :type message: pypipes.message.Message
    :type logger: logging.LogAdapter
    """
    logger.info('got %s', message)


@pipe_processor
def end_notification(logger, metrics):
    """
    :type logger: logging.LogAdapter
    :type metrics: pypipes.service.metric.IMetrics
    """
    logger.info('Job completed')
    metrics.annotation('Job completed')
    return {}  # this message will be ignored because the processor is last


# -------------------------------------------------------------------
# pipeline program

# processor1 is especially included twice into pipeline1
pipeline1 = (Scheduler.start_period(seconds=1) >> start_notification >>
             Sync(processor0 >> processor1 >> processor1 >> processor2).wait_all >>
             end_notification)


@pipe_contextmanager
def log_processor_lifecycle(logger):
    """
    Trace inline processing
    :param logger: logging.LogAdapter
    """
    logger.info('enter')
    yield {}
    logger.info('exit')


program = Program(name='test',
                  pipelines={'test_pipeline': pipeline1},
                  message_mapping={'upper_level_page': message.total_page})
program.add_contextmanager(log_processor_lifecycle)
program.add_contextmanager(collect_metrics)


# -------------------------------------------------------------------
# define program running infrastructure

def create_logger_service(program_id, processor_id):
    """
    This is a factory that creates a logger with predefined extras for our program
    :param program_id: id of pipeline program
    :param processor_id: id of current pipeline processor
    :return: logger
    :rtype: logging.LoggerAdapter
    """
    logger = logging.getLogger(__name__)
    logger = logging.LoggerAdapter(logger,
                                   extra={'program_id': program_id,
                                          'processor_id': processor_id})
    return logger


services = {'logger': LazyContext(create_logger_service),
            'counter': ContextPool(default=MemCounter()),
            'metrics': log_metrics_context}
infrastructure = RunInline(services)
infrastructure.load(program)

if __name__ == '__main__':
    # The program will be started immediately as only start event is sent
    # Note that program execution pass through all pipeline processors recursively
    # in RunInline infrastructure
    infrastructure.start(program)
