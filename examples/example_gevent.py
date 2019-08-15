"""
This example demonstrate a usage of:
* GeventInf - gevent based infrastructure
* Scheduler - pipeline job scheduling
* Sync.wait_all - pipeline synchronization
* Event - custom event sending and receiving
* @cursor - managing custom processor cursor
* @cache - caching of processor result
* @pagination - managing of processing pages
"""

from gevent import monkey; monkey.patch_all()  # noqa

from datetime import datetime
import logging
import gevent

from pypipes.context import message
from pypipes.context.cache import cache
from pypipes.context import pagination
from pypipes.context import Sync
from pypipes.context import cursor
from pypipes import memory_cache_pool
from pypipes.service.counter import memory_counter_pool
from pypipes import memory_lock_pool
from pypipes import memory_storage_pool
from pypipes import GeventInf
from pypipes.processor import pipe_processor
from pypipes.processor import Event
from pypipes.processor import Scheduler
from pypipes.program import Program

logging.basicConfig(level=logging.INFO)


@pipe_processor
def start_notification(logger):
    logger.info('Job started')
    return {'start_time': datetime.now()}


@pipe_processor
def processor0():
    # processor func could be a generator
    for i in range(10):
        gevent.sleep(0)  # switch gevent context
        yield {'total_page': i}


@pagination
@pipe_processor
def processor1(message, processor_id, response, logger, page=1):
    if page < 9:
        response.page = page + 1
    gevent.sleep(0.5)  # wait a little here
    logger.info('%s process page %s', processor_id, page)
    upper_level_page = message.get('total_page', 0)
    response.message.total_page = upper_level_page * 10 + (page or 0)


@cache(page=message.total_page)  # cache processor results per page
@pipe_processor
def cached_processor(message, logger):
    """
    This processor demonstrates a work of a `@cache` context manager
    :type message: pypipes.message.FrozenMessage
    :type logger: logging.LoggerAdapter
    """
    logger.info('cached_processor does some processing for page %s', message.total_page)
    return {'cached_page': message.total_page}


@pipe_processor
def log_message(processor_id, message, logger):
    logger.info('%s got %s', processor_id, message)


@pipe_processor
def end_notification(logger, message):
    logger.info('Job completed')
    logger.info('Processing time: %s', datetime.now() - message.start_time)


@pipe_processor
def print_separator(processor_id, logger):
    logger.info('%s ---------------------------------------------------------------', processor_id)


@cursor(page=message.total_page)  # each page should have a separate cursor
@pipe_processor
def cursor_processor(message, response, logger, cursor=0):
    cursor += 1
    logger.info('page %s cursor value is %s',
                message.total_page, cursor)

    # lets make cursor processing longer than processor scheduler period (1 second).
    # cursor decorator should prevent cursor processing conflicts
    gevent.sleep(2)

    # save updated cursor into storage
    response.cursor = cursor


pipeline1 = (Scheduler.start_period(seconds=10) >> start_notification >>
             Sync(processor0 >> processor1 >> cached_processor >> log_message).wait_all)

pipeline2 = Scheduler.start_period(seconds=3) >> print_separator

pipeline3 = Scheduler.start_period(seconds=1) >> processor0 >> cursor_processor

program = Program(name='test1', pipelines={
    'pipeline1': pipeline1 >> Event.send('DONE'),
    'pipeline2': pipeline2,
    'pipeline3': pipeline3,
    'end_notification': Event.on('DONE') >> end_notification,
    'print_separator': Event.on_start >> (Event.on('DONE') >> print_separator)})

services = {'logger': logging.getLogger(__name__),
            'counter': memory_counter_pool,
            'lock': memory_lock_pool,
            'cache': memory_cache_pool,
            'storage': memory_storage_pool}

if __name__ == '__main__':
    infrastructure = GeventInf(services)
    infrastructure.load(program)
    infrastructure.start(program)

    # run gevent worker
    infrastructure.run_worker()
