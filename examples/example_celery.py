"""
This example demonstrate a usage of:
* CeleryInf - celery infrastructure
* error handling in CeleryInf
* Scheduler - pipeline job scheduling and terminating
* Sync.wait_all - pipeline synchronization
* custom message_mapping for Program
* logger_context, init_file_logger - building a logger context
* @pagination - managing of processing pages
* run_command - process command args with custom command parser
* cursor_subcommand - args parser is extended with cursor storage commands
"""
from datetime import datetime

from pypipes.config import Config, merge, from_environ
from pypipes.context import message
from pypipes.context.cursor import cursor
from pypipes.context.pagination import pagination
from pypipes.context.sync import Sync
from pypipes.infrastructure.command import run_command, handle_command_error, Command
from pypipes.infrastructure.command.cursor import cursor_subcommand
from pypipes.infrastructure.on_celery import CeleryInf
from pypipes.processor import pipe_processor
from pypipes.processor.event import Event
from pypipes.processor.scheduler import Scheduler
from pypipes.service.counter import redis_counter_pool
from pypipes.service.cursor_storage import versioned_cursor_storage_context
from pypipes.service.lock import redis_lock_pool
from pypipes.service.logger import logger_context, init_pipe_log
from pypipes.service.storage import redis_storage_pool

from pypipes.program import Program


@pipe_processor
def start_notification(logger):
    logger.info('Job started')
    return {'start_time': datetime.now()}


@pipe_processor
def processor0():
    # processor func could be a generator
    for i in range(10):
        yield {'total_page': i}


@pagination
@pipe_processor
def processor1(upper_level_page, response, logger, page=0):
    if not page:
        response.page = 1
    elif page < 9:
        response.page = page + 1

    logger.info('process page %s', page)
    response.message.total_page = upper_level_page * 10 + (page or 0)


@pipe_processor
def processor2(message, logger):
    logger.info('got %s', message)


@pipe_processor
def end_notification(logger, message):
    logger.info('Job completed')
    logger.info('Processing time: %s', datetime.now() - message.start_time)


@pipe_processor
def auto_stop_notification(logger):
    logger.info('===============================================================')
    logger.info('Program automatically stopped')


@pipe_processor
def print_separator(logger):
    logger.info('---------------------------------------------------------------')


@cursor('counter')
@pipe_processor
def cursor_counter(logger, response, counter_cursor=0):
    """
    This processor uses cursor as a counter storage
    :param logger: logger adapter
    :param response: processor response handler
    :param counter_cursor: cursor value provided by @cursor
    """
    logger.info('Cursor counter: %s', counter_cursor)
    response.counter_cursor = counter_cursor + 1


@pipe_processor
def processor_with_error(logger):
    """
    This processor always raises an error.
    It's included into the program to demonstrate an error handling
    :param logger: logging.LogAdapter
    """
    logger.error('PROCESSING ERROR')
    raise ValueError('PROCESSING ERROR')


pipeline1 = (Scheduler.start_period(seconds=20, scheduler_id='pipeline1') >> start_notification >>
             Sync(processor0 >> processor1 >> processor2).wait_all >>
             end_notification)

pipeline2 = (Scheduler.start_period(seconds=10) >> cursor_counter >> print_separator)

error_pipeline = Event.on_start >> processor_with_error

auto_stop = (Scheduler.start_in(seconds=60) >> Scheduler.stop('pipeline1') >>
             auto_stop_notification)

program = Program(name='celery_example', version='1.0.beta',
                  pipelines={
                      'pipeline1': pipeline1,
                      'pipeline2': pipeline2,
                      'auto_stop': auto_stop,
                      'error_pipeline': error_pipeline
                  },
                  message_mapping={'upper_level_page': message.total_page})


config = Config(merge({
    'celery': {
        'queue_name': {
            'processor': '{program_name}.{pipeline}',
            'scheduler': '{program_name}.scheduler',
            'error': '{program_name}.error.{processor_id}.{error_type}'
        },
        'app': {'broker': 'amqp://guest:guest@localhost:5672'},
        'max_error_retries': 2,
        'error_retry_delay': 10,
    },
    'redis': {
        'host': 'localhost',
        'port': '6379'
    },
    'memcached': {
        'servers': ['localhost:11211']
    }
}, from_environ('VNEXT')))


init_pipe_log('example_celery.log')

context = {
    'config': config,
    'logger': logger_context(),
    'lock': redis_lock_pool,
    'counter': redis_counter_pool,
    'storage': redis_storage_pool,
    'cursor_storage': versioned_cursor_storage_context
}

infrastructure = CeleryInf(context)
infrastructure.load(program)

# start celery worker in a separate terminal
# celery -A example_celery worker -c=4 -l=DEBUG
app = infrastructure.app


class VersionCommand(Command):
    help = 'display program version'

    def run(self, infrastructure, args):
        args.print_message('\nVersion: {}'.format(args.program.version),
                           quiet_message=str(args.program.version))


if __name__ == '__main__':
    with handle_command_error(True):
        run_command(infrastructure,
                    commands={'version': VersionCommand(),
                              'cursor': cursor_subcommand})
