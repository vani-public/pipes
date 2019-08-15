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
* JobCommand - handle job related commands
"""
import logging
from time import sleep

from pypipes.config import Config
from pypipes.context import message, context
from pypipes.infrastructure.command import run_command, handle_command_error
from pypipes.infrastructure.command.job import JobCommand
from pypipes.infrastructure.on_celery import CeleryInf
from pypipes.processor import pipe_processor
from pypipes.processor.event import Event
from pypipes.processor.message import Message
from pypipes.program import Program
from pypipes.service.lock import local_redis_lock_pool
from pypipes.service.logger import logger_context, init_logging, pipe_logger
from pypipes.service.storage import local_redis_storage_pool

from pypipes.context.job import Job


@pipe_processor
def multiplicate(message, job_arg1=None, job_arg2=None):
    """
    This processor emit 10 times more messages than receives
    :param message: input message
    :param job_arg1: custom job patameter
    :param job_arg2: custom job patameter
    """
    sleep(1)  # ensure processor execution takes some significant time
    for i in range(10):
        yield {'index': message.get('index', 0) * 10 + i,
               'job_arg1': job_arg1,
               'job_arg2': job_arg2}


@pipe_processor
def print_job_result(message, logger, job_arg1=None, job_arg2=None):
    logger.info('Job with parameters (%s, %s) result: %s',
                job_arg1, job_arg2, message.index)


job1 = Job('job1', job_arg1=context, job_arg2=context)
job2 = Job('job2', job_arg1=context)


job1_pipeline = job1(Message.log('Job 1 start message') >>
                     multiplicate >> multiplicate >> multiplicate >> multiplicate
                     ) >> print_job_result

job2_pipeline = job2(Message.log('Job 2 start message') >>
                     multiplicate >> multiplicate >> multiplicate >> multiplicate
                     ) >> print_job_result

program = Program(name='celery_example',
                  pipelines={
                      'start': Event.on_start >> Message.log('Program started'),
                      'job1': job1_pipeline,
                      'job2': job2_pipeline,
                  },
                  message_mapping={'job_id': message._job,
                                   'job_arg1': message.job_arg1,
                                   'job_arg2': message.job_arg2})

init_logging(default=pipe_logger(
    '%(levelname)s %(program_id)s.%(processor_id)s:[%(job_id)s]%(message)s',
    loglevel=logging.DEBUG))

config = Config({'celery': {'app': {'broker': 'redis://localhost/0'}}})
context = {
    'config': config,
    'logger': logger_context(extras=('program_id', 'processor_id', 'job_id')),
    'lock': local_redis_lock_pool,
    'storage': local_redis_storage_pool,
}

infrastructure = CeleryInf(context)
infrastructure.load(program)

# start celery worker in a separate terminal
# celery -A example_job worker -c=4 -l=DEBUG
app = infrastructure.app

if __name__ == '__main__':
    with handle_command_error(True):
        run_command(infrastructure,
                    commands={'job': JobCommand(job1, job2)})
