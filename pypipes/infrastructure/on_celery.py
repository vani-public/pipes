import logging
import traceback
from datetime import datetime, timedelta
from functools import wraps
from uuid import uuid4

from celery import Celery as CeleryApp
from celery.exceptions import MaxRetriesExceededError, TaskPredicate
from kombu import Exchange, Queue
from kombu.serialization import dumps
from pypipes.config import Config
from pypipes.infrastructure.base import ListenerInfrastructure, ISchedulerCommands
from pypipes.service import key

logger = logging.getLogger(__name__)


def ready_for_celery_bind(func):
    """
    Only not bounded function could be a body of a celery task.
    This decorator wraps bounded or unbounded method to enable a building of celery task on it.
    :param func: a method that should be a task body
    :return: function
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


class BaseCeleryInf(ListenerInfrastructure):

    _app = None
    started_program_key = 'started_program:{infrastructure}:{program}'

    def __init__(self, context=None, app=None):
        super(BaseCeleryInf, self).__init__(context)
        if app:
            assert isinstance(app, CeleryApp)
            self._app = app

    @property
    def app(self):
        if self._app is None:
            self._app = self.init_application()
        return self._app

    def init_application(self):
        config = self.config
        app = CeleryApp(**config.celery.app)

        app.conf.update(TASK_CREATE_MISSING_QUEUES=False,
                        CELERY_ENABLE_UTC=True,
                        CELERY_TASK_SERIALIZER='pickle',
                        CELERY_RESULT_SERIALIZER='json',
                        CELERY_ACCEPT_CONTENT=['pickle'],
                        CELERYD_PREFETCH_MULTIPLIER=2,
                        # no need to save any task result.
                        CELERY_TASK_IGNORE_RESULT=True,
                        # we expect that pipeline tasks will be pretty short
                        CELERYD_TASK_SOFT_TIME_LIMIT=5 * 60,
                        CELERY_DEFAULT_EXCHANGE=Exchange('default', type='direct'),
                        CELERY_ACKS_LATE=True)
        app.conf.update(**config.celery.config)
        max_retries = config.celery.get('max_error_retries', 3)
        error_retry_delay = config.celery.get('error_retry_delay', 3 * 60)
        app.task(name='pipe_process_message', bind=True,
                 max_retries=max_retries,
                 default_retry_delay=error_retry_delay)(
            ready_for_celery_bind(self.process_message_task))
        return app

    def get_program_context(self, program):
        context = super(BaseCeleryInf, self).get_program_context(program)
        context['celery_app'] = self.app
        return context

    def process_message_task(self, task, program_id, processor_id, message):
        """
        Celery task that processes input messages
        :param task: celery task reference
        :type task: celery.task.Task
        :param program_id: id of program that should process the message
        :param processor_id: id of processor that should process the message
        :param message: message to process
        """
        logger.debug('Start process_message_task for: %s',
                     (program_id, processor_id, message))
        program = self.get_program(program_id)
        if not program:
            logger.warning('received a message for unknown program: %s', program_id)
            return
        try:
            self.process_message(program, processor_id, message)
        except AssertionError as exc:
            # there is no sense to retry message processing if message is not correct
            exc_traceback = traceback.format_exc()
            logger.error('Invalid message for: %s', program_id)
            self.handle_error(program, processor_id, message, exc, exc_traceback)
        except Exception as exc:
            # retry message processing on any unhandled error
            exc_traceback = traceback.format_exc()
            try:
                task.retry()
            except MaxRetriesExceededError:
                logger.error('MaxRetriesExceededError for task: %s %s', program_id, processor_id)
                # save error data into a separate queue
                self.handle_error(program, processor_id, message, exc, exc_traceback)

    def _list_queues(self, program):
        return set(self._queue_name(program, processor_id)
                   for processor_id in program.processors)

    def load(self, program):
        """
        Create celery queues.
        """
        app = self.app
        exchange = Exchange('default', type='direct')
        # append new queues into queue list
        app.conf.task_queues = (app.conf.task_queues or []) + [
            Queue(name, exchange, routing_key=name) for name in self._list_queues(program)]
        super(BaseCeleryInf, self).load(program)

    @property
    def config(self):
        """
        Return configuration
        :return: configuration
        :rtype: pypipes.config.Config
        """
        return self.context.get('config') or Config()  # use an empty config by default

    @property
    def program_lock(self):
        """
        Return program lock service
        :return: lock service
        :rtype: pypipes.service.lock.ILock
        """
        lock = self.context.get('lock')
        if not lock:
            raise RuntimeError('lock.celery service is required '
                               'to synchronise started programs')
        return lock.celery

    def try_start_program(self, program):
        if self.program_lock.acquire(key('started', program.id)):
            return super(BaseCeleryInf, self).try_start_program(program)
        return False

    def try_stop_program(self, program):
        if self.program_lock.release(key('started', program.id)):
            return super(BaseCeleryInf, self).try_stop_program(program)
        return False

    def _queue_name(self, program, processor_id, queue_type='processor',
                    default='{program_id}.{processor_id}', **kwargs):
        template = self.config.celery.queue_name.get(queue_type, default)
        return template.format(program_id=program.id,  # name + version
                               program_name=program.name,
                               program_version=program.version,
                               queue_type=queue_type,
                               pipeline=processor_id and processor_id.split('.', 1)[0],
                               processor_id=processor_id,  # includes pipeline name
                               **kwargs)

    def send_message(self, program, processor_id, message, start_in=None, priority=None,
                     queue=None):
        # each processor has a separate task queue
        queue_name = queue or self._queue_name(program, processor_id)
        self.app.send_task('pipe_process_message',
                           kwargs=dict(program_id=program.id,
                                       processor_id=processor_id,
                                       message=message),
                           countdown=start_in or None,
                           queue=queue_name)

    def handle_error(self, program, processor_id, message, exception, exc_traceback):
        """
        Handles a message processing error.
        :param program: program
        :type program: pypipes.program.Program
        :param processor_id: processor id
        :param message: message that coused an error
        :param exception: exception
        :type exception: (str, Exception, str)
        :param exc_traceback:
        """
        raise NotImplementedError()


class CelerySchedulerMixIn(ISchedulerCommands):

    MAX_SCHEDULER_COUNTDOWN = 5 * 60  # 5 minutes
    MIN_SCHEDULER_PRECISION = 3  # run immediately if task delay is less than 3 sec.

    def _scheduler_queue_name(self, program):
        """
        :type self: CeleryErrorHandlerMixIn, BaseCeleryInf
        """
        return self._queue_name(program, None, queue_type='scheduler',
                                default='{program_id}.scheduled_tasks')

    @staticmethod
    def _scheduler_storage_id(program, scheduler_id):
        return key(program.id, scheduler_id)

    @staticmethod
    def _scheduler_collection_id(program):
        return key('schedulers', program.id)

    def try_stop_program(self, program):
        """
        :type self: CelerySchedulerMixIn | BaseCeleryInf
        """
        if super(CelerySchedulerMixIn, self).try_stop_program(program):
            # remove all active schedulers
            self.scheduler_storage.delete_collection(self._scheduler_collection_id(program),
                                                     delete_items=True)
            return True
        return False

    def init_application(self):
        """
        :type self: CelerySchedulerMixIn | BaseCeleryInf
        """
        app = super(CelerySchedulerMixIn, self).init_application()
        app.task(name='pipe_scheduler', bind=True, max_retries=None)(
            ready_for_celery_bind(self.scheduler_task))
        return app

    def _list_queues(self, program):
        result = super(CelerySchedulerMixIn, self)._list_queues(program)
        result.add(self._scheduler_queue_name(program))
        return result

    def scheduler_task(self, task, program_id, scheduler_id, token,
                       processor_id, message, repeat_period, start_time):
        """
        This task repeats itself periodically to implement a scheduler
        :type self: CelerySchedulerMixIn, BaseCeleryInf
        :param task: celery task reference
        :type task: celery.task.Task
        :param program_id: program id
        :param scheduler_id: scheduler id
        :param token: unique scheduler token
        :param processor_id: processor id
        :param message: scheduled message
        :param repeat_period: repeat period of the scheduler
        :param start_time: time when the task have to be started
        """
        countdown = 0
        try:
            logger.debug('Start scheduler_task for: %s',
                         (program_id, token, processor_id, message, repeat_period, start_time))
            program = self.get_program(program_id)
            if not program:
                logger.warning('received a scheduled message for unknown program: %s', program_id)
                return

            if not self.program_lock.get(key('started', program_id)):
                # the program is already stopped
                return

            full_scheduler_id = self._scheduler_storage_id(program, scheduler_id)
            saved_token = self.scheduler_storage.get_item(full_scheduler_id)
            if not saved_token or saved_token.value != token:
                # scheduler was updated. Current scheduler task is not actual anymore
                return

            if self.program_lock.release(key('scheduler', full_scheduler_id)):
                logger.info('Scheduler %s activated on demand', scheduler_id)
            elif start_time:
                countdown = self._get_countdown(start_time)
                if countdown and countdown > self.MIN_SCHEDULER_PRECISION:
                    logger.debug('Start time is not reached yet, wait next %s seconds',
                                 countdown)
                    raise task.retry(countdown=countdown)

            self.send_message(program, processor_id, message)

            if repeat_period:
                # restart this task in repeat_periods
                start_time = datetime.now() + timedelta(seconds=repeat_period)
                countdown = self._get_countdown(start_time)
                logger.debug('Schedule next task execution at %s, next tick in %s seconds',
                             start_time, countdown)

                task.request.retries = 0  # drop retries counter
                raise task.retry(kwargs=dict(task.request.kwargs,
                                             start_time=start_time),
                                 countdown=countdown)

        except MaxRetriesExceededError:
            # just in case, to be sure, that max retry error is not possible here
            logger.error('MaxRetriesExceededError happened in scheduler: %s', scheduler_id)
            task.request.retries = 0
            raise task.retry(countdown=countdown)
        except TaskPredicate:
            # celery service exceptions like Retry
            raise
        except Exception:
            # in case of any other exception, retry the task immediately
            logger.exception('Scheduler task raised an exception for '
                             'args:%r, kwargs:%r', task.request.args, task.request.kwargs)
            raise task.retry(countdown=5)

    @property
    def scheduler_storage(self):
        """
        Return storage for schedulers
        :type self: CelerySchedulerMixIn, BaseCeleryInf
        :return: storage service
        :rtype: pypipes.service.storage.IStorage
        """
        storage = self.context.get('storage')
        if not storage:
            raise RuntimeError('storage.celery service is required '
                               'to save schedulers')
        return storage.celery

    def add_scheduler(self, program, scheduler_id, processor_id, message,
                      start_time=None, repeat_period=None):
        """
        :type self: CelerySchedulerMixIn, BaseCeleryInf
        """
        unique_token = uuid4().hex
        self.scheduler_storage.save(self._scheduler_storage_id(program, scheduler_id), unique_token,
                                    collections=[self._scheduler_collection_id(program)])
        self.app.send_task('pipe_scheduler',
                           kwargs=dict(
                               program_id=program.id,
                               scheduler_id=scheduler_id,
                               token=unique_token,
                               processor_id=processor_id,
                               message=message,
                               repeat_period=repeat_period and repeat_period.total_seconds(),
                               start_time=start_time or None
                           ),
                           countdown=self._get_countdown(start_time),
                           queue=self._scheduler_queue_name(program))

    def _get_countdown(self, start_time):
        if not start_time:
            return None
        countdown = (start_time - datetime.now()).total_seconds()
        if countdown < 0:
            return None
        if countdown > self.MAX_SCHEDULER_COUNTDOWN:
            countdown = self.MAX_SCHEDULER_COUNTDOWN
        return countdown

    def remove_scheduler(self, program, scheduler_id):
        """
        :type self: CelerySchedulerMixIn, BaseCeleryInf
        """
        self.scheduler_storage.delete(self._scheduler_storage_id(program, scheduler_id))

    def list_schedulers(self, program):
        ids = self.scheduler_storage.get_collection(self._scheduler_collection_id(program),
                                                    only_ids=True)
        return [scheduler_id.split('.', 1)[1] for scheduler_id in ids]

    def trigger_scheduler(self, program, scheduler_id):
        """
        :type self: CelerySchedulerMixIn, BaseCeleryInf
        """
        full_scheduler_id = self._scheduler_storage_id(program, scheduler_id)
        if self.scheduler_storage.get_item(full_scheduler_id):
            # this lock is a trigger for scheduler task
            self.program_lock.set(key('scheduler', full_scheduler_id),
                                  expire_in=self.MAX_SCHEDULER_COUNTDOWN * 2)
            return True


class SaveErrorMixIn(object):

    def _error_queue_name(self, program, processor_id, exception):
        """
        :type self: CeleryErrorHandlerMixIn, BaseCeleryInf
        """
        return self._queue_name(program, processor_id, queue_type='error',
                                default='{program_id}.error.{processor_id}.{error_type}',
                                error_type=exception.__class__.__name__)

    def handle_error(self, program, processor_id, message, exception, exc_traceback):
        """
        :type self: CeleryErrorHandlerMixIn, BaseCeleryInf
        """
        queue_name = self._error_queue_name(program, processor_id, exception)
        try:
            # check if the error may be properly serialized
            dumps({'exc': exception}, serializer=self.app.conf.task_serializer)
        except:  # noqa
            # the error instance can't be serialized as is, thus we save the error name instead
            exception = '{}({})'.format(exception.__class__.__name__, exception)
        # include the error into message body. This may be helpful in error processing later
        message['_exception'] = exception
        message['_exc_traceback'] = exc_traceback
        self.send_message(program, processor_id, message, queue=queue_name)
        logger.warning('Failed job was moved into a standby queue: %r', queue_name)


class CeleryInf(SaveErrorMixIn, CelerySchedulerMixIn, BaseCeleryInf):
    """
    Celery infrastructure with scheduler implemented on celery task and error handler.
    """
    pass
