import uuid

from pypipes.context import apply_context_to_kwargs
from pypipes.context.lock import singleton_guard
from pypipes.context.manager import pipe_contextmanager
from pypipes.exceptions import DropMessageException
from pypipes.processor import event_processor, pipe_processor
from pypipes.service import key


class Job(object):
    MAX_JOB_INACTIVE_PERIOD = 60
    JOB_EVENT_TEMPLATE = 'start_job_{}'

    def __init__(self, job_name, expires_in=None, **kwargs):
        self.job_name = job_name
        self.kwargs = kwargs
        self.expires_in = expires_in or self.MAX_JOB_INACTIVE_PERIOD
        self.event_name = self.JOB_EVENT_TEMPLATE.format(job_name)
        self.job_collection = key('started_jobs', job_name)
        self.job_id_key = '_job_{}'.format(job_name)

    def __call__(self, pipeline):
        return self.job_start_processor() >> self.job_guard(pipeline)

    @staticmethod
    def create_job_key(name, params):
        return key(name, **params)

    def job_start_processor(self):

        @singleton_guard(self.job_name, **self.kwargs)  # prevent simultaneous start
        @event_processor(self.event_name)
        def job_start(message, lock, storage, logger, injections):
            job_params = apply_context_to_kwargs(self.kwargs, injections)
            job_key = self.create_job_key(self.job_name, job_params)

            # ensure job is not started
            job_info = storage.job.get_item(job_key)
            if job_info and lock.job.get(job_info.value[0]):
                logger.warning('Job %r %s is still active and cannot be started again.',
                               self.job_name, job_params)
                return

            # start new job
            job_id = uuid.uuid4().hex

            logger.debug('Starting job %s: %s', job_key, job_id)
            lock.job.set(job_id, expire_in=self.expires_in)
            storage.job.save(job_key, (job_id, job_params), collections=[self.job_collection])

            response = dict(message)
            response[self.job_id_key] = job_id
            return response

        return job_start

    def list_active_jobs(self, context):
        storage = context['storage'].job
        lock = context['lock'].job
        result = []
        for active_job in storage.get_collection(self.job_collection):
            job_id, job_params = active_job.value
            if lock.get(job_id):
                # this job is active
                result.append((job_id, job_params))
        return result

    def stop(self, job_id, context):
        lock = context['lock'].job
        return lock.release(job_id)

    @staticmethod
    def stop_job(*jobs):

        @pipe_processor
        def _stop_job(injections, logger):
            for job in jobs:
                for active_job_id, _ in job.list_active_jobs(injections):
                    logger.info('Stopped job %s, id: %s', job.job_name, active_job_id)
                    job.stop(active_job_id, injections)
        return _stop_job

    @property
    def job_guard(self):

        @pipe_contextmanager
        def _job_guard(message, response, lock):
            """
            This context manager check if job is still active
            :type lock: pypipes.context.pool.IContextPool[pypipes.service.lock.ILock]
            :param injections:
            :return:
            """
            job_id = message.get(self.job_id_key)
            if not job_id or not lock.job.prolong(job_id, expire_in=self.expires_in):
                # this job is not active and have to be terminated
                raise DropMessageException('Job {} is terminated'.format(job_id))

            def _filter(msg):
                msg[self.job_id_key] = job_id
                # automatically prolong job when new message emitted
                lock.job.prolong(job_id, expire_in=self.expires_in)
                return msg
            response.add_message_filter(_filter)
            yield {'job_id': job_id}
        return _job_guard
