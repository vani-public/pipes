from pypipes.context import apply_context_to_kwargs, context
from pypipes.context.lock import suspended_guard
from pypipes.context.manager import pipe_contextmanager
from pypipes.exceptions import RateLimitExceededException
from pypipes.service import key


def quota_guard(quota_name=None, suspend=True,
                operation_name=None, **context_kwargs):
    """
    Consume from quota on each message processing begin
    :param quota_name: name of quota
    :param operation_name: name of operation if you have a separate quota per operation
    :param context_kwargs: addition quota sub-key parameters
    :param suspend: suspend processing if quota is exceeded, otherwise drop messages
    :return: pipe_contextmanager
    :rtype: pipe_contextmanager
    """
    lock_name = key(quota_name, operation_name) if operation_name else quota_name

    @suspended_guard(lock_name, lock_category='quota_guard', retry_if_locked=suspend,
                     **context_kwargs)
    @pipe_contextmanager
    def quota_guard_contextmanager(injections, quota=None):
        """
        :type injections: pypipes.context.LazyContextCollection
        :type quota: pypipes.context.pool.IContextPool[pypipes.service.quota.IQuota]
        :raise: pypipes.exceptions.QuotaExceededException
        """
        if quota:
            quota_key = (key(operation_name or '',
                             **apply_context_to_kwargs(context_kwargs, injections))
                         if operation_name or context_kwargs
                         else None)
            # try to consume from quota
            # consume raises a QuotaExceededException when quota is exceeded
            quota[quota_name].consume(quota_key)

        yield {}
    return quota_guard_contextmanager


def rate_limit_guard(rate_limit, rate_threshold=10, suspend=True, **context_kwargs):
    """
    Create a contextmanager that control a processor execution rate
    :param rate_limit: rate limit
    :param rate_threshold: rate threshold
    :param retry_if_locked: retry message execution if processor rate is exceeded
    :param context_kwargs: separate processor rate counter per input context
    :rtype: pipe_contextmanager
    """
    @suspended_guard(None, lock_category='rate_limit', retry_if_locked=suspend,
                     processor=context.processor_id, **context_kwargs)
    @pipe_contextmanager
    def rate_contextmanager(processor_id, rate_counter, injections):
        """
        Calculate processor execution rate
        :param processor_id: processor id
        :type rate_counter: pypipes.context.pool.IContextPool[pypipes.service.rate_counter.IRateCounter]
        :type injections: pypipes.context.LazyContextCollection
        """
        counter_key = key(processor_id,
                          **apply_context_to_kwargs(context_kwargs, injections))
        value, expires_in = rate_counter.rate_limit.increment(counter_key,
                                                              threshold=rate_threshold)
        if value > rate_limit:
            raise RateLimitExceededException(retry_in=expires_in)
        yield {}
    return rate_contextmanager
