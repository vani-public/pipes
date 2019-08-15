from pypipes.context import apply_context_to_kwargs
from pypipes.context.manager import pipe_contextmanager
from pypipes.exceptions import RetryMessageException, DropMessageException, RetryException
from pypipes.service import key

DEFAULT_LOCK_EXPIRATION = 3600  # 1 hour
DEFAULT_GUARD_EXPIRATION = 60  # 1 minute


class Locker(object):
    def __init__(self, lock_service, lock_key, expire_in=None):
        """
        :param lock_service: lock service
        :type lock_service: pypipes.service.lock.ILock
        :param lock_key: lock key
        :param expire_in: expiration time in seconds
        """
        self.lock_service = lock_service
        self.lock_key = lock_key
        self.expire_in = expire_in

    def set(self, expire_in=None):
        return self.lock_service.set(self.lock_key, expire_in or self.expire_in)

    def prolong(self):
        return self.set()

    def acquire(self):
        return self.lock_service.acquire(self.lock_key, self.expire_in)

    def release(self):
        return self.lock_service.release(self.lock_key)

    def get(self):
        return self.lock_service.get(self.lock_key)

    def min_expiration(self, default=None):
        """
        Returns smallest value between default expiration and this lock expiration time
        :param default: default expiration in seconds
        :return: min value
        """
        expire_in = self.get()
        if expire_in is True or expire_in is False:
            return default
        return min(default, expire_in)


def locker(lock_name=None, expire_in=DEFAULT_LOCK_EXPIRATION, lock_category='default',
           **context_kwargs):
    """
    Create a Locker for inner processor.
    :param lock_category: name of lock service
    :param lock_name: lock name. Processor id is used as a name if lock_name is missed.
        Note that processor id depends on program pipeline.
    :param expire_in: lock expiration time in seconds.
    :param context_kwargs: additional lock key parameters based on current context.
        Use ContextPath to extract values from processor context
    :rtype: pypipes.context.manager.PipeContextManager
    """
    @pipe_contextmanager
    def locker_contextmanager(processor_id, injections, lock):
        lock_service = lock[lock_category]
        lock_key = key(lock_name or processor_id,
                       **apply_context_to_kwargs(context_kwargs, injections))
        locker_obj = Locker(lock_service, lock_key, expire_in)
        context = {'locker': locker_obj}
        if lock_name:
            context['{}_locker'.format(lock_name)] = locker_obj
        yield context
    return locker_contextmanager


def singleton_guard(guard_name=None, expire_in=DEFAULT_GUARD_EXPIRATION, lock_category='guard',
                    retry_if_locked=False, **context_kwargs):
    """
    Protects some resource from be used by several processors simultaneously.
    :param guard_name: guard name
    :param context_kwargs: additional guard key parameters based on current context.
    :param expire_in: guard lock expiration in seconds.
        guard automatically prolong a lock each time when new message is emitted by a processor
    :param lock_category: name of lock service
    :param retry_if_locked: if True the guard will retry each message if guard is currently locked
        otherwise message will be dropped.
    :rtype: pypipes.context.manager.PipeContextManager
    """

    @locker(lock_name=guard_name, expire_in=expire_in, lock_category=lock_category,
            **context_kwargs)
    @pipe_contextmanager
    def guard_contextmanager(locker, response):
        """
        :type locker: Locker
        :type response: pypipes.infrastructure.response.IResponseHandler
        """
        if not locker.acquire():
            if retry_if_locked:
                raise RetryMessageException(retry_in=locker.min_expiration(15))
            else:
                raise DropMessageException('locked by singleton_guard')

        _filter = None
        if expire_in:
            # add a filter that automatically prolong the guard lock when a new message is emitted
            def _filter(msg):
                locker.prolong()
                return msg
            response.add_message_filter(_filter)

        try:
            yield {}
        finally:
            if _filter:
                response.remove_message_filter(_filter)
            # release the guard lock
            locker.release()
    return guard_contextmanager


def suspended_guard(guard_name=None, expire_in=None, lock_category='guard',
                    retry_if_locked=False, **context_kwargs):
    """
    Suspend processing of some messages while guard lock is not expired
    :param guard_name: guard name
    :param context_kwargs: additional guard key parameters based on current context.
    :param expire_in: guard lock expiration in seconds.
        guard automatically prolong a lock each time when new message is emitted by a processor
    :param lock_category: name of lock service
    :param retry_if_locked: if True the guard will retry messages if guard is currently locked
        otherwise message will be dropped.
    :rtype: pypipes.context.manager.PipeContextManager
    """

    # some expiration time is required
    expire_in = expire_in or DEFAULT_GUARD_EXPIRATION

    @locker(lock_name=guard_name, expire_in=expire_in, lock_category=lock_category,
            **context_kwargs)
    @pipe_contextmanager
    def guard_contextmanager(locker):
        lock_time = locker.get()
        if lock_time:
            # message processing is suspended
            if retry_if_locked:
                raise RetryMessageException(retry_in=lock_time)
            else:
                raise DropMessageException('locked by suspended_guard')
        try:
            yield {'guard_locker': locker}
        except RetryException as e:
            locker.set(expire_in=e.retry_in)
            if retry_if_locked:
                # suspend current message as well
                raise RetryMessageException(retry_in=e.retry_in)
            else:
                raise DropMessageException('locked by suspended_guard')
    return guard_contextmanager
