import logging
from datetime import datetime, timedelta
from functools import partial
from time import sleep

from pypipes.context import apply_context_to_kwargs, injections_handler
from pypipes.context.manager import pipe_contextmanager
from pypipes.exceptions import DropMessageException, RetryMessageException
from pypipes.service import key

from pypipes.context.factory import lazy_context

logger = logging.getLogger(__name__)


def cache(expires_in=None, **context_kwargs):

    @pipe_contextmanager
    def cache_contextmanager(message, processor_id, response, injections, cache):
        """
        Cache processor response
        :type message: pypipes.message.FrozenMessage
        :param processor_id: unique processor id
        :type response: pypipes.infrastructure.response.IResponseHandler
        :type injections: dict
        :type cache: pypipes.context.pool.IContextPool[pypipes.service.cache.ICache]
        """
        cache_client = cache.processor

        key_params = (apply_context_to_kwargs(context_kwargs, injections)
                      if context_kwargs else message)
        cache_key = key(processor_id, **key_params)
        messages = cache_client.get(cache_key)
        if messages is not None:
            # emit cached results
            for message in messages:
                response.emit_message(message)

            # skip message processing
            raise DropMessageException('Message hit in cache')

        # temp storage for emitted messages
        emitted_messages = []

        def _filter(msg):
            # save emitted message into temp storage
            emitted_messages.append(dict(msg))
            return msg
        response.add_message_filter(_filter)

        yield {'cache_key': cache_key}

        # save messages into the cache if no exception
        def _cache_on_flush(original_flush):
            original_flush()
            # cache processing result
            cache_client.save(cache_key, emitted_messages, expires_in=expires_in)
        response.extend_flush(_cache_on_flush)

    return cache_contextmanager


class MetaValue(object):
    def __init__(self, value, **metadata):
        self.value = value
        self.metadata = metadata or {}

    def __getattr__(self, item):
        try:
            return super(MetaValue, self).__getattr__(item)
        except AttributeError:
            # get value from metadata if exist
            return self.metadata.get(item)


def cached_lazy_context(_gen_func=None, expires_in=None, generation_time=5,
                        **key_params):
    """
    Create a lazy context that handles context generation, caching and refreshing
    :param _gen_func: Context factory that may use context injections
    :type _gen_func: (ANY) -> object
    :param expires_in: context cache expiration time in seconds
    :param generation_time: time needed to generate a new context
    :param key_params: context key. The factory will create a separate context per context key
    :rtype: pypipes.context.factory.LazyContext
    """
    if _gen_func is None:
        return partial(cached_lazy_context, expires_in=expires_in,
                       generate_in=generation_time,
                       **key_params)

    assert generation_time > 0
    context_name = _gen_func.__name__
    context_factory = injections_handler(_gen_func)

    def get_refresh_at(_expires_in):
        # returns time when the context may be refreshed
        if _expires_in:
            return datetime.utcnow() + timedelta(
                seconds=(_expires_in - min(_expires_in / 2, generation_time)))

    @lazy_context
    def _cached_lazy_context(cache, lock, injections):
        """
        Get context from cache or create a new one
        :type cache: IContextPool[ICache]
        :type lock: IContextPool[ILock]
        :param injections: context collection
        :return: context object
        """
        context_cache = cache.context
        context_lock = lock.context

        context_key = key(context_name, apply_context_to_kwargs(key_params, injections))

        def _create_and_save_context():
            new_context = context_factory(injections)

            if isinstance(new_context, MetaValue):
                # factory may override default expiration and other predefined params
                _expires_in = int(new_context.metadata.get('expires_in', expires_in))
                _lock_on = int(new_context.metadata.get('lock_on', 0))
                new_context = new_context.value
            else:
                _expires_in = expires_in
                _lock_on = False

            if _lock_on:
                # context factory demands to lock it for some time
                # that may be caused by rate limit or other resource limitations
                logger.warning('Context factory for %s is locked for %s seconds',
                               context_key, _lock_on)
                context_lock.set(context_key, expire_in=_lock_on)

            if new_context is None:
                raise AssertionError('Context value for %r is not available', context_key)

            # save new value into the cache
            context_cache.save(context_key, (new_context, get_refresh_at(_expires_in)),
                               expires_in=_expires_in)
            return new_context

        tries = 10  # max count of tries to get the context
        while tries > 0:
            tries -= 1
            value = context_cache.get(context_key)

            if value:
                context, refresh_at = value
                if (refresh_at and datetime.utcnow() > refresh_at and
                        context_lock.acquire(context_key, expire_in=generation_time)):
                    # current context value is still valid
                    # but it's a good time to prepare a new one in advance
                    # that will prevents blocking of other processors
                    context = _create_and_save_context()
                return context
            else:
                if context_lock.acquire(context_key, expire_in=generation_time):
                    return _create_and_save_context()
                else:
                    # some other processor is generating the context right now
                    # check when the new context will be ready
                    lock_expires_in = context_lock.get(context_key)
                    if lock_expires_in:
                        if lock_expires_in > generation_time:
                            # the context factory locked itself for a long time
                            raise RetryMessageException(
                                'Context factory for {!r} is locked'.format(context_name),
                                retry_in=lock_expires_in)
                        # sleep a little and check again
                        sleep(1)

        # context is still not ready after all tries
        # retry the message processing some later
        logger.error('Failed to create context: %s', context_name)
        raise RetryMessageException(
            'Context {!r} creation took too much time'.format(context_name), retry_in=60)
    return _cached_lazy_context
