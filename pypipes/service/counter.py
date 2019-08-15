import logging
from collections import defaultdict

from pypipes.context.config import client_config
from pypipes.service.base import ComplexKey
from pypipes.service.base_client import RedisClient, get_redis_client

from pypipes.context.factory import ContextPoolFactory, LazyContextPoolFactory

logger = logging.getLogger(__name__)


class ICounter(object):

    def increment(self, name, value=1):
        """
        Increment counter
        :param name: counter name
        :param value: increment counter on this value. Can be negative
        :return: counter value after increment operation
        """
        raise NotImplementedError()

    def delete(self, name):
        """
        Delete counter
        :param name: counter name
        """
        raise NotImplementedError()


class MemCounter(ICounter):
    def __init__(self):
        self.counters = defaultdict(lambda: 0)

    def increment(self, name, value=1):
        self.counters[name] = result = self.counters[name] + value
        logger.debug('Incremented counter %s by %s = %s', name, value, result)
        return result

    def delete(self, name):
        self.counters.pop(name, None)
        logger.debug('Deleted counter %s', name)


class RedisCounter(RedisClient, ComplexKey, ICounter):

    def __init__(self, prefix=None, client=None, **redis_params):
        ComplexKey.__init__(self, prefix)
        RedisClient.__init__(self, client, **redis_params)

    def increment(self, name, value=1):
        key = self.format_key(name)
        return self.redis.incr(key, int(value))

    def delete(self, name):
        key = self.format_key(name)
        self.redis.delete(key)


memory_counter_pool = ContextPoolFactory(lambda name: MemCounter())
# service name in pool => redis prefix
local_redis_counter_pool = ContextPoolFactory(RedisCounter)
redis_counter_pool = LazyContextPoolFactory(
    lambda name, redis_config=client_config.redis:
    RedisCounter('c:{}'.format(name), client=get_redis_client(redis_config.counter[name])))
