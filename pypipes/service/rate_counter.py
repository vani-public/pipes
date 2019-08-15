import time
from datetime import datetime, timedelta
from threading import Lock

from pypipes.context.config import client_config
from pypipes.service.base import ComplexKey
from pypipes.service.base_client import MemcachedClient, get_memcached_client

from pypipes.context.factory import ContextPoolFactory, LazyContextPoolFactory


class IRateCounter(object):
    def increment(self, name, value=1, threshold=1):
        """
        Increment a rate counter. Counter expires in `threshold` period
        :param name: counter name
        :param value: increment on value
        :param threshold: threshold period in seconds or callable that returns a threshold
        :return: (counter value, time till counter expiration in seconds)
        :rtype: (int, float)
        """
        raise NotImplementedError()


class MemoryRateCounter(IRateCounter):
    def __init__(self):
        self.counters = {}
        self._sync = Lock()

    def increment(self, name, value=1, threshold=1):
        with self._sync:
            current_time = int(time.time())
            current_value, expiration_time = None, None
            if name in self.counters:
                current_value, expiration_time = self.counters[name]
                if expiration_time <= current_time:
                    current_value = None

            if current_value:
                result = current_value + value
            else:
                result = value
                if callable(threshold):
                    threshold = threshold()
                expiration_time = current_time + int(threshold)
            self.counters[name] = (result, expiration_time)
        return result, expiration_time - current_time


class MemcachedRateCounter(MemcachedClient, ComplexKey, IRateCounter):
    MAX_KEY_LENGTH = 250  # memcached key length is limited

    def __init__(self, prefix=None, client=None, **memcached_params):
        ComplexKey.__init__(self, prefix)
        MemcachedClient.__init__(self, client, **memcached_params)

    def increment(self, name, value=1, threshold=1):
        counter_key = self.format_key(name, 'c')
        expiration_key = self.format_key(name, 'e')
        try:
            result = self.memcached.incr(counter_key, value)
            result = result and self.memcached.get_multi([counter_key, expiration_key])
            if not result:
                raise KeyError('counter expired')
            # convert expiration time into seconds till expiration
            expires_in = (max((result[expiration_key] - datetime.utcnow()).total_seconds(), 0)
                          if expiration_key in result else 0)
            return result.get(counter_key, 0), expires_in
        except Exception:
            # add a counter key with expiration time if it doesn't exist

            if callable(threshold):
                threshold = threshold()
            threshold = int(threshold)  # ensure threshold value is an integer

            # memcached client doesn't have an ability to get a ttl of a key
            # therefore we save an expiration time into cache as well
            expiration_time = datetime.utcnow() + timedelta(seconds=threshold)
            self.memcached.add_multi({
                counter_key: value,  # initial counter value
                expiration_key: expiration_time  # counter expiration time
            }, time=threshold)
            return value, threshold


memory_rate_pool = ContextPoolFactory(lambda name: MemoryRateCounter())
local_memcached_rate_pool = ContextPoolFactory(MemcachedRateCounter)  # service name => prefix
memcached_rate_pool = LazyContextPoolFactory(
    lambda name, memcached_config=client_config.memcached:
    MemcachedRateCounter('r:{}'.format(name),
                         client=get_memcached_client(memcached_config.rate_counter[name])))
