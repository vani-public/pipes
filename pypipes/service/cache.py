import time
from copy import deepcopy
from functools import partial

from pypipes.context.config import client_config
from pypipes.service import key
from pypipes.service.base import ComplexKey, MemcachedComplexKey
from pypipes.service.base_client import RedisClient, MemcachedClient, get_redis_client, \
    get_memcached_client
from pypipes.service.hash import IHash

from pypipes.context.factory import ContextPoolFactory, LazyContextPoolFactory


class ICache(IHash):
    def save(self, key, value, expires_in=None):
        """
        Save value into the cache
        :param key: key
        :param value: any python object
        :param expires_in: expiration time in seconds
        """
        raise NotImplementedError()

    def save_many(self, values, expires_in=None):
        """
        Save many values into the cache
        :param values: value mapping
        :type values: dict[str, object]
        :param expires_in: expiration time in seconds
        """
        raise NotImplementedError()

    def get(self, key, default=None):
        """
        Get value from the cache if exists otherwise returns default value
        :param key: key to lookup
        :param default: default value
        :return: cached value or default
        """
        raise NotImplementedError()

    def get_many(self, keys, default=None):
        """
        Get many values from the cache if exists otherwise returns default value
        :param keys: list of key to lookup
        :type keys: list[str]
        :param default: default value
        :return: value mapping {key: <cached value or default>}
        :rtype: dict[str, object]
        """
        raise NotImplementedError()

    def delete(self, key):
        """
        Delete key from the cache.
        :param key: key to delete
        """
        raise NotImplementedError()

    def delete_many(self, keys):
        """
        Delete many keys from the cache.
        :param keys: list of keys to delete
        :type keys: list[str]
        """
        raise NotImplementedError()


class MemoryCache(ICache):

    def __init__(self):
        self.storage = {}

    def save(self, key, value, expires_in=None):
        self.storage[key] = (deepcopy(value), expires_in and time.time() + expires_in)

    def save_many(self, values, expires_in=None):
        # dict operations are thread-safe therefore no need to use any additional lock here
        expiration_time = expires_in and time.time() + expires_in
        self.storage.update({key: (deepcopy(values[key]), expiration_time)
                             for key in values})

    def get(self, key, default=None):
        value, expiration_time = self.storage.get(key, (default, None))
        if expiration_time and expiration_time < time.time():
            # value expired
            return default
        return deepcopy(value)

    def get_many(self, keys, default=None):
        # Note! This operation is not thread safe :(
        # because some value might be updated during operation
        return {key: self.get(key, default) for key in keys}

    def delete(self, key):
        default = object()
        result = self.storage.pop(key, default)
        return result != default

    def delete_many(self, keys):
        # Note! This operation is not thread safe :(
        # because some value might be updated during operation
        for k in keys:
            self.storage.pop(k, None)


class RedisCache(RedisClient, ComplexKey, ICache):
    def __init__(self, prefix=None, client=None, **redis_params):
        ComplexKey.__init__(self, prefix)
        RedisClient.__init__(self, client, **redis_params)

    def save(self, key, value, expires_in=None):
        key = self.format_key(key)
        self.redis.set(key, self._serialize(value), ex=expires_in)

    def save_many(self, values, expires_in=None):
        values = tuple((self.format_key(key), self._serialize(values[key]))
                       for key in values)
        pipe = self.redis.pipeline()
        for k, v in values:
            pipe.set(k, v, ex=expires_in)
        pipe.execute()

    def get(self, key, default=None):
        key = self.format_key(key)
        value = self.redis.get(key)
        return default if value is None else self._deserialize(value)

    def get_many(self, keys, default=None):
        values = self.redis.mget(list(map(self.format_key, keys)))
        return dict(map(lambda key, value: (key, default if value is None
                                            else self._deserialize(value)), keys, values))

    def delete(self, key):
        key = self.format_key(key)
        return bool(self.redis.delete(key))

    def delete_many(self, keys):
        keys = map(self.format_key, keys)
        self.redis.delete(*keys)


class MemcachedCache(MemcachedClient, ComplexKey, ICache):
    COMPRESSION_CUTOFF_LEN = 10000  # memcached client implements compression by itself
    MAX_KEY_LENGTH = 250  # memcached key length is limited

    def __init__(self, prefix=None, client=None, **memcached_params):
        ComplexKey.__init__(self, prefix)
        MemcachedClient.__init__(self, client, **memcached_params)

    def save(self, key, value, expires_in=None):
        key = self.format_key(key)
        self.memcached.set(key, value, time=expires_in or 0,
                           min_compress_len=self.COMPRESSION_CUTOFF_LEN)

    def save_many(self, values, expires_in=None):
        values = {self.format_key(key): values[key]
                  for key in values}
        self.memcached.set_multi(values, time=expires_in or 0,
                                 min_compress_len=self.COMPRESSION_CUTOFF_LEN)

    def get(self, key, default=None):
        key = self.format_key(key)
        # get_multi is used here to know when key is missing
        result = self.memcached.get_multi([key])
        return result.get(key, default)

    def get_many(self, keys, default=None):
        formatted_keys = list(map(self.format_key, keys))
        result = self.memcached.get_multi(formatted_keys)
        return {key: result.get(formatted_key, default)
                for key, formatted_key in zip(keys, formatted_keys)}

    def delete(self, key):
        key = self.format_key(key)
        return bool(self.memcached.delete(key))

    def delete_many(self, keys):
        self.memcached.delete_multi(map(self.format_key, keys))


class DefaultExpirationWrapper(ICache):
    """
    Overrides save and save_many in order to use default TTL value
    if caller doesn't pass TTL explicitly
    """

    USE_DEFAULT = object()  # this marker will indicate that default ttl value have to be used

    def __init__(self, cache, default_ttl):
        self.default_ttl = default_ttl
        self.cache = cache

    def _get_ttl(self, expires_in):
        return self.default_ttl if expires_in == self.USE_DEFAULT else expires_in

    def save(self, key, value, expires_in=USE_DEFAULT):
        return self.cache.save(key, value, expires_in=self._get_ttl(expires_in))

    def save_many(self, values, expires_in=USE_DEFAULT):
        return self.cache.save_many(values, expires_in=self._get_ttl(expires_in))

    def get(self, key, default=None):
        return self.cache.get(key, default)

    def get_many(self, keys, default=None):
        return self.cache.get_many(keys, default)

    def delete(self, key):
        return self.cache.delete(key)

    def delete_many(self, keys):
        return self.cache.delete_many(keys)


func_key_builder = MemcachedComplexKey('f')


def cached_method(func=None, expires_in=300, key_builder=None):
    if not func:
        return partial(cached_method, expires_in=expires_in, key_builder=key_builder)

    def wrapper(self, *args, **kwargs):
        global func_key_builder
        _key_builder = key_builder or func_key_builder
        func_key = _key_builder.format_key(self.__class__.__name__, func.__name__,
                                           key(*args, **kwargs))
        if self.cache:
            result = self.cache.get(func_key)
            if result:
                return result
        result = func(self, *args, **kwargs)
        if self.cache:
            self.cache.save(func_key, result, expires_in=expires_in)
        return result
    return wrapper


memory_cache_pool = ContextPoolFactory(lambda name: MemoryCache())
local_redis_cache_pool = ContextPoolFactory(RedisCache)  # service name => redis prefix
local_memcached_cache_pool = ContextPoolFactory(MemcachedCache)  # service name => redis prefix

redis_cache_pool = LazyContextPoolFactory(
    lambda name, redis_config=client_config.redis:
    RedisCache('h:{}'.format(name), client=get_redis_client(redis_config.cache[name])))

memcached_cache_pool = LazyContextPoolFactory(
    lambda name, memcached_config=client_config.memcached:
    MemcachedCache('h:{}'.format(name), client=get_memcached_client(memcached_config.cache[name])))
