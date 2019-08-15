import logging
from datetime import datetime, timedelta
from threading import Lock

from pypipes.context.config import client_config
from pypipes.service.base import ComplexKey
from pypipes.service.base_client import RedisClient, get_redis_client

from pypipes.context.factory import ContextPoolFactory, LazyContextPoolFactory

logger = logging.getLogger(__name__)


class ILock(object):

    def acquire(self, name, expire_in=None):
        """
        Try to acquire a lock
        :param name: lock name
        :param expire_in: lock expiration time
        :type expire_in: lock expiration time in seconds
        :return: True if lock was successfully acquired, otherwise False
        """
        raise NotImplementedError()

    def set(self, name, expire_in=None):
        """
        Set lock without checking if lock is already acquired.
        May be used to prolong lock's expiration time
        :param name: lock name
        :param expire_in: lock expiration time in seconds
        :return:
        """
        raise NotImplementedError()

    def get(self, name):
        """
        Check if lock is set
        :param name: lock name
        :return: Lock expiration time in seconds,
            True if lock has no expiration time,
            False if lock is not set.
        """
        raise NotImplementedError

    def prolong(self, name, expire_in):
        """
        Prolong the lock if it exists
        :param name: lock name
        :param expire_in: lock expiration time in seconds. None - set infinite expiration time
        :return: True if lock exists,
                 False if lock doesn't exist.
        """
        raise NotImplementedError()

    def release(self, name):
        """
        Release a lock
        :param name: lock name
        :return: True if lock released, False if lock was not set.
        """
        raise NotImplementedError()


class MemLock(ILock):
    def __init__(self):
        super(MemLock, self).__init__()
        self._locks = {}
        self._sync = Lock()

    def acquire(self, key, expire_in=None):
        with self._sync:
            if self._get_lock(key):
                return False
            else:
                self._set_lock(key, expire_in)
                return True

    def set(self, key, expire_in=None):
        with self._sync:
            self._set_lock(key, expire_in)

    def get(self, key):
        with self._sync:
            return self._get_lock(key)

    def prolong(self, key, expire_in):
        assert expire_in is not None
        with self._sync:
            if key in self._locks:
                self._set_lock(key, expire_in)
                return True
            else:
                return False

    def release(self, key):
        with self._sync:
            if self._get_lock(key):
                del self._locks[key]
                return True
            else:
                return False

    def _get_lock(self, key):
        lock_timeout = self._locks.get(key)
        if not lock_timeout:
            return False
        elif lock_timeout is True:
            return True
        elif lock_timeout > datetime.now():
            return (lock_timeout - datetime.now()).total_seconds()
        else:
            # lock expired
            del self._locks[key]
            return False

    def _set_lock(self, key, expire_in=None):
        self._locks[key] = (datetime.now() + timedelta(seconds=expire_in)
                            if expire_in else True)


class RedisLock(RedisClient, ComplexKey, ILock):
    lua_get = None

    # KEYS[1] - lock name
    # return TTL value if lock has expiration time
    #       -1 if the lock has no TTL
    #       0 if lock not found
    LUA_GET_SCRIPT = """
             local token = redis.call('get', KEYS[1])
             if not token then
                 return 0
             end
             local expiration = redis.call('pttl', KEYS[1])
             if not expiration then
                 return -1
             end
             return expiration
         """

    def __init__(self, prefix=None, client=None, **kwargs):
        ComplexKey.__init__(self, prefix)
        RedisClient.__init__(self, client=client, **kwargs)
        RedisLock.register_scripts(self.redis)

    @classmethod
    def register_scripts(cls, redis):
        if cls.lua_get is None:
            cls.lua_get = redis.register_script(cls.LUA_GET_SCRIPT)

    def acquire(self, name, expire_in=None):
        key = self.format_key(name)
        logger.debug('Set lock: %s', key)
        timeout = expire_in and int(expire_in * 1000)
        return bool(self.redis.set(key, 1, px=timeout, nx=True))

    def set(self, name, expire_in=None):
        key = self.format_key(name)
        logger.debug('Set lock: %s', key)
        timeout = expire_in and int(expire_in * 1000)
        self.redis.set(key, 1, px=timeout)

    def get(self, name):
        key = self.format_key(name)
        logger.debug('Get lock: %s', key)
        result = self.lua_get(keys=[key],
                              client=self.redis)
        if not result:
            return False
        elif result == -1:
            return True
        else:
            return result / 1000.0

    def prolong(self, name, expire_in):
        timeout = int(expire_in * 1000)
        key = self.format_key(name)
        logger.debug('Prolong lock: %s', key)
        return bool(self.redis.pexpire(key, timeout))

    def release(self, name):
        key = self.format_key(name)
        logger.debug('Release lock: %s', key)
        return bool(self.redis.delete(key))


memory_lock_pool = ContextPoolFactory(lambda name: MemLock())
local_redis_lock_pool = ContextPoolFactory(RedisLock)  # lock service name => redis key prefix
redis_lock_pool = LazyContextPoolFactory(
    lambda name, redis_config=client_config.redis:
    RedisLock('l:{}'.format(name), client=get_redis_client(redis_config.lock[name])))
