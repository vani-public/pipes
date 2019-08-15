import zlib

import six
from pypipes.service import config_singleton

if six.PY3:
    import pickle
else:
    import cPickle as pickle


class PickleSerializer(object):
    ZLIB_COMPRESSED_HEADER = b'__ZLIB__'
    COMPRESSION_CUTOFF_LEN = 100000

    @classmethod
    def _serialize(cls, obj):
        """
        Common serialization for storing objects in cache.

        If the object is huge, it will be transparently compressed.
        """
        serialized = pickle.dumps(obj)

        # If the pickle string is larger than our cutoff, compress the
        # value automatically. Prefix the value with our magic
        # ZLIB_COMPRESSED_HEADER, so that at deserialization time we know
        # that we are dealing with a compressed value.
        if cls.COMPRESSION_CUTOFF_LEN and len(serialized) > cls.COMPRESSION_CUTOFF_LEN:
            serialized = cls.ZLIB_COMPRESSED_HEADER + zlib.compress(serialized)
        return serialized

    @classmethod
    def _deserialize(cls, obj):
        """
        Common deserialization for fetching objects from cache. `obj`
        must be not-None.

        Handles objects which may have been serialized in compressed format.
        """
        if obj[:len(cls.ZLIB_COMPRESSED_HEADER)] == cls.ZLIB_COMPRESSED_HEADER:
            obj = zlib.decompress(obj[len(cls.ZLIB_COMPRESSED_HEADER):])
        return pickle.loads(obj)


class RedisClient(PickleSerializer):

    def __init__(self, client, **redis_params):
        if client:
            self._client = client
        else:
            self._client = get_redis_client(redis_params)

    @property
    def redis(self):
        return self._client


class MemcachedClient(object):
    def __init__(self, client, **memcached_params):
        """
        :type client: pylibmc.Client
        :param memcached_params: pylibmc.Client init parameters
        """
        if client:
            self._client = client
        else:
            self._client = get_memcached_client(memcached_params)

    @property
    def memcached(self):
        """
        :return: memcached client
        :rtype: pylibmc.Client
        """
        return self._client


@config_singleton
def get_redis_client(config=None):
    from redis import StrictRedis
    return StrictRedis(**config or {})


@config_singleton
def get_memcached_client(config=None):
    try:
        from pylibmc import Client
    except ImportError:
        # try to use an alternative client
        from memcache import Client as BaseClient

        class Client(BaseClient):
            def add_multi(self, mapping, time=0, key_prefix='', min_compress_len=0, noreply=False):
                """
                Behaviour of set_multi is some different but there is no other alternative
                """
                return self.set_multi(mapping, time, key_prefix, min_compress_len, noreply)

            def delete(self, key, time=None, noreply=False):
                """
                The logic of delete method is some different in memcache and pylibmc client
                pylibmc client returns True only if key exists in cache
                memcache client always returns True
                This patch fixes the situation
                """
                return self._deletetouch([b'DELETED'], "delete", key, time, noreply)
    memcached_params = dict({'servers': ['localhost']}, **config or {})
    return Client(**memcached_params)
