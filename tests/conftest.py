import os

import pytest
from mock import Mock

from pypipes.config import ClientConfig, get_config
from pypipes.context.factory import ContextPoolFactory
from pypipes.infrastructure.base import Infrastructure
from pypipes.infrastructure.response.listener import ListenerResponseHandler
from pypipes.model import Model
from pypipes.service.base_client import get_redis_client, get_memcached_client
from pypipes.service.cache import MemoryCache, RedisCache, MemcachedCache
from pypipes.service.counter import MemCounter, RedisCounter
from pypipes.service.cursor_storage import CursorStorage, VersionedCursorStorage, ICursorStorage
from pypipes.service.lock import RedisLock, MemLock, ILock
from pypipes.service.metric import DataDogMetrics, LogMetrics, MetricDecorator
from pypipes.service.rate_counter import MemoryRateCounter, MemcachedRateCounter
from pypipes.service.storage import RedisStorage, MemStorage


@pytest.fixture(scope='session')
def config():
    return get_config(
        config_path=os.path.join(os.path.dirname(__file__), 'config.yaml'))


@pytest.fixture()
def redis_client(config):
    client = get_redis_client(ClientConfig(config.redis)['test'])
    client.flushall()
    return client


@pytest.fixture()
def memcached_client(config):
    client = get_memcached_client(ClientConfig(config.memcached)['test'])
    client.flush_all()
    return client


# ------------------------ IStorage fixtures
STORAGE_LIST = ['memory_storage', 'redis_storage']


@pytest.fixture()
def memory_storage():
    return MemStorage()


@pytest.fixture()
def redis_storage(redis_client):
    return RedisStorage('s:test', client=redis_client)


@pytest.fixture(params=STORAGE_LIST)
def storage(request):
    return request.getfixturevalue(request.param)


# ------------------------ ICache fixtures
CACHE_LIST = ['memory_cache', 'redis_cache', 'memcached_cache']


@pytest.fixture()
def memory_cache():
    return MemoryCache()


@pytest.fixture()
def redis_cache(redis_client):
    return RedisCache('h:test', client=redis_client)


@pytest.fixture()
def memcached_cache(memcached_client):
    return MemcachedCache('h:test', client=memcached_client)


@pytest.fixture(params=CACHE_LIST)
def cache(request):
    return request.getfixturevalue(request.param)


# ------------------------ IHash fixtures
@pytest.fixture(params=STORAGE_LIST + CACHE_LIST)
def hash_storage(request):
    return request.getfixturevalue(request.param)


# ------------------------ ICounter fixtures
COUNTER_LIST = ['memory_counter', 'redis_counter']


@pytest.fixture()
def memory_counter():
    return MemCounter()


@pytest.fixture()
def redis_counter(redis_client):
    return RedisCounter('c:test', client=redis_client)


@pytest.fixture(params=COUNTER_LIST)
def counter(request):
    return request.getfixturevalue(request.param)


# ------------------------ ICursorStorage fixtures
CURSOR_STORAGE_LIST = ['plain_cursor_storage', 'versioned_cursor_storage']


@pytest.fixture
def plain_cursor_storage(memory_storage):
    return CursorStorage(memory_storage)


@pytest.fixture
def versioned_cursor_storage(memory_storage):
    return VersionedCursorStorage(memory_storage, version='1.0.0')


@pytest.fixture(params=CURSOR_STORAGE_LIST)
def cursor_storage(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def cursor_storage_mock():
    return Mock(spec=ICursorStorage)


# ------------------------ ILock fixtures
LOCK_LIST = ['memory_lock', 'redis_lock']


@pytest.fixture
def memory_lock():
    return MemLock()


@pytest.fixture
def redis_lock(redis_client):
    return RedisLock('l:test', client=redis_client)


@pytest.fixture(params=LOCK_LIST)
def lock(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def lock_pool():
    return ContextPoolFactory(lambda name: MemLock())


@pytest.fixture
def lock_pool_mock():
    return ContextPoolFactory(lambda name: Mock(spec=ILock))


# ------------------------ IMetrics fixtures
METRICS_LIST = ['log_metrics', 'decorated_metrics', 'datadog_metrics']


@pytest.fixture
def log_metrics():
    return LogMetrics()


@pytest.fixture
def decorated_metrics(log_metrics):
    return MetricDecorator(log_metrics, tags={'tag1': 'value1'})


@pytest.fixture
def datadog_metrics(config):
    return DataDogMetrics(config, tags={'tag1': 'value1'})


@pytest.fixture(params=METRICS_LIST)
def metrics(request):
    return request.getfixturevalue(request.param)


# ------------------------ IRateCounter fixtures
RATE_COUNTER_LIST = ['memory_rate_counter', 'memcached_rate_counter']


@pytest.fixture
def memory_rate_counter():
    return MemoryRateCounter()


@pytest.fixture
def memcached_rate_counter(memcached_client):
    return MemcachedRateCounter('r:test', client=memcached_client)


@pytest.fixture(params=RATE_COUNTER_LIST)
def rate_counter(request):
    return request.getfixturevalue(request.param)


# ---------------------------------------
OBJECTS = {
    'int': 100,
    'string': 'value',
    'list': ['list_value1', 'list_value2'],
    'tuple': ('tuple_value1', 'tuple_value2'),
    'dict': {'key1': 'dict_value1', 'key2': 'dict_value2'},
    'object': Model({'name': 'custom_model'})
}


@pytest.fixture(params=OBJECTS)
def any_object(request):
    return OBJECTS[request.param]


# ---------------------------------------
@pytest.fixture
def message():
    return {}


@pytest.fixture
def infrastructure_mock():
    return Mock(spec=Infrastructure)


@pytest.fixture
def program_mock():
    return Mock(get_next_processor=Mock(return_value='next_processor_id'))


@pytest.fixture
def response_mock(infrastructure_mock, program_mock, message):
    return ListenerResponseHandler(infrastructure_mock, program_mock, 'processor_id', message)
