import pytest
from mock import Mock
from pypipes.service.cache import DefaultExpirationWrapper


@pytest.mark.parametrize('default_ttl', [None, 10])
def test_default_expiration_save(default_ttl):
    inner_cache_mock = Mock()
    cache = DefaultExpirationWrapper(inner_cache_mock, default_ttl)
    cache.save('key1', 'value1')
    inner_cache_mock.save.assert_called_once_with('key1', 'value1', expires_in=default_ttl)


@pytest.mark.parametrize('expires_in', [100, None, 10])
def test_default_expiration_save_set_explicitly(expires_in):
    inner_cache_mock = Mock()
    cache = DefaultExpirationWrapper(inner_cache_mock, 10)
    cache.save('key1', 'value1', expires_in=expires_in)
    inner_cache_mock.save.assert_called_once_with('key1', 'value1', expires_in=expires_in)


@pytest.mark.parametrize('default_ttl', [None, 10])
def test_default_expiration_save_many(default_ttl):
    inner_cache_mock = Mock()
    cache = DefaultExpirationWrapper(inner_cache_mock, default_ttl)
    cache.save_many({'key1': 'value1'})
    inner_cache_mock.save_many.assert_called_once_with({'key1': 'value1'}, expires_in=default_ttl)


@pytest.mark.parametrize('expires_in', [100, None, 10])
def test_default_expiration_save_many_set_explicitly(expires_in):
    inner_cache_mock = Mock()
    cache = DefaultExpirationWrapper(inner_cache_mock, 10)
    cache.save_many({'key1': 'value1'}, expires_in=expires_in)
    inner_cache_mock.save_many.assert_called_once_with({'key1': 'value1'}, expires_in=expires_in)
