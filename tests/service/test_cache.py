from time import sleep


def test_save_get_delete(cache, any_object):
    cache.save('key', any_object)
    assert any_object == cache.get('key')

    assert cache.delete('key') is True
    assert cache.get('key') is None

    assert cache.delete('key') is False


def test_get_unknown(cache, any_object):
    assert cache.get('unknown') is None
    assert cache.get('unknown', default=any_object) == any_object


def test_save_get_delete_many(cache, any_object):
    cache.save('key1', any_object)
    cache.save_many({'key2': any_object, 'key3': any_object})
    assert cache.get_many(['key1', 'key2', 'unknown']) == {'key2': any_object,
                                                           'key1': any_object,
                                                           'unknown': None}
    cache.delete_many(['key1', 'key3'])
    assert cache.get_many(['key1', 'key2', 'key3'], default='NOT FOUND') == {
        'key3': 'NOT FOUND',
        'key2': any_object,
        'key1': 'NOT FOUND'}


def test_expiration(cache):
    cache.save('key1', 'value1', expires_in=1)
    cache.save_many({'key2': 'value2', 'key3': 'value3'}, expires_in=1)
    # assert value exists
    assert cache.get_many(['key1', 'key2', 'key3']) == {
        'key3': 'value3',
        'key2': 'value2',
        'key1': 'value1'}
    sleep(1.1)
    assert cache.get_many(['key1', 'key2', 'key3']) == {
        'key3': None,
        'key2': None,
        'key1': None}
