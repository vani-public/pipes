
def test_hash_save_get(hash_storage, any_object):
    hash_storage.save('key1', any_object)
    assert hash_storage.get('key1') == any_object


def test_hash_save_delete(hash_storage):
    hash_storage.save('key1', 'value1')
    assert hash_storage.delete('key1') is True  # value deleted
    assert hash_storage.delete('key1') is False  # value is already deleted


def test_hash_unknown_key(hash_storage):
    assert hash_storage.get('unknown') is None
    assert hash_storage.get('unknown', default='DEFAULT') == 'DEFAULT'
