from pypipes.service.storage import StorageItem


def test_get_item(storage):
    storage.save('key1', 'value1',
                 aliases=['alias1'])
    assert storage.get_item('key1').value == 'value1'
    assert storage.get_item('alias1').value == 'value1'

    # update alias
    storage.save('key1', 'new_value1',
                 aliases=['alias1', 'alias2'])
    expected = StorageItem('key1', 'new_value1', {'alias1', 'alias2'}, set())
    assert storage.get_item('key1') == expected
    assert storage.get_item('alias1') == expected
    assert storage.get_item('alias2') == expected


def test_update_alias(storage):
    storage.save('key1', 'value1')
    assert storage.get_item('selected') is None

    storage.add_alias('key1', 'selected')
    assert storage.get_item('selected').id == 'key1'

    storage.save('key4', 'value4', aliases=['selected'])
    assert storage.get_item('selected').id == 'key4'

    storage.delete_alias('selected')
    assert storage.get_item('selected') is None
    # ensure item is not deleted
    assert storage.get_item('key1').value == 'value1'


def test_get_unknown(storage):
    assert storage.get_item('unknown') is None


def test_delete(storage):
    storage.save('key1', 'value1', aliases=['alias1'], collections=['col1'])
    storage.delete('key1')
    assert not storage.get_item('key1')
    assert not storage.get_item('alias1')
    assert len(list(storage.get_collection('col1'))) == 0


def test_collections(storage):
    # one item may be included into more than one collections
    storage.save('key1', 'value1',
                 collections=['all_values'])
    storage.save('key2', 'value2',
                 collections=['all_values', 'active_values'])
    storage.save('key3', 'value3',
                 collections=['all_values', 'active_values'])

    # get all collection items
    assert (sorted([i.value for i in storage.get_collection('all_values')]) ==
            sorted(['value1', 'value2', 'value3']))
    assert (sorted([i.value for i in storage.get_collection('active_values')]) ==
            sorted(['value2', 'value3']))
    # get only item ids from collection
    assert (sorted(list(storage.get_collection('active_values', only_ids=True))) ==
            sorted(['key2', 'key3']))

    # deleting collection deletes only collection references but not items
    storage.delete_collection('active_values')
    assert not list(storage.get_collection('active_values'))
    assert storage.get_item('key2').value == 'value2'

    # to delete collection items call delete_collection with delete_items=True parameter
    storage.delete_collection('all_values', delete_items=True)
    assert not storage.get_item('key2')
