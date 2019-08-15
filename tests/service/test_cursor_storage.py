from pypipes.service.cursor_storage import VersionedCursorStorage, CursorStorage


def test_hash_save_get(cursor_storage, any_object):
    cursor_storage.save('key1', 'value')
    assert cursor_storage.get('key1') == 'value'

    # update cursor value
    cursor_storage.save('key1', any_object)
    assert cursor_storage.get('key1') == any_object


def test_list_clear(cursor_storage):
    cursor_storage.save('key1', 'value1')
    cursor_storage.save('key2', 'value2')
    cursor_storage.save('key3', 'value3')
    assert len(cursor_storage.list()) == 3
    cursor_storage.clear()
    assert cursor_storage.get('key1') is None
    assert len(cursor_storage.list()) == 0


def test_versioned_cursor(memory_storage):
    """
    Test specific behaviour of versioned cursor storage
    Versioned cursor storage inherits cursors of previous version but ignore upper versions
    """

    # all cursor storages are using the same IStorage backend for saving the cursor data
    cursor_storage = CursorStorage(memory_storage)
    cursor_storage_1_0_0 = VersionedCursorStorage(memory_storage, '1.0.0')
    cursor_storage_1_0_1 = VersionedCursorStorage(memory_storage, '1.0.1')
    cursor_storage_2_0_0 = VersionedCursorStorage(memory_storage, '2.0.0')

    # save some cursors into plain cursor storage
    cursor_storage.save('cursor1', 'cursor1 value')
    cursor_storage.save('cursor2', 'cursor2 value')
    cursor_storage.save('cursor3', 'cursor3 value')

    assert cursor_storage.get('cursor1') == 'cursor1 value'
    assert cursor_storage_1_0_0.get('cursor1') == 'cursor1 value'

    cursor_storage_1_0_0.save('cursor1', 'cursor1 for 1.0.0')
    cursor_storage_1_0_0.save('cursor2', 'cursor2 for 1.0.0')

    assert cursor_storage_1_0_0.get('cursor3') == 'cursor3 value'
    assert cursor_storage_1_0_0.get('cursor1') == 'cursor1 for 1.0.0'
    assert cursor_storage_2_0_0.get('cursor1') == 'cursor1 for 1.0.0'
    assert cursor_storage.get('cursor1') == 'cursor1 value'

    cursor_storage_1_0_1.save('cursor1', 'cursor1 for 1.0.1')
    cursor_storage_1_0_1.save('cursor2', 'cursor2 for 1.0.1')

    assert cursor_storage_1_0_1.get('cursor1') == 'cursor1 for 1.0.1'
    assert cursor_storage_1_0_1.get('cursor2') == 'cursor2 for 1.0.1'
    assert cursor_storage_1_0_1.get('cursor3') == 'cursor3 value'

    cursor_storage_2_0_0.save('cursor1', 'cursor1 for 2.0.0')

    assert cursor_storage_1_0_0.get('cursor1') == 'cursor1 for 1.0.0'
    assert cursor_storage_1_0_1.get('cursor1') == 'cursor1 for 1.0.1'

    assert cursor_storage_2_0_0.get('cursor1') == 'cursor1 for 2.0.0'
    assert cursor_storage_2_0_0.get('cursor2') == 'cursor2 for 1.0.1'
    assert cursor_storage_2_0_0.get('cursor3') == 'cursor3 value'

    # clear all cursors of version 1.0.1
    cursor_storage_1_0_1.clear()

    assert cursor_storage_1_0_0.get('cursor1') == 'cursor1 for 1.0.0'
    assert cursor_storage_1_0_1.get('cursor1') == 'cursor1 for 1.0.0'

    assert cursor_storage_2_0_0.get('cursor1') == 'cursor1 for 2.0.0'
    assert cursor_storage_2_0_0.get('cursor2') == 'cursor2 for 1.0.0'
    assert cursor_storage_2_0_0.get('cursor3') == 'cursor3 value'
