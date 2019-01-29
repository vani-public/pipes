
def test_increment(counter):
    assert counter.increment('key') == 1
    assert counter.increment('key', 100) == 101
    assert counter.increment('key2', 200) == 200


def test_delete(counter):
    assert counter.increment('key1') == 1
    assert counter.increment('key2') == 1
    counter.delete('key1')
    assert counter.increment('key1') == 1
    assert counter.increment('key2') == 2


def test_delete_unknown(counter):
    counter.delete('unknown')
