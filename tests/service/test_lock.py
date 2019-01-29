from time import sleep


def test_acquire_release(lock):
    assert lock.acquire('lock1') is True
    assert lock.acquire('lock1') is False

    assert lock.acquire('lock2') is True

    assert lock.release('lock1') is True
    assert lock.release('unknown') is False

    assert lock.acquire('lock1') is True
    assert lock.acquire('lock2') is False


def test_set_get(lock):
    lock.acquire('lock1')
    lock.acquire('lock2', expire_in=10)

    assert lock.get('lock1') is True
    assert 9 < lock.get('lock2') <= 10
    assert lock.get('unknown') is False

    # update existing locks
    lock.set('lock1', expire_in=5)
    lock.set('lock2')
    # create a new lock
    lock.set('lock3')

    assert 4 < lock.get('lock1') <= 5
    assert lock.get('lock2') is True
    assert lock.get('lock3') is True


def test_prolong(lock):
    lock.acquire('lock1')
    lock.acquire('lock2', expire_in=10)

    # prolong existing locks
    assert lock.prolong('lock1', expire_in=5) is True
    assert lock.prolong('lock2', expire_in=20) is True

    # try to prolong not existing lock
    assert lock.prolong('lock3', expire_in=5) is False

    assert 4 < lock.get('lock1') <= 5
    assert 19 < lock.get('lock2') <= 20
    assert lock.get('lock3') is False  # lock is not created by prolong


def test_expiration(lock):
    lock.acquire('lock1')
    lock.acquire('lock2', expire_in=1)
    lock.acquire('lock3', expire_in=5)
    lock.acquire('lock4')
    lock.acquire('lock5')

    lock.prolong('lock4', expire_in=1)
    lock.prolong('lock5', expire_in=5)

    # all locks are locked now
    assert all(map(lock.get, ['lock1', 'lock2', 'lock3', 'lock4', 'lock5']))

    sleep(1)

    # these locks are still locked
    assert all(map(lock.get, ['lock1', 'lock3', 'lock5']))

    # there locks are expired
    assert not any(map(lock.get, ['lock2', 'lock4']))
