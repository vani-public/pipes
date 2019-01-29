from time import sleep


def test_increment(rate_counter):
    result = rate_counter.increment('rate1')
    assert result[0] == 1
    assert 0 < result[1] <= 1

    rate_counter.increment('rate1')
    result = rate_counter.increment('rate1', value=10)
    assert result[0] == 12
    assert 0 < result[1] <= 1


def test_expiration(rate_counter):
    rate_counter.increment('rate1', threshold=1)
    result = rate_counter.increment('rate1', threshold=1)
    assert result[0] == 2
    sleep(1)
    result = rate_counter.increment('rate1', threshold=1)
    assert result[0] == 1


def test_callable_threshold(rate_counter):
    def threshold():
        # this function returns threshold value
        return 100

    result = rate_counter.increment('rate1', threshold=threshold)
    assert 99 < result[1] <= 100
