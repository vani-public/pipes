from time import sleep

import pytest
from pypipes.config import Config
from pypipes.context.pool import ContextPool
from pypipes.exceptions import QuotaExceededException

from pypipes.service.quota import Quota, HourQuota, DayQuota, MonthQuota, QuotaPool, UnlimitedQuota


def test_quota(memory_rate_counter):
    quota = Quota('quota_name', memory_rate_counter, limit=5, threshold=1)
    for i in range(5):
        quota.consume('key1')
    with pytest.raises(QuotaExceededException) as e:
        quota.consume('key1')
        assert 0 < e.retry_in <= 1

    with pytest.raises(QuotaExceededException):
        quota.consume('key1')

    sleep(1)
    quota.consume('key1')


def test_hour_quota(memory_rate_counter):
    quota = HourQuota('quota_name', memory_rate_counter, limit=5)

    for i in range(5):
        quota.consume('key1')
    with pytest.raises(QuotaExceededException) as e:
        quota.consume('key1')
        assert 0 < e.retry_in <= 60 * 60


def test_day_quota(memory_rate_counter):
    quota = DayQuota('quota_name', memory_rate_counter, limit=5)

    for i in range(5):
        quota.consume('key1')
    with pytest.raises(QuotaExceededException) as e:
        quota.consume('key1')
        assert 0 < e.retry_in <= 24 * 60 * 60


def test_month_quota(memory_rate_counter):
    quota = MonthQuota('quota_name', memory_rate_counter, limit=5)

    for i in range(5):
        quota.consume('key1')
    with pytest.raises(QuotaExceededException) as e:
        quota.consume('key1')
        assert 0 < e.retry_in <= 31 * 24 * 60 * 60


def test_quota_pool(memory_rate_counter):
    config = Config({
        'quota': {
            'default': {
                'limit': 10,
                'threshold': 60
            },
            'day_quota': {
                'limit': 10,
                'days': 2
            },
            'hour_quota': {
                'limit': 10,
                'hours': 3
            },
            'month_quota': {
                'limit': 10,
                'months': 4
            },
            'unlimited': {
                'limit': 0
            }
        }
    })

    rate_counter_pool = ContextPool(memory_rate_counter)
    pool = QuotaPool(config, rate_counter_pool)

    assert isinstance(pool.default, Quota)
    pool.default.consume('key1')

    assert isinstance(pool.day_quota, DayQuota)
    pool.day_quota.consume('key1')

    assert isinstance(pool.hour_quota, HourQuota)
    pool.hour_quota.consume('key1')

    assert isinstance(pool.month_quota, MonthQuota)
    pool.month_quota.consume('key1')

    assert isinstance(pool.unlimited, UnlimitedQuota)
    pool.unlimited.consume('key1')

    # if quota is not configured the default is used
    assert isinstance(pool.not_configured, Quota)
    pool.default.consume('key1')


def test_default_quote_pool(memory_rate_counter):
    config = Config({})
    rate_counter_pool = ContextPool(memory_rate_counter)
    pool = QuotaPool(config, rate_counter_pool)

    # quota is unlimited if no quota configuration
    assert isinstance(pool.default, UnlimitedQuota)
    assert isinstance(pool.any_name, UnlimitedQuota)
