from __future__ import print_function

from time import sleep

from pypipes.config import Config
from pypipes.context import LazyContextCollection
from pypipes.exceptions import QuotaExceededException
from pypipes.service.rate_counter import memory_rate_pool

from pypipes.service.quota import quota_pool

# quota_pool is a LazyContext that initializes a QuotaPool object
# from config and rate_counter contexts.
config = {
    'quota': {
        'rate_quota': {'limit': 10, 'threshold': 2},  # 10 / 2 sec
        'hour_quota': {'limit': 5, 'hours': 2},  # 5 / each 2 full hours, starting from this one
        'day_quota': {'limit': 5, 'days': 1},  # 5 / 1 day
        'month_quota': {'limit': 5, 'months': 1},  # 5 / 1 month
    }
}
context = LazyContextCollection(
    quota=quota_pool,
    config=Config(config),
    rate_counter=memory_rate_pool,
)

# lets initialize a quota pool like infrastructure does it.
pool = context['quota']
print('Quota pool:', pool)

# we have several quotas configured
print('Get rate_quota:', pool.rate_quota)
print('Get hour_quota:', pool.hour_quota)

# UnlimitedQuota is returned if you try to get some not configured quota.
# UnlimitedQuota will never expire
print('Get unknown_quota:', pool.unknown_quota)

# rate_quota allows to consume 10 times per 2 sec
for j in range(10):
    pool.rate_quota.consume()

# next 'consume' operation will raise an error because of `rate_quota` is exceeded
try:
    pool.rate_quota.consume()
except QuotaExceededException as e:
    print('QuotaExceededException is raised on {} consume, retry_in {} sec'.format(e, e.retry_in))

# lets wait a little till the quota is refreshed
sleep(2)
pool.rate_quota.consume()
print('rate_quota is refreshed')

# hour_quota refreshes the quota counter at hour begin
print('2 hours quota:', pool.hour_quota)

for j in range(5):
    pool.hour_quota.consume()

try:
    # 6th consume should raise an exception
    pool.hour_quota.consume()
except QuotaExceededException as e:
    print('QuotaExceededException is raised on {} consume, retry_in {} sec'.format(e, e.retry_in))

# day_quota refreshes its counter on day begin
print('Day quota:', pool.day_quota)
for j in range(5):
    pool.day_quota.consume()

try:
    # 6th consume should raise an exception
    pool.day_quota.consume()
except QuotaExceededException as e:
    print('QuotaExceededException is raised on {} consume, retry_in {} sec'.format(e, e.retry_in))


# month_quota refreshes its counter on month begin
print('Month quota:', pool.month_quota)
for j in range(5):
    pool.month_quota.consume()

try:
    # 6th consume should raise an exception
    pool.month_quota.consume()
except QuotaExceededException as e:
    print('QuotaExceededException is raised on {} consume, retry_in {} sec'.format(e, e.retry_in))

# you may use a sub-key if you plan to use several quotes with same configuration
# default month_quota is expired but each sub-quote is calculated separately
# so next `consume` will not raise an exception because it's a different quote
pool.month_quota.consume('sub-key')
