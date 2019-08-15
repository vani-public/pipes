"""
Unlike ICounter service IRateCounter service calculates value increments during some period
Counter is restarted as only counting period is expired
"""
from __future__ import print_function

from time import sleep

if False:
    # this example uses MemcachedRateCounter
    from pypipes import MemcachedRateCounter
    counter = MemcachedRateCounter('test')
    counter.memcached.flush_all()
else:
    # this example uses MemoryRateCounter
    from pypipes import MemoryRateCounter
    counter = MemoryRateCounter()

print('increment key1 on 1:', counter.increment('key1', threshold=2))
# sleep a little an increment again
sleep(1)
print('increment key1 on 10:', counter.increment('key1', value=10, threshold=2))
sleep(1)
# period is expired, the counter have to be refreshed now.
print('increment key1 on 1:', counter.increment('key1'))
# you may use a function that returns a threshold instead of using a fixed threshold value
print('increment key2 on 1:', counter.increment('key2', threshold=lambda: 2))
