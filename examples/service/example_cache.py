from __future__ import print_function

import time

from pypipes import MemoryCache  # noqa

cache = MemoryCache()
# cache = MemcachedCache(prefix='example')
# cache = RedisCache(prefix='example')

# save key
cache.save('key', 'value')

# get value from cache
print('key =', cache.get('key'))

# delete key
cache.delete('key')

# get not existing value
print('deleted key =', cache.get('key'), cache.get('key', default='- NOT EXIST'))

# save key with expiration
cache.save('key', 'value', expires_in=1)

# get value from cache
print('temporary key =', cache.get('key'))

# sleep a little
time.sleep(2)

# try to get expired value from cache
print('key =', cache.get('key', default='ESPIRED'))

# same operations are available for multiple keys
cache.save_many({'key1': 'value1', 'key2': 'value2'})

# get value from cache
print('values =', cache.get_many(['key1', 'key2', 'unknown']))

cache.delete_many(['key1', 'key2', 'key3'])
print('deleted values =', cache.get_many(['key1', 'key2', 'unknown'], default='NOT EXIST'))

# expiration works for many keys as well
cache.save_many({'key1': 'value1', 'key2': 'value2'}, expires_in=1)
print('temporary key1 =', cache.get('key1'))

# sleep a little and check again
time.sleep(2)
print('key1 =', cache.get('key1', default='EXPIRED'))
