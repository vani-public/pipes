from __future__ import print_function

from time import sleep

from pypipes import MemLock as Lock

# from pypipes.service.lock import RedisLock as Lock

lock = Lock()

# acquire lock
print('RELEASE not existing lock:', lock.release('test'))
print('ACQUIRE lock:', lock.acquire('test', expire_in=1))
print('get lock timeout:', lock.get('test'))
print('try to acquire again:', lock.acquire('test', expire_in=1))

sleep(1)
print('check if lock exists:', lock.get('test'))
print('lock expired, try to acquire again:', lock.acquire('test'))
print('lock has no expiration:', lock.get('test'))

print('set lock expiration:', lock.set('test', expire_in=2))
print('now lock has an expiration:', lock.get('test'))
print('set infinite lock:', lock.set('test'), lock.get('test'))
print('prolong not existing lock:', lock.prolong('unknown', expire_in=2))
print('prolong existing lock:', lock.prolong('test', expire_in=2), lock.get('test'))

print('RELEASE existing lock:', lock.release('test'))
