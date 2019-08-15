import time

from pypipes.service.storage import MemStorage as Storage
# from pypipes.service.storage import RedisStorage as Storage


service = Storage()

# Service operation timings
count = 10000
start_time = time.time()
for i in range(count):
    service.save(str(i), {'message': {'a': 'value_a', 'b': 'value_b', 'c': 'value_c'}},
                 aliases=['alias_%s' % i],
                 collections=['all_values'])
print('SAVE time', time.time() - start_time)

start_time = time.time()
for i in range(count):
    service.save(str(i), {'message': {'a': 'value_a', 'b': 'value_b', 'c': 'value_c'}},
                 aliases=['alias_%s' % i],
                 collections=['all_values'])
print('UPDATE time', time.time() - start_time)

start_time = time.time()
for i in range(count):
    service.delete('alias_%s' % i)
print('DELETE time', time.time() - start_time)

start_time = time.time()
for i in range(count):
    service.save(str(i), {'message': {'a': 'value_a', 'b': 'value_b', 'c': 'value_c'}},
                 aliases=['alias_%s' % i],
                 collections=['all_values'])
print('SAVE again time', time.time() - start_time)

start_time = time.time()
for i in range(count):
    service.get_item('alias_%s' % i)
print('GET time', time.time() - start_time)

start_time = time.time()
for i in range(count):
    service.add_alias(str(i), 'alias2_%s' % i)
print('ADD ALIAS time', time.time() - start_time)

start_time = time.time()
print('Saved values count: ', len(list(service.get_collection('all_values', only_ids=True))))
print('SCAN ids time', time.time() - start_time)

start_time = time.time()
print('LAST collection item (collection is unordered):',
      list(service.get_collection('all_values'))[-1].id)
print('SCAN items time', time.time() - start_time)

start_time = time.time()
service.delete_collection('all_values', delete_items=True)
print('DELETE items time', time.time() - start_time)


"""
Benchmark results:

SAVE time 4.69242596626
UPDATE time 4.68091511726
DELETE time 4.03947687149
SAVE again time 4.64807295799
GET time 5.18303322792
ADD ALIAS time 3.88651204109
Saved values count:  10000
SCAN ids time 0.140254020691
LAST collection item (collection is unordered): 987
SCAN items time 1.08175992966
DELETE items time 0.343858957291
"""
