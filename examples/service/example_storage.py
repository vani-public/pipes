from __future__ import print_function

from pprint import pformat

from pypipes import MemStorage as Storage

# from pypipes.service.storage import RedisStorage as Storage


service = Storage()
service.save('key1', 'value1',
             aliases=['alias1'],
             collections=['all_values', 'key1_collection'])
# value is accessible by primary key
print('key1:', service.get('key1'))

# value will be updated is you save a new value with same primary key
service.save('key1', 'new_value1',
             aliases=['alias1', 'alias2'],
             collections=['all_values'])

print('updated key1:', service.get('key1'))

# or any alias
print('alias1:', service.get('alias1'))
print('alias2:', service.get('alias2'))

# service returns None if value not exists or default
print('unknown:', service.get('unknown', default='NOT FOUND'))

# some alias could be added later
service.add_alias('key1', 'selected')
print('selected:', service.get_item('selected'))

# if a new item is saved with existing alias the alias will be redirected to new item
service.save('key4', 'value4', aliases=['selected'])
print('selected:', service.get_item('selected'))

print('selected alias is moved to a new item:', service.get_item('selected'))

# item could be deleted by item key
service.delete('key1')
print('item is deleted:', service.get_item('key1'))
print('all item aliases are also deleted: alias1 =', service.get_item('alias1'))

# or it can be deleted by alias
service.delete('selected')
print('selected item is deleted:', service.get_item('key4'))

# one item may be included into more than one collections
service.save('key1', 'value1',
             aliases=['alias1', 'alias2'],
             collections=['all_values'])

service.save('key2', 'value2',
             collections=['all_values', 'active_values'])

service.save('key3', 'value3',
             collections=['all_values', 'active_values'])

# get all collection items
print('all_values collection:', pformat(list(service.get_collection('all_values'))))
print('active_values collection:', pformat(list(service.get_collection('active_values'))))

# get only item ids from collection
print('active_values collection ids:',
      pformat(list(service.get_collection('active_values', only_ids=True))))

# deleting collection deletes only collection references but not items
service.delete_collection('active_values')
print('deleted collection is empty:', pformat(list(service.get_collection('active_values'))))
print('item from deleted collection still exist:', service.get_item('key2'))

# to delete collection items call delete_collection with delete_items=True parameter
service.delete_collection('all_values', delete_items=True)
print('now collection item is deleted:', service.get_item('key2'))
