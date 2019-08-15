# ICursorStorage instances were designed to be used as a storage for
# @cursor context manager

# CursorStorage is a simple cursor storage with IStorage backend
# that acts like regular key-value storage

# VersionedCursorStorage also uses IStorage as a storage backend but it saves separate cursor
# per each program version. This allows easy rollback
# to any previous program version (previous cursor version),
# if new program version faced with some problem and its cursors can't be further used
from __future__ import print_function

from pypipes.service.cursor_storage import cursor_storage_context, versioned_cursor_storage_context
from pypipes.service.storage import memory_storage_pool

from pypipes.context import LazyContextCollection

# we will use LazyContextCollection to emulate context creation by Infrastructure
context = LazyContextCollection({
    'storage': memory_storage_pool,
    'cursor_storage': cursor_storage_context,
})

cursor_storage = context['cursor_storage']

cursor_storage.save('cursor1', 'cursor1 value')
cursor_storage.save('cursor2', 'cursor2 value')
cursor_storage.save('cursor3', 'cursor3 value')

print('cursor1 =', cursor_storage.get('cursor1'))
print('cursor2 =', cursor_storage.get('cursor2'))
print('cursor3 =', cursor_storage.get('cursor3'))
print('unknown =', cursor_storage.get('unknown'))

# update cursor value
cursor_storage.save('cursor1', 'updated cursor1 value')
print('\nupdated cursor1 =', cursor_storage.get('cursor1'))
print('cursor2 =', cursor_storage.get('cursor2'))

# simple cursor storage allows direct data migration to VersionedCursorStorage
# so we just update existing context to use the same cursor backend storage.
context['cursor_storage'] = versioned_cursor_storage_context
context['cursor_version'] = 'v1.0.0'
cursor_storage_1_0_0 = context['cursor_storage']

# lets save some cursors for version v1.0.0
cursor_storage_1_0_0.save('cursor1', 'cursor1 for 1.0.0')
cursor_storage_1_0_0.save('cursor2', 'cursor2 for 1.0.0')

print('\ncursor1 =', cursor_storage_1_0_0.get('cursor1'))
print('cursor2 =', cursor_storage_1_0_0.get('cursor2'))
# cursor3 is not set for the version
# but VersionedCursorStorage inherits its value from CursorStorage
print('cursor3 =', cursor_storage_1_0_0.get('cursor3'))
print('unknown =', cursor_storage_1_0_0.get('unknown'))

# now lets increase the version and save some cursors for it
context['cursor_storage'] = versioned_cursor_storage_context
context['cursor_version'] = 'v1.0.1'
cursor_storage_1_0_1 = context['cursor_storage']

cursor_storage_1_0_1.save('cursor1', 'cursor1 for 1.0.1')

print('\ncursor1 =', cursor_storage_1_0_1.get('cursor1'))
# this cursor is inherited from version 1.0.0
print('cursor2 =', cursor_storage_1_0_1.get('cursor2'))
# this cursor is inherited from simple cursor storage
print('cursor3 =', cursor_storage_1_0_1.get('cursor3'))

# lets save one more cursor for version 2.0.0
context['cursor_storage'] = versioned_cursor_storage_context
context['cursor_version'] = 'v2.0.0'
cursor_storage_2_0_0 = context['cursor_storage']

cursor_storage_2_0_0.save('cursor1', 'cursor1 for 2.0.0')

print('\ncursor1 =', cursor_storage_2_0_0.get('cursor1'))
# this cursor is inherited from version 1.0.0
print('cursor2 =', cursor_storage_2_0_0.get('cursor2'))
# this cursor is inherited from simple cursor storage
print('cursor3 =', cursor_storage_2_0_0.get('cursor3'))

# now the storage backend already contains 3 versions of cursor1
# ['cursor1.v2.0.0', 'cursor1.v1.0.1', 'cursor1.v1.0.0']
print('\ncursor1 versions =',
      list(context['storage'].cursor.get_collection('cursor1', only_ids=True)))

# Cursor storage for version 1.0.1 ignores 'cursor1.v2.0.0' value
# because the version is higher than its version
print('\ncursor1 (v1.0.1) =', cursor_storage_1_0_1.get('cursor1'))
# cursor storage for version 1.0.0 ignores 'cursor1.v1.0.1' and 'cursor1.v2.0.0'
print('cursor1 (v1.0.0) =', cursor_storage_1_0_0.get('cursor1'))

# you may clear all cursors created for some version
cursor_storage_1_0_1.clear()
# only cursor for version 1.0.1 is deleted
print('\ncursor1 versions =',
      list(context['storage'].cursor.get_collection('cursor1', only_ids=True)))

# therefore cursor_storage_1_0_1 returns a cursor value for version 1.0.0 now
print('\ncursor1 (v1.0.1) =', cursor_storage_1_0_1.get('cursor1'))
print('cursor2 (v1.0.1) =', cursor_storage_1_0_1.get('cursor2'))
