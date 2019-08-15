from __future__ import print_function

from pypipes.context import LazyContextCollection

from pypipes.context.factory import (
    ContextPoolFactory, LazyContextPoolFactory, LazyContext, distributed_pool)


class MyStorageService(object):
    def __init__(self, name, conn_id=None):
        self.name = name
        self.value = conn_id

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.name)

    def get_name(self):
        return self.name


storage = ContextPoolFactory(MyStorageService)

print(storage)
print('storage.wait_all.get_name() = ', storage.wait_all.get_name())
print('storage.cursor.get_name() = ', storage.cursor.get_name())

print('storage["registry"].get_name() =', storage['registry'].get_name())

# LazyContextPoolFactory implements IContextFactory interface
# and provides context injections for factory_func parameters
context = {
    'conn_id': 123,
    'extra': {},
    'name': 'my name'
}

storage = LazyContextPoolFactory(lambda name, conn_id: MyStorageService(name, conn_id))
context_storage = storage(context)

print('\ncontext_storage.wait_all.get_name() = ', context_storage.wait_all.get_name())
print('context_storage.wait_all.value = ', context_storage.cursor.value)

# usually service pool is used as a context item in LazyContextCollection
# and sometimes it's useful to have an ability to specify different initialization factories
# for different pool items. In this case you may want use a DistributedPool helper

lazy_context = LazyContextCollection(context)

# Lets append our pool with custom `library` storage into the context collection
# distributed_pool always checks if pool item exists in context.
# Name of pool item have to be prefixed with distributed pool name: <pool_name>.<service_name>
# <pool_name>.default is used when the item is not found in context.

lazy_context.update({
    'storage': distributed_pool,
    'storage.default': LazyContextPoolFactory(lambda name, conn_id:
                                              MyStorageService(name, conn_id)),
    'storage.library': LazyContext(lambda conn_id:
                                   MyStorageService('Custom library service', conn_id))
})

context_storage = lazy_context['storage']  # LazyContextCollection cares about lazy initialization
print('\ncontext_storage.wait_all.get_name() = ', context_storage.wait_all.get_name())
print('context_storage.library.get_name() = ', context_storage.library.get_name())


# use LazyContext if you need to initialize not pool but only one service with context injections
storage = LazyContext(lambda name, conn_id: MyStorageService(name, conn_id))
context_storage = storage(context)
print('\ncontext_storage.value =', context_storage.value)
