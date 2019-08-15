"""
ServicePoolProvider is designed to be compatible with ServicePoolFactory so it has similar behaviour
but provides pre-created service instances.
If some service is not included into ServicePoolProvider's collection,
ServicePoolProvider returns default service
"""
from __future__ import print_function

from pypipes.context import ContextPool, LazyContextPool
from pypipes.context import context, UseContextSubstitution


class MyStorageService(object):
    def __repr__(self):
        return self.__class__.__name__

    def get_name(self):
        return self.__class__.__name__


class MyExtendedService(MyStorageService):
    pass


storage = ContextPool(default=MyStorageService(), wait_all=MyExtendedService())

print(storage)
print('storage.default.get_name() = ', storage.default.get_name())
print('storage.wait_all.get_name() = ', storage.wait_all.get_name())
print('storage.unknown.get_name() = ', storage.unknown.get_name())

print('storage["wait_all"].get_name() =', storage['wait_all'].get_name())
print('storage["unknown"].get_name() =', storage['unknown'].get_name())


# LazyContextPool can provide instances of IContextFactory initialized with context.
# MyContextStorageService is based on UseContextSubstitution(IContextFactory)
# that can lookup for default method parameters in context

class MyContextStorageService(MyStorageService, UseContextSubstitution):

    # this method expects that conn_id and cursor parameters will be automatically taken
    # from context because of their default values are a ContextPath instances
    def run(self, command, conn_id=context.conn_id, cursor=context.cursor):
        return command, conn_id, cursor


class MyContextStorageService2(MyContextStorageService):
    pass


context_dict = {
    'conn_id': 123,
    'extra': {},
    'cursor': 'my cursor',
    'command': 'command2',
}

storage = LazyContextPool(MyContextStorageService(), wait_all=MyContextStorageService2())
context_storage = storage(context_dict)

print('storage.wait_all.get_name() =', context_storage.wait_all.get_name())
print('storage.wait_all.run("test") =', context_storage.wait_all.run('command1'))
print('storage["wait_all"].run("test") =', context_storage["wait_all"].run(context.command))

print('storage.cursor.get_name() =', context_storage.cursor.get_name())
print('storage.cursor.run("test") =', context_storage.cursor.run('command1'))
print('storage["cursor"].run("test") =', context_storage["cursor"].run(context.command))
