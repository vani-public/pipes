from __future__ import print_function

from pypipes.config import Config
from pypipes.service.storage import redis_storage_pool

from pypipes.context import LazyContextCollection

# most pipe processor and content managers expects that storage is an IContentPool
# see service/example_pool.py for pool usage example
# There are several predefined storage pools that you may use in your application
# This example demonstrates a usage of redis_storage_pool

# lets specify a context for lazy storage initialization
config = Config({
    'redis': {
        'host': 'localhost',
        'port': 6379,
    }
})

context = {
    'config': config,
    'storage': redis_storage_pool
}

# redis_storage_pool is a lazy context
# lets initialize the storage like pipeline application does it.
storage = LazyContextCollection(context)['storage']

# storage is now a ContextPoolFactory of RedisStorage objects
print('Storage:', storage, type(storage.default))

# all the storages in pool will use the same redis client because configuration is the same
# but will have different key prefix that is a name of storage

print('Client is the same:', storage.default.redis == storage.cursor.redis)
print('Prefix:', repr(storage.default._prefix), repr(storage.cursor._prefix))

# if you need a physically separated storages
# you may set a different redis config for some storage name
# lets change our config a little

context['config'] = Config({
    'redis': {
        'host': 'localhost',
        'port': 6379,
        'storage': {
            'cursor': {
                'port': 5555
            },
            'content': {
                'port': 5556,
                'db': 1,
            }
        }
    }
})

storage = LazyContextCollection(context)['storage']

# now default storage will have different Redis client than cursor and content storages
print('\nClient is not the same:', storage.default.redis != storage.cursor.redis)
print('Cursor storage connection params:', storage.cursor.redis.connection_pool.connection_kwargs)
print('Content storage connection params:', storage.content.redis.connection_pool.connection_kwargs)

# all not especially specified storages will use default redis configuration
print('Default storage connection params:', storage.default.redis.connection_pool.connection_kwargs)
