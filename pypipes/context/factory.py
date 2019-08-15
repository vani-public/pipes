from pypipes.context import apply_injections, IContextFactory, injections_handler, \
    INamedContextFactory
from pypipes.context.pool import IContextPool


class ContextPoolFactory(IContextPool):
    """
    This is a factory of named contexts
    Creates and provide a names context from context pool.
    Usage:

    def counter_factory(name):
        return CounterService(name)

    counter = ContextPoolFactory(counter_factory)
    counter.wait_all.increment()

    this calls factory func to create a counter context CounterService('wait_all')
    and than calls increment() of this counter
    """
    def __init__(self, factory_func):
        self._factory_func = factory_func
        self._pool = {}

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._factory_func.__name__)

    def __getitem__(self, item):
        if item not in self._pool:
            self._pool[item] = self._factory_func(item) or self.default
        return self._pool[item]


class CustomContextPoolFactory(ContextPoolFactory):
    def __init__(self, default, **custom_factories):
        self._default_factory = default
        self._custom_factories = custom_factories
        super(CustomContextPoolFactory, self).__init__(self._extended_factory)

    def _extended_factory(self, name):
        # try to use custom factory for the item if exists otherwise use default one
        factory = self._custom_factories.get(name, self._default_factory)
        return factory(name)


class LazyContextPoolFactory(IContextFactory):
    """
    This ContextPoolFactory implement IContextFactory interface and thus supports
    a lazy context initialization.
    This enables context injections for factory function parameters.
    """
    def __init__(self, factory_func):
        self._factory_func = factory_func

    def __call__(self, context_dict):
        return ContextPoolFactory(apply_injections(self._factory_func, context_dict))


class DistributedPool(INamedContextFactory):
    """
    This lazy pool works like regular pool
    but it uses multiple context keys for pool definition.
    It checks if context collection contains an item with such name and returns it.
    Name of pool item have to be prefixed in context with name of the pool: '<pool_name>.<item>'
    Item value could be an object or an IContextPool

    Example of context collection with distributed pool.
    {
        'storage': distributed_pool,
        'storage.default' ContextPoolFactory(lambda name: Storage(name)),  # is used by default
        'storage.cursor': CursorStorage(),
    }
    """
    def __init__(self, **factories):
        self.factories = factories

    def __call__(self, context_dict, context_name=None):
        context_dict.update({'{}.{}'.format(context_name, key): value
                             for key, value in self.factories.items()})
        return ContextPoolFactory(self.create_context_factory(context_name, context_dict))

    @staticmethod
    def create_context_factory(pool_name, context_dict):
        default_item_name = '{}.default'.format(pool_name)

        def _distributed_factory(item):
            # lookup for context responsible for the item creation
            item_name = '{}.{}'.format(pool_name, item)
            value = context_dict.get(item_name) or context_dict.get(default_item_name)
            if value and isinstance(value, IContextPool):
                # get value from the pool
                value = value[item]
            return value
        return _distributed_factory


class LazyContext(IContextFactory):
    def __init__(self, factory_func):
        self._factory_func = injections_handler(factory_func)

    def __call__(self, context_dict):
        return self._factory_func(context_dict)


distributed_pool = DistributedPool()
lazy_context = LazyContext
