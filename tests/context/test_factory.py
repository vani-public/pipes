from mock import Mock

from pypipes.context import LazyContextCollection, context
from pypipes.context.factory import ContextPoolFactory, CustomContextPoolFactory, \
    LazyContextPoolFactory, distributed_pool, DistributedPool
from pypipes.context.pool import ContextPool


def test_context_pool_factory():

    factory_mock = Mock(side_effect=lambda name: name.upper())
    pool = ContextPoolFactory(factory_mock)

    assert pool.default == 'DEFAULT'
    assert pool.cursor == 'CURSOR'
    assert pool.lock == 'LOCK'

    assert factory_mock.call_count == 3
    # call pool again but factory must be not called
    assert pool.default == 'DEFAULT'
    assert pool.cursor == 'CURSOR'
    assert pool.lock == 'LOCK'

    assert factory_mock.call_count == 3


def test_custom_context_pool_factory():

    factory_mock = Mock(side_effect=lambda name: name.upper())
    cursor_factory_mock = Mock(side_effect=lambda name: name)
    pool = CustomContextPoolFactory(factory_mock, cursor=cursor_factory_mock)

    assert pool.default == 'DEFAULT'
    assert pool.cursor == 'cursor'
    assert pool.lock == 'LOCK'

    assert factory_mock.call_count == 2
    assert cursor_factory_mock.call_count == 1

    # call pool again but factory must be not called
    assert pool.default == 'DEFAULT'
    assert pool.cursor == 'cursor'
    assert pool.lock == 'LOCK'

    assert factory_mock.call_count == 2
    assert cursor_factory_mock.call_count == 1


def test_lazy_context_pool_factory():

    def factory(name, prefix):
        return '{}{}'.format(prefix, name.upper())

    lazy_pool = LazyContextPoolFactory(factory)

    context_dict = {
        'prefix': '->'
    }

    # initialize lazy pool
    pool = lazy_pool(context_dict)
    assert pool.default == '->DEFAULT'
    assert pool.cursor == '->CURSOR'
    assert pool.lock == '->LOCK'


def test_distributed_pool():
    lazy_context = LazyContextCollection({
        'cursor': 'CURSOR',
        'pool': distributed_pool,
        'pool.default': 'DEFAULT',  # may be fixed value
        'pool.cursor': context.cursor,  # may be IContextFactory
        'pool.lock': ContextPool(default='INVALID', lock='LOCK')  # may be IPool
    })

    # initialize distributed pool
    pool = lazy_context['pool']
    assert pool.default == 'DEFAULT'
    assert pool.cursor == 'CURSOR'
    assert pool.lock == 'LOCK'


def test_distributed_pool_init():
    lazy_context = LazyContextCollection({
        'cursor': 'CURSOR',
        'pool': DistributedPool(
            default='DEFAULT',  # may be fixed value
            cursor=context.cursor,  # may be IContextFactory
            lock=ContextPool(default='INVALID', lock='LOCK')  # may be IPool
        )
    })

    # initialize distributed pool
    pool = lazy_context['pool']
    assert pool.default == 'DEFAULT'
    assert pool.cursor == 'CURSOR'
    assert pool.lock == 'LOCK'
