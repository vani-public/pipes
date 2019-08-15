from pypipes.context import context
from pypipes.context.pool import ContextPool, LazyContextPool


def test_context_pool():

    pool = ContextPool('DEFAULT', cursor='CURSOR', lock='LOCK')

    assert pool.default == 'DEFAULT'
    assert pool.not_set == 'DEFAULT'

    assert pool.cursor == 'CURSOR'
    assert pool.lock == 'LOCK'


def test_lazy_context_pool():
    lazy_pool = LazyContextPool(default=context.default,
                                cursor=context.cursor, lock=context.lock)

    context_dict = {
        'default': 'DEFAULT',
        'not_set': 'NOT SET',  # this value must be not taken
        'cursor': 'CURSOR',
        'lock': 'LOCK'
    }

    # initialize lazy pool
    pool = lazy_pool(context_dict)
    assert pool.default == 'DEFAULT'
    assert pool.not_set == 'DEFAULT'

    assert pool.cursor == 'CURSOR'
    assert pool.lock == 'LOCK'
