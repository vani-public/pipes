import pytest
from pypipes.context import LazyContextCollection, apply_injections, context, injections_handler, \
    UseContextSubstitution, ContextWrapper

from pypipes.context.config import config


class MyService(UseContextSubstitution):
    def __init__(self, inner=True):
        if inner:
            self.inner = MyService(False)  # stop recursion

    def run(self, command, message=context, config=config):
        # this method expects that message and config parameters are automatically taken
        # from context because of their default values are a ContextPath instances
        return command, message, config


@pytest.fixture
def context_dict():
    return LazyContextCollection({
        'key1': 'value1',
        'key2': {
            'key2_1': 'value2_1',
            'key2_2': 'value2_2',
            'key2_3': {
                'key2_3_1': 'value2_3_1',
                'key2_3_2': 'value2_3_2'
            }
        },
        'message': {
            'key1': 'message_value1',
            'key2': 'message_value2'
        },
        'config': {
            'key1': 'config_value1',
            'key2': 'config_value2'
        },
        'service': MyService(),
    })


def test_apply_injections(context_dict):
    def _context_func(key1, message, my_config=config, default=context.key2.key2_3.key2_3_2):
        return key1, message, my_config, default

    func = apply_injections(_context_func, context_dict)

    # call without any parameter
    key1, message, my_config, default = func()
    assert key1 == 'value1'
    assert message == {
        'key1': 'message_value1',
        'key2': 'message_value2'
    }
    assert my_config == {
        'key1': 'config_value1',
        'key2': 'config_value2'
    }
    assert default == 'value2_3_2'

    # call with parameters
    key1, message, my_config, default = func('call_value1', default='call_default')
    assert key1 == 'call_value1'
    assert message == {
        'key1': 'message_value1',
        'key2': 'message_value2'
    }
    assert my_config == {
        'key1': 'config_value1',
        'key2': 'config_value2'
    }
    assert default == 'call_default'


def test_apply_injections_unknown():
    def _context_func(unknown):
        pytest.xfail('Should not enter into the function')

    func = apply_injections(_context_func, {})

    with pytest.raises(TypeError):
        func()


def test_injections_handler(context_dict):

    @injections_handler
    def _context_func(key1, message, my_config=config, default=context.key2.key2_3.key2_3_2):
        return key1, message, my_config, default

    # call function with context
    key1, message, my_config, default = _context_func(context_dict)
    assert key1 == 'value1'
    assert message == {
        'key1': 'message_value1',
        'key2': 'message_value2'
    }
    assert my_config == {
        'key1': 'config_value1',
        'key2': 'config_value2'
    }
    assert default == 'value2_3_2'


def test_injections_handler_unknown():

    @injections_handler
    def _context_func(unknown):
        pytest.xfail('Should not enter into the function')

    with pytest.raises(TypeError):
        _context_func({})


def test_context_wrapper(context_dict):
    service = context_dict['service']

    assert isinstance(service, ContextWrapper)
    assert service.run('test') == (
        'test',
        {'key1': 'message_value1', 'key2': 'message_value2'},
        {'key1': 'config_value1', 'key2': 'config_value2'}
    )
    assert service.run('test', None, config={'key': 'value'}) == (
        'test',
        None,
        {'key': 'value'}
    )

    # inner UseContextSubstitution instance must be automatically converted
    # into ContextWrapper as well
    assert isinstance(service.inner, ContextWrapper)
    assert service.inner.run('test') == (
        'test',
        {'key1': 'message_value1', 'key2': 'message_value2'},
        {'key1': 'config_value1', 'key2': 'config_value2'}
    )
