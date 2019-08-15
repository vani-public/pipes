import pytest
from pypipes.context import context, apply_context_to_kwargs, message, use_context_lookup

from pypipes.context.config import config


@pytest.fixture
def context_dict():
    return {
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
    }


@pytest.mark.parametrize('lookup_path, expected', [
    # context lookups
    (context.key1, 'value1'),
    (context.key2.key2_3.key2_3_2, 'value2_3_2'),
    (context.key2.key2_3, {
        'key2_3_1': 'value2_3_1',
        'key2_3_2': 'value2_3_2'
    }),
    (context.unknown, None),

    # message context lookups
    (message.key1, 'message_value1'),
    (message.unknown, None),

    # config context lookups
    (config.key1, 'config_value1'),
    (config.unknown, None),

    # complex lookups
    (context.key1.unknown | context.key2.key2_3.unknown | context.key1, 'value1'),
    (context.key1 | 'default', 'value1'),
    (context.unknown | 'default', 'default'),

    (context.key2 & context.key2.key2_3 & context.key2.key2_3.key2_3_2, 'value2_3_2'),
    (context.key1 & 'value_exist', 'value_exist'),
    (context.unknown & 'value_exist', None),
])
def test_lookup_by_path(context_dict, lookup_path, expected):
    lookup_path(context_dict)


@pytest.mark.parametrize('input, default, expected', [
    (dict(a=1, b=2, c=context.key1), None, dict(a=1, b=2, c='value1')),
    (dict(key1=context, message=message, config=config), None,
     dict(key1='value1',
          message={'key1': 'message_value1', 'key2': 'message_value2'},
          config={'key1': 'config_value1', 'key2': 'config_value2'})),
    (dict(key1=context.key1, key2_1=context, key2_2=context), context.key2,
     dict(key1='value1', key2_1='value2_1', key2_2='value2_2'))
])
def test_apply_context_to_kwargs(context_dict, input, default, expected):
    assert apply_context_to_kwargs(input, context_dict, default_context_path=default) == expected


@pytest.mark.parametrize('parameter, expected_value', [
    (100, 100),
    (context, 'value1'),
    (context.key1, 'value1'),
    (context.unknown, None)
])
def test_use_context_lookup(context_dict, parameter, expected_value):
    def _context_func(key1):
        return key1

    func = use_context_lookup(_context_func, context_dict)
    assert func(parameter) == expected_value
    assert func(key1=parameter) == expected_value


def test_use_context_lookup_defaults(context_dict):
    def _context_func(key1=context, key2=message.key2, config=config):
        assert key1 == 'value1'
        assert key2 == 'message_value2'
        assert config == {
            'key1': 'config_value1',
            'key2': 'config_value2'
        }
        return True

    func = use_context_lookup(_context_func, context_dict)
    assert func()  # default parameters must be used
