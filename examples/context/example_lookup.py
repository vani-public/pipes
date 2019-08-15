from __future__ import print_function

from pypipes.context import context, apply_context_to_kwargs

from pypipes.context.config import config

context_dict = {
    'key1': 'value1',
    'key2': {
        'key2_1': 'value2_1',
        'key2_2': 'value2_2',
        'key2_3': {
            'key2_3_1': 'value2_3_1',
            'key2_3_2': 'value2_3_2'
        }
    }
}

# lookup context by path
lookup_path = context.key2
print(lookup_path, '=', lookup_path(context_dict))

lookup_path = context.key2.key2_3
print(lookup_path, '=', lookup_path(context_dict))

lookup_path = context.key2.key2_3.key2_3_2
print(lookup_path, '=', lookup_path(context_dict))

lookup_path = context.key1.unknown
print(lookup_path, '=', lookup_path(context_dict))

# path lookup supports bitwise operations
lookup_path = context.key1.unknown | context.key2.key2_3.unknown | context.key2.key2_3.key2_3_2
print(lookup_path, '=', lookup_path(context_dict))

lookup_path = context.key2 & context.key2.key2_3 & context.key2.key2_3.key2_3_2
print(lookup_path, '=', lookup_path(context_dict))

# bitwise operations between ContextPath and value of other types are also supported
lookup_path = context.key1.unknown | 0
print(lookup_path, '=', lookup_path(context_dict))

lookup_path = context.key2 & True
print(lookup_path, '=', lookup_path(context_dict))

# apply_context_to_kwargs lookups for key's value in context if value is IContextLookup
kwargs = dict(a=1, b=2, c=context.key1)
print(kwargs, '=', apply_context_to_kwargs(kwargs, context_dict))

# if context path is empty, apply_context_to_kwargs uses key name as default context path
kwargs = dict(key1=context, key2=context)
print(kwargs, '=', apply_context_to_kwargs(kwargs, context_dict))

# you can specify which context is default
kwargs = dict(key1=context.key1, key2_1=context, key2_2=context)
print(kwargs, '=', apply_context_to_kwargs(kwargs, context_dict, default_context_path=context.key2))

# if context dict is a configuration you may use special config path adapter
# that automatically converts result into Config object
# but this lookup factory expects that context dictionary has a 'config' key
config_context_dict = {'config': context_dict}

kwargs = dict(config=config.key2)
print(kwargs, '=', apply_context_to_kwargs(kwargs, config_context_dict))
