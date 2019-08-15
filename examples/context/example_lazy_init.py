from __future__ import print_function

from pypipes.context import (
    context, IContextFactory, LazyContextCollection, apply_context_to_kwargs)


# LazyContextCollection provides lazy initialization of context
# if context value is an instance of IContextFactory

# this context factory builds key1 context from key2 and key3
class MyContextFactory(IContextFactory):
    def __call__(self, context_dict):
        return 'MyCustomContext: {}'.format(context_dict['key2'] + context_dict['key3'])


lazy_context = LazyContextCollection(key1=MyContextFactory(), key2=2, key3=3)

print('lazy_context = ', lazy_context)
print('lazy_context.get("key1") = ', LazyContextCollection(lazy_context).get('key1'))
print('lazy_context["key1"] = ', LazyContextCollection(lazy_context)['key1'])

# this works in apply_context_to_kwargs as well
kwargs = dict(key1=context.key1)
print(kwargs, '=', apply_context_to_kwargs(kwargs, LazyContextCollection(lazy_context)))

# if kay not found in a LazyContextCollection, the KeyError message will contain a list
# of available context for better error tracing.
try:
    LazyContextCollection(lazy_context)['unknown']
except KeyError as e:
    print('KeyError: ', e)

# Also LazyContextCollection ensures that context initialization has no initialization loops
# For example, if 'key2' is also a lazy context with same factory,
# key1 context initialization will have a loop
lazy_context['key2'] = MyContextFactory()
try:
    LazyContextCollection(lazy_context)['key1']
except KeyError as e:
    print('KeyError: ', e)
