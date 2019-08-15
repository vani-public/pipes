from __future__ import print_function

from pypipes.context import apply_injections, injections_handler


def target_function(context1, context2, context3='func_default3', context4='func_default4'):
    print('Got values:', context1, context2, context3, context4)


context = {
    'context2': 'context_value2',
    'context3': 'context_value3',
    'another': 'another_value'  # this context is not used as parameter injection
}

# apply_injections creates a function that takes default parameter value from context
func_with_injections = apply_injections(target_function, context)

# call function with positional args. Arguments have priority over value taken from context
func_with_injections('arg1', 'arg2')

# call function with keywords. They also have priority over default values.
func_with_injections(context1='keyword1', context3='keyword3')

# function raises TypeError if some context is not provided.
try:
    func_with_injections()
except TypeError as e:
    print('TypeError:', e)


# another way to use a parameter injection is creating of an injection handler that will
# translate input context into function parameters
func_with_injections = injections_handler(target_function)

# call injection handler with context dictionary.
# Context should have some values for each func parameter otherwise TypeError is raised
try:
    func_with_injections(context)
except TypeError as e:
    print('TypeError:', e)

# append missing context into collection and try again
context['context1'] = 'context_value1'
func_with_injections(context)


# target function receives an entire context if some parameter name is 'injections'
@injections_handler
def print_context(injections, context1):
    print('Entire context:', injections)
    print('context1 =', context1)


print_context(context)
