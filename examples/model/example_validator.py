# Although Validator is designed to be used in Model.validate(),
# you can use it to validate any kind of model
from __future__ import print_function

from datetime import datetime

from pypipes.model import Validator


now = datetime.now()
model = {
    'key1': 'value',
    'key2': None,
    'key3': False,
    # nested model
    'now': now,
    'list1': [True, 1, 'string'],
    'list2': [False, 0, None],
    'dict1': {'key1': True, 'key2': 'value2'},
    'dict2': {'key1': False, 'key2': None},
}

validator = Validator('model', model)

# if validation result is True, str(validator) returns an empty message
print('validator[key1] = ', bool(validator['key1']), validator['key1'])

# if validation result is False, str(validator) returns validation error details
print('validator[key2] = ', bool(validator['key2']), validator['key2'])

# you might use boolean operations to combine validation_results into a complex condition
# However it's not recommended to use NOT and IS operator, because it converts validation result
# into a bool. Use Validator.check_empty, Validator.check_exists, Validator.check_not_exists instead
print('\nkey1 and key2 or key3 = (False)', (validator['key1'] and validator['key2'] or
                                            validator['key3']))

print('key2 is None and not key3 = (True)', (validator['key2'].check_not_exist() and
                                             validator['key3'].check_empty()))

# also validator allows to validate object properties
print('\nvalidator[now].year <= now.year - 1: ', validator['now'].year <= (now.year - 1))
print('\nvalidator[now].month == now.month: ', validator['now'].month <= now.month)
print('\nvalidator[now].day > now.day: ', validator['now'].day > now.day)

# validating list of items
print('\nall(validator[list1]) = (True)', validator['list1'].check_all())
print('any(validator[list1]) = (True)', validator['list1'].check_any())

print('\nall(validator[list2]) = (False)', validator['list2'].check_all())
print('\nany(validator[list2]) = (False)', validator['list2'].check_any())

# check_all and check_any operations works for dictionary as well
print('\nall(validator[dict1]) = (True)', validator['dict1'].check_all())
print('any(validator[dict1]) = (True)', validator['dict1'].check_any())

print('\nall(validator[dict2]) = (False)', validator['dict2'].check_all())
print('\nany(validator[dict2]) = (False)', validator['dict2'].check_any())

# result of IN operator is always a bool, please use a check_contains instead
print('\nkey1 in dict1 = (True)', validator['dict1'].check_contains('key1'))
print('\nunknown in dict1 = (False)', validator['dict1'].check_contains('unknown'))
