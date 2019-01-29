import six


class Model(object):

    def __init__(self, model=None):
        """
        :param model: model data.
        :type model: dict
        """
        self._model = {} if model is None else model

    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, self._model)

    def __eq__(self, other):
        return isinstance(other, Model) and self._model == other._model

    def validate(self, name=None):
        """
        Check if model is valid.
        Although you may use any validation approach, it's recommended to use a Validator
        to have more information about validation failure.
        :return: True if model is valid, otherwise False
        """
        return Validator(name or 'model', self)  # check that model is not empty

    def to_dict(self):
        """
        Convert model into a dict
        :return: model dictionary
        """
        return self._model


class IDestroyable(object):
    """
    Model that supports this interface has a specific destroyer that have be be especially called
    """
    def destroy(self):
        """
        Destroy the model
        """
        raise NotImplementedError


def compare(first, second, template, compare_func):
    # first value is always a Validator
    if isinstance(second, Validator):
        second_name = second._name
        second_value = second._value
        # append second's parents into first's parents chain
        first._append_parent(second)
    else:
        second_name = second
        second_value = second

    return Validator(template.format(first._name, second_name),
                     compare_func(first._value, second_value),
                     parent=first)


class Validator(object):

    def __init__(self, name, value, parent=None):
        """
        :param name: validator name
        :param value: validator value
        :param parent: parent validator
        :type parent:  Validator
        """
        self._name = name
        self._value = value
        # make a copy to prevent cycles
        self._parent = (Validator(parent._name, parent._value, parent._parent)
                        if parent else None)

    def _append_parent(self, parent):
        """
        :type parent: Validator
        """
        if parent is not None:
            last_parent = self
            while last_parent._parent is not None:
                last_parent = last_parent._parent
            # make a copy to prevent cycles
            last_parent._parent = Validator(parent._name, parent._value, parent._parent)

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return bool(self._value)

    def __call__(self, *args, **kwargs):
        parameters = []
        if args:
            parameters.extend(map(str, args))
        if kwargs:
            parameters.extend(map(lambda item: '{}={!r}'.format(*item), six.iteritems(kwargs)))
        return Validator('{}({})'.format(self._name, ', '.join(parameters)),
                         self._value(*args, **kwargs), parent=self._parent)

    def __getattr__(self, item):
        try:
            return super(Validator, self).__getattr__(item)
        except AttributeError:
            return Validator('{}.{}'.format(self._name, item),
                             getattr(self._value, item), parent=self)

    def __getitem__(self, item):
        return Validator('{}[{!r}]'.format(self._name, item), self._value[item], parent=self)

    def __len__(self):
        return Validator('len({})'.format(self._name), len(self._value), parent=self)

    def __gt__(self, other):
        return compare(self, other, '{} > {}', lambda x, y: x > y)

    def __ge__(self, other):
        return compare(self, other, '{} >= {}', lambda x, y: x >= y)

    def __lt__(self, other):
        return compare(self, other, '{} < {}', lambda x, y: x < y)

    def __le__(self, other):
        return compare(self, other, '{} <= {}', lambda x, y: x <= y)

    def __eq__(self, other):
        return compare(self, other, '{} == {}', lambda x, y: x == y)

    def __str__(self):
        parent_str = ''
        parent_names = set()
        parent = self._parent
        while parent is not None:
            if parent._name not in parent_names:
                parent_names.add(parent._name)  # exclude duplicates
                parent_str += '\n{} = {}'.format(parent._name, parent._value)
            parent = parent._parent
        return '' if self._value else '{} check is failed{}'.format(self._name, parent_str)

    def check_empty(self):
        return Validator('(not {})'.format(self._name), not self._value, parent=self)

    def check_not_exist(self):
        return Validator('({} is None)'.format(self._name), self._value is None, parent=self)

    def check_exist(self):
        return Validator('({} is not None)'.format(self._name),
                         self._value is not None, parent=self)

    def validate(self):
        return self._value.validate(self._name)

    def check_all(self):
        failed_result = Validator('all({})'.format(self._name), False)
        for index, item in (six.iteritems(self._value) if isinstance(self._value, dict)
                            else enumerate(self._value)):
            item_name = '{}[{!r}]'.format(self._name, index)
            result = item.validate(item_name) if isinstance(item, Model) else item
            if not result:
                result = result if isinstance(result, Validator) else Validator(item_name, result)
                failed_result._append_parent(result)
                failed_result._append_parent(self)
                return failed_result
        return Validator('all({})'.format(self._name), True, self)

    def check_any(self):
        failed_result = Validator('any({})'.format(self._name), False)
        for index, item in (six.iteritems(self._value) if isinstance(self._value, dict)
                            else enumerate(self._value)):
            item_name = '{}[{!r}]'.format(self._name, index)
            result = item.validate(item_name) if isinstance(item, Model) else item
            if result:
                return Validator('any({})'.format(self._name), True, self)
            else:
                result = result if isinstance(result, Validator) else Validator(item_name, result)
                failed_result._append_parent(result)
        failed_result._append_parent(self)
        return failed_result

    def check_contains(self, item):
        return Validator('({!r} in {})'.format(item, self._name), item in self._value, parent=self)
