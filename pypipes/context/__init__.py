import inspect
import threading
import types
import six
from functools import wraps


if six.PY2:
    getfullargspec = inspect.getargspec
else:
    getfullargspec = inspect.getfullargspec


class IContextFactory(object):
    def __call__(self, context_dict):
        """
        Build context object.
        :param context_dict: full context
        :rtype context_dict: LazyContextCollection | dict
        :return: context object
        """
        raise NotImplementedError()


class INamedContextFactory(IContextFactory):
    def __call__(self, context_dict, context_name=None):
        """
        Build named context object.
        :param context_dict: full context
        :rtype context_dict: LazyContextCollection | dict
        :return: context object
        """
        raise NotImplementedError()


class LazyContextCollection(dict):
    _local = None

    @property
    def loop(self):
        if not self._local:
            self._local = threading.local()
        if not hasattr(self._local, 'loop'):
            self._local.loop = []
        return self._local.loop

    def get(self, item, default=None):
        self._ensure_no_loop(item)
        return self._build_lazy_context(item, super(LazyContextCollection, self).get(item, default))

    def __getitem__(self, item):
        self._ensure_no_loop(item)
        try:
            value = super(LazyContextCollection, self).__getitem__(item)
        except KeyError:
            # Context not found.
            # Re-raise the error with more information about context collection.
            raise KeyError('Context {!r} not found. '
                           'Available context: {}'.format(item, self.keys()))
        return self._build_lazy_context(item, value)

    def _build_lazy_context(self, item, value):
        if isinstance(value, IContextFactory):
            loop = self.loop
            loop.append(item)
            try:
                # try to initialize a lazy context
                if isinstance(value, INamedContextFactory):
                    value = value(self, context_name=item)
                else:
                    value = value(self)
                self[item] = value
            finally:
                loop.pop()
        return value

    def _ensure_no_loop(self, item):
        loop = self.loop
        if loop and item in loop:
            raise KeyError('Infinite loop in references found while processing {!r} context. '
                           'Processing path: {}->{}'.format(loop[0], '->'.join(loop),
                                                            item))


class IContextLookup(IContextFactory):
    def __nonzero__(self):
        return True

    def __bool__(self):
        return True

    def __call__(self, context_dict):
        """
        Lookup for a context in context collection.
        :param context_dict: dict
        :return: context value or None if context not found
        """
        raise NotImplementedError()

    def __and__(self, other):
        return ContextOperation(ContextOperation.AND, self, other)

    def __or__(self, other):
        return ContextOperation(ContextOperation.OR, self, other)


class ContextOperation(IContextLookup):
    def __init__(self, operation, *lookups):
        assert lookups
        self.lookups = lookups
        self.operation = operation

    def __repr__(self):
        return '{}{}'.format(self.operation.__name__, self.lookups)

    def __call__(self, context_dict):
        return self.operation(context_dict, self.lookups)

    @staticmethod
    def _and(context_dict, lookups):
        result = lookups[0]
        result = result(context_dict) if isinstance(result, IContextFactory) else result
        if len(lookups) > 1:
            return result and ContextOperation._and(context_dict, lookups[1:])
        else:
            return result

    @staticmethod
    def _or(context_dict, lookups):
        result = lookups[0]
        result = result(context_dict) if isinstance(result, IContextFactory) else result
        if len(lookups) > 1:
            return result or ContextOperation._or(context_dict, lookups[1:])
        else:
            return result

    AND = _and
    OR = _or


class ContextPath(IContextLookup):
    def __init__(self, names=None):
        self.names = list(names or tuple())

    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, self.names)

    def __getattr__(self, item):
        return self.__class__(self.names + [item])

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return bool(self.names)

    def __iter__(self):
        return iter(self.names)

    def __call__(self, context_dict):
        """
        Retrieve context by path from context_dict
        >> context_dict = {'a': {'b': 'value'}}
        >> path_lookup = context.a.b
        >> path_lookup(context_dict)
        'value'
        :param context_dict: context mapping
        :type context_dict: LazyContext, dict
        :return: context value
        """
        for name in self.names:
            if not isinstance(context_dict, dict):
                return None
            context_dict = context_dict.get(name)
        return context_dict


context = ContextPath()
message = context.message


def apply_context_to_kwargs(kwargs, context, default_context_path=None):
    """
    Assign values from context to a dict items if value is an IContextLookup object
    """
    default_context_path = list(default_context_path) if default_context_path else []
    result = dict(kwargs)
    for name, value in six.iteritems(kwargs):
        if isinstance(value, INamedContextFactory):
            result[name] = value(context, context_name=name)
        elif isinstance(value, IContextFactory):
            result[name] = (value(context) if value else
                            # get from default path if value is empty IContextFactory
                            ContextPath(default_context_path + [name])(context))
    return result


def use_context_lookup(func, context):
    """
    Function decorator that automatically substitute IContextLookup parameters
    with values from context
    """
    if six.PY3:
        call_func = func.__func__ if hasattr(func, '__func__') else func
    else:
        call_func = func.im_func if hasattr(func, 'im_func') else func
    args_spec = getfullargspec(call_func)
    if six.PY3:
        varkw = args_spec.varkw
    else:
        varkw = args_spec.keywords
    if args_spec.varargs:
        # funcs with varargs are not supported for now
        return func

    @wraps(call_func)
    def wrapped(*args, **kwargs):
        # add values from context
        call_args = inspect.getcallargs(func, *args, **kwargs)
        if varkw:
            call_args.update(call_args.pop(varkw))
        call_args = apply_context_to_kwargs(call_args, context)
        return call_func(**call_args)
    return wrapped


def apply_injections(func, injections):
    """
    Apply parameter injection for a function. This decorator is close similar to `functools.patrial`
    but `injections` may contain additional keys that are not parameters of the function.
    Usage:
      func_with_context_injections = apply_injections(func, context)
      func_with_context_injections()

    :param param: target function.
    :param injections: context dictionary. Defaults for target function
    """
    argspec = getfullargspec(func)
    keys = argspec.args
    if getattr(func, 'im_self', None):
        # skip first parameter if bound method
        keys = keys[1:]

    not_exist_marker = object()
    defaults = dict(zip(reversed(keys), reversed(argspec.defaults))) if argspec.defaults else {}
    # add more defaults from context injections
    defaults = {inj_name: injections.get(inj_name,
                                         defaults.get(inj_name, not_exist_marker))
                for inj_name in keys}

    @wraps(func)
    def wrapper(*args, **kwargs):
        parameters = dict(defaults)
        if kwargs:
            parameters.update(kwargs)
        if args:
            parameters.update(zip(keys, args))
        not_found = tuple(name for name, value in six.iteritems(parameters)
                          if value == not_exist_marker)
        if not_found:
            raise TypeError('Some context {} was not provided for {!r} function. '
                            'Available context: {}'.format(not_found,
                                                           func.__name__,
                                                           injections.keys()))
        return func(**parameters)
    return wrapper


def injections_handler(func):
    """
    Decorator that creates a function wrapper that receives injections collection as a parameter
    and convert it into parameters for original function.
    """
    argspec = getfullargspec(func)
    keys = argspec.args
    defaults = dict(zip(reversed(keys), reversed(argspec.defaults))) if argspec.defaults else {}
    if getattr(func, 'im_self', None):
        # skip first parameter if bound method
        keys = keys[1:]

    @wraps(func)
    def wrapper(injections):
        parameters = {}
        for inj_name in keys:
            if inj_name == 'injections':
                parameters[inj_name] = injections
            elif inj_name in injections:
                parameters[inj_name] = injections[inj_name]
            elif inj_name in defaults:
                parameters[inj_name] = try_apply_context(defaults[inj_name], injections)
            else:
                raise TypeError('Context {!r} was not provided for {!r} function. '
                                'Available context: {}'.format(inj_name,
                                                               func.__name__,
                                                               injections.keys()))
        return func(**parameters)
    return wrapper


class ContextWrapper(object):
    """
    Try to apply a context to all attributes of an object instance
    When wrapped object method is called the wrapper will substitute all IContextLookup
    parameters with corresponding value from context dictionary.
    """
    def __init__(self, service, context):
        self._service = service
        self._context = context

    def __getattr__(self, item):
        attribute = getattr(self._service, item)
        if isinstance(attribute, (types.FunctionType, types.MethodType)):
            attribute = use_context_lookup(attribute, self._context)
        else:
            attribute = try_apply_context(attribute, self._context)
        setattr(self, item, attribute)
        return attribute


class UseContextSubstitution(IContextFactory):
    def __call__(self, context_dict):
        return ContextWrapper(self, context_dict)


def try_apply_context(obj, context):
    """
    Try to apply a context to an object
    """
    return obj(context) if isinstance(obj, IContextFactory) else obj
