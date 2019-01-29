from functools import wraps


def key(*names, **kwargs):
    """
    Helper func to build an unique key from list of names and parameters

    Returns string in format:
    <name1>.<name2>.<par1>:<str(val1)>).<par2>:<str(val2)>
    """
    result = '.'.join(map(str, names))
    if kwargs:
        result = '{}.{}'.format(result, '.'.join(map(
            lambda kv: '{}:{}'.format(kv[0], kv[1]),
            sorted(kwargs.items()))))
    return result


_config_singletons = {}


def config_singleton(func):
    """
    A wrapper to create and use single object instance per object configuration
    :param func: object factory
    :return: wrapped function
    """
    global _config_singletons

    @wraps(func)
    def wrapped(config):
        func_key = key(func.__module__, func.__name__, **config)
        if func_key not in _config_singletons:
            _config_singletons[func_key] = func(config)
        return _config_singletons[func_key]
    return wrapped
