from pypipes.context import try_apply_context, IContextFactory


class IContextPool(object):
    """
    Returns named context from a pool of contexts as attribute or as a mapping item.
    Different processors and context managers may require the same context for their work
    but it might really limit a context initialization.

    Context pool provides several contexts with same interface
    but with unique initialization.

    Usage:
    counter = ServicePool()  # IContextPool instance

    # to get a service named `lock` from the pool call
    counter.lock
    # or
    counter['lock']

    IContextPool should provide some context for any name
    """
    def __getattr__(self, item):
        try:
            return super(IContextPool, self).__getattr__(item)
        except AttributeError:
            service = self[item]
            setattr(self, item, service)  # save as an attribute to speedup a further call
            return service

    def __getitem__(self, item):
        raise NotImplementedError()


class ContextPool(IContextPool):
    """
    Stores a set of named context instances. User can define a separate context for any name.
    Pool returns default context if context for requested name is not defined.
    Usage:
    counter = ContextPool(default=CounterClient(), wait_all=PersistentCounterClient())

    # default service is used when user calls
    counter.default.increment()
    counter.unknown.increment()

    # special will be used when user especially calls it
    counter.wait_all.increment()
    """
    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, dict(self._special, default=self.default))

    def __init__(self, default, **kwargs):
        self.default = default
        self._special = kwargs

    def __getitem__(self, item):
        return self._special.get(item, self.default)


class ContextPoolWrapper(ContextPool):
    """
    Applies a wrapper function on each context provided by a ContextPool
    """
    def __init__(self, provider, wrap_func):
        """
        :type provider: ContextPool
        :param wrap_func: a function that should wrap each context provided by provider.
        :type wrap_func: (object) -> object
        """
        super(ContextPoolWrapper, self).__init__(
            wrap_func(provider.default),
            ** {key: wrap_func(service) for key, service in provider._special.items()})


class LazyContextPool(IContextFactory, ContextPool):
    """
    This ContextPool may provide a pool of lazy contexts.
    However this is not obligatory, you can mix lazy and regular contexts in this pool.
    All lazy contexts will be initialized as only pool is accessed first time.
    """
    def __call__(self, context_dict):
        return ContextPoolWrapper(self, lambda service: try_apply_context(service, context_dict))
