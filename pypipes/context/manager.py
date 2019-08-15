try:
    import collections.abc as collections
except ImportError:
    import collections

import sys
import six
from contextlib import contextmanager

from pypipes.context import injections_handler, LazyContextCollection
from pypipes.line import ICloneable


class MultiContextManager(object):
    def __init__(self, context_managers=None):
        self.context_managers = []
        for mgr in context_managers or []:
            self.add_contextmanager(mgr)

    def add_contextmanager(self, *contextmanagers):
        for mgr in contextmanagers:
            if isinstance(mgr, MultiContextManager):
                self.context_managers.append(mgr.context)
            else:
                self.context_managers.append(mgr)

    @contextmanager
    def context(self, injections):
        """ Combine multiple context managers into a single nested context manager.
            This code is mostly copied from contextmanager.nested func
            that is unfortunately deprecated now.
        """
        exits = []
        context = {}  # custom context injections provided by context managers
        exc = (None, None, None)
        try:
            for mgr_factory in reversed(self.context_managers):
                mgr = mgr_factory(injections)
                exit = mgr.__exit__
                enter = mgr.__enter__
                # each context manager is isolated but
                # outer context has a priority over inner context
                context = dict(enter(), **context)
                exits.append(exit)
            yield context
        except Exception:  # noqa
            exc = sys.exc_info()
        finally:
            while exits:
                exit = exits.pop()
                try:
                    if exit(*exc):
                        exc = (None, None, None)
                except Exception:
                    exc = sys.exc_info()
            if exc != (None, None, None):
                # Don't rely on sys.exc_info() still containing
                # the right information. Another exception may
                # have been raised and caught by an exit method
                if six.PY3:
                    raise exc[1]
                else:
                    exec('raise exc[0], exc[1], exc[2]')


class PipeContextManager(MultiContextManager):
    """
    It's a decorator for MultiContextManager that creates a context manager from function
    and appends in into MultiContextManager context collection.
    You may use this to prepare some custom context for ContextProcessor

    @pipe_contextmanager
    def my_context():
        # do something before
        yield {'custom_context': value}
        # do something after

    This will build a decorator that could be applied to any ContextProcessor in format

    @my_context
    @pipe_processor
    def my_processor(message, response, custom_context):
        # process custom_context
        pass
    """
    def __init__(self, gen_func):
        super(PipeContextManager, self).__init__()
        self._gen_func = injections_handler(gen_func)

    @property
    def context(self):
        # a generator for GeneratorContextManager
        if self.context_managers:
            # contextmanager_func is wrapped into several context managers
            # that should prepare some additional context injections for him.
            return contextmanager(self._wrapped_context_manager)
        else:
            return contextmanager(self._gen_func)

    def _wrapped_context_manager(self, injections):
        with super(PipeContextManager, self).context(injections) as additional_injections:
            if additional_injections:
                injections = LazyContextCollection(injections, **additional_injections)
            for context in self._gen_func(injections):
                yield context

    def __call__(self, wrapped):
        # type: (MultiContextManager) -> MultiContextManager
        if isinstance(wrapped, ICloneable):
            # decorate a copy of the object
            wrapped = wrapped.clone()

        if isinstance(wrapped, MultiContextManager):
            # extend a MultiContextManager with current context
            wrapped.add_contextmanager(self)
        elif isinstance(wrapped, collections.Iterable) and all(isinstance(p, MultiContextManager)
                                                               for p in wrapped):
            # wrap all in collection of MultiContextManager items
            for processor in wrapped:
                processor.add_contextmanager(self)
        else:
            raise ValueError('{!r} decorator is compatible '
                             'with MultiContextManager only ({!r})'.format(self._gen_func.__name__,
                                                                           wrapped))
        return wrapped


pipe_contextmanager = PipeContextManager
