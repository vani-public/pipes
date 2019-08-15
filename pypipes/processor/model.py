from pypipes.context import context
from pypipes.context.model import model

from pypipes.processor import ContextProcessor


def model_processor(_func=None, **models):
    if not models:
        models = {'model': context.model_type}

    def wrapper(func):
        """
        A wrapper that builds a pipe processor
        Processor includes DlpEvent model context by default
        :param func: processor function
        :return: pipe processor
        :rtype: pypipes.processor.ContextProcessor
        """
        return ContextProcessor(func, context_managers=[model(**models)])

    if _func:
        return wrapper(_func)

    return wrapper
