import logging
import types
from copy import deepcopy

from pypipes.context import LazyContextCollection, injections_handler
from pypipes.context.manager import MultiContextManager
from pypipes.line import Pipeline, PipelineJoin, ICloneable

logger = logging.getLogger(__name__)


class IProcessor(ICloneable, PipelineJoin):

    @property
    def name(self):
        """
        Processor name. The name is used to build unique processor ID.
        :return: processor name
        ":rtype: str
        """
        raise NotImplementedError()

    @property
    def monitor_events(self):
        """
        Returns list of events that the processor is listening for
        :return: list of event names
        :rtype: list[str]
        """
        raise NotImplementedError()

    def add_monitor(self, *event_names):
        """
        Add more events into events monitoring list
        :param event_names: list of event names
        :type event_names: list[str]
        """
        raise NotImplementedError()

    def process(self, injections):
        """
        Process input message
        :param injections: processor context provided by infrastructure and program
        :type injections: LazyContext, dict
        """
        raise NotImplementedError()


class Processor(IProcessor):

    def __init__(self, events=None):
        self._monitor_events = list(events) if events else []

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.name)

    @property
    def monitor_events(self):
        """
        Return list of custom events (out of the pipeline) that this processor wants to monitor
        As only this event is triggered an engine will send a message to this processor
        :return: list of event names
        """
        return self._monitor_events

    def add_monitor(self, *event_names):
        self._monitor_events.extend(event_names)

    def process(self, injections):
        result = self._do_process(injections)
        # convert function result into messages
        response = injections['response']
        if result is not None:
            if isinstance(result, types.GeneratorType):
                for message in result:
                    response.emit_message(dict(message))
            else:
                response.emit_message(dict(result))

    def _do_process(self, injections):
        raise NotImplementedError()

    def clone(self):
        return deepcopy(self)

    def to_pipeline(self):
        # return a cloned copy of this processor
        # because of pipeline can extend processor with additional features
        # but original processor should left unchanged
        return Pipeline([self.clone()])


class ContextProcessor(MultiContextManager, Processor):
    """
    This processor wraps a processor function with set of context managers
    Each context manager can apply some pre and post processing of a message
    Also it may specify additional context for message processing
    """
    def __init__(self, processor_func, context_managers=None, events=None):
        MultiContextManager.__init__(self, context_managers)
        Processor.__init__(self, events)
        self.processor_func = processor_func
        self.injections_handler = injections_handler(processor_func)

    @property
    def name(self):
        return self.processor_func.__name__

    def __call__(self, *args, **kwargs):
        return self.processor_func(*args, **kwargs)

    def process(self, injections):
        with self.context(injections) as context:
            if context:
                injections = LazyContextCollection(injections, **context)
            super(ContextProcessor, self).process(injections)

    def _do_process(self, injections):
        return self.injections_handler(injections)


pipe_processor = ContextProcessor


class ProcessorAttachment(PipelineJoin):
    """
    ProcessorAttachment updates next processor in a pipeline
    """
    def __init__(self, attach_to):
        assert callable(attach_to)
        self.attach_to = attach_to

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.attach_to.__name__)

    def to_pipeline(self):
        raise AssertionError(
            '{} must be a first join in a pipeline. Use parentheses if needed.'.format(self))

    def join(self, other):
        pipeline = other.to_pipeline()
        result = Pipeline([self.attach_to(pipeline[0])])
        if len(pipeline) > 1:
            result = result.join(Pipeline(pipeline[1:]))
        return result


pipe_attachment = ProcessorAttachment


def event_processor(*events):
    """
    Func decorator for easily creating a ContextProcessor that handles custom events
    :param events: list of event names
    """
    def wrapper(func):
        return ContextProcessor(func, events=events)

    return wrapper
