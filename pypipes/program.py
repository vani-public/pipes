import logging
from collections import defaultdict, OrderedDict

from pypipes.context import LazyContextCollection
from pypipes.context.manager import MultiContextManager
from pypipes.line import PipelineJoin
from pypipes.processor import IProcessor

logger = logging.getLogger(__name__)


class Program(MultiContextManager):

    def __init__(self, name, pipelines, version=None, message_mapping=None,
                 global_context_managers=None):
        super(Program, self).__init__(global_context_managers)
        self.event_processor_map = defaultdict(list)
        self.processor_map = OrderedDict()
        self.next_processor_map = {}
        self.message_mapping = message_mapping or {}

        self.name = name
        self.version = version or None

        self.pipelines = {}
        for pipeline_name in sorted(pipelines):
            self.add_pipeline(pipeline_name, pipelines[pipeline_name])

    @property
    def id(self):
        if self.version:
            return '{}={}'.format(self.name, self.version)
        else:
            return self.name

    def add_pipeline(self, pipeline_name, pipeline):
        if pipeline and isinstance(pipeline, PipelineJoin):
            assert pipeline_name not in self.pipelines
            self.pipelines[pipeline_name] = pipeline = pipeline.to_pipeline()
            self.build_processor_maps(pipeline_name, pipeline)
        else:
            raise ValueError(pipeline)

    def build_processor_maps(self, pipeline_name, pipeline):
        prev_proc_name = None
        for processor in pipeline:
            assert isinstance(processor, IProcessor)

            proc_name = '{}.{}'.format(pipeline_name, processor.name)
            proc_index = 1
            while proc_name in self.processor_map:
                # proc name should be unique but this name is already registered
                # append unique suffix to proc_name
                proc_index = proc_index + 1
                proc_name = '{}.{}.{}'.format(pipeline_name, processor.name, proc_index)

            self.processor_map[proc_name] = processor
            if prev_proc_name:
                self.next_processor_map[prev_proc_name] = proc_name
            prev_proc_name = proc_name

            # register processor as custom event handler
            for event in processor.monitor_events:
                self.event_processor_map[event].append(proc_name)

    def run_processor(self, processor_id, injections):
        processor = self.get_processor(processor_id)
        if not processor:
            logger.error('Processor with ID %s is not registered in program %s',
                         processor_id, self.id)
            return

        # apply global context managers to each processor
        with self.context(injections) as context:
            if context:
                injections = LazyContextCollection(injections, **context)
            processor.process(injections)

    @property
    def processors(self):
        return self.processor_map

    @property
    def events(self):
        return self.event_processor_map

    def get_processor(self, processor_id):
        """
        Get processor by id
        :param processor_id: processor id
        :return: pipe processor
        :rtype: IProcessor
        """
        return self.processor_map.get(processor_id)

    def get_next_processor(self, processor_id):
        return self.next_processor_map.get(processor_id)

    def get_listeners(self, event_name, processor=None):
        """
        Return ids of all processors that listen for this event
        :param event_name: event name
        :param processor: processor id
        :return: list of processor ids
        """
        if processor:
            # only one processor will receive this event
            return [processor]
        else:
            return list(self.event_processor_map.get(event_name, []))
