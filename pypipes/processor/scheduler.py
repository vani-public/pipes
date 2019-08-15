from datetime import timedelta

from pypipes.events import EVENT_START
from pypipes.service import key

from pypipes.processor import event_processor, pipe_processor


class Scheduler(object):
    """
    Scheduler pipe processor. This processor creates a scheduler
    that should send an empty message to next processor periodically.
    """
    @staticmethod
    def repeat(*args, **kwargs):
        """
        Repeat input message periodically
        """
        scheduler_id = kwargs.pop('scheduler_id', None)
        split_by = kwargs.pop('split_by', None)

        period = timedelta(*args, **kwargs)

        @pipe_processor
        def scheduler(processor_id, message, response, injections):
            # scheduler id might be formatted with data from message or other context
            # that allows creating a separate scheduler per specific context value
            split_by_value = split_by and split_by(injections)  # initialize lazy factory
            complex_scheduler_id = scheduler_id
            if split_by_value is not None:
                complex_scheduler_id = (key(scheduler_id, split_by_value)
                                        if scheduler_id else
                                        key(processor_id, split_by_value))
            scheduler_period = message.pop('_scheduler_period', period)
            if scheduler_period:
                response.schedule_message(message, scheduler_id=complex_scheduler_id,
                                          period=scheduler_period)
        return scheduler

    @staticmethod
    def delay(*args, **kwargs):
        """
        Delay processing of a message
        """
        delay = timedelta(*args, **kwargs)

        @pipe_processor
        def scheduler(message, response):
            response.emit_message(message, _start_in=delay.total_seconds())
        return scheduler

    @staticmethod
    def start_period(*args, **kwargs):
        scheduler_id = kwargs.pop('scheduler_id', None)
        period = timedelta(*args, **kwargs)

        @event_processor(EVENT_START)
        def scheduler(message, response):
            scheduler_period = message.get('_scheduler_period', period)
            if scheduler_period:
                response.schedule_message({}, scheduler_id=scheduler_id, period=scheduler_period)

        return scheduler

    @staticmethod
    def start_in(*args, **kwargs):
        delay = timedelta(*args, **kwargs)

        @event_processor(EVENT_START)
        def scheduler(response, event):
            if event == EVENT_START:
                response.emit_message({}, _start_in=delay.total_seconds())
        return scheduler

    @staticmethod
    def stop(scheduler_id, split_by=None):
        @pipe_processor
        def scheduler_stop(processor_id, message, response, injections):
            split_by_value = split_by and split_by(injections)  # initialize lazy factory
            complex_scheduler_id = scheduler_id
            if split_by_value is not None:
                complex_scheduler_id = (key(scheduler_id, split_by_value)
                                        if scheduler_id else
                                        key(processor_id, split_by_value))
            response.stop_scheduler(complex_scheduler_id)
            return message
        return scheduler_stop
