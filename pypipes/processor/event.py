from pypipes.events import EVENT_START, EVENT_STOP

from pypipes.processor import pipe_processor, IProcessor, pipe_attachment


class Event(object):

    @staticmethod
    def on(*event_names):
        @pipe_attachment
        def attachment(processor):
            # attach event monitors to next processor
            assert isinstance(processor, IProcessor)
            processor.add_monitor(*event_names)
            return processor
        return attachment

    on_start = on.__func__(EVENT_START)
    on_stop = on.__func__(EVENT_STOP)

    @staticmethod
    def send(event_name, **kwargs):
        @pipe_processor
        def processor(message, response):
            # send an input message as an event to all listeners
            response.send_event(event_name, message=dict(message, **kwargs))
        return processor
